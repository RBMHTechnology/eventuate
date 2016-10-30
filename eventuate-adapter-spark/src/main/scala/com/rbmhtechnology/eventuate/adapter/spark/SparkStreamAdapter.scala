/*
 * Copyright 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.eventuate.adapter.spark

import akka.actor._
import akka.pattern.{ ask, pipe }
import akka.util.Timeout

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.typesafe.config.{ ConfigFactory, Config }

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * Eventuate Spark adapter for stream processing events from event logs.
 *
 * @param context Spark streaming context.
 * @param config Configuration for application-specific event serializers.
 */
class SparkStreamAdapter(val context: StreamingContext, val config: Config) {

  // --------------------------------------------
  //  TODO: avoid redundancies with replicators
  // --------------------------------------------

  /**
   * Creates a Spark `DStream[DurableEvent]` from an event log that is managed by a [[ReplicationEndpoint]]
   * under given `logName`. The stream is updated with both, replayed events starting at `fromSequenceNr`
   * and live events that have been written to the event log after the streaming `context` has been started.
   *
   * @param id Unique, application-defined stream id.
   * @param host Replication endpoint host.
   * @param port Replication endpoint port.
   * @param logName Event log name.
   * @param sysName Remote actor system name.
   * @param fromSequenceNr Sequence number from where to start the stream.
   * @param storageLevel Storage level of the RDDs in the event stream.
   */
  def eventStream(id: String, host: String, port: Int, logName: String, sysName: String = ReplicationConnection.DefaultRemoteSystemName, fromSequenceNr: Long = 1L, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): DStream[DurableEvent] =
    context.receiverStream(new DurableEventReceiver(id, ReplicationConnection(host, port, sysName), logName, fromSequenceNr, storageLevel, config))
}

private class DurableEventReceiver(id: String, connection: ReplicationConnection, logName: String, fromSequenceNr: Long, storageLevel: StorageLevel, config: Config) extends Receiver[DurableEvent](storageLevel) {
  @volatile private var system: Option[ActorSystem] = None

  override def onStart(): Unit = {
    system = Some(ActorSystem("receiver", ConfigFactory.parseString("akka.remote.netty.tcp.port = 0").withFallback(config)))
    system.get.actorOf(Props(new EndpointConnector))
  }

  override def onStop(): Unit = {
    system.foreach(_.terminate())
    system = None
  }

  private class EndpointConnector extends Actor {
    import context.dispatcher

    private val acceptor = remoteAcceptor
    private var acceptorRequestSchedule: Option[Cancellable] = None

    private var connected = false

    def receive = {
      case GetReplicationEndpointInfoSuccess(info) if !connected =>
        if (info.logNames.contains(logName)) {
          val sourceLogId = info.logId(logName)
          val source = ReplicationSource(info.endpointId, logName, sourceLogId, acceptor)
          context.actorOf(Props(new SourceReader(source)))
          acceptorRequestSchedule.foreach(_.cancel())
          connected = true
        } else stop(s"invalid event log name: $logName")
    }

    private def scheduleAcceptorRequest(acceptor: ActorSelection): Cancellable =
      context.system.scheduler.schedule(0.seconds, 5.seconds, new Runnable {
        override def run() = acceptor ! GetReplicationEndpointInfo
      })

    private def remoteAcceptor: ActorSelection =
      remoteActorSelection(Acceptor.Name)

    private def remoteActorSelection(actor: String): ActorSelection = {
      import connection._

      val protocol = context.system match {
        case sys: ExtendedActorSystem => sys.provider.getDefaultAddress.protocol
        case sys                      => "akka.tcp"
      }
      context.actorSelection(s"$protocol://$name@$host:$port/user/$actor")
    }

    override def preStart(): Unit =
      acceptorRequestSchedule = Some(scheduleAcceptorRequest(acceptor))

    override def postStop(): Unit =
      acceptorRequestSchedule.foreach(_.cancel())
  }

  private class SourceReader(source: ReplicationSource) extends Actor with ActorLogging {
    import context.dispatcher

    val settings = new ReplicationSettings(context.system.settings.config)

    val readerId: String = s"${source.logId}-reader-$id"
    val readScheduler: Scheduler = context.system.scheduler
    var readSchedule: Option[Cancellable] = None
    var readProgress: Long = fromSequenceNr - 1L

    val fetching: Receive = {
      case GetReplicationProgress(sourceLogId) =>
        sender() ! GetReplicationProgressSuccess(null, readProgress, VectorTime.Zero)
      case GetReplicationProgressSuccess(_, storedReplicationProgress, currentTargetVersionVector) =>
        context.become(reading)
        read(storedReplicationProgress, currentTargetVersionVector)
      case GetReplicationProgressFailure(cause) =>
        log.error(cause, "progress read failed")
        scheduleFetch()
    }

    val idle: Receive = {
      case ReplicationDue =>
        readSchedule.foreach(_.cancel()) // if it's notification from source concurrent to a scheduled read
        context.become(fetching)
        fetch()
    }

    val reading: Receive = {
      case ReplicationReadSuccess(events, from, replicationProgress, _, currentSourceVersionVector) =>
        context.become(writing)
        write(events, replicationProgress, currentSourceVersionVector, replicationProgress >= from)
      case ReplicationReadFailure(cause, _) =>
        log.error(cause, s"event read failed")
        context.become(idle)
        scheduleRead()
    }

    val writing: Receive = {
      case writeSuccess @ ReplicationWriteSuccess(_, metadata, false) =>
        context.become(idle)
        scheduleRead()

        // ------------------------------------
        //  TODO: write progress to source log
        // ------------------------------------

        readProgress = metadata(source.logId).replicationProgress

      case writeSuccess @ ReplicationWriteSuccess(_, metadata, true) =>
        val sourceMetadata = metadata(source.logId)
        context.become(reading)
        read(sourceMetadata.replicationProgress, sourceMetadata.currentVersionVector)

        // ------------------------------------
        //  TODO: write progress to source log
        // ------------------------------------

        readProgress = sourceMetadata.replicationProgress

      case ReplicationWriteFailure(cause) =>
        log.error(cause, "event write failed")
        context.become(idle)
        scheduleRead()
    }

    def receive = fetching

    override def unhandled(message: Any): Unit = message match {
      case ReplicationDue => // currently replicating, ignore
      case other          => super.unhandled(message)
    }

    private def scheduleFetch(): Unit =
      readScheduler.scheduleOnce(settings.retryDelay)(fetch())

    private def scheduleRead(): Unit =
      readSchedule = Some(readScheduler.scheduleOnce(settings.retryDelay, self, ReplicationDue))

    private def fetch(): Unit = {
      implicit val timeout = Timeout(settings.readTimeout)

      // --------------------------------------
      //  TODO: fetch progress from source log
      // --------------------------------------

      self ? GetReplicationProgress(source.logId) recover {
        case t => GetReplicationProgressFailure(t)
      } pipeTo self
    }

    private def read(storedReplicationProgress: Long, currentTargetVersionVector: VectorTime): Unit = {
      implicit val timeout = Timeout(settings.remoteReadTimeout)
      val replicationRead = ReplicationRead(storedReplicationProgress + 1, settings.writeBatchSize, settings.remoteScanLimit, NoFilter, readerId, self, currentTargetVersionVector)

      (source.acceptor ? ReplicationReadEnvelope(replicationRead, source.logName, ReplicationEndpoint.DefaultApplicationName, ReplicationEndpoint.DefaultApplicationVersion)) recover {
        case t => ReplicationReadFailure(ReplicationReadTimeoutException(settings.remoteReadTimeout), readerId)
      } pipeTo self
    }

    private def write(events: Seq[DurableEvent], replicationProgress: Long, currentSourceVersionVector: VectorTime, continueReplication: Boolean): Unit = {
      Future(store(events.iterator)) map {
        case _ => ReplicationWriteSuccess(events, Map(source.logId -> ReplicationMetadata(replicationProgress, VectorTime.Zero)), continueReplication)
      } recover {
        case t => ReplicationWriteFailure(t)
      } pipeTo self
    }

    override def preStart(): Unit =
      fetch()

    override def postStop(): Unit =
      readSchedule.foreach(_.cancel())
  }
}

