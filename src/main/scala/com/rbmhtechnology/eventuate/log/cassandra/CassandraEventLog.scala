/*
 * Copyright (C) 2015 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.eventuate.log.cassandra

import java.lang.{Long => JLong}

import akka.actor._

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.log._
import com.rbmhtechnology.eventuate.snapshot.filesystem._

import scala.collection.immutable.Seq
import scala.language.implicitConversions
import scala.util._

/**
 * An event log actor with [[http://cassandra.apache.org/ Apache Cassandra]] as storage backend. It uses
 * the [[Cassandra]] extension to connect to a Cassandra cluster. Applications should create an instance
 * of this actor using the `props` method of the `CassandraEventLog` [[CassandraEventLog$ companion object]].
 *
 * {{{
 *   val factory: ActorRefFactory = ... // ActorSystem or ActorContext
 *   val logId: String = "example"      // Unique log id
 *
 *   val log = factory.actorOf(CassandraEventLog.props(logId))
 * }}}
 *
 * Each event log actor creates two tables in the configured keyspace (see also [[Cassandra]]). Assuming
 * the following table prefix
 *
 * {{{
 *   eventuate.log.cassandra.table-prefix = "log"
 * }}}
 *
 * and a log `id` with value `example`, the names of these two tables are
 *
 *  - `log_example` which represents the local event log.
 *  - `log_example_agg` which is an index of the local event log for those events that have non-empty
 *    [[DurableEvent#destinationAggregateIds destinationAggregateIds]] set. It is used for fast recovery
 *    of event-sourced actors or views that have an [[EventsourcedView#aggregateId aggregateId]] defined.
 *
 * @param id unique log id.
 *
 * @see [[Cassandra]]
 * @see [[DurableEvent]]
 */
class CassandraEventLog(val id: String) extends Actor with Stash {
  import CassandraEventLog._

  val eventStream = context.system.eventStream
  val cassandra: Cassandra = Cassandra(context.system)

  cassandra.createEventTable(id)
  cassandra.createAggregateEventTable(id)

  private val statement = cassandra.prepareWriteEvent(id)

  private val snapshotStore = new FilesystemSnapshotStore(new FilesystemSnapshotStoreSettings(context.system))
  private val reader = createReader(cassandra, id)
  private val index = createIndex(cassandra, reader, id)
  private var registry = SubscriberRegistry()

  private[eventuate] val generator =
    new SequenceNumberGenerator(cassandra.settings.partitionSizeMax)

  def initializing: Receive = {
    case Initialize(snr) =>
      generator.sequenceNr = snr
      unstashAll()
      context.become(initialized)
    case other =>
      stash()
  }

  def initialized: Receive = {
    case cmd: GetLastSourceLogReadPosition =>
      index.forward(cmd)
    case cmd @ Replay(_, requestor, Some(emitterAggregateId), _) =>
      registry = registry.registerAggregateSubscriber(context.watch(requestor), emitterAggregateId)
      index.forward(cmd)
    case Replay(from, requestor, None, iid) =>
      import cassandra.readDispatcher
      registry = registry.registerDefaultSubscriber(context.watch(requestor))
      reader.replayAsync(from)(event => requestor ! Replaying(event, iid) ) onComplete {
        case Success(_) => requestor ! ReplaySuccess(iid)
        case Failure(e) => requestor ! ReplayFailure(e, iid)
      }
    case r @ ReplicationRead(from, max, filter, targetLogId) =>
      import cassandra.readDispatcher
      val sdr = sender()
      eventStream.publish(r)
      reader.readAsync(from, max, filter, targetLogId) onComplete {
        case Success(result) =>
          val r = ReplicationReadSuccess(result.events, result.to, targetLogId)
          sdr ! r
          eventStream.publish(r)
        case Failure(cause)  =>
          val r = ReplicationReadFailure(cause.getMessage, targetLogId)
          sdr ! r
          eventStream.publish(r)
      }
    case Write(events, initiator, requestor, iid) =>
      val result = for {
        partition <- Try(generator.adjustSequenceNr(events.size))
        updated    = prepareWrite(id, events, generator.nextSequenceNr())
        _         <- Try(write(partition, updated))
      } yield updated

      result match {
        case Success(updated) =>
          registry.pushWriteSuccess(updated, initiator, requestor, iid)
          publishUpdateNotification(updated)
          requestIndexUpdate()
        case Failure(e) =>
          registry.pushWriteFailure(events, initiator, requestor, iid, e)
      }
    case r @ WriteN(writes) =>
      val result = for {
        partition     <- Try(generator.adjustSequenceNr(r.size))
        updatedWrites  = writes.map(w => w.copy(prepareWrite(id, w.events, generator.nextSequenceNr())))
        updatedEvents  = updatedWrites.flatMap(_.events)
        _             <- Try(write(partition, updatedEvents))
      } yield (updatedWrites, updatedEvents)

      result match {
        case Success((updatedWrites, updatedEvents)) =>
          updatedWrites.foreach(w => registry.pushWriteSuccess(w.events, w.initiator, w.requestor, w.instanceId))
          publishUpdateNotification(updatedEvents)
          requestIndexUpdate()
        case Failure(e) =>
          writes.foreach(w => registry.pushWriteFailure(w.events, w.initiator, w.requestor, w.instanceId, e))
      }
      sender() ! WriteNComplete // notify batch layer that write completed
    case r @ ReplicationWrite(Seq(), _, _) =>
      index.forward(r)
    case ReplicationWrite(events, sourceLogId, lastSourceLogSequenceNrRead) =>
      val result = for {
        partition <- Try(generator.adjustSequenceNr(events.size))
        updated    = prepareReplicate(id, events, lastSourceLogSequenceNrRead, generator.nextSequenceNr())
        _         <- Try(write(partition, updated))
      } yield updated

      result match {
        case Success(updated) =>
          sender() ! ReplicationWriteSuccess(events.size, lastSourceLogSequenceNrRead)
          registry.pushReplicateSuccess(updated)
          publishUpdateNotification(updated)
          requestIndexUpdate()
        case Failure(e) =>
          sender() ! ReplicationWriteFailure(e)
      }
    case LoadSnapshot(emitterId, requestor, iid) =>
      import cassandra.readDispatcher
      snapshotStore.loadAsync(emitterId) onComplete {
        case Success(s) => requestor ! LoadSnapshotSuccess(s, iid)
        case Failure(e) => requestor ! LoadSnapshotFailure(e, iid)
      }
    case SaveSnapshot(snapshot, initiator, requestor, iid) =>
      import context.dispatcher
      snapshotStore.saveAsync(snapshot) onComplete {
        case Success(_) => requestor.tell(SaveSnapshotSuccess(snapshot.metadata, iid), initiator)
        case Failure(e) => requestor.tell(SaveSnapshotFailure(snapshot.metadata, e, iid), initiator)
      }
    case Terminated(requestor) =>
      registry = registry.unregisterSubscriber(requestor)
  }

  override def receive =
    initializing

  private[eventuate] def createReader(cassandra: Cassandra, logId: String) =
    new CassandraEventReader(cassandra, logId)

  private[eventuate] def createIndex(cassandra: Cassandra, eventReader: CassandraEventReader, logId: String) =
    context.actorOf(CassandraIndex.props(cassandra, eventReader, logId))

  private[eventuate] def write(partition: Long, events: Seq[DurableEvent]): Unit = cassandra.executeBatch { batch =>
    events.foreach(event => batch.add(statement.bind(partition: JLong, event.sequenceNr: JLong, cassandra.eventToByteBuffer(event))))
  }

  private def publishUpdateNotification(events: Seq[DurableEvent] = Seq()): Unit =
    if (events.nonEmpty) eventStream.publish(Updated(id, events))

  private def requestIndexUpdate(): Unit =
    if (generator.sequenceNrUpdates >= cassandra.settings.indexUpdateLimit) {
      index ! CassandraIndex.UpdateIndex()
      generator.resetSequenceNumberUpdates()
    }
}

object CassandraEventLog {
  private[eventuate] case class Initialize(sequenceNr: Long)

  /**
   * Creates a [[CassandraEventLog]] configuration object.
   *
   * @param logId unique log id.
   * @param batching `true` if write-batching shall be enabled (recommended).
   */
  def props(logId: String, batching: Boolean = true): Props = {
    val logProps = Props(new CassandraEventLog(logId)).withDispatcher("eventuate.log.cassandra.write-dispatcher")
    if (batching) Props(new BatchingEventLog(logProps)) else logProps
  }
}
