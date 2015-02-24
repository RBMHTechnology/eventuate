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

package com.rbmhtechnology.eventuate

import java.util.concurrent.TimeUnit

import akka.actor._

import scala.collection.immutable.Seq
import scala.concurrent.duration._

import ReplicationEndpoint.InstanceId

object ReplicationProtocol {
  private[eventuate] case class ClientInfo(logName: String, filter: ReplicationFilter)
  private[eventuate] case class ServerInfo(logName: String, logId: String, server: ActorRef)

  private[eventuate] case class Connect(clientInfos: Seq[ClientInfo], instanceId: InstanceId)
  private[eventuate] case class ConnectAccepted(serverInfos: Seq[ServerInfo], instanceId: InstanceId)
  private[eventuate] case class ConnectRequested(instanceId: InstanceId)

  private[eventuate] case object TransferDue
  private[eventuate] case class Transfer(fromSequenceNr: Long, max: Int, correlationId: Int)
  private[eventuate] case class TransferSuccess(events: Seq[DurableEvent], lastSourceLogSequenceNrStored: Long, correlationId: Int)
  private[eventuate] case class TransferFailure(cause: Throwable, correlationId: Int)

  /**
   * Requests from a target log the last read position in the given source log.
   */
  case class GetLastSourceLogReadPosition(sourceLogId: String)

  /**
   * Success reply after a [[GetLastSourceLogReadPosition]].
   */
  case class GetLastSourceLogReadPositionSuccess(sourceLogId: String, lastSourceLogSequenceNrStored: Long)

  /**
   * Failure reply after a [[GetLastSourceLogReadPosition]].
   */
  case class GetLastSourceLogReadPositionFailure(cause: Throwable)

  /**
   * Instructs a target log to store replicated `events` from the source log identified by
   * `sourceLogId` along with the last read position in the source log (`lastSourceLogSequenceNrRead`).
   */
  case class Replicate(events: Seq[DurableEvent], sourceLogId: String, lastSourceLogSequenceNrRead: Long)

  /**
   * Success reply after a [[Replicate]].
   *
   * @param num Number of events actually replicated.
   * @param lastSourceLogSequenceNrStored Last source log read position stored in the target log.
   */
  case class ReplicateSuccess(num: Int, lastSourceLogSequenceNrStored: Long)

  /**
   * Failure reply after a [[Replicate]].
   */
  case class ReplicateFailure(cause: Throwable)

  /**
   * published by event logs to the actor system's event stream whenever new events have been written,
   * either by replication or by event-sourced actors.
   *
   * @param events Written events.
   */
  case class Updated(events: Seq[DurableEvent])
}

private class ReplicationServer(sourceLog: ActorRef, filter: ReplicationFilter) extends Actor {
  import ReplicationProtocol._
  import EventsourcingProtocol._

  var replicationClient: Option[ActorRef] = None

  //
  // TODO: reliability improvements
  //
  // - response timeout for communication with source log
  //   (low prio, local communication at the moment)
  //

  val idle: Receive = {
    case Updated(events) if events.exists(filter.apply) =>
      replicationClient.foreach(_ ! TransferDue)
    case Transfer(fromSequenceNr, max, correlationId) =>
      sourceLog ! Read(fromSequenceNr, max, filter)
      replicationClient = Some(sender())
      context.become(transferring(correlationId))
  }

  def transferring(correlationId: Int): Receive = {
    case ReadSuccess(events, lastSourceLogSequenceNrRead) =>
      replicationClient.foreach(_ ! TransferSuccess(events, lastSourceLogSequenceNrRead, correlationId))
      context.become(idle)
    case ReadFailure(cause) =>
      replicationClient.foreach(_ ! TransferFailure(cause, correlationId))
      context.become(idle)
  }

  def receive = idle

  override def preStart(): Unit = {
    context.system.eventStream.subscribe(self, classOf[ConnectRequested])
    context.system.eventStream.subscribe(self, classOf[Updated])
  }

  override def postStop(): Unit = {
    context.system.eventStream.unsubscribe(self)
  }
}

private class ReplicationClient(logName: String, sourceLogId: String, targetLog: ActorRef, replicationServer: ActorRef, remoteInstanceId: InstanceId) extends Actor {
  import ReplicationServerFailureDetector._
  import ReplicationProtocol._

  val config = context.system.settings.config.getConfig("log.replication")
  val batchSize = config.getInt("transfer-batch-size-max")

  val failureDetector = context.actorOf(Props(new ReplicationServerFailureDetector(remoteInstanceId, logName)))
  var correlationId = 0

  context.setReceiveTimeout(config.getDuration("transfer-retry-interval", TimeUnit.MILLISECONDS).millis)
  context.system.eventStream.subscribe(self, classOf[ConnectRequested])

  //
  // TODO: reliability improvements
  //
  // - response timeout for communication with target log
  //   (low prio, local communication at the moment)
  //

  val idle: Receive = {
    case TransferDue =>
      targetLog ! GetLastSourceLogReadPosition(sourceLogId)
      context.become(replicating(nextCorrelationId()))
      failureDetector ! Ping
    case ReceiveTimeout =>
      targetLog ! GetLastSourceLogReadPosition(sourceLogId)
      context.become(replicating(nextCorrelationId()))
  }

  def replicating(correlationId: Int): Receive = {
    case GetLastSourceLogReadPositionSuccess(sourceLogId, lastSourceLogSequenceNrStored) =>
      replicationServer ! Transfer(lastSourceLogSequenceNrStored + 1, batchSize, correlationId)
    case GetLastSourceLogReadPositionFailure(cause) =>
      // TODO: log cause
      context.become(idle)
    case TransferSuccess(events, lastSourceLogSequenceNrRead, `correlationId`) =>
      targetLog ! Replicate(events, sourceLogId, lastSourceLogSequenceNrRead)
      failureDetector ! Ping
    case TransferFailure(cause, `correlationId`) =>
      // TODO: log cause
      context.become(idle)
      failureDetector ! Ping
    case ReplicateSuccess(num, lastSourceLogSequenceNrStored) if num > 0 =>
      replicationServer ! Transfer(lastSourceLogSequenceNrStored + 1, batchSize, correlationId)
    case ReplicateSuccess(_, _) =>
      context.become(idle)
    case ReplicateFailure(cause) =>
      // TODO: log cause
      context.become(idle)
    case ReceiveTimeout =>
      context.become(idle)
  }

  def receive = idle

  override def unhandled(message: Any): Unit = message match {
    case ConnectRequested(rid) if rid.newIncarnationOf(remoteInstanceId) =>
      context.stop(self)
    case other =>
      super.unhandled(other)
  }

  override def preStart(): Unit =
    self ! TransferDue

  private def nextCorrelationId(): Int = {
    correlationId += 1
    correlationId
  }
}

/**
 * Sends connection requests to a server connector and creates [[ReplicationClient]]s
 * that read events from [[ReplicationServer]]s and write them to target logs.
 *
 * @param host host where the remote server connector is running.
 * @param port port where the remote server connector is listening.
 * @param name name of the [[ActorSystem]] that runs the remote server connector.
 * @param targetLogs target logs indexed by log name.
 * @param filters replication filters indexed by log name.
 * @param localInstanceId local instance id of this connector.
 */
private[eventuate] class ReplicationClientConnector(host: String, port: Int, name: String, targetLogs: Map[String, ActorRef], filters: Map[String, ReplicationFilter], localInstanceId: InstanceId) extends Actor with ActorLogging {
  import ReplicationProtocol._

  val config = context.system.settings.config.getConfig("log.replication")
  val retry = config.getDuration("connect-retry-interval", TimeUnit.MILLISECONDS).millis
  val selection = context.actorSelection(s"${protocol}://${name}@${host}:${port}/user/${ReplicationServerConnector.name}")

  context.system.eventStream.subscribe(self, classOf[ConnectRequested])

  def protocol = context.system match {
    case sys: ExtendedActorSystem => sys.provider.getDefaultAddress.protocol
    case sys                      => "akka.tcp"
  }

  val identifying: Receive = {
    case ReceiveTimeout =>
      selection ! Identify(1)
    case ActorIdentity(1, Some(connector)) =>
      val clientInfos = filters.map {
        case (logName, filter) => ClientInfo(logName, filter)
      }
      connector ! Connect(clientInfos.toList, localInstanceId)
      context.become(connecting)
  }

  val connecting: Receive = {
    case ReceiveTimeout =>
      context.become(identifying)
    case ConnectAccepted(serverInfos, rid) =>
      serverInfos.foreach {
        case ServerInfo(logName, sourceLogId, server) =>
          context.actorOf(Props(new ReplicationClient(logName, sourceLogId, targetLogs(logName), server, rid)))
      }
      context.setReceiveTimeout(Duration.Undefined)
      context.become(connected(rid))
      log.info(s"Opened replication connection to ${host}:${port}")
  }

  def connected(remoteInstanceId: InstanceId): Receive = {
    case ConnectRequested(rid) if rid.newIncarnationOf(remoteInstanceId) =>
      context.setReceiveTimeout(retry)
      context.become(identifying)
  }

  def receive = identifying

  override def preStart(): Unit =
    context.setReceiveTimeout(retry)
}

private[eventuate] object ReplicationServerConnector {
  val name: String = "connector"
}

/**
 * Receives connection requests from client connectors and creates [[ReplicationServer]]s
 * that read events from source logs.
 *
 * @param sourceLogs source logs indexed by log name.
 * @param sourceLogId function that maps source log names to source log ids.
 * @param localInstanceId local instance id of this connector.
 */
private[eventuate] class ReplicationServerConnector(sourceLogs: Map[String, ActorRef], sourceLogId: String => String, localInstanceId: InstanceId) extends Actor {
  import ReplicationProtocol._

  var currentServerInfos: Map[InstanceId, Seq[ServerInfo]] = Map.empty

  def receive = {
    case Connect(clientInfos, remoteInstanceId) if currentServerInfos.contains(remoteInstanceId) =>
      // this is a duplicate from the client. Just return the existing server infos
      sender() ! ConnectAccepted(currentServerInfos(remoteInstanceId), localInstanceId)
    case Connect(clientInfos, rid) =>
      currentServerInfos.find {
        case (remoteInstanceId, _) => rid.newIncarnationOf(remoteInstanceId)
      }.foreach {
        case (remoteInstanceId, serverInfos) =>
          currentServerInfos -= remoteInstanceId
          serverInfos.foreach(info => context.stop(info.server))
      }
      val serverInfos = clientInfos.collect {
        case ClientInfo(logName, filter) if sourceLogs.contains(logName) =>
          val server = context.actorOf(Props(new ReplicationServer(sourceLogs(logName), filter)))
          ServerInfo(logName, sourceLogId(logName), server)
      }
      sender() ! ConnectAccepted(serverInfos, localInstanceId)
      currentServerInfos += (rid -> serverInfos)
      context.system.eventStream.publish(ConnectRequested(rid))
  }
}

private[eventuate] object ReplicationServerFailureDetector {
  case object Ping
}

private[eventuate] class ReplicationServerFailureDetector(remoteInstanceId: InstanceId, logName: String) extends Actor {
  import ReplicationServerFailureDetector._
  import ReplicationEndpoint._

  val config = context.system.settings.config.getConfig("log.replication")
  val limit = config.getDuration("failure-detection-limit", TimeUnit.MILLISECONDS)

  var lastTick: Long = 0L

  context.setReceiveTimeout(limit.millis)

  def receive = {
    case Ping =>
      val currentTime = System.currentTimeMillis
      val lastInterval =  currentTime - lastTick
      if (lastInterval >= limit) {
        context.system.eventStream.publish(Available(remoteInstanceId.uid, logName))
        lastTick = currentTime
      }
    case ReceiveTimeout =>
      context.system.eventStream.publish(Unavailable(remoteInstanceId.uid, logName))
  }
}
