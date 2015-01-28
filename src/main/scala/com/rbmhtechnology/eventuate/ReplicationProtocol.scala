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

// -----------------------------------------------------------------------------------------
//  A simple, fault-tolerant event replication protocol between sites (replicated actors).
//
//  An uni-directional replication link operates a replication server on the source site
//  and a replication client on the (remote) target site:
//
//  source site -> replication server --(remote)--> replication client -> target site
//
//  A replication client replicates events by pulling them from the replication server
//  (in batches). A replication server notifies a client when new events are available
//  for pulling. A bi-directional replication link between two sites is composed of two
//  uni-directional replication links.
// -----------------------------------------------------------------------------------------

object ReplicationProtocol {
  case object TransferDue

  case class Connect(filter: ReplicationFilter, instanceId: InstanceId)
  case class ConnectAccepted(sourceLogId: String, replicationServer: ActorRef, instanceId: InstanceId)
  case class ConnectRequested(instanceId: InstanceId)
  case object ConnectionRenewal

  case class Transfer(fromSequenceNr: Long, max: Int, correlationId: Int)
  case class TransferSuccess(events: Seq[DurableEvent], lastSourceLogSequenceNrRead: Long, correlationId: Int)
  case class TransferFailure(cause: Throwable, correlationId: Int)

  case class GetLastSourceLogSequenceNrReplicated(sourceLogId: String)
  case class GetLastSourceLogSequenceNrReplicatedSuccess(sourceLogId: String, sourceLogSequenceNr: Long)
  case class GetLastSourceLogSequenceNrReplicatedFailure(cause: Throwable)

  case class Replicate(events: Seq[DurableEvent], sourceLogId: String, lastSourceLogSequenceNrRead: Long)
  case class ReplicateFailure(cause: Throwable)
  case class ReplicateSuccess(num: Int, lastSourceLogSequenceNrReplicated: Long)

  case class Updated(events: Seq[DurableEvent])
}

class ReplicationServer(sourceLog: ActorRef, filter: ReplicationFilter, remoteInstanceId: InstanceId) extends Actor {
  import ReplicationProtocol._
  import EventLogProtocol._

  var replicationClient: Option[ActorRef] = None

  // ----------------------------------------------------------
  //  TODO: handle missing responses to commands
  //  - Read
  // ----------------------------------------------------------

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
    case removeMe =>
  }

  def receive = idle

  override def unhandled(message: Any): Unit = message match {
    case ConnectRequested(rid) if rid.newIncarnationOf(remoteInstanceId) => context.stop(self)
    case other => super.unhandled(other)
  }

  override def preStart(): Unit = {
    context.system.eventStream.subscribe(self, classOf[ConnectRequested])
    context.system.eventStream.subscribe(self, classOf[Updated])
  }

  override def postStop(): Unit = {
    context.system.eventStream.unsubscribe(self)
  }
}

class ReplicationClient(sourceLogId: String, targetLog: ActorRef, replicationServer: ActorRef, remoteInstanceId: InstanceId) extends Actor {
  import ReplicationServerFailureDetector._
  import ReplicationProtocol._

  val config = context.system.settings.config.getConfig("log.replication")
  val batchSize = config.getInt("transfer-batch-size")
  val failureDetector = context.actorOf(Props(new ReplicationServerFailureDetector(remoteInstanceId)))

  var correlationId = 0

  context.setReceiveTimeout(config.getDuration("transfer-retry-interval", TimeUnit.MILLISECONDS).millis)
  context.system.eventStream.subscribe(self, classOf[ConnectRequested])

  // ----------------------------------------------------------
  //  TODO: handle missing responses to commands
  //  - GetLastSourceLogSequenceNrReplicated
  //  - Replicate
  //  - Transfer (?)
  // ----------------------------------------------------------

  val idle: Receive = {
    case TransferDue =>
      targetLog ! GetLastSourceLogSequenceNrReplicated(sourceLogId)
      context.become(replicating(nextCorrelationId()))
      failureDetector ! Tick
    case ReceiveTimeout =>
      targetLog ! GetLastSourceLogSequenceNrReplicated(sourceLogId)
      context.become(replicating(nextCorrelationId()))
  }

  def replicating(correlationId: Int): Receive = {
    case GetLastSourceLogSequenceNrReplicatedSuccess(sourceLogId, lastSourceLogSequenceNrReplicated) =>
      replicationServer ! Transfer(lastSourceLogSequenceNrReplicated + 1, batchSize, correlationId)
    case GetLastSourceLogSequenceNrReplicatedFailure(cause) =>
      // TODO: log cause
      context.become(idle)
    case TransferSuccess(events, lastSourceLogSequenceNrRead, `correlationId`) =>
      targetLog ! Replicate(events, sourceLogId, lastSourceLogSequenceNrRead)
      failureDetector ! Tick
    case TransferFailure(cause, `correlationId`) =>
      // TODO: log cause
      context.become(idle)
      failureDetector ! Tick
    case ReplicateSuccess(num, lastSourceLogSequenceNrReplicated) if num >= batchSize =>
      replicationServer ! Transfer(lastSourceLogSequenceNrReplicated + 1, batchSize, correlationId)
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
      context.parent ! ConnectionRenewal
      context.stop(self)
    case other => super.unhandled(other)
  }

  override def preStart(): Unit =
    self ! TransferDue

  private def nextCorrelationId(): Int = {
    correlationId += 1
    correlationId
  }
}

class ReplicationClientConnector(host: String, port: Int, filter: ReplicationFilter, targetLog: ActorRef, localInstanceId: InstanceId) extends Actor with ActorLogging {
  import ReplicationProtocol._

  val config = context.system.settings.config.getConfig("log.replication")
  val retry = config.getDuration("connect-retry-interval", TimeUnit.MILLISECONDS).millis
  val selection = context.actorSelection(s"akka.tcp://site@${host}:${port}/user/${ReplicationServerConnector.name}")

  val connecting: Receive = {
    case ReceiveTimeout =>
      selection ! Identify(1)
    case ActorIdentity(1, Some(connector)) =>
      connector ! Connect(filter, localInstanceId)
      context.setReceiveTimeout(Duration.Undefined)
      context.become(connected)
  }

  val connected: Receive = {
    case ConnectAccepted(sourceLogId, replicationServer, remoteInstanceId) =>
      context.actorOf(Props(new ReplicationClient(sourceLogId, targetLog, replicationServer, remoteInstanceId)))
      log.info(s"Opened replication connection to ${host}:${port}")
    case ConnectionRenewal =>
      context.setReceiveTimeout(retry)
      context.become(connecting)
  }

  def receive = connecting

  override def preStart(): Unit =
    context.setReceiveTimeout(retry)
}

object ReplicationServerConnector {
  val name: String = "connector"
}

class ReplicationServerConnector(sourceLogId: String, sourceLog: ActorRef, localInstanceId: InstanceId) extends Actor {
  import ReplicationProtocol._

  def receive = {
    case Connect(filter, remoteInstanceId) =>
      val server = context.actorOf(Props(new ReplicationServer(sourceLog, filter, remoteInstanceId)))
      sender() ! ConnectAccepted(sourceLogId, server, localInstanceId)
      context.system.eventStream.publish(ConnectRequested(remoteInstanceId))
  }
}

class ReplicationServerFailureDetector(remoteInstanceId: InstanceId) extends Actor {
  import ReplicationServerFailureDetector._
  import ReplicationEndpoint._

  val config = context.system.settings.config.getConfig("log.replication")
  val limit = config.getDuration("failure-detection-limit", TimeUnit.MILLISECONDS)

  var lastTick: Long = 0L

  context.setReceiveTimeout(limit.millis)

  def receive = {
    case Tick =>
      val currentTime = System.currentTimeMillis
      val lastInterval =  currentTime - lastTick
      if (lastInterval >= limit) {
        context.system.eventStream.publish(Available(remoteInstanceId.uid))
        lastTick = currentTime
      }
    case ReceiveTimeout =>
      context.system.eventStream.publish(Unavailable(remoteInstanceId.uid))
  }
}

object ReplicationServerFailureDetector {
  case object Tick
}