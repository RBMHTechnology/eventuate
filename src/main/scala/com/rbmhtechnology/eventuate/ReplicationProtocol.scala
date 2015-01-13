/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
 */

package com.rbmhtechnology.eventuate

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

  case class Connect(exclusion: String, instanceId: InstanceId)
  case class ConnectAccepted(sourceLogId: String, replicationServer: ActorRef, instanceId: InstanceId)
  case class ConnectRequested(instanceId: InstanceId)
  case object ConnectionRenewal

  case class Transfer(fromSequenceNr: Long, max: Int, correlationId: Int)
  case class TransferSuccess(events: Seq[DurableEvent], correlationId: Int)
  case class TransferFailure(cause: Throwable, correlationId: Int)

  case class GetLastSourceLogSequenceNrReplicated(sourceLogId: String)
  case class GetLastSourceLogSequenceNrReplicatedSuccess(sourceLogId: String, sourceLogSequenceNr: Long)

  case class Replicate(events: Seq[DurableEvent])
  case class ReplicateFailure(cause: Throwable)
  case class ReplicateSuccess(num: Int)

  case class Updated(sourceLogId: String)
}

class ReplicationServer(sourceLog: ActorRef, exclusion: String, remoteInstanceId: InstanceId) extends Actor {
  import ReplicationProtocol._
  import EventLogProtocol._

  var replicationClient: Option[ActorRef] = None

  // ----------------------------------------------------------
  //  TODO: handle missing responses to commands
  //  - Read
  // ----------------------------------------------------------

  val idle: Receive = {
    case Updated(sourceLogId) if sourceLogId != exclusion =>
      replicationClient.foreach(_ ! TransferDue)
    case Transfer(fromSequenceNr, max, correlationId) =>
      sourceLog ! Read(fromSequenceNr, max, exclusion)
      replicationClient = Some(sender())
      context.become(transferring(correlationId))
  }

  def transferring(correlationId: Int): Receive = {
    case ReadSuccess(events) =>
      replicationClient.foreach(_ ! TransferSuccess(events, correlationId))
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
  import ReplicationProtocol._

  context.setReceiveTimeout(5.seconds)

  val batchSize = 20
  var correlationId = 0

  context.system.eventStream.subscribe(self, classOf[ConnectRequested])

  // ----------------------------------------------------------
  //  TODO: handle missing responses to commands
  //  - GetLastSourceLogSequenceNrReplicated
  //  - Replicate
  //  - Transfer (?)
  // ----------------------------------------------------------

  val idle: Receive = {
    case TransferDue | ReceiveTimeout =>
      targetLog ! GetLastSourceLogSequenceNrReplicated(sourceLogId)
      context.become(replicating(nextCorrelationId()))
  }

  def replicating(correlationId: Int): Receive = {
    case GetLastSourceLogSequenceNrReplicatedSuccess(sourceLogId, sourceLogSequenceNr) =>
      replicationServer ! Transfer(sourceLogSequenceNr + 1, batchSize, correlationId)
    case TransferSuccess(events, `correlationId`) =>
      targetLog ! Replicate(events)
    case TransferFailure(cause, `correlationId`) =>
      // TODO: log cause
      context.become(idle)
    case ReplicateSuccess(num) =>
      // TODO: trigger TransferDue if num > 0 (?)
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

class ReplicationClientConnector(host: String, port: Int, exclusion: String, targetLog: ActorRef, localInstanceId: InstanceId) extends Actor {
  import ReplicationProtocol._

  val selection = context.actorSelection(s"akka.tcp://site@${host}:${port}/user/${ReplicationServerConnector.name}")

  val connecting: Receive = {
    case ReceiveTimeout =>
      selection ! Identify(1)
    case ActorIdentity(1, Some(connector)) =>
      connector ! Connect(exclusion, localInstanceId)
      context.setReceiveTimeout(Duration.Undefined)
      context.become(connected)
  }

  val connected: Receive = {
    case ConnectAccepted(sourceLogId, replicationServer, remoteInstanceId) =>
      context.actorOf(Props(new ReplicationClient(sourceLogId, targetLog, replicationServer, remoteInstanceId)))
      println(s"Opened replication connection to ${host}:${port}")
    case ConnectionRenewal =>
      context.setReceiveTimeout(1.seconds)
      context.become(connecting)
  }

  def receive = connecting

  override def preStart(): Unit =
    context.setReceiveTimeout(1.seconds)
}

object ReplicationServerConnector {
  val name: String = "connector"
}

class ReplicationServerConnector(sourceLogId: String, sourceLog: ActorRef, localInstanceId: InstanceId) extends Actor {
  import ReplicationProtocol._

  def receive = {
    case Connect(exclusion, remoteInstanceId) =>
      val server = context.actorOf(Props(new ReplicationServer(sourceLog, exclusion, remoteInstanceId)))
      sender() ! ConnectAccepted(sourceLogId, server, localInstanceId)
      context.system.eventStream.publish(ConnectRequested(remoteInstanceId))
  }
}