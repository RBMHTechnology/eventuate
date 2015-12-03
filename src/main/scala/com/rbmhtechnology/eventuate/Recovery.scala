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
import akka.pattern.{ ask, pipe }
import akka.util.Timeout

import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.log.EventLog
import com.rbmhtechnology.eventuate.log.EventLogClock
import com.typesafe.config.Config

import scala.collection.breakOut
import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration._

private class RecoverySettings(config: Config) {
  val remoteOperationRetryMax: Int =
    config.getInt("eventuate.disaster-recovery.remote-operation-retry-max")

  val remoteOperationRetryDelay: FiniteDuration =
    config.getDuration("eventuate.disaster-recovery.remote-operation-retry-delay", TimeUnit.MILLISECONDS).millis

  val remoteOperationTimeout: FiniteDuration =
    config.getDuration("eventuate.disaster-recovery.remote-operation-timeout", TimeUnit.MILLISECONDS).millis

  val snapshotDeletionTimeout: FiniteDuration =
    config.getDuration("eventuate.disaster-recovery.snapshot-deletion-timeout", TimeUnit.MILLISECONDS).millis
}

/**
 * Represents a link between a local and remote event log that are subject to disaster recovery.
 *
 * @param logName Common name of the linked local and remote event log.
 * @param localClock Local event log clock at the beginning of disaster recovery.
 * @param localLogActor Local [[EventLog]] actor
 * @param remoteLogId Remote event log id.
 * @param remoteSequenceNr Current sequence nr of the remote log
 */
private case class RecoveryLink(logName: String, localLogActor: ActorRef, localClock: EventLogClock, remoteLogId: String, remoteSequenceNr: Long)

/**
 * Provides disaster recovery primitives.
 *
 * @param endpoint endpoint to be recovered.
 *
 * @see [[ReplicationEndpoint.recover()]]
 */
private class Recovery(endpoint: ReplicationEndpoint) {
  private val settings = new RecoverySettings(endpoint.system.settings.config)

  import settings._
  import endpoint.system.dispatcher

  private implicit val timeout = Timeout(remoteOperationTimeout)
  private implicit val scheduler = endpoint.system.scheduler

  /**
   * Reads [[LogInfo]]s of all local logs.
   */
  def readLogInfos: Future[Set[LogInfo]] = {
    readClocksImpl.map(_.map { case (logName, clock) => LogInfo(logName, clock.sequenceNr) }(breakOut))
  }

  /**
   * Reads the clocks from local event logs.
   */
  def readEventLogClocks: Future[Map[String, EventLogClock]] = {
    println(s"[recovery of ${endpoint.id}] Read clocks from local event logs ...")
    readClocksImpl
  }

  private def readClocksImpl: Future[Map[String, EventLogClock]] = {
    Future.traverse(endpoint.logNames)(name => readEventLogClock(endpoint.logs(name)).map(name -> _)).map(_.toMap)
  }

  /**
   * Reads endpoint metadata from remote endpoints.
   */
  def readEndpointInfos: Future[Set[ReplicationEndpointInfo]] = {
    println(s"[recovery of ${endpoint.id}] Read metadata from remote replication endpoints ...")
    Future.sequence(endpoint.connectors.map(connector => readEndpointInfo(connector.remoteAcceptor)))
  }

  /**
   * Deletes all invalid snapshots from local event logs. A snapshot is invalid if it covers
   * events that have been lost.
   */
  def deleteSnapshots(links: Set[RecoveryLink]): Future[Unit] = {
    println(s"[recovery of ${endpoint.id}] Delete invalidated snapshots at local endpoint ...")
    Future.sequence(links.map(deleteSnapshots)).map(_ => ())
  }

  def readEventLogClock(targetLog: ActorRef): Future[EventLogClock] =
    targetLog.ask(GetEventLogClock).mapTo[GetEventLogClockSuccess].map(_.clock)

  def readEndpointInfo(targetAcceptor: ActorSelection): Future[ReplicationEndpointInfo] =
    Retry(targetAcceptor.ask(GetReplicationEndpointInfo), remoteOperationRetryDelay, remoteOperationRetryMax).mapTo[GetReplicationEndpointInfoSuccess].map(_.info)

  def deleteSnapshots(link: RecoveryLink): Future[Unit] =
    endpoint.logs(link.logName).ask(DeleteSnapshots(link.localClock.sequenceNr + 1L))(Timeout(snapshotDeletionTimeout)).flatMap {
      case DeleteSnapshotsSuccess    => Future.successful(())
      case DeleteSnapshotsFailure(e) => Future.failed(e)
    }

  def recoveryLinks(endpointInfos: Set[ReplicationEndpointInfo], clocks: Map[String, EventLogClock]) = for {
    endpointInfo <- endpointInfos
    logName <- endpoint.commonLogNames(endpointInfo)
  } yield RecoveryLink(logName, endpoint.logs(logName), clocks(logName), endpointInfo.logId(logName), endpointInfo.logInfo(logName).get.sequenceNr)
}

/**
 * [[ReplicationEndpoint]]-scoped singleton that receives all requests from remote endpoints. These are
 *
 *  - [[GetReplicationEndpointInfo]] requests.
 *  - [[ReplicationRead]] requests (inside [[ReplicationReadEnvelope]]s).
 *
 * This actor is also involved in disaster recovery and implements a state machine with the following
 * possible transitions:
 *
 *  - `initializing` -> `recovering` -> `processing` (when calling `endpoint.recover()`)
 *  - `initializing` -> `processing`                 (when calling `endpoint.activate()`)
 */
private class Acceptor(endpoint: ReplicationEndpoint) extends Actor {
  import Acceptor._
  import context.dispatcher

  private val recovery = new Recovery(endpoint)

  def initializing: Receive = {
    case Process =>
      context.become(processing)
    case Recover(links, promise) =>
      println(s"[recovery of ${endpoint.id}] Checking replication progress with remote endpoints ...")
      context.become(recovering(context.actorOf(Props(new RecoveryManager(endpoint.id, links))), promise))
  }

  def recovering(recoveryManager: ActorRef, promise: Promise[Unit]): Receive = {
    case re: ReplicationReadEnvelope =>
      recoveryManager forward re
    case MetadataRecoveryCompleted =>
      endpoint.connectors.foreach(_.activate())
    case EventRecoveryCompleted =>
      promise.success(())
      context.become(processing)
  }

  def processing: Receive = {
    case ReplicationReadEnvelope(r, logName) =>
      endpoint.logs(logName) forward r
  }

  override def unhandled(message: Any): Unit = message match {
    case GetReplicationEndpointInfo =>
      recovery.readLogInfos.map { logInfos =>
        GetReplicationEndpointInfoSuccess(ReplicationEndpointInfo(endpoint.id, logInfos))
      } pipeTo sender()
    case _ =>
      super.unhandled(message)
  }

  def receive =
    initializing
}

private object Acceptor {
  val Name = "acceptor"

  case object Process
  case class Recover(links: Set[RecoveryLink], promise: Promise[Unit])
  case class RecoveryStepCompleted(link: RecoveryLink)
  case object MetadataRecoveryCompleted
  case object EventRecoveryCompleted
}

/**
 * Recovery of an [[ReplicationEndpoint]] is a two steps process:
 *
 * - Metadata recovery: A [[ReplicationReadEnvelope]] from remote is responded with
 *   [[ReplicationReadSuccess]] with the current sequence nr to update remote's replication progress
 * - Event recovery: A [[ReplicationReadEnvelope]] from remote is intercepted to detect when all
 *   events, known to exist remotely at the beginning of recovery, are replicated and recovery is completed.
 *
 * [[RecoveryActor]]s are created to execute the disaster recovery steps for each replication link in `links`
 * and the progress made by these actors is tracked. When all replication links have been processed this actor
 * notifies [[Acceptor]] (= parent) that recovery completed and ends itself and its childs.
 */
private class RecoveryManager(endpointId: String, links: Set[RecoveryLink]) extends Actor {
  import Acceptor._

  // (remote) logid -> RecoveryActor for this link
  val compensators: Map[String, ActorRef] =
    links.map(link => link.remoteLogId -> context.actorOf(Props(new RecoveryActor(endpointId, link)))).toMap

  def receive = recoveringMetadata(links)

  def recoveringMetadata(active: Set[RecoveryLink]): Receive = {
    case ReplicationReadEnvelope(r, _) =>
      compensators(r.targetLogId) forward r
    case RecoveryStepCompleted(link) if active.contains(link) =>
      val updatedActive = active - link
      val prg = links.size - updatedActive.size
      val all = links.size
      println(s"[recovery of ${endpointId}] Confirm existence of consistent replication progress at ${link.remoteLogId} ($prg of $all) ...")
      if (updatedActive.isEmpty) {
        context.parent ! MetadataRecoveryCompleted
        context.become(recoveringEvents(links))
      } else
        context.become(recoveringMetadata(updatedActive))
  }

  def recoveringEvents(active: Set[RecoveryLink]): Receive = {
    case ReplicationReadEnvelope(readRequest, _) =>
      compensators(readRequest.targetLogId) forward readRequest
    case RecoveryStepCompleted(link) if active.contains(link) =>
      val updatedActive = active - link
      if (updatedActive.isEmpty) {
        context.parent ! EventRecoveryCompleted
        self ! PoisonPill
      } else
        context.become(recoveringEvents(updatedActive))
  }
}

/**
 * Executes the disaster recovery step for a given recovery `link`. It processes [[ReplicationRead]] requests from
 * the remote event log in the following way:
 *
 *  - if [[ReplicationRead.fromSequenceNr]] is greater than the local log's current sequence number + 1 then the
 *    replication progress in the empty [[ReplicationReadSuccess]] reply is set to the local log's current sequence
 *    number. This causes the remote log to update its stored replication progress for the local log. A successful
 *    update is verified by processing a follow-up [[ReplicationRead]] request from the remote log (which should now
 *    have a valid [[ReplicationRead.fromSequenceNr]] of exactly current sequence number + 1).
 *  - if [[ReplicationRead.fromSequenceNr]] is not greater than the local log's current sequence number + 1, the
 *    remote log's stored replication progress doesn't need an update and the [[RecoveryManager]] (= parent) is
 *    informed about recovery progress. The replication progress in the empty [[ReplicationReadSuccess]] reply is
 *    set to [[ReplicationRead.fromSequenceNr]] - 1.
 */
private class RecoveryActor(endpointId: String, link: RecoveryLink) extends Actor {
  import Acceptor._

  def receive = recoveringMetadata

  def recoveringMetadata: Receive = {
    case r: ReplicationRead if r.fromSequenceNr > link.localClock.sequenceNr + 1L =>
      println(s"[recovery of ${endpointId}] Trigger update of inconsistent replication progress at ${link.remoteLogId} ...")
      sender() ! ReplicationReadSuccess(Seq(), link.localClock.sequenceNr, link.remoteLogId, link.localClock.versionVector)
    case r: ReplicationRead =>
      sender() ! ReplicationReadSuccess(Seq(), r.fromSequenceNr - 1L, link.remoteLogId, link.localClock.versionVector)
      context.parent ! RecoveryStepCompleted(link)
      context.become(recoveringEvents)
  }

  def recoveringEvents: Receive = {
    case readRequest: ReplicationRead =>
      link.localLogActor forward readRequest
      if (link.remoteSequenceNr < readRequest.fromSequenceNr)
        context.parent ! RecoveryStepCompleted(link)
  }
}

