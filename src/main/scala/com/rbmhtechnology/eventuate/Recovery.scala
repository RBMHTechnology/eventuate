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
import akka.pattern.{after, ask}
import akka.util.Timeout

import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.log.TimeTracker
import com.typesafe.config.Config

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
 * @param localLogId Local event log id.
 * @param remoteLogId Remote event log id.
 * @param tracker Time tracker value of the local event log at the beginning of disaster recovery.
 */
private case class RecoveryLink(logName: String, localLogId: String, remoteLogId: String, tracker: TimeTracker)

/**
 * Provides disaster recovery primitives.
 *
 * @param endpoint endpoint to be recovered.
 *
 * @see [[ReplicationEndpoint.recover()]]
 */
private class Recovery(endpoint: ReplicationEndpoint) {
  private val settings = new RecoverySettings(endpoint.system.settings.config)

  import Recovery._
  import settings._
  import endpoint.system.dispatcher

  private implicit val timeout = Timeout(remoteOperationTimeout)
  private implicit val scheduler = endpoint.system.scheduler

  /**
   * Reads the time trackers from local event logs.
   */
  def readTimeTrackers: Future[Map[String, TimeTracker]] = {
    println(s"[recovery of ${endpoint.id}] Read time trackers from local event logs ...")
    Future.sequence(endpoint.logNames.map(name => readTimeTracker(endpoint.logs(name)).map(name -> _))).map(_.toMap)
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

  def readTimeTracker(targetLog: ActorRef): Future[TimeTracker] =
    targetLog.ask(GetTimeTracker).mapTo[GetTimeTrackerSuccess].map(_.tracker)

  def readEndpointInfo(targetAcceptor: ActorSelection): Future[ReplicationEndpointInfo] =
    retry(targetAcceptor.ask(GetReplicationEndpointInfo), remoteOperationRetryDelay, remoteOperationRetryMax).mapTo[GetReplicationEndpointInfoSuccess].map(_.info)

  def deleteSnapshots(link: RecoveryLink): Future[Unit] =
    endpoint.logs(link.logName).ask(DeleteSnapshots(link.tracker.sequenceNr + 1L))(Timeout(snapshotDeletionTimeout)).flatMap {
      case DeleteSnapshotsSuccess    => Future.successful(())
      case DeleteSnapshotsFailure(e) => Future.failed(e)
    }

  def recoveryLinks(endpointInfos: Set[ReplicationEndpointInfo], timeTrackers: Map[String, TimeTracker]) = for {
    endpointInfo <- endpointInfos
    logName      <- endpoint.commonLogNames(endpointInfo)
  } yield RecoveryLink(logName, endpoint.logId(logName), endpointInfo.logId(logName), timeTrackers(logName))
}

private object Recovery {
  def retry[T](async: => Future[T], delay: FiniteDuration, retries: Int)(implicit ec: ExecutionContext, s: Scheduler): Future[T] =
    async recoverWith { case _ if retries > 0 => after(delay, s)(retry(async, delay, retries - 1)(ec, s)) }
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

  def initializing: Receive = {
    case Process =>
      context.become(processing)
    case Recover(links, promise) =>
      println(s"[recovery of ${endpoint.id}] Checking replication progress with remote endpoints ...")
      context.become(recovering(context.actorOf(Props(new RecoveryManager(endpoint.id, links))), promise))
  }

  def recovering(recovery: ActorRef, promise: Promise[Unit]): Receive = {
    case re: ReplicationReadEnvelope =>
      recovery forward re
    case RecoveryCompleted =>
      promise.success(())
      context.become(processing)
  }

  def processing: Receive = {
    case ReplicationReadEnvelope(r, logName) =>
    endpoint.logs(logName) forward r
  }

  override def unhandled(message: Any): Unit = message match {
    case GetReplicationEndpointInfo =>
      sender() ! GetReplicationEndpointInfoSuccess(endpoint.info)
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
  case object RecoveryCompleted
}

/**
 * Manages [[RecoveryActor]]s to execute the disaster recovery steps for each replication link in `links`
 * and tracks the progress made by these actors. When all replication links have been processed this actor
 * notifies [[Acceptor]] (= parent) that recovery completed.
 */
private class RecoveryManager(endpointId: String, links: Set[RecoveryLink]) extends Actor {
  import Acceptor._

  var active = links

  var compensators: Map[String, ActorRef] =
    links.map(link => link.remoteLogId -> context.actorOf(Props(new RecoveryActor(endpointId, link)))).toMap

  def receive = {
    case ReplicationReadEnvelope(r, _) =>
      compensators(r.targetLogId) forward r
    case RecoveryStepCompleted(link) if active.contains(link) =>
      active = active - link
      val prg = links.size - active.size
      val all = links.size
      println(s"[recovery of ${endpointId}] Confirm existence of consistent replication progress at ${link.remoteLogId} ($prg of $all) ...")
      if (active.isEmpty) context.parent ! RecoveryCompleted
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

  def receive = {
    case r: ReplicationRead if r.fromSequenceNr > link.tracker.sequenceNr + 1L =>
      println(s"[recovery of ${endpointId}] Trigger update of inconsistent replication progress at ${link.remoteLogId} ...")
      sender() ! ReplicationReadSuccess(Seq(), link.tracker.sequenceNr, link.remoteLogId, link.tracker.vectorTime)
    case r: ReplicationRead =>
      sender() ! ReplicationReadSuccess(Seq(), r.fromSequenceNr - 1L, link.remoteLogId, link.tracker.vectorTime)
      context.parent ! RecoveryStepCompleted(link)
  }
}

