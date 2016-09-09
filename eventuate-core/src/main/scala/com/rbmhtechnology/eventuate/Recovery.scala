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

package com.rbmhtechnology.eventuate

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import com.rbmhtechnology.eventuate.Acceptor.Recover
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.log.EventLogClock
import com.typesafe.config.Config

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration._
import scala.reflect.ClassTag

/**
 * [[ReplicationEndpoint.recover]] completes with this exception if recovery fails.
 *
 * @param cause Recovery failure cause.
 * @param partialUpdate Set to `true` if recovery already made partial updates, `false` if recovery
 *                      failed without having made partial updates to replication partners.
 */
class RecoveryException(cause: Throwable, val partialUpdate: Boolean) extends RuntimeException(cause)

private class RecoverySettings(config: Config) {
  val remoteOperationRetryMax: Int =
    config.getInt("eventuate.log.recovery.remote-operation-retry-max")

  val remoteOperationRetryDelay: FiniteDuration =
    config.getDuration("eventuate.log.recovery.remote-operation-retry-delay", TimeUnit.MILLISECONDS).millis

  val remoteOperationTimeout: FiniteDuration =
    config.getDuration("eventuate.log.recovery.remote-operation-timeout", TimeUnit.MILLISECONDS).millis

  val snapshotDeletionTimeout: FiniteDuration =
    config.getDuration("eventuate.log.recovery.snapshot-deletion-timeout", TimeUnit.MILLISECONDS).millis
}

/**
 * Represents a link between a local and remote event log that are subject to disaster recovery.
 *
 * @param logName Common name of the linked local and remote event log.
 * @param localSequenceNr sequence number of the local event log at the beginning of disaster recovery.
 * @param remoteLogId Remote event log id.
 * @param remoteSequenceNr Current sequence nr of the remote log
 */
private case class RecoveryLink(logName: String, localSequenceNr: Long, remoteLogId: String, remoteSequenceNr: Long)

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
   * Read [[ReplicationEndpointInfo]] from local [[ReplicationEndpoint]]
   */
  def readEndpointInfo: Future[ReplicationEndpointInfo] =
    readLogSequenceNrs.map(ReplicationEndpointInfo(endpoint.id, _))

  private def readLogSequenceNrs: Future[Map[String, Long]] =
    readEventLogClocks.map(_.mapValues(_.sequenceNr).view.force)

  /**
   * Reads the clocks from local event logs.
   */
  def readEventLogClocks: Future[Map[String, EventLogClock]] =
    Future.traverse(endpoint.logNames)(name => readEventLogClock(endpoint.logs(name)).map(name -> _)).map(_.toMap)

  /**
   * Synchronize sequence numbers of local logs with replication progress stored in remote replicas.
   * @return a set of [[ReplicationEndpointInfo]] of all remote replicas
   */
  def synchronizeReplicationProgressesWithRemote(info: ReplicationEndpointInfo): Future[Set[ReplicationEndpointInfo]] =
    Future.sequence(endpoint.connectors.map(connector => synchronizeReplicationProgressWithRemote(connector.remoteAcceptor, info)))

  private def synchronizeReplicationProgressWithRemote(remoteAcceptor: ActorSelection, info: ReplicationEndpointInfo): Future[ReplicationEndpointInfo] =
    readResult[SynchronizeReplicationProgressSuccess, SynchronizeReplicationProgressFailure, ReplicationEndpointInfo](
      Retry(remoteAcceptor.ask(SynchronizeReplicationProgress(info)), remoteOperationRetryDelay, remoteOperationRetryMax), _.info, _.cause)

  /**
   * Update the locally stored replication progress of remote replicas with the sequence numbers given in ``info``.
   * Replication progress that is greater than the corresponding sequence number in ``info`` is reset to that
   */
  def synchronizeReplicationProgress(info: ReplicationEndpointInfo): Future[Unit] = {
    Future.traverse(endpoint.commonLogNames(info)) { name =>
      val logActor = endpoint.logs(name)
      val logId = info.logId(name)
      val remoteSequenceNr = info.logSequenceNrs(name)
      for {
        currentProgress <- readReplicationProgress(logActor, logId)
        _ <- if (currentProgress > remoteSequenceNr) updateReplicationProgress(logActor, logId, info.logSequenceNrs(name))
        else Future.successful(currentProgress)
      } yield ()
    } map (_ => ())
  }

  private def readReplicationProgress(logActor: ActorRef, logId: String): Future[Long] =
    readResult[GetReplicationProgressSuccess, GetReplicationProgressFailure, Long](
      logActor.ask(GetReplicationProgress(logId)), _.storedReplicationProgress, _.cause)

  private def updateReplicationProgress(logActor: ActorRef, logId: String, sequenceNr: Long): Future[Long] = {
    readResult[ReplicationWriteSuccess, ReplicationWriteFailure, Long](
      logActor.ask(ReplicationWrite(Seq.empty, sequenceNr, logId, VectorTime.Zero)), _.storedReplicationProgress, _.cause)
  }

  /**
   * Initiates event recovery for the given [[ReplicationLink]]s. The returned [[Future]] completes when
   * all events are successfully recovered.
   */
  def recoverLinks(recoveryLinks: Set[RecoveryLink])(implicit ec: ExecutionContext): Future[Unit] = {
    if (recoveryLinks.isEmpty) {
      Future.successful(())
    } else {
      val recoveryFinishedPromise = Promise[Unit]()
      deleteSnapshots(recoveryLinks).onSuccess {
        case _ => endpoint.acceptor ! Recover(recoveryLinks, recoveryFinishedPromise)
      }
      recoveryFinishedPromise.future
    }
  }

  /**
   * Deletes all invalid snapshots from local event logs. A snapshot is invalid if it covers
   * events that have been lost.
   */
  private def deleteSnapshots(links: Set[RecoveryLink]): Future[Unit] =
    Future.sequence(links.map(deleteSnapshots)).map(_ => ())

  def readEventLogClock(targetLog: ActorRef): Future[EventLogClock] =
    targetLog.ask(GetEventLogClock).mapTo[GetEventLogClockSuccess].map(_.clock)

  def readRemoteEndpointInfo(targetAcceptor: ActorSelection): Future[ReplicationEndpointInfo] =
    Retry(targetAcceptor.ask(GetReplicationEndpointInfo), remoteOperationRetryDelay, remoteOperationRetryMax)
      .mapTo[GetReplicationEndpointInfoSuccess]
      .map(_.info)

  private def deleteSnapshots(link: RecoveryLink): Future[Unit] =
    readResult[DeleteSnapshotsSuccess.type, DeleteSnapshotsFailure, Unit](
      endpoint.logs(link.logName).ask(DeleteSnapshots(link.localSequenceNr + 1L))(Timeout(snapshotDeletionTimeout)), _ => (), _.cause)

  def recoveryLinks(remoteEndpointInfos: Set[ReplicationEndpointInfo], localEndpointInfo: ReplicationEndpointInfo, filtered: Boolean): Set[RecoveryLink] = for {
    endpointInfo <- remoteEndpointInfos
    logName <- endpoint.commonLogNames(endpointInfo)
    if (endpoint.endpointFilters.filterFor(endpointInfo.logId(logName), logName) != NoFilter) == filtered
  } yield RecoveryLink(logName, localEndpointInfo.logSequenceNrs(logName), endpointInfo.logId(logName), endpointInfo.logSequenceNrs(logName))

  private def readResult[S: ClassTag, F: ClassTag, R](f: Future[Any], result: S => R, cause: F => Throwable): Future[R] = f.flatMap {
    case success: S => Future.successful(result(success))
    case failure: F => Future.failed(cause(failure))
  }
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

  def initializing: Receive = recovering orElse {
    case Process =>
      context.become(processing)
  }

  def recovering: Receive = {
    case Recover(links, promise) =>
      endpoint.connectors.foreach(_.activate(Some(links.map(_.remoteLogId))))
      val recoveryManager = context.actorOf(Props(new RecoveryManager(endpoint.id, links)))
      context.become(recoveringEvents(recoveryManager, promise) orElse processing)
    case RecoveryFinished =>
      context.become(processing)
  }

  def recoveringEvents(recoveryManager: ActorRef, promise: Promise[Unit]): Receive = {
    case writeSuccess: ReplicationWriteSuccess =>
      recoveryManager forward writeSuccess
    case EventRecoveryCompleted =>
      promise.success(())
      context.become(recovering orElse processing)
  }

  def processing: Receive = {
    case re: ReplicationReadEnvelope if re.incompatibleWith(endpoint.applicationName, endpoint.applicationVersion) =>
      sender ! ReplicationReadFailure(IncompatibleApplicationVersionException(endpoint.id, endpoint.applicationVersion, re.targetApplicationVersion), re.payload.targetLogId)
    case ReplicationReadEnvelope(r, logName, _, _) =>
      val r2 = r.copy(filter = endpoint.endpointFilters.filterFor(r.targetLogId, logName) and r.filter)
      endpoint.logs(logName) forward r2
    case _: ReplicationWriteSuccess =>
  }

  override def unhandled(message: Any): Unit = message match {
    case GetReplicationEndpointInfo =>
      recovery.readEndpointInfo.map(GetReplicationEndpointInfoSuccess).pipeTo(sender())
    case SynchronizeReplicationProgress(remoteInfo) =>
      val localInfo = for {
        _ <- recovery.synchronizeReplicationProgress(remoteInfo)
        localInfo <- recovery.readEndpointInfo.map(SynchronizeReplicationProgressSuccess)
      } yield localInfo
      localInfo.recover {
        case ex: Throwable => SynchronizeReplicationProgressFailure(SynchronizeReplicationProgressSourceException(ex.getMessage))
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
  case object RecoveryFinished
  case class RecoveryStepCompleted(link: RecoveryLink)
  case object MetadataRecoveryCompleted
  case object EventRecoveryCompleted
}

/**
 * If disaster recovery is initiated events are recovered until
 * a [[ReplicationWriteSuccess]] sent as notification from the local [[Replicator]] is received indicating that all
 * events, known to exist remotely at the beginning of recovery, are replicated.
 *
 * When all replication links have been processed this actor
 * notifies [[Acceptor]] (= parent) that recovery completed and ends itself.
 */
private class RecoveryManager(endpointId: String, links: Set[RecoveryLink]) extends Actor with ActorLogging {
  import Acceptor._

  def receive = recoveringEvents(links)

  private def recoveringEvents(active: Set[RecoveryLink]): Receive = {
    case writeSuccess: ReplicationWriteSuccess if active.exists(_.remoteLogId == writeSuccess.sourceLogId) =>
      active.find(recoveryForLinkFinished(_, writeSuccess)).foreach { link =>
        val updatedActive = removeLink(active, link)
        if (updatedActive.isEmpty) {
          context.parent ! EventRecoveryCompleted
          self ! PoisonPill
        } else
          context.become(recoveringEvents(updatedActive))
      }
  }

  private def recoveryForLinkFinished(link: RecoveryLink, writeSuccess: ReplicationWriteSuccess): Boolean =
    link.remoteLogId == writeSuccess.sourceLogId && link.remoteSequenceNr <= writeSuccess.storedReplicationProgress

  private def removeLink(active: Set[RecoveryLink], link: RecoveryLink): Set[RecoveryLink] = {
    val updatedActive = active - link
    val finished = links.size - updatedActive.size
    val all = links.size
    log.info("[recovery of {}] Event recovery finished for remote log {} ({} of {})", endpointId, link.remoteLogId, finished, all)
    updatedActive
  }
}
