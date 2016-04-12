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

import akka.actor._

import com.rbmhtechnology.eventuate.log.EventLogClock

import scala.collection.immutable.Seq

object ReplicationProtocol {
  /**
   * Marker trait for protobuf-serializable replication protocol messages.
   */
  trait Format extends Serializable

  /**
   * Info about an event log. Part of [[ReplicationEndpointInfo]].
   *
   * @param logName Name of the log this info object is about
   * @param sequenceNr Current sequence number of this log
   */
  case class LogInfo(logName: String, sequenceNr: Long) extends Format

  object ReplicationEndpointInfo {
    /**
     * Creates a log identifier from `endpointId` and `logName`.
     */
    def logId(endpointId: String, logName: String): String =
      s"${endpointId}_${logName}"
  }

  /**
   * [[ReplicationEndpoint]] info object. Exchanged between replication endpoints to establish replication connections.
   *
   * @param endpointId Replication endpoint id.
   * @param logInfos [[LogInfo]]s of logs managed by the replication endpoint.
   */
  case class ReplicationEndpointInfo(endpointId: String, logInfos: Set[LogInfo]) extends Format {
    /**
     * The names of logs managed by the [[ReplicationEndpoint]].
     */
    val logNames: Set[String] = logInfos.map(_.logName)

    /**
     * Creates a log identifier from this info object's `endpointId` and `logName`.
     */
    def logId(logName: String): String =
      ReplicationEndpointInfo.logId(endpointId, logName)

    /**
     * Returns a [[LogInfo]] for the given `logName`.
     */
    def logInfo(logName: String): Option[LogInfo] = logInfos.find(_.logName == logName)
  }

  /**
   * Instructs a remote [[ReplicationEndpoint]] to return a [[ReplicationEndpointInfo]] object.
   */
  private[eventuate] case object GetReplicationEndpointInfo extends Format

  /**
   * Success reply to [[GetReplicationEndpointInfo]].
   */
  private[eventuate] case class GetReplicationEndpointInfoSuccess(info: ReplicationEndpointInfo) extends Format

  /**
   * Update notification sent to a replicator indicating that new events are available for replication.
   */
  case object ReplicationDue extends Format

  /**
   * Requests the clock from an event log.
   */
  case object GetEventLogClock

  /**
   * Success reply after a [[GetEventLogClock]].
   */
  case class GetEventLogClockSuccess(clock: EventLogClock)

  /**
   * Requests all local replication progresses from a log. The local replication progress is the sequence number
   * in the remote log up to which the local log has replicated all events from the remote log.
   */
  case object GetReplicationProgresses

  /**
   * Success reply after a [[GetReplicationProgresses]].
   */
  case class GetReplicationProgressesSuccess(progresses: Map[String, Long])

  /**
   * Failure reply after a [[GetReplicationProgresses]].
   */
  case class GetReplicationProgressesFailure(cause: Throwable)

  /**
   * Requests the local replication progress for given `sourceLogId` from a target log.
   *
   * @see GetReplicationProgresses
   */
  case class GetReplicationProgress(sourceLogId: String)

  /**
   * Success reply after a [[GetReplicationProgress]].
   */
  case class GetReplicationProgressSuccess(sourceLogId: String, storedReplicationProgress: Long, currentTargetVersionVector: VectorTime)

  /**
   * Failure reply after a [[GetReplicationProgress]].
   */
  case class GetReplicationProgressFailure(cause: Throwable)

  /**
   * Requests a target log to set the given `replicationProgress` for `sourceLogId`.
   */
  case class SetReplicationProgress(sourceLogId: String, replicationProgress: Long)

  /**
   * Success reply after a [[SetReplicationProgress]].
   */
  case class SetReplicationProgressSuccess(sourceLogId: String, storedReplicationProgress: Long)

  /**
   * Failure reply after a [[SetReplicationProgress]].
   */
  case class SetReplicationProgressFailure(cause: Throwable)

  /**
   * [[ReplicationRead]] requests are sent within this envelope to allow a remote acceptor to
   * dispatch the request to the appropriate log.
   */
  case class ReplicationReadEnvelope(payload: ReplicationRead, logName: String, targetApplicationName: String, targetApplicationVersion: ApplicationVersion) extends Format {
    import Ordered.orderingToOrdered

    def incompatibleWith(sourceApplicationName: String, sourceApplicationVersion: ApplicationVersion): Boolean =
      targetApplicationName == sourceApplicationName && targetApplicationVersion < sourceApplicationVersion
  }

  /**
   * Failure reply after a [[ReplicationReadEnvelope]] if the source application version is
   * incompatible with the target application version.
   */
  case class ReplicationReadEnvelopeIncompatible(sourceApplicationVersion: ApplicationVersion) extends Format

  /**
   * Instructs a source log to read up to `max` events starting at `fromSequenceNr` and applying
   * the given replication `filter`.
   */
  case class ReplicationRead(fromSequenceNr: Long, max: Int, scanLimit: Int, filter: ReplicationFilter, targetLogId: String, replicator: ActorRef, currentTargetVersionVector: VectorTime) extends Format

  /**
   * Success reply after a [[ReplicationRead]].
   */
  case class ReplicationReadSuccess(events: Seq[DurableEvent], fromSequenceNr: Long, replicationProgress: Long, targetLogId: String, currentSourceVersionVector: VectorTime) extends DurableEventBatch with Format

  /**
   * Failure reply after a [[ReplicationRead]].
   */
  case class ReplicationReadFailure(cause: String, targetLogId: String) extends Format

  /**
   * Instructs an event log to batch-execute the given `writes`.
   */
  case class ReplicationWriteN(writes: Seq[ReplicationWrite])

  /**
   * Completion reply after a [[ReplicationWriteN]].
   */
  case object ReplicationWriteNComplete

  /**
   * Instructs a target log to write replicated `events` from the source log identified by
   * `sourceLogId` along with the last read position in the source log (`replicationProgress`).
   */
  case class ReplicationWrite(events: Seq[DurableEvent], replicationProgress: Long, sourceLogId: String, currentSourceVersionVector: VectorTime, continueReplication: Boolean = false, replyTo: ActorRef = null) extends UpdateableEventBatch[ReplicationWrite] {
    override def update(events: Seq[DurableEvent]): ReplicationWrite = copy(events = events)
  }

  /**
   * Success reply after a [[ReplicationWrite]].
   */
  case class ReplicationWriteSuccess(num: Int, storedReplicationProgress: Long, sourceLogId: String, currentTargetVersionVector: VectorTime, continueReplication: Boolean = false)

  /**
   * Failure reply after a [[ReplicationWrite]].
   */
  case class ReplicationWriteFailure(cause: Throwable)
}
