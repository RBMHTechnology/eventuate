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
import scala.concurrent.duration.FiniteDuration

object ReplicationProtocol {
  /**
   * Marker trait for protobuf-serializable replication protocol messages.
   */
  trait Format extends Serializable

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
   * @param logSequenceNrs sequence numbers of logs managed by the replication endpoint.
   */
  case class ReplicationEndpointInfo(endpointId: String, logSequenceNrs: Map[String, Long]) extends Format {
    /**
     * The names of logs managed by the [[ReplicationEndpoint]].
     */
    val logNames: Set[String] = logSequenceNrs.keySet

    /**
     * Creates a log identifier from this info object's `endpointId` and `logName`.
     */
    def logId(logName: String): String =
      ReplicationEndpointInfo.logId(endpointId, logName)
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
   * Used to synchronize replication progress between two [[ReplicationEndpoint]]s when disaster recovery
   * is initiated (through [[ReplicationEndpoint.recover()]]. Sent by the [[ReplicationEndpoint]]
   * that is recovered to instruct a remote [[ReplicationEndpoint]] to
   *
   * - reset locally stored replication progress according to the sequence numbers given in ``info`` and
   * - respond with a [[ReplicationEndpointInfo]] containing the after disaster progress of its logs
   */
  private[eventuate] case class SynchronizeReplicationProgress(info: ReplicationEndpointInfo) extends Format

  /**
   * Successful response to a [[SynchronizeReplicationProgress]] request.
   */
  private[eventuate] case class SynchronizeReplicationProgressSuccess(info: ReplicationEndpointInfo) extends Format

  /**
   * Failure response to a [[SynchronizeReplicationProgress]] request.
   */
  private[eventuate] case class SynchronizeReplicationProgressFailure(cause: SynchronizeReplicationProgressException) extends Format

  /**
   * Base class for all [[SynchronizeReplicationProgressFailure]] `cause`s.
   */
  private[eventuate] abstract class SynchronizeReplicationProgressException(message: String) extends RuntimeException(message)

  /**
   * Indicates a problem synchronizing the replication progress of a remote [[ReplicationEndpoint]]
   */
  private[eventuate] case class SynchronizeReplicationProgressSourceException(message: String)
    extends SynchronizeReplicationProgressException(s"Failure when updating local replication progress: $message")
    with Format

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
  case class ReplicationReadFailure(cause: ReplicationReadException, targetLogId: String) extends Format

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

  /**
   * Base class for all [[ReplicationReadFailure]] `cause`s.
   */
  abstract class ReplicationReadException(message: String) extends RuntimeException(message)

  /**
   * Indicates a problem reading events from the backend store at the source endpoint.
   */
  case class ReplicationReadSourceException(message: String) extends ReplicationReadException(message) with Format

  /**
   * Indicates that a [[ReplicationRead]] request timed out.
   */
  case class ReplicationReadTimeoutException(timeout: FiniteDuration) extends ReplicationReadException(s"Replication read timed out after $timeout")

  /**
   * Instruct a log to adjust the sequence nr of the internal [[EventLogClock]] to the version vector.
   * This is ensures that the sequence nr is greater than or equal to the log's entry in the version vector.
   */
  case object AdjustEventLogClock

  /**
   * Success reply after a [[AdjustEventLogClock]]. Contains the adjusted clock.
   */
  case class AdjustEventLogClockSuccess(clock: EventLogClock)

  /**
   * Failure reply after a [[AdjustEventLogClock]].
   */
  case class AdjustEventLogClockFailure(cause: Throwable)

  /**
   * Indicates that events cannot be replication from a source [[ReplicationEndpoint]] to a target [[ReplicationEndpoint]]
   * because their application versions are incompatible.
   */
  case class IncompatibleApplicationVersionException(
    sourceEndpointId: String,
    sourceApplicationVersion: ApplicationVersion,
    targetApplicationVersion: ApplicationVersion)
    extends ReplicationReadException(s"Event replication rejected by remote endpoint $sourceEndpointId. Target $targetApplicationVersion not compatible with source $sourceApplicationVersion") with Format
}
