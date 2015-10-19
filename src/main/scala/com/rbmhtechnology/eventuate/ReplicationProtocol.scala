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

import akka.actor._

import com.rbmhtechnology.eventuate.log.TimeTracker

import scala.collection.immutable.Seq

object ReplicationProtocol {
  /**
   * Marker trait for protobuf-serializable replication protocol messages.
   */
  trait Format extends Serializable

  object ReplicationEndpointInfo {
    /**
     * Creates a log identifier from `endpointId` and `logName`
     */
    def logId(endpointId: String, logName: String): String =
      s"${endpointId}_${logName}"
  }

  /**
   * [[ReplicationEndpoint]] info object. Exchanged between replication endpoints
   * to establish replication connections.
   *
   * @param endpointId Replication endpoint id.
   * @param logNames Names of logs managed by the replication endpoint.
   */
  case class ReplicationEndpointInfo(endpointId: String, logNames: Set[String]) extends Format {
    /**
     * Creates a log identifier from this info object's `endpointId` and `logName`
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
   * Update notification sent to a [[Replicator]] indicating that new events are available for replication.
   */
  private[eventuate] case object ReplicationDue extends Format

  /**
   * Requests the time tracker from an event log.
   */
  private[eventuate] case object GetTimeTracker

  /**
   * Success reply after a [[GetTimeTracker]].
   */
  private[eventuate] case class GetTimeTrackerSuccess(tracker: TimeTracker)

  /**
   * Requests all replication progresses from a log.
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
   * Requests the replication progress for given `sourceLogId` from a target log.
   */
  case class GetReplicationProgress(sourceLogId: String)

  /**
   * Success reply after a [[GetReplicationProgress]].
   */
  case class GetReplicationProgressSuccess(sourceLogId: String, storedReplicationProgress: Long, currentTargetVectorTime: VectorTime)

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
  case class ReplicationReadEnvelope(payload: ReplicationRead, logName: String) extends Format

  /**
   * Instructs a source log to read up to `maxNumEvents` starting `fromSequenceNr`
   * and applying the given replication `filter`.
   */
  case class ReplicationRead(fromSequenceNr: Long, maxNumEvents: Int, filter: ReplicationFilter, targetLogId: String, replicator: ActorRef, currentTargetVectorTime: VectorTime) extends Format

  /**
   * Success reply after a [[ReplicationRead]].
   *
   * @param events read events.
   * @param replicationProgress last read sequence number. This is greater than
   *                            or equal to the sequence number of the last read
   *                            event (if any).
   */
  case class ReplicationReadSuccess(events: Seq[DurableEvent], replicationProgress: Long, targetLogId: String, currentSourceVectorTime: VectorTime) extends Format

  /**
   * Failure reply after a [[ReplicationRead]].
   */
  case class ReplicationReadFailure(cause: String, targetLogId: String) extends Format

  /**
   * Instructs a target log to write replicated `events` from the source log identified by
   * `sourceLogId` along with the last read position in the source log (`replicationProgress`).
   */
  case class ReplicationWrite(events: Seq[DurableEvent], sourceLogId: String, replicationProgress: Long, currentSourceVectorTime: VectorTime)

  /**
   * Success reply after a [[ReplicationWrite]].
   *
   * @param num Number of events actually replicated.
   * @param storedReplicationProgress Last source log read position stored in the target log.
   */
  case class ReplicationWriteSuccess(num: Int, storedReplicationProgress: Long, currentTargetVectorTime: VectorTime)

  /**
   * Failure reply after a [[ReplicationWrite]].
   */
  case class ReplicationWriteFailure(cause: Throwable)
}
