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

import scala.collection.immutable.Seq

object ReplicationProtocol {
  /**
   * Marker trait for protobuf-serializable replication protocol messages.
   */
  trait Format extends Serializable

  private[eventuate] object ReplicationEndpointInfo {
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
  private[eventuate] case class ReplicationEndpointInfo(endpointId: String, logNames: Set[String]) extends Format {
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
   * Subscribes a `replicator`, replicating events from a source log identified by `sourceLogId`
   * to a target log identified by `targetLogId`, at a source [[ReplicationEndpoint]] with given
   * replication `filter`. The `replicator` will receive [[ReplicationDue]] notifications if any
   * events written to the source log pass the replication filter (indicating that new events are
   * available for replication).
   */
  private[eventuate] case class SubscribeReplicator(sourceLogId: String, targetLogId: String, replicator: ActorRef, filter: ReplicationFilter) extends Format

  /**
   * Update notification sent to a [[Replicator]] indicating that new events are available for
   * replication.
   *
   * @see [[SubscribeReplicator]]
   */
  private[eventuate] case object ReplicationDue extends Format

  /**
   * Instructs a source log to read up to `maxNumEvents` starting `fromSequenceNr`
   * and applying the given replication `filter`.
   */
  case class ReplicationRead(fromSequenceNr: Long, maxNumEvents: Int, filter: ReplicationFilter, targetLogId: String) extends Format

  /**
   * Success reply after a [[ReplicationRead]].
   *
   * @param events read events.
   * @param lastSourceLogSequenceNrRead last read sequence number. This is greater than
   *                                    or equal the sequence number of the last read
   *                                    event (if any).
   */
  case class ReplicationReadSuccess(events: Seq[DurableEvent], lastSourceLogSequenceNrRead: Long, targetLogId: String) extends Format

  /**
   * Failure reply after a [[ReplicationRead]].
   */
  case class ReplicationReadFailure(cause: String, targetLogId: String) extends Format

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
   * Instructs a target log to write replicated `events` from the source log identified by
   * `sourceLogId` along with the last read position in the source log (`lastSourceLogSequenceNrRead`).
   */
  case class ReplicationWrite(events: Seq[DurableEvent], sourceLogId: String, lastSourceLogSequenceNrRead: Long)

  /**
   * Success reply after a [[ReplicationWrite]].
   *
   * @param num Number of events actually replicated.
   * @param lastSourceLogSequenceNrStored Last source log read position stored in the target log.
   */
  case class ReplicationWriteSuccess(num: Int, lastSourceLogSequenceNrStored: Long)

  /**
   * Failure reply after a [[ReplicationWrite]].
   */
  case class ReplicationWriteFailure(cause: Throwable)

  /**
   * Published by event logs to the actor system's event stream whenever new events have been written,
   * either by replication or by event-sourced actors.
   *
   * @param logId id of the event log that published the update notification.
   * @param events Written events.
   */
  case class Updated(logId: String, events: Seq[DurableEvent])
}
