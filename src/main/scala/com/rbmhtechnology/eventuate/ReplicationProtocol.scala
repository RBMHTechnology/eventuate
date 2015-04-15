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
  private[eventuate] object ReplicationEndpointInfo {
    /**
     * Creates a log identifier from `endpointId` and `logName`
     */
    def logId(endpointId: String, logName: String): String =
      s"${endpointId}-${logName}"
  }

  /**
   * [[ReplicationEndpoint]] info object. Exchanged between replication endpoints
   * to establish replication connections.
   *
   * @param endpointId Replication endpoint id.
   * @param logNames Names of logs managed by the replication endpoint.
   */
  private[eventuate] case class ReplicationEndpointInfo(endpointId: String, logNames: Set[String]) {
    /**
     * Creates a log identifier from this info object's `endpointId` and `logName`
     */
    def logId(logName: String): String =
      ReplicationEndpointInfo.logId(endpointId, logName)
  }

  private[eventuate] case object GetReplicationEndpointInfo
  private[eventuate] case class GetReplicationEndpointInfoSuccess(info: ReplicationEndpointInfo)

  private[eventuate] case object ReplicationDue
  private[eventuate] case class SubscribeReplicator(targetLogId: String, replicator: ActorRef, filter: ReplicationFilter)


  /**
   * Instructs a source log to read up to `maxNumEvents` starting `fromSequenceNr`
   * and applying the given replication `filter`.
   */
  case class ReplicationRead(fromSequenceNr: Long, maxNumEvents: Int, filter: ReplicationFilter, targetLogId: String, correlationId: Int)

  /**
   * Success reply after a [[ReplicationRead]].
   *
   * @param events read events.
   * @param lastSourceLogSequenceNrRead last read sequence number. This is greater than
   *                                    or equal the sequence number of the last read
   *                                    event (if any).
   */
  case class ReplicationReadSuccess(events: Seq[DurableEvent], lastSourceLogSequenceNrRead: Long, targetLogId: String, correlationId: Int)

  /**
   * Failure reply after a [[ReplicationRead]].
   */
  case class ReplicationReadFailure(cause: Throwable, targetLogId: String, correlationId: Int)

  /**
   * Requests from a target log the last read position in the given source log.
   */
  case class GetLastSourceLogReadPosition(sourceLogId: String, correlationId: Int)

  /**
   * Success reply after a [[GetLastSourceLogReadPosition]].
   */
  case class GetLastSourceLogReadPositionSuccess(sourceLogId: String, lastSourceLogSequenceNrStored: Long, correlationId: Int)

  /**
   * Failure reply after a [[GetLastSourceLogReadPosition]].
   */
  case class GetLastSourceLogReadPositionFailure(cause: Throwable, correlationId: Int)

  /**
   * Instructs a target log to write replicated `events` from the source log identified by
   * `sourceLogId` along with the last read position in the source log (`lastSourceLogSequenceNrRead`).
   */
  case class ReplicationWrite(events: Seq[DurableEvent], sourceLogId: String, lastSourceLogSequenceNrRead: Long, correlationId: Int)

  /**
   * Success reply after a [[ReplicationWrite]].
   *
   * @param num Number of events actually replicated.
   * @param lastSourceLogSequenceNrStored Last source log read position stored in the target log.
   */
  case class ReplicationWriteSuccess(num: Int, lastSourceLogSequenceNrStored: Long, correlationId: Int)

  /**
   * Failure reply after a [[ReplicationWrite]].
   */
  case class ReplicationWriteFailure(cause: Throwable, correlationId: Int)

  /**
   * Published by event logs to the actor system's event stream whenever new events have been written,
   * either by replication or by event-sourced actors.
   *
   * @param events Written events.
   */
  case class Updated(events: Seq[DurableEvent])
}
