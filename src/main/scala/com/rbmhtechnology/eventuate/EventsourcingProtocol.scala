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

object EventsourcingProtocol {
  /**
   * Instructs an event log to batch-execute the given `writes`.
   */
  case class WriteN(writes: Seq[Write]) {

    /**
     * Number of events in this batch write request.
     */
    def size: Int = writes.map(_.events.size).sum
  }

  /**
   * Completion reply after a [[WriteN]].
   */
  case object WriteNComplete

  /**
   * Instructs an event log to write the given `events` and send the written events one-by-one
   * to the given `requestor`. In case of a successful write, events are sent within [[WriteSuccess]]
   * messages, otherwise within [[WriteFailure]] messages with `initiator` as message sender.
   */
  case class Write(events: Seq[DurableEvent], initiator: ActorRef, requestor: ActorRef, instanceId: Int)

  /**
   * Success reply after a [[Write]].
   */
  case class WriteSuccess(event: DurableEvent, instanceId: Int)

  /**
   * Failure reply after a [[Write]].
   */
  case class WriteFailure(event: DurableEvent, cause: Throwable, instanceId: Int)

  /**
   * Sent by an event log to all registered participants, if `event` has been successfully written.
   * This message is not sent to a participant if that participant has sent a corresponding [[Write]].
   */
  case class Written(event: DurableEvent)

  /**
   * Instructs an event log to replay events from sequence number `from` to the given `requestor`.
   * Replayed events are sent within [[Replaying]] messages. If replay successfully completes the
   * event log must additionally send a [[ReplaySuccess]] message, otherwise, a [[ReplayFailure]]
   * message.
   *
   * If `aggregateId` is defined, only events with a matching `aggregateId` are replayed, otherwise,
   * all events.
   */
  case class Replay(from: Long, requestor: ActorRef, aggregateId: Option[String], instanceId: Int)

  object Replay {
    /**
     * Creates a [[Replay]] command where `aggregateId` is not defined.
     */
    def apply(from: Long, requestor: ActorRef, instanceId: Int): Replay =
      new Replay(from, requestor, None, instanceId)
  }

  /**
   * Single `event` replay after a [[Replay]].
   */
  case class Replaying(event: DurableEvent, instanceId: Int)

  /**
   * Success reply after a [[Replay]], sent when all [[Replaying]] messages have been sent.
   */
  case class ReplaySuccess(instanceId: Int)

  /**
   * Failure reply after a [[Replay]].
   */
  case class ReplayFailure(cause: Throwable, instanceId: Int)

  /**
   * Instructs an event log to save the given `snapshot`.
   */
  case class SaveSnapshot(snapshot: Snapshot, initiator: ActorRef, requestor: ActorRef, instanceId: Int)

  /**
   * Success reply after a [[SaveSnapshot]].
   */
  case class SaveSnapshotSuccess(metadata: SnapshotMetadata, instanceId: Int)

  /**
   * Failure reply after a [[SaveSnapshot]].
   */
  case class SaveSnapshotFailure(metadata: SnapshotMetadata, cause: Throwable, instanceId: Int)

  /**
   * Instructs an event log to load the most recent snapshot for `requestor` identified by `emitterId`.
   */
  case class LoadSnapshot(emitterId: String, requestor: ActorRef, instanceId: Int)

  /**
   * Success reply after a [[LoadSnapshot]].
   */
  case class LoadSnapshotSuccess(snapshot: Option[Snapshot], instanceId: Int)

  /**
   * Failure reply after a [[LoadSnapshot]].
   */
  case class LoadSnapshotFailure(cause: Throwable, instanceId: Int)

  /**
   * Instructs an event log to delete all snapshots with a sequence number greater than or equal to `lowerSequenceNr`.
   */
  case class DeleteSnapshots(lowerSequenceNr: Long)

  /**
   * Success reply after a [[DeleteSnapshots]].
   */
  case object DeleteSnapshotsSuccess

  /**
   * Failure reply after a [[DeleteSnapshots]].
   */
  case class DeleteSnapshotsFailure(cause: Throwable)
}
