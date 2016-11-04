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

import scala.collection.immutable.Seq

object EventsourcingProtocol {
  /**
   * Instructs an event log to batch-execute the given `writes`.
   */
  case class WriteN(writes: Seq[Write])

  /**
   * Completion reply after a [[WriteN]].
   */
  case object WriteNComplete

  /**
   * Instructs an event log to write the given `events`.
   */
  case class Write(events: Seq[DurableEvent], initiator: ActorRef, replyTo: ActorRef, correlationId: Int, instanceId: Int) extends UpdateableEventBatch[Write] {
    override def update(events: Seq[DurableEvent]): Write =
      copy(events = events)

    def withReplyToDefault(replyTo: ActorRef): Write =
      if (this.replyTo eq null) copy(replyTo = replyTo) else this
  }

  /**
   * Success reply after a [[Write]].
   */
  case class WriteSuccess(events: Seq[DurableEvent], correlationId: Int, instanceId: Int)

  /**
   * Failure reply after a [[Write]].
   */
  case class WriteFailure(events: Seq[DurableEvent], cause: Throwable, correlationId: Int, instanceId: Int)

  /**
   * Sent by an event log to all registered participants, if `event` has been successfully written.
   * This message is not sent to a participant if that participant has sent a corresponding [[Write]].
   */
  case class Written(event: DurableEvent)

  object Replay {
    def apply(from: Long, max: Int, subscriber: Option[ActorRef], instanceId: Int): Replay =
      new Replay(from, max, subscriber, None, instanceId)

    def apply(from: Long, subscriber: Option[ActorRef], aggregateId: Option[String], instanceId: Int): Replay =
      new Replay(from, 4096, subscriber, aggregateId, instanceId)

    def apply(from: Long, subscriber: Option[ActorRef], instanceId: Int): Replay =
      new Replay(from, 4096, subscriber, None, instanceId)
  }

  /**
   * Instructs an event log to read up to `max` events starting at `fromSequenceNr`. If `aggregateId`
   * is defined, only those events that have the aggregate id in their  `destinationAggregateIds` are
   * returned.
   */
  case class Replay(fromSequenceNr: Long, max: Int, subscriber: Option[ActorRef], aggregateId: Option[String], instanceId: Int)

  /**
   * Success reply after a [[Replay]].
   */
  case class ReplaySuccess(events: Seq[DurableEvent], replayProgress: Long, instanceId: Int) extends DurableEventBatch

  /**
   * Failure reply after a [[Replay]].
   */
  case class ReplayFailure(cause: Throwable, replayProgress: Long, instanceId: Int)

  /**
   * Internal message to trigger a new [[Replay]] attempt
   */
  private[eventuate] case class ReplayRetry(replayProgress: Long)

  /**
   * Instructs an event log to delete events with a sequence nr less or equal a given one.
   * Deleted events are not replayed any more, however depending on the log implementation
   * and `remoteLogIds` they might still be replicated.
   *
   * @param toSequenceNr All events with a less or equal sequence nr are not replayed any more.
   * @param remoteLogIds A set of remote log ids that must have replicated events before they
   *                     are allowed to be physically deleted.
   */
  case class Delete(toSequenceNr: Long, remoteLogIds: Set[String] = Set.empty)

  /**
   * Success reply after a [[Delete]]
   *
   * @param deletedTo The actually written deleted to marker. Minimum of [[Delete.toSequenceNr]] and
   *                  the current sequence nr
   */
  case class DeleteSuccess(deletedTo: Long)

  /**
   * Failure reply after a [[Delete]]
   */
  case class DeleteFailure(cause: Throwable)

  /**
   * Instructs an event log to save the given `snapshot`.
   */
  case class SaveSnapshot(snapshot: Snapshot, initiator: ActorRef, instanceId: Int)

  /**
   * Success reply after a [[SaveSnapshot]].
   */
  case class SaveSnapshotSuccess(metadata: SnapshotMetadata, instanceId: Int)

  /**
   * Failure reply after a [[SaveSnapshot]].
   */
  case class SaveSnapshotFailure(metadata: SnapshotMetadata, cause: Throwable, instanceId: Int)

  /**
   * Instructs an event log to load the most recent snapshot for `emitterId`.
   */
  case class LoadSnapshot(emitterId: String, instanceId: Int)

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
