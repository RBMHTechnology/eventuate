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

import com.rbmhtechnology.eventuate.ConfirmedDelivery.DeliveryAttempt

/**
 * Snapshot metadata.
 *
 * @param emitterId Id of the [[EventsourcedActor]] or [[EventsourcedView]] that saves the snapshot.
 * @param sequenceNr The highest event sequence number covered by the snapshot.
 */
case class SnapshotMetadata(emitterId: String, sequenceNr: Long)

/**
 * Represents a snapshot of internal state of an [[EventsourcedActor]] or [[EventsourcedView]].
 *
 * @param payload The actual user-defined snapshot passed as argument to [[EventsourcedActor#save]] or [[EventsourcedView#save]].
 * @param emitterId Id of the actor that saves the snapshot.
 * @param highestReceivedEvent Event with the highest sequence number that has ever been received by an actor (delivered or not).
 * @param lastDeliveredEvent Last event that has actually been delivered to an actor's event handler.
 * @param deliveryAttempts Unconfirmed delivery attempts of an actor (when implementing [[ConfirmedDelivery]]).
 * @param stashedEvents Events on the internal event stash of an actor.
 * @param timestamp Vector time when the snapshot has been taken.
 */
case class Snapshot(
    payload: Any,
    emitterId: String,
    highestReceivedEvent: DurableEvent,
    lastDeliveredEvent: DurableEvent,
    stashedEvents: Vector[DurableEvent] = Vector.empty,
    deliveryAttempts: Vector[DeliveryAttempt] = Vector.empty,
    timestamp: VectorTime = VectorTime()) {

  def metadata: SnapshotMetadata =
    SnapshotMetadata(emitterId, highestReceivedEvent.sequenceNr)

  def add(deliveryAttempt: DeliveryAttempt): Snapshot =
    copy(deliveryAttempts = deliveryAttempts :+ deliveryAttempt)

  def add(stashedEvent: DurableEvent): Snapshot =
    copy(stashedEvents = stashedEvents :+ stashedEvent)
}

object Snapshot {
  def apply(payload: Any, emitterId: String): Snapshot =
    new Snapshot(payload, emitterId, DurableEvent(emitterId), DurableEvent(emitterId))
}