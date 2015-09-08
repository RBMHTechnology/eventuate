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
 * @param emitterId Id of the [[EventsourcedActor]] or [[EventsourcedView]] that generated this snapshot.
 * @param lastEvent Last event that has been delivered to an actor's event handler.
 * @param lastHandledTime Vector time that covers all handled events up to `lastEvent`.
 * @param deliveryAttempts Unconfirmed delivery attempts of an actor (when implementing [[ConfirmedDelivery]]).
 */
case class Snapshot(
    payload: Any,
    emitterId: String,
    lastEvent: DurableEvent,
    lastHandledTime: VectorTime,
    deliveryAttempts: Vector[DeliveryAttempt] = Vector.empty) {

  val metadata: SnapshotMetadata =
    SnapshotMetadata(emitterId, lastEvent.sequenceNr)

  def add(deliveryAttempt: DeliveryAttempt): Snapshot =
    copy(deliveryAttempts = deliveryAttempts :+ deliveryAttempt)
}
