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
 * Provider API.
 *
 * Snapshot storage format. [[EventsourcedActor]]s and [[EventsourcedView]]s can save snapshots of
 * internal state by calling [[EventsourcedActor#save]] or [[EventsourcedView#save]], respectively.
 *
 * @param payload Application-specific snapshot.
 * @param emitterId Id of the [[EventsourcedActor]] or [[EventsourcedView]] that saved the snapshot.
 * @param lastEvent Last handled event before the snapshot was saved.
 * @param currentTime Current vector time when the snapshot was saved.
 * @param deliveryAttempts Unconfirmed delivery attempts when the snapshot was saved (can only be
 *                         non-empty if the actor implements [[ConfirmedDelivery]]).
 */
case class Snapshot(
    payload: Any,
    emitterId: String,
    lastEvent: DurableEvent,
    currentTime: VectorTime,
    deliveryAttempts: Vector[DeliveryAttempt] = Vector.empty) {

  val metadata: SnapshotMetadata =
    SnapshotMetadata(emitterId, lastEvent.sequenceNr)

  def add(deliveryAttempt: DeliveryAttempt): Snapshot =
    copy(deliveryAttempts = deliveryAttempts :+ deliveryAttempt)
}
