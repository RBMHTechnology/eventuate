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
 * @param sequenceNr The latest event sequence number covered by the snapshot.
 * @param systemTimestamp The latest event system timestamp covered by the snapshot.
 * @param vectorTimestamp The latest event vector timestamp covered by the snapshot.
 *
 * @see [[EventsourcedView#lastSequenceNr]]
 * @see [[EventsourcedView#lastSystemTimestamp]]
 * @see [[EventsourcedView#lastVectorTimestamp]]
 */
case class SnapshotMetadata(emitterId: String, sequenceNr: Long, systemTimestamp: Long, vectorTimestamp: VectorTime)

/**
 * Represents a snapshot of internal state of an [[EventsourcedActor]] or [[EventsourcedView]].
 *
 * @param metadata Snapshot metadata.
 * @param deliveryAttempts Unconfirmed delivery attempts of an [[EventsourcedActor]] that implements
 *                         [[ConfirmedDelivery]].
 * @param payload The actual user-defined snapshot passed as argument to [[EventsourcedActor#save]]
 *                or [[EventsourcedView#save]].
 */
case class Snapshot(metadata: SnapshotMetadata, deliveryAttempts: Vector[DeliveryAttempt] = Vector.empty, payload: Any) {
  def add(deliveryAttempt: DeliveryAttempt): Snapshot = copy(deliveryAttempts = deliveryAttempts :+ deliveryAttempt)
}