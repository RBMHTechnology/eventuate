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

import com.rbmhtechnology.eventuate.DurableEvent._

/**
 * ...
 */
trait EventsourcedClock extends EventsourcedView {
  private var _clock: VectorTime = VectorTime.Zero

  /**
   * If `true`, this actor shares a vector clock entry with those actors on the same local `eventLog`
   * that have set `sharedClockEntry` to `true` as well. Otherwise, this actor has its own entry in
   * the vector clock.
   */
  final def sharedClockEntry: Boolean = // TODO: remove and implement #267 instead
    true

  /**
   * Internal API.
   */
  override private[eventuate] def currentVectorTime: VectorTime =
    _clock

  /**
   * Updates the vector clock from the given `event`.
   */
  private def updateVectorTime(event: DurableEvent): Unit =
    _clock = _clock.merge(lastVectorTimestamp)

  /**
   * Internal API.
   */
  private[eventuate] def durableEvent(payload: Any, customDestinationAggregateIds: Set[String],
    deliveryId: Option[String] = None, persistOnEventSequenceNr: Option[Long] = None): DurableEvent = {
    DurableEvent(
      payload = payload,
      emitterId = id,
      emitterAggregateId = aggregateId,
      customDestinationAggregateIds = customDestinationAggregateIds,
      vectorTimestamp = currentVectorTime,
      deliveryId = deliveryId,
      persistOnEventSequenceNr = persistOnEventSequenceNr)
  }

  /**
   * Internal API.
   */
  override private[eventuate] def snapshotLoaded(snapshot: Snapshot): Unit = {
    super.snapshotLoaded(snapshot)
    _clock = snapshot.currentTime
  }

  /**
   * Internal API.
   */
  override private[eventuate] def receiveEventInternal(event: DurableEvent): Unit = {
    super.receiveEventInternal(event)
    updateVectorTime(event)
  }
}
