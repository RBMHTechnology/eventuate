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

/**
 * Maintains the current version of an event-sourced component. The current version is updated by merging
 * [[DurableEvent.vectorTimestamp]]s of handled events.
 */
trait EventsourcedVersion extends EventsourcedView {
  private var _currentVersion: VectorTime = VectorTime.Zero

  /**
   * Internal API.
   */
  override private[eventuate] def currentVersion: VectorTime =
    _currentVersion

  /**
   * Updates the current version from the given `event`.
   */
  private def updateVersion(event: DurableEvent): Unit =
    _currentVersion = _currentVersion.merge(event.vectorTimestamp)

  /**
   * Internal API.
   */
  private[eventuate] def durableEvent(payload: Any, customDestinationAggregateIds: Set[String],
    deliveryId: Option[String] = None, persistOnEventSequenceNr: Option[Long] = None): DurableEvent =
    DurableEvent(
      payload = payload,
      emitterId = id,
      emitterAggregateId = aggregateId,
      customDestinationAggregateIds = customDestinationAggregateIds,
      vectorTimestamp = currentVersion,
      deliveryId = deliveryId,
      persistOnEventSequenceNr = persistOnEventSequenceNr)

  /**
   * Internal API.
   */
  override private[eventuate] def snapshotLoaded(snapshot: Snapshot): Unit = {
    super.snapshotLoaded(snapshot)
    _currentVersion = snapshot.currentTime
  }

  /**
   * Internal API.
   */
  override private[eventuate] def receiveEventInternal(event: DurableEvent): Unit = {
    super.receiveEventInternal(event)
    updateVersion(event)
  }
}
