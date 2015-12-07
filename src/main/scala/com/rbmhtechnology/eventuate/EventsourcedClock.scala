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

/**
 * Manages a [[VectorClock]] and implements rules for updating the clock from [[DurableEvent]]s.
 */
trait EventsourcedClock extends EventsourcedView {
  private var _clock: VectorClock = _

  /**
   * If `true`, this actor shares a vector clock entry with those actors on the same local `eventLog`
   * that have set `sharedClockEntry` to `true` as well. Otherwise, this actor has its own entry in
   * the vector clock.
   */
  def sharedClockEntry: Boolean =
    true

  /**
   * Internal API.
   */
  override private[eventuate] def currentVectorTime: VectorTime =
    _clock.currentTime

  /**
   * Internal API.
   */
  private[eventuate] def updateLocalTime(): VectorTime = {
    _clock = _clock.tick()
    _clock.currentTime
  }

  /**
   * Updates the vector clock from the given `event`.
   */
  private def updateVectorTime(event: DurableEvent): Unit = {
    if (sharedClockEntry) {
      // set local clock to local time (= sequence number) of event log
      _clock = _clock.set(event.localLogId, event.localSequenceNr)
      if (event.emitterId != id)
        // merge clock with non-self-emitted event timestamp
        _clock = _clock.merge(event.vectorTimestamp)
    } else {
      if (event.emitterId != id)
        // update clock with non-self-emitted event timestamp (incl. increment of local time)
        _clock = _clock.update(event.vectorTimestamp)
      else if (recovering)
        // merge clock with self-emitted event timestamp only during recovery
        _clock = _clock.merge(event.vectorTimestamp)
    }
  }

  /**
   * Internal API.
   */
  private[eventuate] def durableEvent(payload: Any, customDestinationAggregateIds: Set[String]): DurableEvent = {
    if (sharedClockEntry) {
      DurableEvent(
        payload = payload,
        emitterId = id,
        emitterAggregateId = aggregateId,
        customDestinationAggregateIds = customDestinationAggregateIds,
        vectorTimestamp = currentVectorTime,
        processId = DurableEvent.UndefinedLogId)
    } else {
      DurableEvent(
        payload = payload,
        emitterId = id,
        emitterAggregateId = aggregateId,
        customDestinationAggregateIds = customDestinationAggregateIds,
        vectorTimestamp = updateLocalTime(),
        processId = id)
    }
  }

  /**
   * Internal API.
   */
  override private[eventuate] def snapshotLoaded(snapshot: Snapshot): Unit = {
    super.snapshotLoaded(snapshot)
    _clock = _clock.copy(currentTime = snapshot.currentTime)
  }

  /**
   * Internal API.
   */
  override private[eventuate] def receiveEventInternal(event: DurableEvent): Unit = {
    super.receiveEventInternal(event)
    updateVectorTime(event)
  }

  /**
   * Initializes the vector clock and sets the clock's `processId` to the log `id`.
   */
  override def preStart(): Unit = {
    _clock = VectorClock(id)
    super.preStart()
  }
}
