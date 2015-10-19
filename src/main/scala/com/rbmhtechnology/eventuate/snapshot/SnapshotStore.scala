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

package com.rbmhtechnology.eventuate.snapshot

import com.rbmhtechnology.eventuate.Snapshot

import scala.concurrent.Future

/**
 * Snapshot store provider interface.
 */
trait SnapshotStore {
  /**
   * Asynchronously deletes all snapshots with a sequence number greater than or equal `lowerSequenceNr`.
   */
  def deleteAsync(lowerSequenceNr: Long): Future[Unit]

  /**
   * Asynchronously saves the given `snapshot`.
   */
  def saveAsync(snapshot: Snapshot): Future[Unit]

  /**
   * Asynchronously loads the latest snapshot saved by an event-sourced actor or view identified by `emitterId`.
   */
  def loadAsync(emitterId: String): Future[Option[Snapshot]]
}
