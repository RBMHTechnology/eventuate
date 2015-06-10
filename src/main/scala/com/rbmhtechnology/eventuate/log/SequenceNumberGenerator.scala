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

package com.rbmhtechnology.eventuate.log

private[eventuate] class SequenceNumberGenerator(partitionSizeMax: Long) {
  import partition._

  private var _sequenceNrUpdates = 0L
  private var _sequenceNr = 0L

  def sequenceNrUpdates: Long =
    _sequenceNrUpdates

  def sequenceNr: Long =
    _sequenceNr

  def sequenceNr_=(snr: Long): Unit =
    _sequenceNr = snr

  def resetSequenceNumberUpdates(): Unit =
    _sequenceNrUpdates = 0L

  def nextSequenceNr(): Long = {
    _sequenceNr += 1L
    _sequenceNrUpdates += 1
    _sequenceNr
  }

  def adjustSequenceNr(batchSize: Long): Long = {
    require(batchSize <= partitionSizeMax, s"write batch size (${batchSize}) must not be greater than maximum partition sitze (${partitionSizeMax})")

    val currentPartition = partitionOf(sequenceNr, partitionSizeMax)
    val remainingPartitionSize = partitionSize(sequenceNr, partitionSizeMax)
    if (remainingPartitionSize < batchSize) {
      _sequenceNr += remainingPartitionSize
      currentPartition + 1L
    } else {
      currentPartition
    }
  }
}
