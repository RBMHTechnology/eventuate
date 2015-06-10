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

import scala.collection.immutable.Seq

package object log {

  private[eventuate] object partition {
    /**
     * Partition number for given `sequenceNr`.
     */
    def partitionOf(sequenceNr: Long, partitionSizeMax: Long): Long =
      if (sequenceNr == 0L) -1L else (sequenceNr - 1L) / partitionSizeMax

    /**
     * Remaining partition size given the current `sequenceNr`.
     */
    def partitionSize(sequenceNr: Long, partitionSizeMax: Long): Long = {
      val m = sequenceNr % partitionSizeMax
      if (m == 0L) m else partitionSizeMax - m
    }

    /**
     * First sequence number of given `partition`.
     */
    def firstSequenceNr(partition: Long, partitionSizeMax: Long): Long =
      partition * partitionSizeMax + 1L
  }

  /**
   * Prepares given emitted `events` to be written to log identified by `logId`
   * using the given `nextSequenceNr` generator.
   */
  def prepareWrite(logId: String, events: Seq[DurableEvent], nextSequenceNr: => Long): Seq[DurableEvent] = {
    events.map { event =>
      val snr = nextSequenceNr
      event.copy(
        sourceLogId = logId,
        targetLogId = logId,
        sourceLogSequenceNr = snr,
        targetLogSequenceNr = snr)
    }
  }

  /**
   * Prepares given replicated `events` to be written to log identified by `logId`
   * using the given `nextSequenceNr` generator.
   */
  def prepareReplicate(logId: String, events: Seq[DurableEvent], lastSourceLogSequenceNrRead: Long, nextSequenceNr: => Long): Seq[DurableEvent] = {
    events.map { event =>
      val snr = nextSequenceNr
      event.copy(
        sourceLogId = event.targetLogId,
        targetLogId = logId,
        sourceLogReadPosition = lastSourceLogSequenceNrRead,
        sourceLogSequenceNr = event.targetLogSequenceNr,
        targetLogSequenceNr = snr)
    }
  }
}
