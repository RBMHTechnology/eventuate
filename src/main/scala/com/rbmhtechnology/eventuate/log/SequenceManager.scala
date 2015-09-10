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

import com.rbmhtechnology.eventuate._

import scala.collection.immutable.Seq

private[eventuate] class SequenceManager(logId: String) {
  import DurableEvent.UndefinedLogId

  private var counter: Long = 0L
  private var updates = 0L

  def setSequenceNr(sequenceNr: Long): Unit =
    counter = sequenceNr

  def advanceSequenceNr(delta: Long): Unit =
    counter = counter + delta

  def currentSequenceNr: Long =
    counter

  def resetSequenceNrUpdates(): Unit =
    updates = 0L

  def advanceSequenceNrUpdates(): Unit =
    updates = updates + 1L

  def currentSequenceNrUpdates: Long =
    updates

  def prepareWrite(events: Seq[DurableEvent], systemTimestamp: Long): Seq[DurableEvent] = {
    events.map { event =>
      advanceSequenceNr(1L)
      advanceSequenceNrUpdates()

      val st = if (event.processId == UndefinedLogId) systemTimestamp else event.systemTimestamp
      val vt = if (event.processId == UndefinedLogId) event.vectorTimestamp.setLocalTime(logId, currentSequenceNr) else event.vectorTimestamp

      event.copy(
        systemTimestamp = st,
        vectorTimestamp = vt,
        processId = logId,
        sourceLogId = logId,
        targetLogId = logId,
        sourceLogSequenceNr = currentSequenceNr,
        targetLogSequenceNr = currentSequenceNr)
    }
  }

  def prepareReplicate(events: Seq[DurableEvent], lastSourceLogSequenceNrRead: Long): Seq[DurableEvent] = {
    events.map { event =>
      advanceSequenceNr(1L)
      advanceSequenceNrUpdates()

      event.copy(
        sourceLogId = event.targetLogId,
        targetLogId = logId,
        sourceLogSequenceNr = event.targetLogSequenceNr,
        targetLogSequenceNr = currentSequenceNr,
        sourceLogReadPosition = lastSourceLogSequenceNrRead)
    }
  }
}
