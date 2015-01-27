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

import DurableEvent._

case class DurableEvent(
  payload: Any,
  timestamp: VectorTime,
  processId: String,
  sourceLogId: String = UndefinedLogId,
  targetLogId: String = UndefinedLogId,
  sourceLogSequenceNr: Long = UndefinedSequenceNr,
  targetLogSequenceNr: Long = UndefinedSequenceNr) {

  def sequenceNr: Long =
    targetLogSequenceNr

  def local(processId: String): Boolean =
    this.processId == processId
}

object DurableEvent {
  val UndefinedLogId = ""
  val UndefinedSequenceNr = 1L
}
