/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
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
