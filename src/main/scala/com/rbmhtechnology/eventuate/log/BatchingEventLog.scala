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

import akka.actor._

import com.rbmhtechnology.eventuate.EventsourcingProtocol
import com.rbmhtechnology.eventuate.EventsourcingProtocol._

/**
 * An event log wrapper that batches [[EventsourcingProtocol.Write]] commands. Batched write commands are
 * sent as [[EventsourcingProtocol.WriteN]] batch to the wrapped event log.
 *
 * Batch sizes dynamically increase up to a configurable maximum under increasing load. The maximum batch
 * size can be configured with `log.write-batch-size-max`. If there is no current write operation in progress,
 * a new `Write` command is served immediately (as `WriteN` batch of size 1), keeping latency at a minimum.
 *
 * @param eventLogProps configuration object of the wrapped event log actor. The wrapped event log actor is
 *                      created as child actor of this wrapper.
 */
class BatchingEventLog(eventLogProps: Props) extends Actor {
  val batchSizeLimit = context.system.settings.config.getInt("eventuate.log.write.batch-size-max")
  val eventLog = context.actorOf(eventLogProps)

  var batch: Vector[Write] = Vector.empty

  val idle: Receive = {
    case w: Write =>
      batch = batch :+ w
      writeBatch()
      context.become(writing)
    case cmd =>
      eventLog forward cmd
  }

  val writing: Receive = {
    //
    // TODO: consider using a receive timeout here
    //
    case w: Write =>
      batch = batch :+ w
    case WriteNComplete if batch.isEmpty =>
      context.become(idle)
    case WriteNComplete =>
      writeBatch()
    case r: Replay =>
      writeAll() // ensures that Replay commands are properly ordered relative to Write commands
      eventLog forward r
      context.become(idle)
    case cmd =>
      eventLog forward cmd
  }

  def receive = idle

  @annotation.tailrec
  private def writeAll(): Unit =
    if (writeBatch()) writeAll()

  private def writeBatch(): Boolean = if (batch.size > 0) {
    var num = 0
    val (w, r) = batch.span { w =>
      num += w.events.size
      num <= batchSizeLimit || num == w.events.size
    }
    eventLog ! WriteN(w)
    batch = r
    batch.nonEmpty
  } else false
}
