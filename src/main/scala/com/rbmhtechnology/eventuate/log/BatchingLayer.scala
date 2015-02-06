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

import com.rbmhtechnology.eventuate.EventLogProtocol._

class BatchingLayer(eventLogProps: Props) extends Actor {
  val batchSizeLimit = context.system.settings.config.getInt("log.write-batch-size")
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
    case d: Delay =>
      writeAll() // ensures that Delay commands are properly ordered relative to Write commands
      eventLog forward d
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
      num <= batchSizeLimit
    }
    eventLog ! WriteN(w)
    batch = r
    batch.nonEmpty
  } else false
}
