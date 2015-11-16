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
import com.rbmhtechnology.eventuate.ReplicationProtocol
import com.rbmhtechnology.eventuate.ReplicationProtocol._

import com.typesafe.config.Config

private[eventuate] class BatchingSettings(config: Config) {
  val batchSizeLimit = config.getInt("eventuate.log.batching.batch-size-limit")
}

/**
 * An event log wrapper that batches write commands. Batched [[EventsourcingProtocol.Write]] commands are sent as
 * [[EventsourcingProtocol.WriteN]] batch to the wrapped event log. Batched [[ReplicationProtocol.ReplicationWrite]]
 * commands are sent as [[ReplicationProtocol.ReplicationWriteN]] batch to the wrapped event event log.
 *
 * Batch sizes dynamically increase to a configurable limit under increasing load. The batch size limit can be
 * configured with `eventuate.log.batch-size-limit`. If there is no current write operation in progress, a new
 * `Write` or `ReplicationWrite` command is served immediately (as `WriteN` or `ReplicationWriteN` batch of size
 * 1, respectively), keeping latency at a minimum.
 *
 * @param eventLogProps configuration object of the wrapped event log actor. The wrapped event log actor is
 *                      created as child actor of this wrapper.
 */
class BatchingEventLog(eventLogProps: Props) extends Actor {
  val eventLog: ActorRef =
    context.actorOf(eventLogProps)
  
  val emissionBatcher: ActorRef =
    context.actorOf(Props(new EmissionBatcher(eventLog)))

  val replicationBatcher: ActorRef =
    context.actorOf(Props(new ReplicationBatcher(eventLog)))

  def receive = {
    case r: ReplicationWrite =>
      replicationBatcher forward r.copy(initiator = sender())
    case cmd =>
      emissionBatcher forward cmd
  }
}

private class EmissionBatcher(eventLog: ActorRef) extends Actor {
  val settings = new BatchingSettings(context.system.settings.config)
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

  private def writeBatch(): Boolean = if (batch.nonEmpty) {
    var num = 0
    val (w, r) = batch.span { w =>
      num += w.events.size
      num <= settings.batchSizeLimit || num == w.events.size
    }
    eventLog ! WriteN(w)
    batch = r
    batch.nonEmpty
  } else false
} 

private class ReplicationBatcher(eventLog: ActorRef) extends Actor {
  val settings = new BatchingSettings(context.system.settings.config)
  var batch: Vector[ReplicationWrite] = Vector.empty

  val idle: Receive = {
    case w: ReplicationWrite =>
      batch = batch :+ w.copy(initiator = sender())
      writeBatch()
      context.become(writing)
  }

  val writing: Receive = {
    case w: ReplicationWrite =>
      batch = batch :+ w.copy(initiator = sender())
    case ReplicationWriteNComplete if batch.isEmpty =>
      context.become(idle)
    case ReplicationWriteNComplete =>
      writeBatch()
  }

  def receive = idle

  private def writeBatch(): Boolean = if (batch.nonEmpty) {
    var num = 0
    val (w, r) = batch.span { w =>
      num += w.events.size
      num <= settings.batchSizeLimit || num == w.events.size
    }
    eventLog ! ReplicationWriteN(w)
    batch = r
    batch.nonEmpty
  } else false
}
