/*
 * Copyright 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.eventuate.log

import akka.actor._

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationProtocol._

import com.typesafe.config.Config

import scala.collection.immutable.Seq

private class BatchingSettings(config: Config) {
  val writeBatchSize = config.getInt("eventuate.log.write-batch-size")
}

/**
 * An event log wrapper that batches write commands. Batched [[EventsourcingProtocol.Write]] commands are sent as
 * [[EventsourcingProtocol.WriteN]] batch to the wrapped event log. Batched [[ReplicationProtocol.ReplicationWrite]]
 * commands are sent as [[ReplicationProtocol.ReplicationWriteN]] batch to the wrapped event event log.
 *
 * Batch sizes dynamically increase to a configurable limit under increasing load. The batch size limit can be
 * configured with `eventuate.log.write-batch-size`. If there is no current write operation in progress, a new
 * `Write` or `ReplicationWrite` command is served immediately (as `WriteN` or `ReplicationWriteN` batch of size
 * 1, respectively), keeping latency at a minimum.
 *
 * @param logProps configuration object of the wrapped event log actor. The wrapped event log actor is
 *                 created as child actor of this wrapper.
 */
class BatchingLayer(logProps: Props) extends Actor {
  import CircuitBreaker._

  val eventLog: ActorRef =
    context.watch(context.actorOf(logProps))

  val defaultBatcher: ActorRef =
    context.actorOf(Props(new DefaultBatcher(eventLog)))

  val replicationBatcher: ActorRef =
    context.actorOf(Props(new ReplicationBatcher(eventLog)))

  def receive = {
    case r: ReplicationWrite =>
      replicationBatcher forward r
    case r: ReplicationRead =>
      replicationBatcher forward r
    case e: ServiceEvent =>
      context.parent ! e
    case Terminated(_) =>
      context.stop(self)
    case cmd =>
      defaultBatcher forward cmd
  }
}

private trait Batcher[A <: DurableEventBatch] extends Actor {
  val settings = new BatchingSettings(context.system.settings.config)
  var batch: Vector[A] = Vector.empty

  def eventLog: ActorRef
  def writeRequest(batches: Seq[A]): Any
  def idle: Receive

  def receive = idle

  @annotation.tailrec
  final def writeAll(): Unit =
    if (writeBatch()) writeAll()

  final def writeBatch(): Boolean = if (batch.nonEmpty) {
    var num = 0
    val (w, r) = batch.span { w =>
      num += w.size
      num <= settings.writeBatchSize || num == w.size
    }
    eventLog ! writeRequest(w)
    batch = r
    batch.nonEmpty
  } else false

  override def unhandled(message: Any): Unit =
    eventLog forward message
}

private class DefaultBatcher(val eventLog: ActorRef) extends Batcher[Write] {
  val idle: Receive = {
    case w: Write =>
      batch = batch :+ w.withReplyToDefault(sender())
      writeBatch()
      context.become(writing)
  }

  val writing: Receive = {
    case w: Write =>
      batch = batch :+ w.withReplyToDefault(sender())
    case WriteNComplete if batch.isEmpty =>
      context.become(idle)
    case WriteNComplete =>
      writeBatch()
    case r: Replay =>
      // ----------------------------------------------------------------------------
      // Ensures that Replay commands are properly ordered relative to Write commands
      // TODO: only force a writeAll() for replays that come from EventsourcedActors
      // ----------------------------------------------------------------------------
      writeAll()
      eventLog forward r
      context.become(idle)
  }

  def writeRequest(batches: Seq[Write]) =
    WriteN(batches)
}

private class ReplicationBatcher(val eventLog: ActorRef) extends Batcher[ReplicationWrite] {
  val idle: Receive = {
    case w: ReplicationWrite =>
      batch = batch :+ w.withReplyToDefault(sender())
      writeBatch()
      context.become(writing)
  }

  val writing: Receive = {
    case w: ReplicationWrite =>
      batch = batch :+ w.withReplyToDefault(sender())
    case ReplicationWriteNComplete if batch.isEmpty =>
      context.become(idle)
    case ReplicationWriteNComplete =>
      writeBatch()
  }

  def writeRequest(batches: Seq[ReplicationWrite]) =
    ReplicationWriteN(batches)
}
