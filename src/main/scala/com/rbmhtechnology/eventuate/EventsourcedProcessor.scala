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

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import com.typesafe.config.Config

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration._

private class EventsourcedProcessorSettings(config: Config) {
  val readTimeout =
    config.getDuration("eventuate.processor.read-timeout", TimeUnit.MILLISECONDS).millis

  val writeTimeout =
    config.getDuration("eventuate.processor.write-timeout", TimeUnit.MILLISECONDS).millis
}

object EventsourcedProcessor {
  /**
   * Type of an [[EventsourcedProcessor]]'s event handler.
   */
  //#process
  type Process = PartialFunction[Any, Seq[Any]]
  //#
}

/**
 * An [[EventsourcedWriter]] that writes processed events to a `targetEventLog`. `EventsourcedProcessor`
 * is an idempotent writer that guarantees that no duplicates are ever written to the target event log,
 * also under failure conditions. Hence, applications don't need to take extra care about idempotency.
 * Processed events are returned by `processEvent`, an application-defined event handler that is called
 * with events stored in the source `eventLog`.
 *
 * The `processEvent` event handler may also update actor state. When a processor (re)starts, its state is
 * automatically recovered by replaying events, optionally starting from a snapshot. Snapshots of processor
 * state can be saved with the `save` method in the processor's `onCommand` handler. An event-sourced processor
 * that doesn't need to maintain internal state (= stateless processor) should implement [[StatelessProcessor]]
 * instead.
 *
 * Usually, a processor's source event log is different from its target event log. If a processor needs to
 * write processed events back to its source event log, it must reserve its own entry in the vector clock by
 * setting `sharedClockEntry` to `false`.
 *
 * During initialization, a processor reads the processing progress from the target event log. The timeout
 * for this read operation can be configured with the `eventuate.processor.read-timeout` parameter for all
 * event-sourced processors or defined on a per class or instance basis by overriding `readTimeout`. The timeout
 * for write operations to the target log can be configured with the `eventuate.processor.write-timeout` parameter
 * for all event-sourced processors or defined on a per class or instance basis by overriding `writeTimeout`.
 */
trait EventsourcedProcessor extends EventsourcedWriter[Long, Long] {
  import ReplicationProtocol._
  import context.dispatcher

  /**
   * Type of this processor's event handler.
   */
  type Process = EventsourcedProcessor.Process

  private val settings = new EventsourcedProcessorSettings(context.system.settings.config)

  private var processedEvents: Vector[DurableEvent] = Vector.empty
  private var storedSequenceNr: Long = 0L

  /**
   * Internal API.
   */
  override private[eventuate] def trackVectorTime: Boolean = true

  /**
   * This processor's target event log.
   */
  def targetEventLog: ActorRef

  /**
   * This processor's event handler. It may generate zero or more processed events per source event.
   */
  def processEvent: Process

  override final val onEvent: Receive = {
    case payload if processEvent.isDefinedAt(payload) =>
      if (lastSequenceNr > storedSequenceNr)
        processedEvents = processEvent(payload).map(durableEvent(_, Set.empty)).foldLeft(processedEvents)(_ :+ _)
  }

  override final def write(): Future[Long] =
    if (lastSequenceNr > storedSequenceNr) {
      val result = targetEventLog.ask(ReplicationWrite(processedEvents, id, lastSequenceNr, VectorTime()))(Timeout(writeTimeout)).flatMap {
        case ReplicationWriteSuccess(_, progress, _) => Future.successful(progress)
        case ReplicationWriteFailure(cause)          => Future.failed(cause)
      }
      processedEvents = Vector.empty
      result
    } else Future.successful(storedSequenceNr)

  override final def read(): Future[Long] = {
    targetEventLog.ask(GetReplicationProgress(id))(Timeout(readTimeout)).flatMap {
      case GetReplicationProgressSuccess(_, progress, _) => Future.successful(progress)
      case GetReplicationProgressFailure(cause)          => Future.failed(cause)
    }
  }

  override def writeSuccess(progress: Long): Unit = {
    storedSequenceNr = progress
    super.writeSuccess(progress)
  }

  override def readSuccess(progress: Long): Option[Long] = {
    storedSequenceNr = progress
    super.readSuccess(progress)
  }

  /**
   * The default write timeout configured with the `eventuate.processor.write-timeout` parameter.
   * Can be overridden.
   */
  def writeTimeout: FiniteDuration =
    settings.writeTimeout

  /**
   * The default read timeout configured with the `eventuate.processor.read-timeout` parameter.
   * Can be overridden.
   */
  def readTimeout: FiniteDuration =
    settings.readTimeout

  override def preStart(): Unit = {
    if (eventLog == targetEventLog) require(!sharedClockEntry, "A processor writing to the source log must set sharedClockEntry=false")
    super.preStart()
  }
}

/**
 * An [[EventsourcedProcessor]] whose `processEvent` handler does '''not''' update actor state. A
 * [[StatelessProcessor]] is optimized for fast startup and failure recovery times, at the cost of
 * not being able to have its own entry in the vector clock (see overridden method `sharedClockEntry`).
 * Consequently, a stateless processor can not write processed events back to its source event log i.e.
 * the source and the target event log of a stateless processor must be different.
 *
 * @see [[EventsourcedProcessor]].
 */
trait StatelessProcessor extends EventsourcedProcessor {
  /**
   * Always returns `true`. Therefore, a stateless processor cannot have its own entry in the vector clock
   * because its clock can not be recovered during processor (re)start.
   */
  override final def sharedClockEntry: Boolean =
    true

  /**
   * Returns the last successfully processed source log sequence number.
   */
  override final def readSuccess(progress: Long): Option[Long] = {
    super.readSuccess(progress)
    Some(progress + 1L)
  }
}