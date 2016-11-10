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

package com.rbmhtechnology.eventuate.adapter.stream

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern.{ ask, pipe }
import akka.stream._
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage._
import akka.stream.scaladsl.Source
import akka.util.Timeout

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.typesafe.config.Config

import scala.collection.immutable.Seq
import scala.concurrent.duration._

private class DurableEventSourceSettings(config: Config) {
  val replayBatchSize =
    config.getInt("eventuate.log.replay-batch-size")

  val readTimeout =
    config.getDuration("eventuate.log.read-timeout", TimeUnit.MILLISECONDS).millis

  val replayRetryDelay =
    config.getDuration("eventuate.log.replay-retry-delay", TimeUnit.MILLISECONDS).millis
}

/**
 * Stream-based alternative to [[EventsourcedView]].
 */
object DurableEventSource {
  /**
   * Creates an [[http://doc.akka.io/docs/akka/2.4/scala/stream/index.html Akka Streams]] source stage that emits the
   * [[DurableEvent]]s stored in `eventLog`. The source also emits events that have been written to the event log after
   * source creation. Behavior of the source can be configured with:
   *
   *  - `eventuate.log.replay-batch-size`. Maximum number of events to read from the event log when the internal
   *    buffer (of same size) is empty.
   *  - `eventuate.log.read-timeout`. Timeout for reading events from the event log.
   *  - `eventuate.log.replay-retry-delay`. Delay between a failed read and the next read retry.
   *
   * @param eventLog source event log.
   * @param fromSequenceNr sequence number from where the [[DurableEvent]] stream should start.
   * @param aggregateId if defined, emit only [[DurableEvent]]s with that aggregate id, otherwise, emit
   *                    [[DurableEvent]]s with any aggregate id (see also
   *                    [[http://rbmhtechnology.github.io/eventuate/reference/event-sourcing.html#event-routing event routing]]).
   */
  def apply(eventLog: ActorRef, fromSequenceNr: Long = 1L, aggregateId: Option[String] = None): Graph[SourceShape[DurableEvent], ActorRef] =
    Source.actorPublisher[DurableEvent](DurableEventSourceActor.props(eventLog, fromSequenceNr, aggregateId))
}

private object DurableEventSourceActor {
  case object Paused

  def props(eventLog: ActorRef, fromSequenceNr: Long, aggregateId: Option[String]): Props =
    Props(new DurableEventSourceActor(eventLog, fromSequenceNr, aggregateId))
}

private class DurableEventSourceActor(eventLog: ActorRef, fromSequenceNr: Long, aggregateId: Option[String]) extends ActorPublisher[DurableEvent] with ActorLogging {
  import DurableEventSourceActor._

  private val settings = new DurableEventSourceSettings(context.system.settings.config)

  private var buf: Vector[DurableEvent] = Vector.empty
  private var progress: Long = fromSequenceNr - 1L
  private var schedule: Option[Cancellable] = None

  import context.dispatcher

  def reading: Receive = {
    case ReplaySuccess(Seq(), _, _) =>
      schedule = Some(schedulePaused())
      context.become(pausing)
    case ReplaySuccess(events, to, _) =>
      buffer(events, to)
      if (emit()) {
        read()
      } else {
        context.become(waiting)
      }
    case ReplayFailure(cause, _, _) =>
      schedule = Some(schedulePaused())
      context.become(pausing)
      log.warning(s"reading from log failed: $cause")
  }

  def waiting: Receive = {
    case Request(_) if buf.isEmpty =>
      read()
      context.become(reading)
    case Request(_) =>
      if (emit()) {
        read()
        context.become(reading)
      }
  }

  def pausing: Receive = {
    case Paused =>
      schedule = None
      if (totalDemand > 0) {
        read()
        context.become(reading)
      } else {
        context.become(waiting)
      }
    case Written(_) =>
      schedule.foreach(_.cancel())
      schedule = None
      if (totalDemand > 0) {
        read()
        context.become(reading)
      } else {
        context.become(waiting)
      }
  }

  override def unhandled(message: Any): Unit = message match {
    case Cancel | SubscriptionTimeoutExceeded =>
      context.stop(self)
    case Terminated(`eventLog`) =>
      onCompleteThenStop()
    case other =>
      super.unhandled(other)
  }

  def receive = reading

  override def preStart(): Unit = {
    super.preStart()
    context.watch(eventLog)
    read(subscribe = true)
  }

  private def read(subscribe: Boolean = false): Unit = {
    implicit val timeout = Timeout(settings.readTimeout)
    val subscriber = if (subscribe) Some(self) else None
    val fromSequenceNr = progress + 1L

    eventLog ? Replay(fromSequenceNr, settings.replayBatchSize, subscriber, aggregateId, 1) recover {
      case t => ReplayFailure(t, fromSequenceNr, 1)
    } pipeTo self
  }

  private def emit(): Boolean = {
    val splitPos = if (totalDemand > Int.MaxValue) Int.MaxValue else totalDemand.toInt
    val (use, keep) = buf.splitAt(splitPos)

    use.foreach(onNext)
    buf = keep
    buf.isEmpty && totalDemand > 0L
  }

  private def buffer(events: Seq[DurableEvent], to: Long): Unit = {
    buf = events.foldLeft(buf)(_ :+ _)
    progress = to
  }

  private def schedulePaused(): Cancellable =
    context.system.scheduler.scheduleOnce(settings.replayRetryDelay, self, Paused)
}
