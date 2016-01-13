/*
 * Copyright (C) 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
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

package com.rbmhtechnology.eventuate.log.cassandra

import akka.actor._

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.log.EventLogClock

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.util._

private[eventuate] class CassandraIndex(cassandra: Cassandra, eventLogStore: CassandraEventLogStore, indexStore: CassandraIndexStore, logId: String) extends Actor with Stash with ActorLogging {
  import CassandraIndex._
  import context.dispatcher

  private val scheduler = context.system.scheduler
  private val eventLog = context.parent

  private val indexUpdater = context.actorOf(Props(new CassandraIndexUpdater(cassandra, eventLogStore, indexStore)))

  /**
   * Contains the sequence number of the last event in event log that
   * has been successfully processed and written to the index.
   */
  private var clock: EventLogClock = EventLogClock()

  def instantiated: Receive = {
    case InitIndex =>
      indexStore.readEventLogClockAsync onComplete {
        case Success(t) => self ! ReadClockSuccess(t)
        case Failure(e) => self ! ReadClockFailure(e)
      }
      context.become(initializing(sender()))
  }

  def initializing(sdr: ActorRef): Receive = {
    case ReadClockSuccess(t) =>
      indexUpdater ! UpdateIndex(t, Long.MaxValue)
    case u @ UpdateIndexSuccess(t, _) =>
      clock = t
      eventLog ! u
      sdr ! InitIndexSuccess(t)
      context.become(initialized)
    case u @ UpdateIndexFailure(cause) =>
      log.error(cause, "UpdateIndex failure")
      eventLog ! u
      sdr ! InitIndexFailure(cause)
      context.become(instantiated)
    case r @ ReadClockFailure(cause) =>
      log.error(cause, "ReadClock failure")
      eventLog ! r
      sdr ! InitIndexFailure(cause)
      context.become(instantiated)
  }

  def initialized: Receive = {
    case UpdateIndex(_, toSequenceNr) =>
      indexUpdater ! UpdateIndex(clock, toSequenceNr)
    case u @ UpdateIndexSuccess(t, _) =>
      clock = t
      eventLog ! u
    case u @ UpdateIndexFailure(cause) =>
      log.error(cause, "UpdateIndex failure")
      eventLog ! u
  }

  def receive =
    instantiated
}

private[eventuate] object CassandraIndex {
  case object InitIndex
  case class InitIndexSuccess(clock: EventLogClock)
  case class InitIndexFailure(cause: Throwable)

  case object ReadClock
  case class ReadClockSuccess(clock: EventLogClock)
  case class ReadClockFailure(cause: Throwable)

  case class UpdateIndex(clock: EventLogClock, toSequenceNr: Long)
  case class UpdateIndexProgress(increment: IndexIncrement)
  case class UpdateIndexSuccess(clock: EventLogClock, steps: Int = 0)
  case class UpdateIndexFailure(cause: Throwable)

  case class AggregateEvents(events: Map[String, Vector[DurableEvent]] = Map.empty) {
    def update(event: DurableEvent): AggregateEvents =
      if (event.destinationAggregateIds.isEmpty) this else copy(event.destinationAggregateIds.foldLeft(events) {
        case (acc, dst) => acc.get(dst) match {
          case Some(events) => acc + (dst -> (events :+ event))
          case None         => acc + (dst -> Vector(event))
        }
      })
  }

  case class IndexIncrement(aggregateEvents: AggregateEvents, clock: EventLogClock) {
    def update(events: Seq[DurableEvent]): IndexIncrement =
      events.foldLeft(this) { case (acc, event) => acc.update(event) }

    def update(event: DurableEvent): IndexIncrement =
      copy(aggregateEvents.update(event), clock.update(event))

    def clearAggregateEvents: IndexIncrement = {
      copy(AggregateEvents(), clock)
    }
  }

  def props(cassandra: Cassandra, eventLogStore: CassandraEventLogStore, indexStore: CassandraIndexStore, logId: String): Props =
    Props(new CassandraIndex(cassandra, eventLogStore, indexStore, logId))
}

private class CassandraIndexUpdater(cassandra: Cassandra, eventLogStore: CassandraEventLogStore, indexStore: CassandraIndexStore) extends Actor {
  import CassandraIndex._
  import context.dispatcher

  val index = context.parent

  val idle: Receive = {
    case UpdateIndex(clock, toSequenceNr) =>
      update(clock.sequenceNr + 1L, toSequenceNr, IndexIncrement(AggregateEvents(), clock))
      context.become(updating(0, toSequenceNr))
  }

  def updating(steps: Int, toSequenceNr: Long): Receive = {
    case UpdateIndexFailure(err) =>
      index ! UpdateIndexFailure(err)
      context.become(idle)
    case UpdateIndexSuccess(t, _) =>
      index ! UpdateIndexSuccess(t, steps)
      context.become(idle)
    case UpdateIndexProgress(inc) =>
      update(inc.clock.sequenceNr + 1L, toSequenceNr, inc.clearAggregateEvents)
      context.become(updating(steps + 1, toSequenceNr))
  }

  def receive = idle

  def update(fromSequenceNr: Long, toSequenceNr: Long, increment: IndexIncrement): Unit =
    updateAsync(fromSequenceNr, toSequenceNr, increment) onComplete {
      case Success((inc, true))  => self ! UpdateIndexProgress(inc)
      case Success((inc, false)) => self ! UpdateIndexSuccess(inc.clock)
      case Failure(err)          => self ! UpdateIndexFailure(err)
    }

  def updateAsync(fromSequenceNr: Long, toSequenceNr: Long, increment: IndexIncrement): Future[(IndexIncrement, Boolean)] =
    for {
      res <- eventLogStore.readAsync(fromSequenceNr, toSequenceNr, cassandra.settings.indexUpdateLimit, cassandra.settings.indexUpdateLimit + 1)
      inc <- writeAsync(increment.update(res.events))
    } yield (inc, res.events.nonEmpty)

  def writeAsync(increment: IndexIncrement): Future[IndexIncrement] =
    indexStore.writeAsync(increment.aggregateEvents, increment.clock).map(_ => increment)
}
