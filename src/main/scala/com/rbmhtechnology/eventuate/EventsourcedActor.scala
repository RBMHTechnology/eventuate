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

import java.util.function.BiConsumer

import scala.util._

import akka.actor._

/**
 * An `EventsourcedActor` is an [[EventsourcedView]] that can also produce (= emit) new events to its
 * event log. New events can be produced with methods `persist`, `persistN` and `persistWithLocalTime`.
 * They must only be used within the `onCommand` command handler. The command handler may only read
 * internal state but must not modify it. Internal state may only be modified within `onEvent`.
 *
 * An `EventsourcedActor` maintains a [[VectorClock]] used to time-stamp the events it emits
 * to the event log. Events that are handled by its event handler update the vector clock.
 * Events that are pushed from the `eventLog` actor but not handled by `onEvent` do not
 * update the vector clock.
 *
 * @see [[EventsourcedView]]
 */
trait EventsourcedActor extends EventsourcedView {
  import EventsourcingProtocol._
  import EventsourcedActor._

  private var writeRequests: Vector[DurableEvent] = Vector.empty
  private var writeHandlers: Vector[Handler[Any]] = Vector.empty
  private var writing: Boolean = false

  /**
   * Whether to deliver received events in causal order to the event handler. Defaults to `true`
   * and can be overriden by implementations.
   */
  override def causalDelivery: Boolean =
    true

  /**
   * State synchronization. If set to `true`, commands see internal state that is
   * consistent with the event log. This is achieved by stashing new commands if this actor
   * is currently writing events.
   *
   * If set to `false`, commands see internal state that might be stale. To see state updates
   * from any previously persisted events, applications can `delay` these commands. In this mode,
   * commands are not stashed and events can be batched for write which significantly increases
   * write throughput.
   */
  //#state-sync
  def stateSync: Boolean = true
  //#

  /**
   * Delays handling of `command` to that point in the future where all previously called `persist`
   * or `persistN` operations completed. Therefore, using this method only makes sense if `stateSync`
   * is set to `false`. The `handler` is called during a separate message dispatch by this actor,
   * hence, it is safe to access internal state within `handler`.
   *
   * @see [[ConditionalCommand]]
   */
  final def delay[A](command: A)(handler: A => Unit): Unit =
    self ! ConditionalCommand(currentTime, Delayed(command, handler, instanceId))

  /**
   * Asynchronously persists a sequence of `events` and calls `handler` with the persist
   * results. The `handler` is called for each event in the sequence during a separate
   * message dispatch by this actor, hence, it is safe to modify internal state within
   * `handler`. The `handler` can also obtain a reference to the initial command sender
   * via `sender()`. The `onLast` handler is additionally called for the last event in
   * the sequence.
   *
   * By default, the event is routed to event-sourced destinations with an undefined `aggregateId`.
   * If this actor's `aggregateId` is defined it is additionally routed to all actors with the same
   * `aggregateId`. Further routing destinations can be defined with the `customDestinationAggregateIds`
   * parameter.
   */
  final def persistN[A](events: Seq[A], onLast: Handler[A] = (_: Try[A]) => (), customDestinationAggregateIds: Set[String] = Set())(handler: Handler[A]): Unit = events match {
    case Seq()   =>
    case es :+ e =>
      es.foreach { event =>
        persist(event, customDestinationAggregateIds)(handler)
      }
      persist(e, customDestinationAggregateIds) { r =>
        handler(r)
        onLast(r)
      }
  }

  /**
   * Asynchronously persists the given `event` and calls `handler` with the persist result.
   * The `handler` is called during a separate message dispatch by this actor, hence, it is
   * safe to modify internal state within `handler`. The `handler` can also obtain a reference
   * to the initial command sender via `sender()`.
   *
   * By default, the event is routed to event-sourced destinations with an undefined `aggregateId`.
   * If this actor's `aggregateId` is defined it is additionally routed to all actors with the same
   * `aggregateId`. Further routing destinations can be defined with the `customDestinationAggregateIds`
   * parameter.
   */
  final def persist[A](event: A, customDestinationAggregateIds: Set[String] = Set())(handler: Handler[A]): Unit =
    persistWithLocalTime(_ => event, customDestinationAggregateIds)(handler)

  /**
   * Asynchronously persists the event returned by  `f` and calls `handler` with the persist
   * result. The input parameter to `f` is the current local time which is the actor's logical
   * time, taken from its internal vector clock, and not the current system time. The `handler`
   * is called during a separate message dispatch by this actor, hence, it is safe to modify
   * internal state within `handler`. The `handler` can also obtain a reference to the initial
   * command sender via `sender()`.
   *
   * By default, the event is routed to event-sourced destinations with an undefined `aggregateId`.
   * If this actor's `aggregateId` is defined it is additionally routed to all actors with the same
   * `aggregateId`. Further routing destinations can be defined with the `customDestinationAggregateIds`
   * parameter.
   */
  final def persistWithLocalTime[A](f: Long => A, customDestinationAggregateIds: Set[String] = Set())(handler: Handler[A]): A =
    advanceClock { clock =>
      val event = f(clock.currentLocalTime())
      writeRequests = writeRequests :+ DurableEvent(event, System.currentTimeMillis, clock.currentTime, id, aggregateId, customDestinationAggregateIds)
      writeHandlers = writeHandlers :+ handler.asInstanceOf[Try[Any] => Unit]
      event
    }

  /**
   * Internal API.
   */
  override private[eventuate] def unhandledMessage(msg: Any): Unit = msg match {
    case Delayed(command: Any, handler: (Any => Unit), iid) => if (iid == instanceId) {
      handler(command)
    }
    case WriteSuccess(event, iid) => if (iid == instanceId) {
      highestReceivedEvent = event
      lastDeliveredEvent = event
      conditionChanged(lastVectorTimestamp)
      writeHandlers.head(Success(event.payload))
      writeHandlers = writeHandlers.tail
      if (stateSync && writeHandlers.isEmpty) {
        writing = false
        commandStash.unstash()
      }
    }
    case WriteFailure(event, cause, iid) => if (iid == instanceId) {
      highestReceivedEvent = event
      lastDeliveredEvent = event
      writeHandlers.head(Failure(cause))
      writeHandlers = writeHandlers.tail
      if (stateSync && writeHandlers.isEmpty) {
        writing = false
        commandStash.unstash()
      }
    }
    case cmd =>
      if (writing) commandStash.stash() else {
        onCommand(cmd)

        val wPending = writePending
        if (wPending) write()
        if (wPending && stateSync) writing = true else if (stateSync) commandStash.unstash()
      }
  }

  private def writePending: Boolean =
    writeRequests.nonEmpty

  private def write(): Unit = {
    eventLog ! Write(writeRequests, sender(), self, instanceId)
    writeRequests = Vector.empty
  }
}

private[eventuate] object EventsourcedActor {
  /**
   * Wraps a command that has been delayed with `EventsourcedActor.delay`.
   */
  case class Delayed[A](command: A, handler: A => Unit, instanceId: Int)
}

/**
 * Java API.
 *
 * @see [[EventsourcedActor]]
 */
abstract class AbstractEventsourcedActor(id: String, eventLog: ActorRef) extends AbstractEventsourcedView(id, eventLog) with EventsourcedActor with ConfirmedDelivery {
  /**
   * Persists the given `event` and asynchronously calls `handler` with the persist result.
   * The `handler` is called during a separate message dispatch by this actor, hence, it
   * is safe to modify internal state within `handler`.
   */
  def persist[A](event: A, handler: BiConsumer[A, Throwable]): Unit = persist[A](event) {
    case Success(a) => handler.accept(a, null)
    case Failure(e) => handler.accept(null.asInstanceOf[A], e)
  }
}
