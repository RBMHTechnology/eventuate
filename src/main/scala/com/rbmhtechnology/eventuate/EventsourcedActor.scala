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
 * An `EventsourcedActor` is an [[EventsourcedView]] that can also write new events to its event log.
 * New events are written with the asynchronous [[persist]] and [[persistN]] methods. They must only
 * be used within the `onCommand` command handler. After successful persistence, the `onEvent` handler
 * is automatically called with the persisted event(s). The `onEvent` handler is the place where actor
 * state may be updated. The `onCommand` handler should not update actor state but only read it e.g.
 * for command validation. `EventsourcedActor`s that want to persist new events within the `onEvent`
 * handler should additionally mixin the [[PersistOnEvent]] trait and use the methods
 * [[PersistOnEvent.persistOnEvent persistOnEvent]] and
 * [[PersistOnEvent.persistOnEventN persistOnEventN]].
 *
 * @see [[EventsourcedView]]
 * @see [[PersistOnEvent]]
 */
trait EventsourcedActor extends EventsourcedView with EventsourcedClock {
  import EventsourcingProtocol._

  private var writeRequests: Vector[DurableEvent] = Vector.empty
  private var writeHandlers: Vector[Handler[Any]] = Vector.empty
  private var writing: Boolean = false

  /**
   * State synchronization. If set to `true`, commands see internal state that is consistent
   * with the event log. This is achieved by stashing new commands if this actor is currently
   * writing events. If set to `false`, commands see internal state that is eventually
   * consistent with the event log.
   */
  //#state-sync
  def stateSync: Boolean = true
  //#

  /**
   * Asynchronously persists a sequence of `events` and calls `handler` with the persist result
   * for each event in the sequence. If persistence was successful, `onEvent` is called with a
   * persisted event before `handler` is called. Both, `onEvent` and `handler`, are called on a
   * dispatcher thread of this actor, hence, it is safe to modify internal state within them.
   * The `handler` can also obtain a reference to the initial command sender via `sender()`. The
   * `onLast` handler is additionally called for the last event in the sequence.
   *
   * By default, the event is routed to event-sourced destinations with an undefined `aggregateId`.
   * If this actor's `aggregateId` is defined it is additionally routed to all actors with the same
   * `aggregateId`. Further routing destinations can be defined with the `customDestinationAggregateIds`
   * parameter.
   */
  final def persistN[A](events: Seq[A], onLast: Handler[A] = (_: Try[A]) => (), customDestinationAggregateIds: Set[String] = Set())(handler: Handler[A]): Unit = events match {
    case Seq() =>
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
   * Asynchronously persists the given `event` and calls `handler` with the persist result. If
   * persistence was successful, `onEvent` is called with the persisted event before `handler`
   * is called. Both, `onEvent` and `handler`, are called on a dispatcher thread of this actor,
   * hence, it is safe to modify internal state within them. The `handler` can also obtain a
   * reference to the initial command sender via `sender()`.
   *
   * By default, the event is routed to event-sourced destinations with an undefined `aggregateId`.
   * If this actor's `aggregateId` is defined it is additionally routed to all actors with the same
   * `aggregateId`. Further routing destinations can be defined with the `customDestinationAggregateIds`
   * parameter.
   */
  final def persist[A](event: A, customDestinationAggregateIds: Set[String] = Set())(handler: Handler[A]): Unit = {
    writeRequests = writeRequests :+ durableEvent(event, customDestinationAggregateIds)
    writeHandlers = writeHandlers :+ handler.asInstanceOf[Handler[Any]]
  }

  /**
   * Internal API.
   */
  override private[eventuate] def unhandledMessage(msg: Any): Unit = msg match {
    case WriteSuccess(event, iid) => if (iid == instanceId) {
      receiveEvent(event)
      writeHandlers.head(Success(event.payload))
      writeHandlers = writeHandlers.tail
      if (stateSync && writeHandlers.isEmpty) {
        writing = false
        messageStash.unstash()
      }
    }
    case WriteFailure(event, cause, iid) => if (iid == instanceId) {
      receiveEventInternal(event, cause)
      writeHandlers.head(Failure(cause))
      writeHandlers = writeHandlers.tail
      if (stateSync && writeHandlers.isEmpty) {
        writing = false
        messageStash.unstash()
      }
    }
    case PersistOnEventRequest(deliveryId: String, parameters, handlers, iid) => if (iid == instanceId) {
      writeOrDelay {
        writeHandlers = handlers
        writeRequests = parameters.map {
          case PersistOnEventParameters(event, customDestinationAggregateIds) =>
            durableEvent(event, customDestinationAggregateIds, deliveryId)
        }
      }
    }
    case cmd =>
      writeOrDelay(super.unhandledMessage(cmd))
  }

  private def writeOrDelay(writeRequestProducer: => Unit): Unit = {
    if (writing) messageStash.stash() else {
      writeRequestProducer

      val wPending = writePending
      if (wPending) write()
      if (wPending && stateSync) writing = true else if (stateSync) messageStash.unstash()
    }
  }

  private def write(): Unit = {
    eventLog ! Write(writeRequests, sender(), self, instanceId)
    writeRequests = Vector.empty
  }

  private def writePending: Boolean =
    writeRequests.nonEmpty
}

/**
 * Java API.
 *
 * @see [[EventsourcedActor]]
 */
abstract class AbstractEventsourcedActor(id: String, eventLog: ActorRef) extends AbstractEventsourcedView(id, eventLog) with EventsourcedActor with ConfirmedDelivery {
  /**
   * Asynchronously persists the given `event` and calls `handler` with the persist result. If
   * persistence was successful, `onEvent` is called with the persisted event before `handler`
   * is called. Both, `onReceiveEvent` and `handler`, are called on a dispatcher thread of this
   * actor, hence, it is safe to modify internal state within them. The `handler` can also obtain
   * a reference to the initial command sender via `sender()`.
   */
  def persist[A](event: A, handler: BiConsumer[A, Throwable]): Unit = persist[A](event) {
    case Success(a) => handler.accept(a, null)
    case Failure(e) => handler.accept(null.asInstanceOf[A], e)
  }
}
