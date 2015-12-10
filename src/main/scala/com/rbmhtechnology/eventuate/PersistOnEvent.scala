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

import com.rbmhtechnology.eventuate.DurableEvent.UndefinedDeliveryId
import com.rbmhtechnology.eventuate.EventsourcedView.Handler

import scala.util.Try

private case class PersistOnEventParameters(event: Any, customDestinationAggregateIds: Set[String])
private case class PersistOnEventRequest(deliveryId: String, parameters: Vector[PersistOnEventParameters], handlers: Vector[Handler[Any]], instanceId: Int)

/**
 * Can be mixed in to [[EventsourcedActor]]s for writing new events within the `onEvent` handler.
 * New events are written with the asynchronous [[persistOnEvent]] and [[persistOnEventN]] methods. In contrast
 * to [[EventsourcedActor.persist persist]] and [[EventsourcedActor.persist persistN]], one can '''not''' prevent
 * them from running concurrently to command processing by setting [[EventsourcedActor.stateSync stateSync]]
 * to `true`.
 *
 * Persistence operations executed by `persistOnEvent` and `persistOnEventN` are reliable and idempotent.
 * Once successfully completed, they have no effect on event replay during actor (re)starts. If
 * writing fails, they are automatically retried on next actor (re)start. Applications that want to
 * retry writing earlier should call the [[ConfirmedDelivery.redeliverUnconfirmed redeliverUnconfirmed]]
 * method (for example, scheduled with an application-defined delay).
 *
 * An `EventsourcedActor` that has a `PersistOnEvent` mixin is to some extend comparable to a [[StatefulProcessor]]
 * configured to write events to its source event log. Both write new events on receiving events, however, a
 * `StatefulProcessor` writing new events to its source event log requires its own entry in the vector clock,
 * whereas `PersistOnEvent` does not. Furthermore, a `StatefulProcessor` can not write new events during command
 * processing. As a general rule, when operating on a single event log, applications should prefer
 * `PersistOnEvent` over `StatefulProcessor`.
 */
trait PersistOnEvent extends ConfirmedDelivery {
  private var parameters: Vector[PersistOnEventParameters] = Vector.empty
  private var handlers: Vector[Handler[Any]] = Vector.empty

  override private[eventuate] def receiveEvent(event: DurableEvent): Unit = {
    super.receiveEvent(event)

    if (event.deliveryId != UndefinedDeliveryId && event.emitterId == id)
      confirm(event.deliveryId)

    if (parameters.nonEmpty) {
      val persistenceId = deliveryId(event)

      deliver(persistenceId, PersistOnEventRequest(persistenceId, parameters, handlers, instanceId), self.path)

      parameters = Vector.empty
      handlers = Vector.empty
    }
  }

  /**
   * Asynchronously persists a sequence of `events` and calls `handler` with the persist result
   * for each event in the sequence. If persistence was successful, `onEvent` is called with a
   * persisted event before `handler` is called. Both, `onEvent` and `handler`, are called on a
   * dispatcher thread of this actor, hence, it is safe to modify internal state within them. The
   * `onLast` handler is additionally called for the last event in the sequence. Once called with
   * `Success`, `handler` and `onLast` are not called again during event replay.
   *
   * If `handler` is called with `Failure`, writing to the event log is automatically retried on
   * next actor (re)start. Applications that want to retry writing earlier should call the
   * [[ConfirmedDelivery.redeliverUnconfirmed redeliverUnconfirmed]] method (for example, scheduled
   * with an application-defined delay).
   *
   * By default, the event is routed to event-sourced destinations with an undefined `aggregateId`.
   * If this actor's `aggregateId` is defined it is additionally routed to all actors with the same
   * `aggregateId`. Further routing destinations can be defined with the `customDestinationAggregateIds`
   * parameter.
   */
  final def persistOnEventN[A](events: Seq[A], onLast: Handler[A] = (_: Try[A]) => (), customDestinationAggregateIds: Set[String] = Set())(handler: Handler[A]): Unit = events match {
    case Seq() =>
    case es :+ e =>
      es.foreach { event =>
        persistOnEvent(event, customDestinationAggregateIds)(handler)
      }
      persistOnEvent(e, customDestinationAggregateIds) { r =>
        handler(r)
        onLast(r)
      }
  }

  /**
   * Asynchronously persists the given `event` and calls `handler` with the persist result. If
   * persistence was successful, `onEvent` is called with the persisted event before `handler`
   * is called. Both, `onEvent` and `handler`, are called on a dispatcher thread of this actor,
   * hence, it is safe to modify internal state within them. Once called with `Success`, the
   * `handler` is not called again during event replay.
   *
   * If `handler` is called with a `Failure`, writing to the event log is automatically retried on
   * next actor (re)start. Applications that want to retry writing earlier should call the
   * [[ConfirmedDelivery.redeliverUnconfirmed redeliverUnconfirmed]] method (for example, scheduled
   * with an application-defined delay).
   *
   * By default, the event is routed to event-sourced destinations with an undefined `aggregateId`.
   * If this actor's `aggregateId` is defined it is additionally routed to all actors with the same
   * `aggregateId`. Further routing destinations can be defined with the `customDestinationAggregateIds`
   * parameter.
   */
  final def persistOnEvent[A](event: A, customDestinationAggregateIds: Set[String] = Set())(handler: Handler[A]): Unit = {
    parameters = parameters :+ PersistOnEventParameters(event, customDestinationAggregateIds)
    handlers = handlers :+ handler.asInstanceOf[Handler[Any]]
  }

  def deliveryId(event: DurableEvent): String =
    event.localSequenceNr.toString
}
