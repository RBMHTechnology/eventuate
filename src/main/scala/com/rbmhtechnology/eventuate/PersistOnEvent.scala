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

package com.rbmhtechnology.eventuate

import scala.collection.immutable.SortedMap

import com.rbmhtechnology.eventuate.EventsourcedView.Handler

import scala.util._

object PersistOnEvent {
  /**
   * Records a `persistOnEvent` invocation.
   */
  case class PersistOnEventInvocation(event: Any, customDestinationAggregateIds: Set[String])

  /**
   * A request sent by [[PersistOnEvent]] instances to `self` in order to persist events recorded by `invocations`.
   */
  case class PersistOnEventRequest(persistOnEventSequenceNr: Long, invocations: Vector[PersistOnEventInvocation], instanceId: Int)

  /**
   * Default `persist` handler to use when processing [[PersistOnEventRequest]]s in [[EventsourcedActor]].
   */
  private[eventuate] val DefaultHandler: Handler[Any] = {
    case Success(_) =>
    case Failure(e) => throw new PersistOnEventException(e)
  }
}
/**
 * Thrown to indicate that an asynchronous `persisOnEvent` operation failed.
 */
class PersistOnEventException(cause: Throwable) extends RuntimeException(cause)

/**
 * Can be mixed into [[EventsourcedActor]] for writing new events within the `onEvent` handler. New events are
 * written with the asynchronous [[persistOnEvent]] method. In contrast to [[EventsourcedActor.persist persist]],
 * one can '''not''' prevent command processing from running concurrently to [[persistOnEvent]] by setting
 * [[EventsourcedActor.stateSync stateSync]] to `true`.
 *
 * A `persistOnEvent` operation is reliable and idempotent. Once the event has been successfully written, a repeated
 * `persistOnEvent` call for that event during event replay has no effect. A failed `persistOnEvent` operation will
 * restart the actor by throwing a [[PersistOnEventException]]. After restart, failed `persistOnEvent` operations
 * are automatically re-tried.
 *
 * An `EventsourcedActor` that has a `PersistOnEvent` mixin is to some extend comparable to a [[StatefulProcessor]]
 * that is configured to write events to its source event log. Both write new events on receiving events, however,
 * a `StatefulProcessor` that writes new events to its source event log requires its own entry in the vector clock,
 * whereas `PersistOnEvent` does not. Furthermore, a `StatefulProcessor` can not write new events during command
 * processing. As a general rule, when operating on a single event log, applications should prefer `PersistOnEvent`
 * over `StatefulProcessor`.
 */
trait PersistOnEvent extends EventsourcedActor {
  import PersistOnEvent._

  private var invocations: Vector[PersistOnEventInvocation] = Vector.empty
  private var requests: SortedMap[Long, PersistOnEventRequest] = SortedMap.empty

  /**
   * Asynchronously persists the given `event`. Applications that want to handle the persisted event should define
   * the event handler at that event. By default, the event is routed to event-sourced destinations with an undefined
   * `aggregateId`. If this actor's `aggregateId` is defined it is additionally routed to all actors with the same
   * `aggregateId`. Further routing destinations can be defined with the `customDestinationAggregateIds` parameter.
   */
  final def persistOnEvent[A](event: A, customDestinationAggregateIds: Set[String] = Set()): Unit =
    invocations = invocations :+ PersistOnEventInvocation(event, customDestinationAggregateIds)

  /**
   * Internal API.
   */
  override private[eventuate] def receiveEvent(event: DurableEvent): Unit = {
    super.receiveEvent(event)

    event.persistOnEventSequenceNr.foreach { persistOnEventSequenceNr =>
      if (event.emitterId == id) confirmRequest(persistOnEventSequenceNr)
    }

    if (invocations.nonEmpty) {
      deliverRequest(PersistOnEventRequest(lastSequenceNr, invocations, instanceId))
      invocations = Vector.empty
    }
  }

  /**
   * Internal API.
   */
  override private[eventuate] def snapshotCaptured(snapshot: Snapshot): Snapshot = {
    requests.values.foldLeft(super.snapshotCaptured(snapshot)) {
      case (s, pr) => s.addPersistOnEventRequest(pr)
    }
  }

  /**
   * Internal API.
   */
  override private[eventuate] def snapshotLoaded(snapshot: Snapshot): Unit = {
    super.snapshotLoaded(snapshot)
    snapshot.persistOnEventRequests.foreach { pr =>
      requests = requests + (pr.persistOnEventSequenceNr -> pr.copy(instanceId = instanceId))
    }
  }

  /**
   * Internal API.
   */
  private[eventuate] override def recovered(): Unit = {
    super.recovered()
    redeliverUnconfirmedRequests()
  }

  /**
   * Internal API.
   */
  private[eventuate] def unconfirmedRequests: Set[Long] =
    requests.keySet

  private def deliverRequest(request: PersistOnEventRequest): Unit = {
    requests = requests + (request.persistOnEventSequenceNr -> request)
    if (!recovering) self ! request
  }

  private def confirmRequest(persistOnEventSequenceNr: Long): Unit = {
    requests = requests - persistOnEventSequenceNr
  }

  private def redeliverUnconfirmedRequests(): Unit = requests.foreach {
    case (_, request) => self ! request
  }
}
