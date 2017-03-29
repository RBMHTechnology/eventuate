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

package com.rbmhtechnology.eventuate

import scala.collection.immutable.SortedMap
import com.rbmhtechnology.eventuate.EventsourcedView.Handler

import scala.util._

private[eventuate] object PersistOnEvent {
  /**
   * Records a `persistOnEvent` invocation.
   */
  case class PersistOnEventInvocation(event: Any, customDestinationAggregateIds: Set[String])

  /**
   * A request sent by [[PersistOnEvent]] instances to `self` in order to persist events recorded by `invocations`.
   * @param persistOnEventSequenceNr the sequence number of the event that caused this request.
   * @param persistOnEventId [[EventId]] of the event that caused this request. This is optional for backwards
   *                         compatibility, as old snapshots might contain `PersistOnEventRequest`s
   *                         without this field being defined.
   */
  case class PersistOnEventRequest(persistOnEventSequenceNr: Long, persistOnEventId: Option[EventId], invocations: Vector[PersistOnEventInvocation], instanceId: Int)

  /**
   * Default `persist` handler to use when processing [[PersistOnEventRequest]]s in [[EventsourcedActor]].
   */
  val DefaultHandler: Handler[Any] = {
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
 */
trait PersistOnEvent extends EventsourcedActor {
  import PersistOnEvent._

  private var invocations: Vector[PersistOnEventInvocation] = Vector.empty
  /**
   * [[PersistOnEventRequest]] by sequence number of the event that caused the persist on event request.
   *
   * This map keeps the requests in the order they were submitted.
   */
  private var requestsBySequenceNr: SortedMap[Long, PersistOnEventRequest] = SortedMap.empty

  /**
   * [[PersistOnEventRequest]] by [[EventId]] of the event that caused the persist on event request.
   *
   * This map ensures that requests can be confirmed properly even if the sequence number of the event
   * that caused the request changed its local sequence number due to a disaster recovery.
   *
   * @see https://github.com/RBMHTechnology/eventuate/issues/385
   */
  private var requestsByEventId: Map[EventId, PersistOnEventRequest] = Map.empty

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
    if (event.emitterId == id) findPersistOnEventRequest(event).foreach(confirmRequest)

    if (invocations.nonEmpty) {
      deliverRequest(PersistOnEventRequest(lastSequenceNr, Some(lastHandledEvent.id), invocations, instanceId))
      invocations = Vector.empty
    }
  }

  /**
   * Internal API.
   */
  override private[eventuate] def snapshotCaptured(snapshot: Snapshot): Snapshot = {
    requestsBySequenceNr.values.foldLeft(super.snapshotCaptured(snapshot)) {
      case (s, pr) => s.addPersistOnEventRequest(pr)
    }
  }

  /**
   * Internal API.
   */
  override private[eventuate] def snapshotLoaded(snapshot: Snapshot): Unit = {
    super.snapshotLoaded(snapshot)
    snapshot.persistOnEventRequests.foreach { pr =>
      val requestWithUpdatedInstanceId = pr.copy(instanceId = instanceId)
      requestsBySequenceNr += (pr.persistOnEventSequenceNr -> requestWithUpdatedInstanceId)
      pr.persistOnEventId.foreach(requestsByEventId += _ -> requestWithUpdatedInstanceId)
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
    requestsBySequenceNr.keySet

  private def deliverRequest(request: PersistOnEventRequest): Unit = {
    requestsBySequenceNr += request.persistOnEventSequenceNr -> request
    request.persistOnEventId.foreach(requestsByEventId += _ -> request)
    if (!recovering) self ! request
  }

  private def confirmRequest(request: PersistOnEventRequest): Unit = {
    request.persistOnEventId.foreach(requestsByEventId -= _)
    requestsBySequenceNr -= request.persistOnEventSequenceNr
  }

  private def findPersistOnEventRequest(event: DurableEvent) =
    event
      .persistOnEventId.flatMap(requestsByEventId.get)
      // Fallback for old events that have no persistOnEventId
      .orElse(event.persistOnEventSequenceNr.flatMap(requestsBySequenceNr.get))

  private def redeliverUnconfirmedRequests(): Unit = requestsBySequenceNr.foreach {
    case (_, request) => self ! request
  }
}
