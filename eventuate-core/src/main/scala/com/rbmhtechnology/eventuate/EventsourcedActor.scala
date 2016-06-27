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

import java.util.concurrent.TimeUnit

import akka.actor._
import com.rbmhtechnology.eventuate.PersistOnEvent._
import com.typesafe.config.Config

import scala.concurrent.duration._
import scala.util._

private class EventsourcedActorSettings(config: Config) {
  val writeTimeout =
    config.getDuration("eventuate.log.write-timeout", TimeUnit.MILLISECONDS).millis
}

/**
 * An `EventsourcedActor` is an [[EventsourcedView]] that can also write new events to its event log.
 * New events are written with the asynchronous [[persist]] and [[persistN]] methods. They must only
 * be used within the `onCommand` command handler. After successful persistence, the `onEvent` handler
 * is automatically called with the persisted event(s). The `onEvent` handler is the place where actor
 * state may be updated. The `onCommand` handler should not update actor state but only read it e.g.
 * for command validation. `EventsourcedActor`s that want to persist new events within the `onEvent`
 * handler should additionally mixin the [[PersistOnEvent]] trait and use the
 * [[PersistOnEvent.persistOnEvent persistOnEvent]] method.
 *
 * @see [[EventsourcedView]]
 * @see [[PersistOnEvent]]
 */
trait EventsourcedActor extends EventsourcedView with EventsourcedClock {
  import EventsourcingProtocol._

  private val settings =
    new EventsourcedActorSettings(context.system.settings.config)

  private val messageStash = new MessageStash()
  private val commandStash = new MessageStash()

  private var writeRequests: Vector[DurableEvent] = Vector.empty
  private var writeHandlers: Vector[Handler[Any]] = Vector.empty

  private var writeRequestCorrelationId: Int = 0
  private var writesInProgress: Set[Int] = Set.empty

  private var writing: Boolean = false
  private var writeReplyHandling: Boolean = false

  /**
   * State synchronization. If set to `true`, commands see internal state that is consistent
   * with the event log. This is achieved by stashing new commands if this actor is currently
   * writing events. If set to `false`, commands see internal state that is eventually
   * consistent with the event log.
   */
  //#state-sync
  def stateSync: Boolean = true // TODO: non-synced emits are tracked as concurrent !!!
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
  final def persist[A](event: A, customDestinationAggregateIds: Set[String] = Set())(handler: Handler[A]): Unit =
    persistDurableEvent(durableEvent(event, customDestinationAggregateIds), handler.asInstanceOf[Handler[Any]])

  /**
   * Internal API.
   */
  private[eventuate] def persistDurableEvent(event: DurableEvent, handler: Handler[Any]): Unit = {
    writeRequests = writeRequests :+ event
    writeHandlers = writeHandlers :+ handler
  }

  /**
   * Internal API.
   */
  override private[eventuate] def unhandledMessage(msg: Any): Unit = msg match {
    case WriteSuccess(events, cid, iid) => if (writesInProgress.contains(cid) && iid == instanceId) writeReplyHandling(cid) {
      events.foreach { event =>
        receiveEvent(event)
        writeHandlers.head(Success(event.payload))
        writeHandlers = writeHandlers.tail
      }
      if (stateSync) {
        writing = false
        messageStash.unstash()
      }
    }
    case WriteFailure(events, cause, cid, iid) => if (writesInProgress.contains(cid) && iid == instanceId) writeReplyHandling(cid) {
      events.foreach { event =>
        receiveEventInternal(event, cause)
        writeHandlers.head(Failure(cause))
        writeHandlers = writeHandlers.tail
      }
      if (stateSync) {
        writing = false
        messageStash.unstash()
      }
    }
    case PersistOnEventRequest(persistOnEventSequenceNr: Long, invocations, iid) => if (iid == instanceId) {
      writeOrDelay {
        writeHandlers = Vector.fill(invocations.length)(PersistOnEvent.DefaultHandler)
        writeRequests = invocations.map {
          case PersistOnEventInvocation(event, customDestinationAggregateIds) =>
            durableEvent(event, customDestinationAggregateIds, None, Some(persistOnEventSequenceNr))
        }
      }
    }
    case cmd =>
      writeOrDelay(super.unhandledMessage(cmd))
  }

  private def writeReplyHandling(correlationId: Int)(body: => Unit): Unit =
    try {
      writeReplyHandling = true
      body
    } finally {
      writeReplyHandling = false
      writesInProgress = writesInProgress - correlationId
    }

  private def writePending: Boolean =
    writeRequests.nonEmpty

  private def writeOrDelay(writeRequestProducer: => Unit): Unit = {
    if (writing) messageStash.stash() else {
      writeRequestProducer

      val wPending = writePending
      if (wPending) write(nextCorrelationId())
      if (wPending && stateSync) writing = true else if (stateSync) messageStash.unstash()
    }
  }

  private def write(correlationId: Int): Unit = {
    eventLog ! Write(writeRequests, sender(), self, correlationId, instanceId)
    writesInProgress = writesInProgress + correlationId
    writeRequests = Vector.empty
  }

  private def nextCorrelationId(): Int = {
    writeRequestCorrelationId += 1
    writeRequestCorrelationId
  }

  /**
   * Adds the current command to the user's command stash. Must not be used in the event handler
   * or `persist` handler.
   */
  override def stash(): Unit =
    if (writeReplyHandling || eventHandling) throw new StashError("stash() must not be used in event handler or persist handler") else commandStash.stash()

  /**
   * Prepends all stashed commands to the actor's mailbox and then clears the command stash.
   * Has no effect if the actor is recovering i.e. if `recovering` returns `true`.
   */
  override def unstashAll(): Unit =
    if (!recovering) {
      commandStash ++: messageStash
      commandStash.clear()
      messageStash.unstashAll()
    }
}
