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

trait EventsourcedActor extends Eventsourced with ConditionalCommands with ExtendedStash {
  import EventLogProtocol._

  type Handler[A] = Try[A] => Unit

  private var clock: VectorClock = _
  private var delayRequests: Vector[Any] = Vector.empty
  private var delayHandlers: Vector[Any => Unit] = Vector.empty
  private var writeRequests: Vector[DurableEvent] = Vector.empty
  private var writeHandlers: Vector[Handler[Any]] = Vector.empty
  private var writing: Boolean = false

  def processId: String
  def sync: Boolean = true

  final def delay[A](command: A)(handler: A => Unit): Unit = {
    if (sync) throw new DelayException("delay not supported with sync = true")
    delayRequests = delayRequests :+ command
    delayHandlers = delayHandlers :+ handler.asInstanceOf[Any => Unit]
  }

  /**
   * Persists a sequence of `events` and asynchronously calls `handler` with the persist
   * results. The `handler` is called for each event in the sequence a during a separate
   * message dispatch by this actor, hence, it is safe to modify internal state within
   * `handler`. The `onLast` handler is additionally called for the last event in the
   * sequence.
   */
  final def persistN[A](events: Seq[A], onLast: Handler[A] = (_: Try[A]) => ())(handler: Handler[A]): Unit = events match {
    case Seq()   =>
    case es :+ e =>
      es.foreach { event =>
        persist(event)(handler)
      }
      persist(e) { r =>
        handler(r)
        onLast(r)
      }
  }

  /**
   * Persists a single `event` and asynchronously calls `handler` with the persist result.
   * The `handler` is called a during separate message dispatch by this actor, hence, it
   * is safe to modify internal state within `handler`.
   */
  final def persist[A](event: A)(handler: Handler[A]): Unit =
    persistWithLocalTime(_ => event)(handler)

  /**
   * Persists a single event returned by  `f` and asynchronously calls  `handler` with the
   * persist result. The input parameter to `f` is the current local time. The `handler` is
   * called a during separate message dispatch by this actor, hence, it is safe to modify
   * internal state within `handler`.
   */
  final def persistWithLocalTime[A](f: Long => A)(handler: Handler[A]): A = {
    clock = clock.tick()
    val event = f(clock.currentLocalTime())
    writeRequests = writeRequests :+ DurableEvent(event, clock.currentTime, processId = processId)
    writeHandlers = writeHandlers :+ handler.asInstanceOf[Try[Any] => Unit]
    event
  }

  private[eventuate] def currentTime: VectorTime =
    clock.currentTime

  private def delayPending: Boolean =
    delayRequests.nonEmpty

  private def writePending: Boolean =
    writeRequests.nonEmpty

  private def delay(): Unit = {
    eventLog forward Delay(delayRequests, self, instanceId)
    delayRequests = Vector.empty
  }

  private def write(): Unit = {
    eventLog forward Write(writeRequests, self, instanceId)
    writeRequests = Vector.empty
  }

  private def onDurableEvent(event: DurableEvent, handled: DurableEvent => Unit = _ => ()): Unit = {
    if (onEvent.isDefinedAt(event.payload)) {
      clock = clock.update(event.timestamp)
      onLastConsumed(event)
      onEvent(event.payload)
      handled(event)
    } else if (event.processId == processId) {
      // Event not handled but it has been previously emitted by this
      // actor. So we need to recover local time, otherwise, we could
      // end up in the past after recovery ....
      clock = clock.merge(event.timestamp.localCopy(processId))
    } else {
      // Ignore unhandled event that has been emitted by another actor.
    }
  }

  private val initiating: Receive = {
    case Replaying(event, iid) => if (iid == instanceId) {
      onDurableEvent(event)
    }
    case ReplaySuccess(iid) => if (iid == instanceId) {
      context.become(initiated)
      conditionChanged(lastTimestamp)
      onRecoverySuccess()
      unstashAll()
    }
    case ReplayFailure(cause, iid) => if (iid == instanceId) {
      context.stop(self)
    }
    case other =>
      stash()
  }

  private val initiated: Receive = {
    case DelaySuccess(command, iid) => if (iid == instanceId) {
      delayHandlers.head(command)
      delayHandlers = delayHandlers.tail
    }
    case WriteSuccess(event, iid) => if (iid == instanceId) {
      onLastConsumed(event)
      conditionChanged(lastTimestamp)
      writeHandlers.head(Success(event.payload))
      writeHandlers = writeHandlers.tail
      if (sync && writeHandlers.isEmpty) {
        writing = false
        unstash()
      }
    }
    case WriteFailure(event, cause, iid) => if (iid == instanceId) {
      onLastConsumed(event)
      writeHandlers.head(Failure(cause))
      writeHandlers = writeHandlers.tail
      if (sync && writeHandlers.isEmpty) {
        writing = false
        unstash()
      }
    }
    case Written(event) => if (event.sequenceNr > lastSequenceNr)
      onDurableEvent(event, e => conditionChanged(e.timestamp))
    case ConditionalCommand(condition, cmd) =>
      conditionalSend(condition, cmd)
    case cmd =>
      if (writing) stash() else {
        onCommand(cmd)

        val dPending = delayPending
        val wPending = writePending

        if (dPending && wPending) throw new DelayException("""
          |delay cannot be used in combination with persist.
          |This limitation will be removed in later versions.""".stripMargin)

        if (dPending) delay()
        if (wPending) write()
        if (wPending && sync) writing = true
      }
  }

  final def receive = initiating

  override def preStart(): Unit = {
    clock = VectorClock(processId)
    eventLog ! Replay(1, self, instanceId)
  }
}

class DelayException(msg: String) extends RuntimeException(msg)

/**
 * Java API.
 */
abstract class AbstractEventsourcedActor(val processId: String, val eventLog: ActorRef) extends AbstractEventsourced with EventsourcedActor with Delivery {
  def persist[A](event: A, handler: BiConsumer[A, Throwable]): Unit = persist[A](event) {
    case Success(a) => handler.accept(a, null)
    case Failure(e) => handler.accept(null.asInstanceOf[A], e)
  }
}