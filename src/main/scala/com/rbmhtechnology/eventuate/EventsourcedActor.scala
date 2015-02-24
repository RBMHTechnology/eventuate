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
 * An [[Eventsourced]] actor that can also produce new events to its event log. New events
 * can be produced with methods `persist` and `persistN`. They must only be used within
 * `onCommand`. An [[EventsourcedActor]] does not only consume its own events but can also
 * consume events produced by other [[EventsourcedActor]]s to the same event log. Commands
 * sent to this actor during recovery are delayed until recovery completes.
 *
 * An `EventsourcedActor` maintains a [[VectorClock]] used to time-stamp the events it writes
 * to the event log. Events that are handled by its event handler update the vector clock.
 * Events that are pushed from the `eventLog` actor but not handled by `onEvent` do not
 * update the vector clock.
 */
trait EventsourcedActor extends Eventsourced with ConditionalCommands with InternalStash {
  import EventsourcingProtocol._

  type Handler[A] = Try[A] => Unit

  private var clock: VectorClock = _
  private var delayRequests: Vector[Any] = Vector.empty
  private var delayHandlers: Vector[Any => Unit] = Vector.empty
  private var writeRequests: Vector[DurableEvent] = Vector.empty
  private var writeHandlers: Vector[Handler[Any]] = Vector.empty
  private var writing: Boolean = false

  /**
   * Unique process id, used to store this actor's logical time in its vector clock.
   */
  def processId: String

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
  def stateSync: Boolean = true

  /**
   * Delays the given command, by looping it through the event log actor, and asynchronously
   * calls `handler` with the command. This allows applications to delay command handling to
   * that point in the future where all previously called `persist` or `persistN` operations
   * completed. This method can only be used if this actor's `stateSync` is set to `false`.
   * The `handler` is called during a separate message dispatch by this actor, hence, it is
   * safe to access internal state within `handler`.
   */
  final def delay[A](command: A)(handler: A => Unit): Unit = {
    if (stateSync) throw new DelayException("delay not supported with stateSync = true")
    delayRequests = delayRequests :+ command
    delayHandlers = delayHandlers :+ handler.asInstanceOf[Any => Unit]
  }

  /**
   * Persists a sequence of `events` and asynchronously calls `handler` with the persist
   * results. The `handler` is called for each event in the sequence during a separate
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
   * Persists the given `event` and asynchronously calls `handler` with the persist result.
   * The `handler` is called during a separate message dispatch by this actor, hence, it
   * is safe to modify internal state within `handler`.
   */
  final def persist[A](event: A)(handler: Handler[A]): Unit =
    persistWithLocalTime(_ => event)(handler)

  /**
   * Persists the event returned by  `f` and asynchronously calls `handler` with the
   * persist result. The input parameter to `f` is the current local time. The `handler` is
   * called during a separate message dispatch by this actor, hence, it is safe to modify
   * internal state within `handler`.
   */
  final def persistWithLocalTime[A](f: Long => A)(handler: Handler[A]): A = {
    clock = clock.tick()
    val event = f(clock.currentLocalTime())
    writeRequests = writeRequests :+ DurableEvent(event, System.currentTimeMillis, clock.currentTime, processId = processId)
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
    eventLog ! Delay(delayRequests, sender(), self, instanceId)
    delayRequests = Vector.empty
  }

  private def write(): Unit = {
    eventLog ! Write(writeRequests, sender(), self, instanceId)
    writeRequests = Vector.empty
  }

  private def onDurableEvent(event: DurableEvent, handled: DurableEvent => Unit = _ => ()): Unit = {
    if (onEvent.isDefinedAt(event.payload)) {
      clock = clock.update(event.vectorTimestamp)
      onLastConsumed(event)
      onEvent(event.payload)
      handled(event)
    } else if (event.processId == processId) {
      // Event not handled but it has been previously emitted by this
      // actor. So we need to recover local time, otherwise, we could
      // end up in the past after recovery ....
      clock = clock.merge(event.vectorTimestamp.localCopy(processId))
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
      conditionChanged(lastVectorTimestamp)
      onRecoverySuccess()
      internalUnstashAll()
    }
    case ReplayFailure(cause, iid) => if (iid == instanceId) {
      context.stop(self)
    }
    case other =>
      internalStash()
  }

  private val initiated: Receive = {

    //
    // TODO: reliability improvements
    //
    // - response timeout for communication with log
    //   (low prio, local communication at the moment)
    //

    case DelayComplete(command, iid) => if (iid == instanceId) {
      delayHandlers.head(command)
      delayHandlers = delayHandlers.tail
    }
    case WriteSuccess(event, iid) => if (iid == instanceId) {
      onLastConsumed(event)
      conditionChanged(lastVectorTimestamp)
      writeHandlers.head(Success(event.payload))
      writeHandlers = writeHandlers.tail
      if (stateSync && writeHandlers.isEmpty) {
        writing = false
        internalUnstash()
      }
    }
    case WriteFailure(event, cause, iid) => if (iid == instanceId) {
      onLastConsumed(event)
      writeHandlers.head(Failure(cause))
      writeHandlers = writeHandlers.tail
      if (stateSync && writeHandlers.isEmpty) {
        writing = false
        internalUnstash()
      }
    }
    case Written(event) => if (event.sequenceNr > lastSequenceNr)
      onDurableEvent(event, e => conditionChanged(e.vectorTimestamp))
    case ConditionalCommand(condition, cmd) =>
      conditionalSend(condition, cmd)
    case cmd =>
      if (writing) internalStash() else {
        onCommand(cmd)

        val dPending = delayPending
        val wPending = writePending

        if (dPending && wPending) throw new DelayException("""
          |delay cannot be used in combination with persist.
          |This limitation will be removed in later versions.""".stripMargin)

        if (dPending) delay()
        if (wPending) write()
        if (wPending && stateSync) writing = true else if (stateSync) internalUnstash()
      }
  }

  final def receive = initiating

  /**
   * Initiates recovery by sending a [[EventsourcingProtocol.Replay]] request to the event log.
   */
  override def preStart(): Unit = {
    clock = VectorClock(processId)
    eventLog ! Replay(1, self, instanceId)
  }
}

/**
 * Thrown to indicate illegal use of method `delay` in [[EventsourcedActor]].
 */
class DelayException(msg: String) extends RuntimeException(msg)

/**
 * Java API.
 *
 * @see [[EventsourcedActor]]
 */
abstract class AbstractEventsourcedActor(val processId: String, val eventLog: ActorRef) extends AbstractEventsourced with EventsourcedActor with ConfirmedDelivery {
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
