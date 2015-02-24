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

import java.util.concurrent.atomic.AtomicInteger

import akka.actor._

object Eventsourced {
  val instanceIdCounter = new AtomicInteger(0)
}

/**
 * Implemented by actors that derive internal state from events stored in an event log. Events
 * are pushed from the `eventLog` actor to this actor and handled with the `onEvent` event handler. An
 * event handler defines how events are projected on internal state. Internal state must only be modified
 * within `onEvent`. External commands are handled with the `onCommand` command handler. A command handler
 * may access internal state but must not modify it.
 */
trait Eventsourced extends Actor {
  import Eventsourced._

  private var _lastSequenceNr: Long = 0L
  private var _lastSystemTimestamp: Long = 0L
  private var _lastVectorTimestamp: VectorTime = VectorTime()
  private var _lastProcessId: String = ""
  private var _recovering: Boolean = true

  val instanceId: Int = instanceIdCounter.getAndIncrement()

  /**
   * Event log actor.
   */
  def eventLog: ActorRef

  /**
   * Command handler.
   */
  def onCommand: Receive

  /**
   * Event handler.
   */
  def onEvent: Receive

  /**
   * Sequence number of the last consumed event.
   */
  def lastSequenceNr: Long =
    _lastSequenceNr

  /**
   * Wall-clock timestamp of the last consumed event.
   */
  def lastSystemTimestamp: Long =
    _lastSystemTimestamp

  /**
   * Vector timestamp of the last consumed event.
   */
  def lastVectorTimestamp: VectorTime =
    _lastVectorTimestamp

  /**
   * Process id of the last consumed event.
   */
  def lastProcessId: String =
    _lastProcessId

  /**
   * Returns `true` if this actor is currently recovering internal state by consuming
   * replayed events from the event log. Returns `false` after recovery completed and
   * the actor switches to consuming live events.
   */
  def recovering: Boolean =
    _recovering

  /**
   * Internal API.
   */
  private[eventuate] def onRecoverySuccess(): Unit =
    _recovering = false

  /**
   * Internal API.
   */
  private[eventuate] def onLastConsumed(d: DurableEvent): Unit = {
    _lastSequenceNr = d.sequenceNr
    _lastSystemTimestamp = d.systemTimestamp
    _lastVectorTimestamp = d.vectorTimestamp
    _lastProcessId = d.processId
  }
}

/**
 * Java API.
 *
 * @see [[Eventsourced]]
 */
abstract class AbstractEventsourced extends Eventsourced {
  private var _onCommand: Receive = Actor.emptyBehavior
  private var _onEvent: Receive = Actor.emptyBehavior

  final def onCommand: Receive = _onCommand
  final def onEvent: Receive = _onEvent

  /**
   * Sets this actor's command handler.
   */
  protected def onReceiveCommand(handler: Receive): Unit =
    _onCommand = handler

  /**
   * Sets this actor's event handler.
   */
  protected def onReceiveEvent(handler: Receive) =
    _onEvent = handler
}
