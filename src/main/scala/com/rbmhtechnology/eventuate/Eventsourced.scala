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

import java.util.{Optional => JOption}
import java.util.concurrent.atomic.AtomicInteger

import akka.actor._

object Eventsourced {
  val instanceIdCounter = new AtomicInteger(0)
}

/**
 * Implemented by actors that derive internal state from events stored in an event log. Events are
 * pushed from the `eventLog` actor to this actor and handled with the `onEvent` event handler. An
 * event handler defines how events are projected on internal state.
 *
 * By default, an `Eventsourced` actor does not define an `aggregateId`. In this case, the `eventLog`
 * pushes all events to this actor. If it defines an `aggregateId`, the `eventLog` actor only pushes
 * those events that contain the `aggregateId` value in their `destinationAggregateIds` set.
 *
 * Commands sent to an `Eventsourced` actor during recovery are delayed until recovery completes.
 */
trait Eventsourced extends Actor {
  import Eventsourced._

  private var _lastSequenceNr: Long = 0L
  private var _lastSystemTimestamp: Long = 0L
  private var _lastVectorTimestamp: VectorTime = VectorTime()
  private var _lastReplicaId: String = ""
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
   * Optional aggregate id. Not defined by default.
   */
  def aggregateId: Option[String] =
    None

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
   * Replica id of the last consumed event.
   */
  def lastReplicaId: String =
    _lastReplicaId

  /**
   * Process id of the last consumed event.
   */
  def lastProcessId: String =
    DurableEvent.processId(lastReplicaId, aggregateId)

  /**
   * Returns `true` if this actor is currently recovering internal state by consuming
   * replayed events from the event log. Returns `false` after recovery completed and
   * the actor switches to consuming live events.
   */
  def recovering: Boolean =
    _recovering

  /**
   * Called after recovery successfully completed. Can be overridden by implementations.
   */
  def recovered(): Unit = ()

  /**
   * Sends a [[EventsourcingProtocol.Replay]] command to the event log. Can be overridden
   * by implementations to customize recovery.
   */
  def replay(): Unit =
    eventLog ! EventsourcingProtocol.Replay(1, self, aggregateId, instanceId)

  /**
   * Internal API.
   */
  private[eventuate] def onRecoverySuccess(): Unit = {
    _recovering = false
    recovered()
  }

  /**
   * Internal API.
   */
  private[eventuate] def onLastConsumed(d: DurableEvent): Unit = {
    _lastSequenceNr = d.sequenceNr
    _lastSystemTimestamp = d.systemTimestamp
    _lastVectorTimestamp = d.vectorTimestamp
    _lastReplicaId = d.replicaId
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

  override def aggregateId: Option[String] =
    Option(getAggregateId.orElse(null));

  /**
   * Java API.
   *
   * Optional aggregate id. Not defined by default.
   */
  def getAggregateId: JOption[String] =
    JOption.empty()

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
