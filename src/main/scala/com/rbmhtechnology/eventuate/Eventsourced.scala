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

trait Eventsourced extends Actor {
  import Eventsourced._

  private var _lastSequenceNr: Long = 0L
  private var _lastTimestamp: VectorTime = VectorTime()
  private var _lastProcessId: String = ""
  private var _recovering: Boolean = true

  val instanceId: Int = instanceIdCounter.getAndIncrement()

  def eventLog: ActorRef
  def onCommand: Receive
  def onEvent: Receive

  def lastSequenceNr: Long =
    _lastSequenceNr

  def lastTimestamp: VectorTime =
    _lastTimestamp

  def lastProcessId: String =
    _lastProcessId

  def recovering: Boolean =
    _recovering

  def onRecoverySuccess(): Unit =
    _recovering = false

  def onLastConsumed(d: DurableEvent): Unit = {
    _lastSequenceNr = d.sequenceNr
    _lastTimestamp = d.timestamp
    _lastProcessId = d.processId
  }
}

/**
 * Java API.
 */
abstract class AbstractEventsourced extends Eventsourced {
  private var _onCommand: Receive = Actor.emptyBehavior
  private var _onEvent: Receive = Actor.emptyBehavior

  final def onCommand: Receive = _onCommand
  final def onEvent: Receive = _onEvent

  protected def onReceiveCommand(handler: Receive): Unit =
    _onCommand = handler

  protected def onReceiveEvent(handler: Receive) =
    _onEvent = handler
}
