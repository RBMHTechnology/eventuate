/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
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