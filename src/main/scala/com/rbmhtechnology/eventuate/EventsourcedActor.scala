/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
 */

package com.rbmhtechnology.eventuate

import java.util.function.BiConsumer

import scala.util._

import akka.actor._

trait EventsourcedActor extends Eventsourced with ConditionalCommands with Stash {
  import EventLogProtocol._

  private var clock: VectorClock = _
  private var delayRequests: Vector[Any] = Vector.empty
  private var delayHandlers: Vector[Any => Unit] = Vector.empty
  private var writeRequests: Vector[DurableEvent] = Vector.empty
  private var writeHandlers: Vector[Try[Any] => Unit] = Vector.empty
  private var writing: Boolean = false

  def processId: String
  def sync: Boolean = true

  final def delay[A](command: A)(handler: A => Unit): Unit = {
    if (sync) throw new DelayException("delay not supported with sync = true")
    delayRequests = delayRequests :+ command
    delayHandlers = delayHandlers :+ handler.asInstanceOf[Any => Unit]
  }

  final def persist[A](event: A)(handler: Try[A] => Unit): Unit =
    persistWithLocalTime(_ => event)(handler)

  final def persistWithLocalTime[A](f: Long => A)(handler: Try[A] => Unit): A = {
    clock = clock.tick()
    val event: A = f(clock.currentLocalTime())
    writeRequests = writeRequests :+ DurableEvent(event, clock.currentTime, processId)
    writeHandlers = writeHandlers :+ handler.asInstanceOf[Any => Unit]
    event
  }

  private[eventuate] def currentTime: VectorTime =
    clock.currentTime

  private def delayPending: Boolean =
    delayRequests.nonEmpty

  private def writePending: Boolean =
    writeRequests.nonEmpty

  private def delay(): Unit = {
    log forward Delay(delayRequests, self, instanceId)
    delayRequests = Vector.empty
  }

  private def write(): Unit = {
    log forward Write(writeRequests, self, instanceId)
    writeRequests = Vector.empty
  }

  private val initiating: Receive = {
    case Replaying(event, iid) => if (iid == instanceId) {
      clock = clock.update(event.timestamp)
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
        unstashAll()
      }
    }
    case WriteFailure(event, cause, iid) => if (iid == instanceId) {
      onLastConsumed(event)
      writeHandlers.head(Failure(cause))
      writeHandlers = writeHandlers.tail
      if (sync && writeHandlers.isEmpty) {
        writing = false
        unstashAll()
      }
    }
    case Written(event) => if (event.sequenceNr > lastSequenceNr) {
      clock = clock.update(event.timestamp)
      onDurableEvent(event)
      conditionChanged(lastTimestamp)
    }
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
    log ! Replay(1, self, instanceId)
  }
}

class DelayException(msg: String) extends RuntimeException(msg)

/**
 * Java API.
 */
abstract class AbstractEventsourcedActor(val processId: String, val log: ActorRef) extends AbstractEventsourced with EventsourcedActor with Delivery {
  def persist[A](event: A, handler: BiConsumer[A, Throwable]): Unit = persist[A](event) {
    case Success(a) => handler.accept(a, null)
    case Failure(e) => handler.accept(null.asInstanceOf[A], e)
  }
}