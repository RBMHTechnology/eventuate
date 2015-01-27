/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
 */

package com.rbmhtechnology.eventuate

import akka.actor._

trait EventsourcedView extends Eventsourced with ConditionalCommands with Stash {
  import EventLogProtocol._

  private def onDurableEvent(event: DurableEvent, handled: DurableEvent => Unit = _ => ()): Unit =
    if (onEvent.isDefinedAt(event.payload)) {
      onLastConsumed(event)
      onEvent(event.payload)
      handled(event)
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
    case Written(event) => if (event.sequenceNr > lastSequenceNr)
      onDurableEvent(event, e => conditionChanged(e.timestamp))
    case ConditionalCommand(condition, cmd) =>
      conditionalSend(condition, cmd)
    case cmd =>
      onCommand(cmd)
  }

  final def receive = initiating

  override def preStart(): Unit =
    eventLog ! Replay(1, self, instanceId)
}

/**
 * Java API.
 */
abstract class AbstractEventsourcedView(val processId: String, val eventLog: ActorRef) extends AbstractEventsourced with EventsourcedView