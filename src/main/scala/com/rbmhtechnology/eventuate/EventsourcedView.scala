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
      conditionChanged(lastVectorTimestamp)
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
      onDurableEvent(event, e => conditionChanged(e.vectorTimestamp))
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
