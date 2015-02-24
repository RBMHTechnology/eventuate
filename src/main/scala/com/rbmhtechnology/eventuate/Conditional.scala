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

/**
 * A conditional command is a command (`cmd`) to an [[Eventsourced]] actor whose
 * delivery is delayed until the actor's `lastVectorTimestamp` becomes greater than
 * or equal the `condition` timestamp. If the condition is met, `cmd` is delivered
 * to the actor's command handler.
 */
case class ConditionalCommand(condition: VectorTime, cmd: Any)

private[eventuate] trait ConditionalCommands extends Actor {
  import ConditionalCommands._

  // TODO: consider starting command manager with context.system.actorOf
  // - command manager stays alive even if owner restarts
  // - command manager must then death-watch owner
  // - command manager must then receive condition changes during replay too
  private val commandManager = context.actorOf(Props(new CommandManager(self)))

  /**
   * Internal API.
   */
  private[eventuate] def conditionalSend(condition: VectorTime, cmd: Any): Unit =
    commandManager ! Command(condition, cmd, sender())

  /**
   * Internal API.
   */
  private[eventuate] def conditionChanged(condition: VectorTime): Unit =
    commandManager ! condition
}

private object ConditionalCommands {
  case class Command (condition: VectorTime, cmd: Any, sdr: ActorRef)

  case class Send(olderThan: VectorTime)
  case class Sent(olderThan: VectorTime, num: Int)

  class CommandManager(owner: ActorRef) extends Actor {
    val commandBuffer = context.actorOf(Props(new CommandBuffer(owner)))
    var currentVersion: VectorTime = VectorTime()

    val idle: Receive = {
      case cc: Command =>
        process(cc)
      case t: VectorTime =>
        currentVersion = currentVersion.merge(t)
        commandBuffer ! Send(currentVersion)
        context.become(sending)
    }

    val sending: Receive = {
      case cc: Command =>
        process(cc)
      case t: VectorTime =>
        currentVersion = currentVersion.merge(t)
      case Sent(olderThan, num) if olderThan == currentVersion =>
        context.become(idle)
      case Sent(olderThan, num) =>
        commandBuffer ! Send(currentVersion)
    }

    def receive = idle

    def process(cc: Command): Unit = {
      if (cc.condition <= currentVersion) owner.tell(cc.cmd, cc.sdr)
      else commandBuffer ! cc
    }
  }

  class CommandBuffer(owner: ActorRef) extends Actor {
    // TODO: cleanup commands older than threshold
    var commands: Vector[Command] = Vector.empty

    def receive = {
      case Send(olderThan) =>
        sender() ! Sent(olderThan, send(olderThan))
      case cc: Command =>
        commands = commands :+ cc
    }

    def send(olderThan: VectorTime): Int = {
      val (older, other) = commands.partition(_.condition <= olderThan)
      commands = other
      older.foreach(cc => owner.tell(cc.cmd, cc.sdr))
      older.length
    }
  }
}
