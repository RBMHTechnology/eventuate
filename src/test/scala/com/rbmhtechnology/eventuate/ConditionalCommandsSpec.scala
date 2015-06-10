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
import akka.testkit._

import org.scalatest._

object ConditionalCommandsSpec {
  class ConditionalCommandReceiver extends Actor with ConditionalCommands {
    def receive = {
      case ConditionalCommand(con, cmd) => conditionalSend(con, cmd)
      case t: VectorTime => conditionChanged(t)
      case cmd => sender() ! s"re: ${cmd}"
    }
  }

  def timestampAB(timeA: Long, timeB: Long): VectorTime =
    VectorTime("A" -> timeA, "B" -> timeB)
}

class ConditionalCommandsSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with ImplicitSender {
  import ConditionalCommandsSpec._

  var receiver: ActorRef = _

  override def beforeEach: Unit =
    receiver = system.actorOf(Props[ConditionalCommandReceiver])

  override def afterAll: Unit =
    TestKit.shutdownActorSystem(system)

  "A ConditionalCommands actor" must {
    "send a conditional command immediately if condition is already met" in {
      receiver ! ConditionalCommand(timestampAB(0, 0), "a")
      expectMsg("re: a")
    }
    "delay a conditional command if condition is not met" in {
      receiver ! ConditionalCommand(timestampAB(0, 0), "a")
      receiver ! ConditionalCommand(timestampAB(1, 0), "b")
      receiver ! ConditionalCommand(timestampAB(0, 0), "c")
      expectMsg("re: a")
      expectMsg("re: c")
    }
    "send a conditional command later if condition is met after an update" in {
      receiver ! ConditionalCommand(timestampAB(0, 0), "a")
      receiver ! ConditionalCommand(timestampAB(1, 0), "b")
      receiver ! ConditionalCommand(timestampAB(0, 0), "c")
      receiver ! timestampAB(1, 0)
      expectMsg("re: a")
      expectMsg("re: c")
      expectMsg("re: b")
    }
    "send delayed conditional commands in correct order if condition is met" in {
      receiver ! ConditionalCommand(timestampAB(1, 0), "a")
      receiver ! ConditionalCommand(timestampAB(2, 0), "b")
      receiver ! ConditionalCommand(timestampAB(3, 0), "c")
      receiver ! ConditionalCommand(timestampAB(0, 0), "x")
      receiver ! timestampAB(3, 0)
      expectMsg("re: x")
      expectMsg("re: a")
      expectMsg("re: b")
      expectMsg("re: c")
    }
    "send delayed conditional commands in batches (scenario 1)" in {
      1 to 1000 foreach { i =>
        receiver ! ConditionalCommand(timestampAB(i, 0), i)
        receiver ! timestampAB(i, 0)
      }
      1 to 1000 foreach { i =>
        expectMsg(s"re: ${i}")
      }
    }
    "send delayed conditional commands in batches (scenario 2)" in {
      1 to 1000 foreach { i =>
        receiver ! ConditionalCommand(timestampAB(i, 0), i)
      }
      1 to 1000 foreach { i =>
        receiver ! timestampAB(i, 0)
      }
      1 to 1000 foreach { i =>
        expectMsg(s"re: ${i}")
      }
    }
  }
}
