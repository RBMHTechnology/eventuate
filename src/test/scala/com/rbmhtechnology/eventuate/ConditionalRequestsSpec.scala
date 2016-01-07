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

import com.typesafe.config.ConfigFactory

import org.scalatest._

object ConditionalRequestsSpec {
  class ConditionalRequestReceiver(logProbe: ActorRef) extends EventsourcedView with ConditionalRequests {
    val id = "test"
    val eventLog = logProbe

    def onCommand = {
      case t: VectorTime => conditionChanged(t)
      case cmd           => sender() ! s"re: ${cmd}"
    }

    def onEvent = {
      case _ =>
    }
  }

  def timestampAB(timeA: Long, timeB: Long): VectorTime =
    VectorTime("A" -> timeA, "B" -> timeB)
}

class ConditionalRequestsSpec extends TestKit(ActorSystem("test", ConfigFactory.parseString("akka.log-dead-letters = 0")))
  with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with ImplicitSender {

  import ConditionalRequestsSpec._
  import EventsourcingProtocol._

  var instanceId: Int = _
  var receiver: ActorRef = _

  override def beforeEach: Unit = {
    val probe = TestProbe()
    instanceId = EventsourcedView.instanceIdCounter.get()
    receiver = system.actorOf(Props(new ConditionalRequestReceiver(probe.ref)))
    probe.expectMsg(LoadSnapshot("test", instanceId))
    probe.sender() ! LoadSnapshotSuccess(None, instanceId)
    probe.expectMsg(Replay(1L, Some(receiver), instanceId))
    probe.sender() ! ReplaySuccess(Nil, 0L, instanceId)
  }

  override def afterAll: Unit =
    TestKit.shutdownActorSystem(system)

  "A ConditionalRequests implementor" must {
    "send a conditional request immediately if condition is already met" in {
      receiver ! ConditionalRequest(timestampAB(0, 0), "a")
      expectMsg("re: a")
    }
    "delay a conditional request if condition is not met" in {
      receiver ! ConditionalRequest(timestampAB(0, 0), "a")
      receiver ! ConditionalRequest(timestampAB(1, 0), "b")
      receiver ! ConditionalRequest(timestampAB(0, 0), "c")
      expectMsg("re: a")
      expectMsg("re: c")
    }
    "send a conditional request later if condition is met after an update" in {
      receiver ! ConditionalRequest(timestampAB(0, 0), "a")
      receiver ! ConditionalRequest(timestampAB(1, 0), "b")
      receiver ! ConditionalRequest(timestampAB(0, 0), "c")
      receiver ! timestampAB(1, 0)
      expectMsg("re: a")
      expectMsg("re: c")
      expectMsg("re: b")
    }
    "send delayed conditional request in correct order if condition is met" in {
      receiver ! ConditionalRequest(timestampAB(1, 0), "a")
      receiver ! ConditionalRequest(timestampAB(2, 0), "b")
      receiver ! ConditionalRequest(timestampAB(3, 0), "c")
      receiver ! ConditionalRequest(timestampAB(0, 0), "x")
      receiver ! timestampAB(3, 0)
      expectMsg("re: x")
      expectMsg("re: a")
      expectMsg("re: b")
      expectMsg("re: c")
    }
    "send delayed conditional request in batches (scenario 1)" in {
      1 to 1000 foreach { i =>
        receiver ! ConditionalRequest(timestampAB(i, 0), i)
        receiver ! timestampAB(i, 0)
      }
      1 to 1000 foreach { i =>
        expectMsg(s"re: ${i}")
      }
    }
    "send delayed conditional request in batches (scenario 2)" in {
      1 to 1000 foreach { i =>
        receiver ! ConditionalRequest(timestampAB(i, 0), i)
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
