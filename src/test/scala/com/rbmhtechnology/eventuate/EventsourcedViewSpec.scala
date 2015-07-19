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

import EventsourcedActorSpec._

object EventsourcedViewSpec {
  class TestEventsourcedView(
     val logProbe: ActorRef,
     val dstProbe: ActorRef, override val causalDelivery: Boolean) extends EventsourcedView {

    val id = idA
    val eventLog = logProbe

    override val onCommand: Receive = {
      case "boom" => throw boom
      case Ping(i) => dstProbe ! Pong(i)
    }

    override val onEvent: Receive = {
      case "boom" => throw boom
      case evt => dstProbe ! ((evt, currentTime, lastVectorTimestamp, lastSequenceNr))
    }
  }
}

class EventsourcedViewSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  import EventsourcedViewSpec._
  import EventsourcingProtocol._

  var instanceId: Int = _
  var logProbe: TestProbe = _
  var dstProbe: TestProbe = _

  override def beforeEach(): Unit = {
    instanceId = EventsourcedView.instanceIdCounter.get
    logProbe = TestProbe()
    dstProbe = TestProbe()
  }

  override def afterAll: Unit =
    TestKit.shutdownActorSystem(system)

  def unrecoveredView(causalDelivery: Boolean = false): ActorRef =
    system.actorOf(Props(new TestEventsourcedView(logProbe.ref, dstProbe.ref, causalDelivery)))

  def recoveredView(causalDelivery: Boolean = false): ActorRef = {
    val actor = unrecoveredView(causalDelivery)
    logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId))
    actor ! LoadSnapshotSuccess(None, instanceId)
    logProbe.expectMsg(Replay(1, actor, instanceId))
    actor ! ReplaySuccess(instanceId)
    actor
  }

  "An EventsourcedView" must {
    "recover from events" in {
      val actor = unrecoveredView()
      logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId))
      actor ! LoadSnapshotSuccess(None, instanceId)
      logProbe.expectMsg(Replay(1, actor, instanceId))
      actor ! Replaying(eventA("a", 1, timestampAB(1, 0)), instanceId)
      actor ! Replaying(eventB("b", 2, timestampAB(0, 1)), instanceId)
      actor ! ReplaySuccess(instanceId)
      dstProbe.expectMsg(("a", timestampAB(1, 0), timestampAB(1, 0), 1))
      dstProbe.expectMsg(("b", timestampAB(2, 1), timestampAB(0, 1), 2))
    }
    "retry recovery on failure" in {
      val actor = unrecoveredView()
      logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId))
      actor ! LoadSnapshotSuccess(None, instanceId)
      logProbe.expectMsg(Replay(1, actor, instanceId))
      actor ! Replaying(eventA("a", 1, timestampAB(1, 0)), instanceId)
      actor ! Replaying(eventA("boom", 2, timestampAB(2, 0)), instanceId)
      actor ! Replaying(eventA("c", 3, timestampAB(3, 0)), instanceId)
      actor ! ReplaySuccess(instanceId)
      logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId + 1))
      actor ! LoadSnapshotSuccess(None, instanceId + 1)
      logProbe.expectMsg(Replay(1, actor, instanceId + 1))
      actor ! Replaying(eventA("a", 1, timestampAB(1, 0)), instanceId + 1)
      actor ! Replaying(eventA("b", 2, timestampAB(2, 0)), instanceId + 1)
      actor ! Replaying(eventA("c", 3, timestampAB(3, 0)), instanceId + 1)
      actor ! ReplaySuccess(instanceId + 1)
      dstProbe.expectMsg(("a", timestampAB(1, 0), timestampAB(1, 0), 1))
      dstProbe.expectMsg(("a", timestampAB(1, 0), timestampAB(1, 0), 1))
      dstProbe.expectMsg(("b", timestampAB(2, 0), timestampAB(2, 0), 2))
      dstProbe.expectMsg(("c", timestampAB(3, 0), timestampAB(3, 0), 3))
    }
    "stash commands during recovery and handle them after initial recovery" in {
      val actor = unrecoveredView()
      actor ! Ping(1)
      actor ! Replaying(eventA("a", 1, timestampAB(1, 0)), instanceId)
      actor ! Ping(2)
      actor ! Replaying(eventA("b", 2, timestampAB(2, 0)), instanceId)
      actor ! Ping(3)
      actor ! ReplaySuccess(instanceId)
      dstProbe.expectMsg(("a", timestampAB(1, 0), timestampAB(1, 0), 1))
      dstProbe.expectMsg(("b", timestampAB(2, 0), timestampAB(2, 0), 2))
      dstProbe.expectMsg(Pong(1))
      dstProbe.expectMsg(Pong(2))
      dstProbe.expectMsg(Pong(3))
    }
    "stash commands during recovery and handle them after retried recovery" in {
      val actor = unrecoveredView()
      logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId))
      actor ! LoadSnapshotSuccess(None, instanceId)
      logProbe.expectMsg(Replay(1, actor, instanceId))
      actor ! Replaying(eventA("a", 1, timestampAB(1, 0)), instanceId)
      actor ! Ping(1)
      actor ! Replaying(eventA("boom", 2, timestampAB(1, 0)), instanceId)
      actor ! Ping(2)
      actor ! Replaying(eventA("c", 3, timestampAB(1, 0)), instanceId)
      actor ! ReplaySuccess(instanceId)
      logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId + 1))
      actor ! LoadSnapshotSuccess(None, instanceId + 1)
      logProbe.expectMsg(Replay(1, actor, instanceId + 1))
      actor ! Replaying(eventA("a", 1, timestampAB(1, 0)), instanceId + 1)
      actor ! Replaying(eventA("b", 2, timestampAB(2, 0)), instanceId + 1)
      actor ! Replaying(eventA("c", 3, timestampAB(3, 0)), instanceId + 1)
      actor ! ReplaySuccess(instanceId + 1)
      dstProbe.expectMsg(("a", timestampAB(1, 0), timestampAB(1, 0), 1))
      dstProbe.expectMsg(("a", timestampAB(1, 0), timestampAB(1, 0), 1))
      dstProbe.expectMsg(("b", timestampAB(2, 0), timestampAB(2, 0), 2))
      dstProbe.expectMsg(("c", timestampAB(3, 0), timestampAB(3, 0), 3))
      dstProbe.expectMsg(Pong(1))
      dstProbe.expectMsg(Pong(2))
    }
    "ignore live events that have already been consumed during recovery" in {
      val actor = unrecoveredView()
      actor ! Replaying(eventA("a", 1, timestampAB(1, 0)), instanceId)
      actor ! Written(eventB("b", 2, timestampAB(0, 1))) // live event
      actor ! Written(eventB("c", 3, timestampAB(0, 2))) // live event
      actor ! Written(eventB("d", 4, timestampAB(0, 3))) // live event
      actor ! Replaying(eventB("b", 2, timestampAB(0, 1)), instanceId)
      actor ! Replaying(eventB("c", 3, timestampAB(0, 2)), instanceId)
      actor ! ReplaySuccess(instanceId)
      dstProbe.expectMsg(("a", timestampAB(1, 0), timestampAB(1, 0), 1))
      dstProbe.expectMsg(("b", timestampAB(2, 1), timestampAB(0, 1), 2))
      dstProbe.expectMsg(("c", timestampAB(3, 2), timestampAB(0, 2), 3))
      dstProbe.expectMsg(("d", timestampAB(4, 3), timestampAB(0, 3), 4))
    }
    "not deliver events in causal order by default" in {
      val actor = recoveredView()
      actor ! Written(eventD("d", 1, timestampABCD(0, 1, 2, 2)))
      actor ! Written(eventC("c", 2, timestampABCD(0, 1, 2, 0)))
      actor ! Written(eventB("b", 3, timestampABCD(0, 1, 0, 0)))
      dstProbe.expectMsg(("d", timestampABCD(1, 1, 2, 2), timestampABCD(0, 1, 2, 2), 1))
      dstProbe.expectMsg(("c", timestampABCD(2, 1, 2, 2), timestampABCD(0, 1, 2, 0), 2))
      dstProbe.expectMsg(("b", timestampABCD(3, 1, 2, 2), timestampABCD(0, 1, 0, 0), 3))
    }
    "deliver events in causal order if configured" in {
      val actor = recoveredView(causalDelivery = true)
      actor ! Written(eventD("d", 1, timestampABCD(0, 1, 2, 2)))
      actor ! Written(eventC("c", 2, timestampABCD(0, 1, 2, 0)))
      actor ! Written(eventB("b", 3, timestampABCD(0, 1, 0, 0)))
      dstProbe.expectMsg(("b", timestampABCD(1, 1, 0, 0), timestampABCD(0, 1, 0, 0), 3))
      dstProbe.expectMsg(("c", timestampABCD(2, 1, 2, 0), timestampABCD(0, 1, 2, 0), 2))
      dstProbe.expectMsg(("d", timestampABCD(3, 1, 2, 2), timestampABCD(0, 1, 2, 2), 1))
    }
  }
}
