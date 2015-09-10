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

object EventsourcedViewSpec {
  val emitterIdA = "A"
  val emitterIdB = "B"

  val logIdA = "logA"
  val logIdB = "logB"

  case class Ping(i: Int)
  case class Pong(i: Int)

  class TestEventsourcedView(
     val logProbe: ActorRef,
     val dstProbe: ActorRef) extends EventsourcedView {

    val id = emitterIdA
    val eventLog = logProbe

    override val onCommand: Receive = {
      case "boom" => throw boom
      case Ping(i) => dstProbe ! Pong(i)
    }

    override val onEvent: Receive = {
      case "boom" => throw boom
      case evt => dstProbe ! ((evt, lastVectorTimestamp, currentTime, lastSequenceNr))
    }
  }

  val event1a = event("a", 1L)
  val event1b = event("b", 2L)
  val event1c = event("c", 3L)

  val event2a = DurableEvent("a", emitterIdA, None, Set(), 0L, timestamp(1, 0), logIdA, logIdA, logIdA, 1L, 1L, 0L)
  val event2b = DurableEvent("b", emitterIdB, None, Set(), 0L, timestamp(0, 1), logIdB, logIdB, logIdA, 1L, 2L, 3L)
  val event2c = DurableEvent("c", emitterIdB, None, Set(), 0L, timestamp(0, 2), logIdB, logIdB, logIdA, 2L, 3L, 3L)
  val event2d = DurableEvent("d", emitterIdB, None, Set(), 0L, timestamp(0, 3), logIdB, logIdB, logIdA, 3L, 4L, 3L)

  def timestamp(a: Long = 0L, b: Long= 0L) = (a, b) match {
    case (0L, 0L) => VectorTime()
    case (a,  0L) => VectorTime(logIdA -> a)
    case (0L,  b) => VectorTime(logIdB -> b)
    case (a,   b) => VectorTime(logIdA -> a, logIdB -> b)
  }

  def event(payload: Any, sequenceNr: Long): DurableEvent =
    DurableEvent(payload, emitterIdA, None, Set(), 0L, timestamp(sequenceNr), logIdA, logIdA, logIdA, sequenceNr, sequenceNr, 0L)
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

  def unrecoveredEventsourcedActor(): ActorRef =
    system.actorOf(Props(new TestEventsourcedView(logProbe.ref, dstProbe.ref)))

  "An EventsourcedView" must {
    "recover from replayed events" in {
      val actor = unrecoveredEventsourcedActor()

      logProbe.expectMsg(LoadSnapshot(emitterIdA, actor, instanceId))
      actor ! LoadSnapshotSuccess(None, instanceId)
      logProbe.expectMsg(Replay(1, actor, instanceId))

      actor ! Replaying(event1a, instanceId)
      actor ! Replaying(event1b, instanceId)
      actor ! ReplaySuccess(instanceId)

      dstProbe.expectMsg(("a", event1a.vectorTimestamp, event1a.vectorTimestamp, event1a.sequenceNr))
      dstProbe.expectMsg(("b", event1b.vectorTimestamp, event1b.vectorTimestamp, event1b.sequenceNr))
    }
    "retry recovery on failure" in {
      val actor = unrecoveredEventsourcedActor()

      logProbe.expectMsg(LoadSnapshot(emitterIdA, actor, instanceId))
      actor ! LoadSnapshotSuccess(None, instanceId)
      logProbe.expectMsg(Replay(1, actor, instanceId))

      actor ! Replaying(event1a, instanceId)
      actor ! Replaying(event1b.copy(payload = "boom"), instanceId)
      actor ! Replaying(event1c, instanceId)
      actor ! ReplaySuccess(instanceId)

      logProbe.expectMsg(LoadSnapshot(emitterIdA, actor, instanceId + 1))
      actor ! LoadSnapshotSuccess(None, instanceId + 1)
      logProbe.expectMsg(Replay(1, actor, instanceId + 1))

      actor ! Replaying(event1a, instanceId + 1)
      actor ! Replaying(event1b, instanceId + 1)
      actor ! Replaying(event1c, instanceId + 1)
      actor ! ReplaySuccess(instanceId + 1)

      dstProbe.expectMsg(("a", event1a.vectorTimestamp, event1a.vectorTimestamp, event1a.sequenceNr))
      dstProbe.expectMsg(("a", event1a.vectorTimestamp, event1a.vectorTimestamp, event1a.sequenceNr))
      dstProbe.expectMsg(("b", event1b.vectorTimestamp, event1b.vectorTimestamp, event1b.sequenceNr))
      dstProbe.expectMsg(("c", event1c.vectorTimestamp, event1c.vectorTimestamp, event1c.sequenceNr))
    }
    "stash commands during recovery and handle them after initial recovery" in {
      val actor = unrecoveredEventsourcedActor()

      actor ! Ping(1)
      actor ! Replaying(event1a, instanceId)
      actor ! Ping(2)
      actor ! Replaying(event1b, instanceId)
      actor ! Ping(3)
      actor ! ReplaySuccess(instanceId)

      dstProbe.expectMsg(("a", event1a.vectorTimestamp, event1a.vectorTimestamp, event1a.sequenceNr))
      dstProbe.expectMsg(("b", event1b.vectorTimestamp, event1b.vectorTimestamp, event1b.sequenceNr))
      dstProbe.expectMsg(Pong(1))
      dstProbe.expectMsg(Pong(2))
      dstProbe.expectMsg(Pong(3))
    }
    "stash commands during recovery and handle them after retried recovery" in {
      val actor = unrecoveredEventsourcedActor()

      logProbe.expectMsg(LoadSnapshot(emitterIdA, actor, instanceId))
      actor ! LoadSnapshotSuccess(None, instanceId)
      logProbe.expectMsg(Replay(1, actor, instanceId))

      actor ! Replaying(event1a, instanceId)
      actor ! Ping(1)
      actor ! Replaying(event1b.copy(payload = "boom"), instanceId)
      actor ! Ping(2)
      actor ! Replaying(event1c, instanceId)
      actor ! ReplaySuccess(instanceId)

      logProbe.expectMsg(LoadSnapshot(emitterIdA, actor, instanceId + 1))
      actor ! LoadSnapshotSuccess(None, instanceId + 1)
      logProbe.expectMsg(Replay(1, actor, instanceId + 1))

      actor ! Replaying(event1a, instanceId + 1)
      actor ! Replaying(event1b, instanceId + 1)
      actor ! Replaying(event1c, instanceId + 1)
      actor ! ReplaySuccess(instanceId + 1)

      dstProbe.expectMsg(("a", event1a.vectorTimestamp, event1a.vectorTimestamp, event1a.sequenceNr))
      dstProbe.expectMsg(("a", event1a.vectorTimestamp, event1a.vectorTimestamp, event1a.sequenceNr))
      dstProbe.expectMsg(("b", event1b.vectorTimestamp, event1b.vectorTimestamp, event1b.sequenceNr))
      dstProbe.expectMsg(("c", event1c.vectorTimestamp, event1c.vectorTimestamp, event1c.sequenceNr))
      dstProbe.expectMsg(Pong(1))
      dstProbe.expectMsg(Pong(2))
    }
    "ignore live events that have already been consumed during recovery" in {
      val actor = unrecoveredEventsourcedActor()
      actor ! Replaying(event2a, instanceId)
      actor ! Written(event2b) // live event
      actor ! Written(event2c) // live event
      actor ! Written(event2d) // live event
      actor ! Replaying(event2b, instanceId)
      actor ! Replaying(event2c, instanceId)
      actor ! ReplaySuccess(instanceId)
      dstProbe.expectMsg(("a", event2a.vectorTimestamp, event2a.vectorTimestamp, event2a.sequenceNr))
      dstProbe.expectMsg(("b", event2b.vectorTimestamp, timestamp(2, 1), event2b.sequenceNr))
      dstProbe.expectMsg(("c", event2c.vectorTimestamp, timestamp(3, 2), event2c.sequenceNr))
      dstProbe.expectMsg(("d", event2d.vectorTimestamp, timestamp(4, 3), event2d.sequenceNr))
    }
  }
}
