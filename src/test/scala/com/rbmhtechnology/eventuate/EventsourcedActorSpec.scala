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

import scala.util._

import akka.actor._
import akka.testkit._

import org.scalatest._

object EventsourcedActorSpec {
  import DurableEvent._

  val replicaIdA = "A"
  val replicaIdB = "B"

  val processIdA = processId(replicaIdA)
  val processIdB = processId(replicaIdB)

  val logId = "log"

  case class Cmd(payload: Any, num: Int = 1)
  case class CmdDelayed(payload: Any)
  case class Ping(i: Int)
  case class Pong(i: Int)

  class TestEventsourcedActor(
      val logProbe: ActorRef,
      val dstProbe: ActorRef,
      val errProbe: ActorRef,
      override val stateSync: Boolean) extends EventsourcedActor {

    val replicaId = EventsourcedActorSpec.replicaIdA
    val eventLog = logProbe

    override val onCommand: Receive = {
      case "boom" => throw boom
      case Ping(i) => dstProbe ! Pong(i)
      case "test-handler-order" =>
        persist("a")(r => dstProbe ! ((s"${r.get}-1", currentTime, lastVectorTimestamp, lastSequenceNr)))
        persist("b")(r => dstProbe ! ((s"${r.get}-2", currentTime, lastVectorTimestamp, lastSequenceNr)))
      case "test-multi-persist" =>
        val handler = (r: Try[String]) => dstProbe ! ((r.get, currentTime, lastVectorTimestamp, lastSequenceNr))
        persistN(Seq("a", "b", "c"), handler)(handler)
      case CmdDelayed(p) =>
        delay(p)(p => dstProbe ! ((p, currentTime, lastVectorTimestamp, lastSequenceNr)))
      case Cmd(p, num) => 1 to num foreach { i =>
        persist(s"${p}-${i}") {
          case Success("boom") => throw boom
          case Success(evt) => dstProbe ! ((evt, currentTime, lastVectorTimestamp, lastSequenceNr))
          case Failure(err) => errProbe ! ((err, currentTime, lastVectorTimestamp, lastSequenceNr))
        }
      }
    }

    override val onEvent: Receive = {
      case "boom" => throw boom
      case evt if evt != "x" => dstProbe ! ((evt, currentTime, lastVectorTimestamp, lastSequenceNr))
    }
  }

  class TestStashingActor(
      val logProbe: ActorRef,
      val dstProbe: ActorRef,
      val errProbe: ActorRef,
      override val stateSync: Boolean) extends EventsourcedActor {

    val replicaId = EventsourcedActorSpec.replicaIdA
    val eventLog = logProbe

    var stashing = false

    override val onCommand: Receive = {
      case "boom" =>
        throw boom
      case "stash-on" =>
        stashing = true
      case "stash-off" =>
        stashing = false
      case "unstash" =>
        unstashAll()
      case Ping(i) if stashing =>
        stash()
      case Ping(i) =>
        dstProbe ! Pong(i)
      case Cmd(p, num) => 1 to num foreach { i =>
        persist(s"${p}-${i}") {
          case Success(evt) => dstProbe ! evt
          case Failure(err) => errProbe ! err
        }
      }
    }

    override val onEvent: Receive = {
      case evt => dstProbe ! evt
    }
  }

  def eventA(payload: Any, sequenceNr: Long, timestamp: VectorTime): DurableEvent =
    DurableEvent(payload, 0L, timestamp, replicaIdA, None, Set(), logId, logId, sequenceNr, sequenceNr)

  def eventB(payload: Any, sequenceNr: Long, timestamp: VectorTime): DurableEvent =
    DurableEvent(payload, 0L, timestamp, replicaIdB, None, Set(), logId, logId, sequenceNr, sequenceNr)

  def timestampA(timeA: Long): VectorTime =
    VectorTime(processIdA -> timeA)

  def timestampAB(timeA: Long, timeB: Long): VectorTime =
    VectorTime(processIdA -> timeA, processIdB -> timeB)
}

class EventsourcedActorSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  import EventsourcedActorSpec._
  import EventsourcingProtocol._

  var instanceId: Int = _
  var logProbe: TestProbe = _
  var dstProbe: TestProbe = _
  var errProbe: TestProbe = _

  override def beforeEach(): Unit = {
    instanceId = Eventsourced.instanceIdCounter.get
    logProbe = TestProbe()
    dstProbe = TestProbe()
    errProbe = TestProbe()
  }

  override def afterAll: Unit =
    TestKit.shutdownActorSystem(system)

  def unrecoveredActor(stateSync: Boolean = true): ActorRef =
    system.actorOf(Props(new TestEventsourcedActor(logProbe.ref, dstProbe.ref, errProbe.ref, stateSync)))

  def recoveredActor(stateSync: Boolean = true): ActorRef = {
    val actor = unrecoveredActor(stateSync)
    logProbe.expectMsg(Replay(1, actor, instanceId))
    actor ! ReplaySuccess(instanceId)
    actor
  }

  def stashingActor(stateSync: Boolean = true): ActorRef = {
    val actor = system.actorOf(Props(new TestStashingActor(logProbe.ref, dstProbe.ref, errProbe.ref, stateSync)))
    logProbe.expectMsg(Replay(1, actor, instanceId))
    actor ! ReplaySuccess(instanceId)
    actor
  }

  def processWrite(actor: ActorRef, snr: Long): Unit = {
    val write = logProbe.expectMsgClass(classOf[Write])
    actor ! WriteSuccess(write.events(0).copy(targetLogSequenceNr = snr), instanceId)
  }

  "An EventsourcedActor" must {
    "recover from replayed local events" in {
      val actor = unrecoveredActor()
      logProbe.expectMsg(Replay(1, actor, instanceId))
      actor ! Replaying(eventA("a", 1, timestampAB(1, 0)), instanceId)
      actor ! Replaying(eventA("b", 2, timestampAB(2, 0)), instanceId)
      actor ! ReplaySuccess(instanceId)
      dstProbe.expectMsg(("a", timestampAB(1, 0), timestampAB(1, 0), 1))
      dstProbe.expectMsg(("b", timestampAB(2, 0), timestampAB(2, 0), 2))
    }
    "recover from replayed local and foreign events" in {
      val actor = unrecoveredActor()
      actor ! Replaying(eventA("a", 1, timestampAB(1, 0)), instanceId)
      actor ! Replaying(eventB("b", 2, timestampAB(0, 1)), instanceId)
      actor ! Replaying(eventB("c", 3, timestampAB(0, 2)), instanceId)
      actor ! Replaying(eventA("d", 4, timestampAB(2, 0)), instanceId)
      actor ! Replaying(eventA("e", 5, timestampAB(3, 0)), instanceId)
      actor ! Replaying(eventA("f", 6, timestampAB(4, 0)), instanceId)
      actor ! Replaying(eventA("g", 7, timestampAB(7, 2)), instanceId)
      // h with snr = 8 not persisted because of write failure
      // i with snr = 9 not persisted because of write failure
      actor ! Replaying(eventA("j", 10, timestampAB(10, 2)), instanceId)
      actor ! ReplaySuccess(instanceId)
      dstProbe.expectMsg(("a", timestampAB(1, 0), timestampAB(1, 0), 1))
      dstProbe.expectMsg(("b", timestampAB(2, 1), timestampAB(0, 1), 2))
      dstProbe.expectMsg(("c", timestampAB(3, 2), timestampAB(0, 2), 3))
      dstProbe.expectMsg(("d", timestampAB(4, 2), timestampAB(2, 0), 4))
      dstProbe.expectMsg(("e", timestampAB(5, 2), timestampAB(3, 0), 5))
      dstProbe.expectMsg(("f", timestampAB(6, 2), timestampAB(4, 0), 6))
      dstProbe.expectMsg(("g", timestampAB(7, 2), timestampAB(7, 2), 7))
      dstProbe.expectMsg(("j", timestampAB(10, 2), timestampAB(10, 2), 10))
    }
    "retry recovery on failure" in {
      val actor = unrecoveredActor()
      logProbe.expectMsg(Replay(1, actor, instanceId))
      actor ! Replaying(eventA("a", 1, timestampAB(1, 0)), instanceId)
      actor ! Replaying(eventA("boom", 2, timestampAB(3, 0)), instanceId)
      actor ! Replaying(eventA("c", 3, timestampAB(2, 0)), instanceId)
      actor ! ReplaySuccess(instanceId)
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
      val actor = unrecoveredActor()
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
      val actor = unrecoveredActor()
      logProbe.expectMsg(Replay(1, actor, instanceId))
      actor ! Replaying(eventA("a", 1, timestampAB(1, 0)), instanceId)
      actor ! Ping(1)
      actor ! Replaying(eventA("boom", 2, timestampAB(1, 0)), instanceId)
      actor ! Ping(2)
      actor ! Replaying(eventA("c", 3, timestampAB(1, 0)), instanceId)
      actor ! ReplaySuccess(instanceId)
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
      val actor = unrecoveredActor()
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
  }

  "An EventsourcedActor" when {
    "receiving unhandled events during replay" must {
      "not update the vector clock if the event has been emitted by another actor" in {
        val actor = unrecoveredActor()
        actor ! Replaying(eventA("a", 1, timestampAB(1, 0)), instanceId)
        actor ! Replaying(eventB("x", 2, timestampAB(0, 1)), instanceId)
        actor ! Replaying(eventA("c", 3, timestampAB(2, 0)), instanceId)
        actor ! ReplaySuccess(instanceId)
        dstProbe.expectMsg(("a", timestampAB(1, 0), timestampAB(1, 0), 1))
        dstProbe.expectMsg(("c", timestampAB(2, 0), timestampAB(2, 0), 3))
      }
      "only update the vector clock's local time if the event has been emitted by itself" in {
        val actor = unrecoveredActor()
        actor ! Replaying(eventA("a", 1, timestampAB(1, 0)), instanceId)
        actor ! Replaying(eventA("x", 3, timestampAB(3, 2)), instanceId)
        // The event log ensures that timestamp (0,1) will never come after
        // (2,2) but we use it here for testing vector clock updates ...
        actor ! Replaying(eventB("c", 4, timestampAB(0, 1)), instanceId)
        actor ! ReplaySuccess(instanceId)
        dstProbe.expectMsg(("a", timestampAB(1, 0), timestampAB(1, 0), 1))
        dstProbe.expectMsg(("c", timestampAB(4, 1), timestampAB(0, 1), 4))
      }
    }
  }


    "An EventsourcedActor" when {
    "in stateSync = true mode" must {
      "stash further commands while persistence is in progress" in {
        val actor = recoveredActor(stateSync = true)
        actor ! Cmd("a", 2)
        actor ! Ping(1)
        actor ! Ping(2)
        val write = logProbe.expectMsgClass(classOf[Write])
        write.events(0).payload should be("a-1")
        write.events(1).payload should be("a-2")
        write.events(0).vectorTimestamp should be(timestampA(1))
        write.events(1).vectorTimestamp should be(timestampA(2))
        actor ! WriteSuccess(write.events(0).copy(targetLogSequenceNr = 1L), instanceId)
        actor ! WriteSuccess(write.events(1).copy(targetLogSequenceNr = 2L), instanceId)
        dstProbe.expectMsg(("a-1", timestampA(2), timestampA(1), 1))
        dstProbe.expectMsg(("a-2", timestampA(2), timestampA(2), 2))
        dstProbe.expectMsg(Pong(1))
        dstProbe.expectMsg(Pong(2))
      }
      "process further commands if persist is aborted by exception in persist handler" in {
        val actor = recoveredActor(stateSync = true)
        actor ! Cmd("a", 2)
        actor ! Cmd("b", 2)
        val write1 = logProbe.expectMsgClass(classOf[Write])
        actor ! WriteSuccess(write1.events(0).copy(targetLogSequenceNr = 1L, payload = "boom"), instanceId)
        actor ! WriteSuccess(write1.events(1).copy(targetLogSequenceNr = 2L), instanceId)
        logProbe.expectMsg(Replay(1, actor, instanceId + 1))
        actor ! Replaying(write1.events(0).copy(targetLogSequenceNr = 1L), instanceId + 1)
        actor ! Replaying(write1.events(1).copy(targetLogSequenceNr = 2L), instanceId + 1)
        actor ! ReplaySuccess(instanceId + 1)
        val write2 = logProbe.expectMsgClass(classOf[Write])
        write2.events(0).payload should be("b-1")
        write2.events(1).payload should be("b-2")
        actor ! WriteSuccess(write2.events(0).copy(targetLogSequenceNr = 3L), instanceId + 1)
        actor ! WriteSuccess(write2.events(1).copy(targetLogSequenceNr = 4L), instanceId + 1)
        dstProbe.expectMsg(("a-1", timestampA(1), timestampA(1), 1))
        dstProbe.expectMsg(("a-2", timestampA(2), timestampA(2), 2))
        dstProbe.expectMsg(("b-1", timestampA(4), timestampA(3), 3))
        dstProbe.expectMsg(("b-2", timestampA(4), timestampA(4), 4))
      }
      "support user stash operations" in {
        val actor = stashingActor(stateSync = true)

        actor ! Cmd("a", 1)
        actor ! "stash-on"
        actor ! Ping(1)
        actor ! "stash-off"
        actor ! Ping(2)

        processWrite(actor, 1)

        actor ! Cmd("b", 1)
        actor ! "unstash"

        processWrite(actor, 1)

        dstProbe.expectMsg("a-1")
        dstProbe.expectMsg(Pong(2))
        dstProbe.expectMsg("b-1")
        dstProbe.expectMsg(Pong(1))

        actor ! Cmd("c", 1)
        actor ! "stash-on"
        actor ! Ping(3)
        actor ! "stash-off"
        actor ! Ping(4)

        processWrite(actor, 3)

        actor ! "unstash"
        actor ! Cmd("d", 1)

        processWrite(actor, 4)

        dstProbe.expectMsg("c-1")
        dstProbe.expectMsg(Pong(4))
        dstProbe.expectMsg(Pong(3))
        dstProbe.expectMsg("d-1")
      }
      "support user stash operations under failure conditions" in {
        val actor = stashingActor(stateSync = true)

        actor ! Cmd("a", 1)
        actor ! "stash-on"
        actor ! Ping(1)
        actor ! "stash-off"
        actor ! "boom"
        actor ! Ping(2)

        processWrite(actor, 1)
        dstProbe.expectMsg("a-1")

        logProbe.expectMsg(Replay(1, actor, instanceId + 1))
        actor ! Replaying(eventA("a-1", 1, timestampAB(1, 0)), instanceId + 1)
        actor ! ReplaySuccess(instanceId + 1)

        dstProbe.expectMsg("a-1")
        dstProbe.expectMsg(Pong(1))
        dstProbe.expectMsg(Pong(2))
      }
    }
    "in stateSync = false mode" must {
      "process further commands while persistence is in progress" in {
        val actor = recoveredActor(stateSync = false)
        actor ! Cmd("a", 2)
        actor ! Ping(1)
        val write = logProbe.expectMsgClass(classOf[Write])
        write.events(0).payload should be("a-1")
        write.events(1).payload should be("a-2")
        write.events(0).vectorTimestamp should be(timestampA(1))
        write.events(1).vectorTimestamp should be(timestampA(2))
        actor ! WriteSuccess(write.events(0).copy(targetLogSequenceNr = 1L), instanceId)
        actor ! WriteSuccess(write.events(1).copy(targetLogSequenceNr = 2L), instanceId)
        dstProbe.expectMsg(Pong(1))
        dstProbe.expectMsg(("a-1", timestampA(2), timestampA(1), 1))
        dstProbe.expectMsg(("a-2", timestampA(2), timestampA(2), 2))
      }
      "process further commands if persist is aborted by exception in command handler" in {
        val actor = recoveredActor(stateSync = false)
        actor ! Cmd("a", 2)
        actor ! "boom"
        actor ! Cmd("b", 2)
        val write1 = logProbe.expectMsgClass(classOf[Write])
        actor ! WriteSuccess(write1.events(0).copy(targetLogSequenceNr = 1L, payload = "boom"), instanceId)
        actor ! WriteSuccess(write1.events(1).copy(targetLogSequenceNr = 2L), instanceId)
        logProbe.expectMsg(Replay(1, actor, instanceId + 1))
        actor ! Replaying(write1.events(0).copy(targetLogSequenceNr = 1L), instanceId + 1)
        actor ! Replaying(write1.events(1).copy(targetLogSequenceNr = 2L), instanceId + 1)
        actor ! ReplaySuccess(instanceId + 1)
        val write2 = logProbe.expectMsgClass(classOf[Write])
        write2.events(0).payload should be("b-1")
        write2.events(1).payload should be("b-2")
        actor ! WriteSuccess(write2.events(0).copy(targetLogSequenceNr = 3L), instanceId + 1)
        actor ! WriteSuccess(write2.events(1).copy(targetLogSequenceNr = 4L), instanceId + 1)
        dstProbe.expectMsg(("a-1", timestampA(1), timestampA(1), 1))
        dstProbe.expectMsg(("a-2", timestampA(2), timestampA(2), 2))
        dstProbe.expectMsg(("b-1", timestampA(4), timestampA(3), 3))
        dstProbe.expectMsg(("b-2", timestampA(4), timestampA(4), 4))
      }
      "delay commands relative to events" in {
        val actor = recoveredActor(stateSync = false)
        actor ! Cmd("a")
        actor ! CmdDelayed("b")
        actor ! Cmd("c")
        val write1 = logProbe.expectMsgClass(classOf[Write])
        val write2 = logProbe.expectMsgClass(classOf[Write])
        actor ! WriteSuccess(write1.events(0).copy(targetLogSequenceNr = 1L), instanceId)
        dstProbe.expectMsg(("a-1", timestampA(2), timestampA(1), 1))
        dstProbe.expectMsg(("b", timestampA(2), timestampA(1), 1))
        actor ! WriteSuccess(write2.events(0).copy(targetLogSequenceNr = 2L), instanceId)
        dstProbe.expectMsg(("c-1", timestampA(2), timestampA(2), 2))
      }
    }
    "in any mode" must {
      "handle foreign events while persistence is in progress" in {
        val actor = recoveredActor(stateSync = true)
        actor ! Cmd("a", 2)
        val write = logProbe.expectMsgClass(classOf[Write])
        write.events(0).payload should be("a-1")
        write.events(1).payload should be("a-2")
        write.events(0).vectorTimestamp should be(timestampA(1))
        write.events(1).vectorTimestamp should be(timestampA(2))
        actor ! Written(eventB("b-1", 1, timestampAB(0, 1)))
        actor ! Written(eventB("b-2", 2, timestampAB(0, 2)))
        actor ! WriteSuccess(write.events(0).copy(targetLogSequenceNr = 3L), instanceId)
        actor ! WriteSuccess(write.events(1).copy(targetLogSequenceNr = 4L), instanceId)
        dstProbe.expectMsg(("b-1", timestampAB(3, 1), timestampAB(0, 1), 1))
        dstProbe.expectMsg(("b-2", timestampAB(4, 2), timestampAB(0, 2), 2))
        dstProbe.expectMsg(("a-1", timestampAB(4, 2), timestampA(1), 3))
        dstProbe.expectMsg(("a-2", timestampAB(4, 2), timestampA(2), 4))
      }
      "invoke persist handler in correct order" in {
        val actor = recoveredActor(stateSync = true)
        actor ! "test-handler-order"
        val write = logProbe.expectMsgClass(classOf[Write])
        write.events(0).payload should be("a")
        write.events(1).payload should be("b")
        actor ! WriteSuccess(write.events(0).copy(targetLogSequenceNr = 1L), instanceId)
        actor ! WriteSuccess(write.events(1).copy(targetLogSequenceNr = 2L), instanceId)
        dstProbe.expectMsg(("a-1", timestampA(2), timestampA(1), 1))
        dstProbe.expectMsg(("b-2", timestampA(2), timestampA(2), 2))
      }
      "additionally invoke onLast handler for multi-persist" in {
        val actor = recoveredActor(stateSync = true)
        actor ! "test-multi-persist"
        val write = logProbe.expectMsgClass(classOf[Write])
        write.events(0).payload should be("a")
        write.events(1).payload should be("b")
        write.events(2).payload should be("c")
        actor ! WriteSuccess(write.events(0).copy(targetLogSequenceNr = 1L), instanceId)
        actor ! WriteSuccess(write.events(1).copy(targetLogSequenceNr = 2L), instanceId)
        actor ! WriteSuccess(write.events(2).copy(targetLogSequenceNr = 3L), instanceId)
        dstProbe.expectMsg(("a", timestampA(3), timestampA(1), 1))
        dstProbe.expectMsg(("b", timestampA(3), timestampA(2), 2))
        dstProbe.expectMsg(("c", timestampA(3), timestampA(3), 3))
        dstProbe.expectMsg(("c", timestampA(3), timestampA(3), 3))
      }
      "report failed writes to persist handler" in {
        val actor = recoveredActor(stateSync = true)
        actor ! Cmd("a", 2)
        val write = logProbe.expectMsgClass(classOf[Write])
        actor ! WriteFailure(write.events(0).copy(targetLogSequenceNr = 1L), boom, instanceId)
        actor ! WriteFailure(write.events(1).copy(targetLogSequenceNr = 2L), boom, instanceId)
        errProbe.expectMsg((boom, timestampA(2), timestampA(1), 1))
        errProbe.expectMsg((boom, timestampA(2), timestampA(2), 2))
      }
      "not send empty write commands to log" in {
        val actor = recoveredActor(stateSync = true)
        actor ! Ping(1)
        actor ! Cmd("a", 2)
        val write = logProbe.expectMsgClass(classOf[Write])
        write.events(0).payload should be("a-1")
        write.events(1).payload should be("a-2")
      }
      "timestamp events with the current system time" in {
        val now = System.currentTimeMillis
        val actor = recoveredActor(stateSync = true)
        actor ! Ping(1)
        actor ! Cmd("a", 2)
        val write = logProbe.expectMsgClass(classOf[Write])
        write.events(0).systemTimestamp should be >= now
        write.events(1).systemTimestamp should be >= now

      }
    }
  }
}
