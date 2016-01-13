/*
 * Copyright (C) 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
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
import akka.pattern.AskTimeoutException
import akka.testkit._

import com.rbmhtechnology.eventuate.ConfirmedDelivery._
import com.typesafe.config.ConfigFactory

import org.scalatest._

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.util._

object EventsourcedActorSpec {
  import EventsourcedViewSpec._

  val config = ConfigFactory.parseString("eventuate.log.write-timeout = 1s")
  val timeout = 0.2.seconds

  case class Cmd(payload: Any, num: Int = 1)
  case class Deliver(payload: Any)
  case class DeliverRequested(payload: Any)
  case class State(state: Vector[String])

  class TestEventsourcedActor(
    val logProbe: ActorRef,
    val cmdProbe: ActorRef,
    val evtProbe: ActorRef,
    override val stateSync: Boolean) extends EventsourcedActor {

    val id = emitterIdA
    val eventLog = logProbe

    override def onCommand = {
      case "boom"   => throw boom
      case "status" => cmdProbe ! (("status", lastVectorTimestamp, currentVectorTime, lastSequenceNr))
      case Ping(i)  => cmdProbe ! Pong(i)
      case "test-handler-order" =>
        persist("a")(r => cmdProbe ! ((s"${r.get}-1", lastVectorTimestamp, currentVectorTime, lastSequenceNr)))
        persist("b")(r => cmdProbe ! ((s"${r.get}-2", lastVectorTimestamp, currentVectorTime, lastSequenceNr)))
      case "test-multi-persist" =>
        val handler = (r: Try[String]) => cmdProbe ! ((r.get, currentVectorTime, lastVectorTimestamp, lastSequenceNr))
        persistN(Seq("a", "b", "c"), handler)(handler)
      case Cmd(p, num) => 1 to num foreach { i =>
        persist(s"${p}-${i}") {
          case Success(evt) =>
          case Failure(err) =>
            cmdProbe ! ((err, lastVectorTimestamp, currentVectorTime, lastSequenceNr))
        }
      }
    }

    override def onEvent = {
      case "boom"            => throw boom
      case evt if evt != "x" => evtProbe ! ((evt, lastVectorTimestamp, currentVectorTime, lastSequenceNr))
    }

    override def unhandled(message: Any): Unit = message match {
      case msg: String => cmdProbe ! msg
      case msg         => super.unhandled(msg)
    }
  }

  class TestStashingActor(
    val logProbe: ActorRef,
    val msgProbe: ActorRef,
    override val stateSync: Boolean) extends EventsourcedActor {

    val id = emitterIdA
    val eventLog = logProbe

    var stashing = false

    override def onCommand = {
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
        msgProbe ! Pong(i)
      case Cmd(p, _) =>
        persist(p) {
          case Success(evt) =>
          case Failure(err) => msgProbe ! err
        }
    }

    override def onEvent = {
      case "unstash" =>
        unstashAll()
      case evt =>
        msgProbe ! evt
    }
  }

  class TestSnapshotActor(
    val logProbe: ActorRef,
    val cmdProbe: ActorRef,
    val evtProbe: ActorRef) extends EventsourcedActor with ConfirmedDelivery {

    val id = emitterIdA
    val eventLog = logProbe

    var state: Vector[String] = Vector.empty

    override def onCommand = {
      case "boom" =>
        throw boom
      case "snap" =>
        save(State(state)) {
          case Success(md)  => cmdProbe ! md
          case Failure(err) => cmdProbe ! err
        }
      case Cmd(p: String, _) =>
        persist(p) {
          case Success(evt) =>
          case Failure(err) => cmdProbe ! err
        }
      case Deliver(p) =>
        persist(DeliverRequested(p)) {
          case Success(evt) =>
          case Failure(err) => cmdProbe ! err
        }
    }

    override def onEvent = {
      case evt: String =>
        state = state :+ evt
        evtProbe ! message(state)
      case DeliverRequested(p: String) =>
        deliver(lastSequenceNr.toString, message(p), cmdProbe.path)
    }

    override def onSnapshot = {
      case State(s) =>
        state = s
        evtProbe ! message(s)
    }

    private def message(payload: Any) =
      (payload, lastVectorTimestamp, currentVectorTime, lastSequenceNr)
  }

  class TestCausalityActor(
    val logProbe: ActorRef,
    val cmdProbe: ActorRef,
    val evtProbe: ActorRef,
    override val sharedClockEntry: Boolean) extends EventsourcedActor {

    val id = emitterIdA
    val eventLog = logProbe

    override def onCommand = {
      case Cmd(p: String, _) =>
        persist(p) {
          case Success(evt) =>
          case Failure(err) => cmdProbe ! err
        }
    }

    override def onEvent = {
      case evt => evtProbe ! ((evt, lastVectorTimestamp, currentVectorTime, lastSequenceNr))
    }
  }
}

class EventsourcedActorSpec extends TestKit(ActorSystem("test", EventsourcedActorSpec.config))
  with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  import EventsourcedViewSpec._
  import EventsourcedActorSpec._
  import EventsourcingProtocol._

  var instanceId: Int = _
  var logProbe: TestProbe = _
  var cmdProbe: TestProbe = _
  var evtProbe: TestProbe = _

  override def beforeEach(): Unit = {
    instanceId = EventsourcedView.instanceIdCounter.get
    logProbe = TestProbe()
    cmdProbe = TestProbe()
    evtProbe = TestProbe()
  }

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  def unrecoveredEventsourcedActor(): ActorRef =
    unrecoveredEventsourcedActor(stateSync = true)

  def unrecoveredEventsourcedActor(stateSync: Boolean): ActorRef =
    system.actorOf(Props(new TestEventsourcedActor(logProbe.ref, cmdProbe.ref, evtProbe.ref, stateSync)))

  def unrecoveredSnapshotActor(): ActorRef =
    system.actorOf(Props(new TestSnapshotActor(logProbe.ref, cmdProbe.ref, evtProbe.ref)))

  def unrecoveredCausalityActor(sharedClockEntry: Boolean): ActorRef =
    system.actorOf(Props(new TestCausalityActor(logProbe.ref, cmdProbe.ref, evtProbe.ref, sharedClockEntry)))

  def recoveredEventsourcedActor(stateSync: Boolean): ActorRef =
    processRecover(unrecoveredEventsourcedActor(stateSync))

  def recoveredSnapshotActor(): ActorRef =
    processRecover(unrecoveredSnapshotActor())

  def recoveredCausalityActor(sharedClockEntry: Boolean): ActorRef =
    processRecover(unrecoveredCausalityActor(sharedClockEntry))

  def recoveredStashingActor(probe: ActorRef, stateSync: Boolean): ActorRef =
    processRecover(system.actorOf(Props(new TestStashingActor(logProbe.ref, probe, stateSync))))

  def processRecover(actor: ActorRef): ActorRef = {
    logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
    logProbe.sender() ! LoadSnapshotSuccess(None, instanceId)
    logProbe.expectMsg(Replay(1L, Some(actor), instanceId))
    logProbe.sender() ! ReplaySuccess(Nil, 0L, instanceId)
    actor
  }

  def processWrite(snr: Long): Unit = {
    val write = logProbe.expectMsgClass(classOf[Write])
    logProbe.sender() ! WriteSuccess(Seq(event(write.events.head.payload, snr)), write.correlationId, instanceId)
  }

  "An EventsourcedActor" when {
    "in stateSync = true mode" must {
      "stash further commands while persistence is in progress" in {
        val actor = recoveredEventsourcedActor(stateSync = true)
        actor ! Cmd("a", 2)
        actor ! Ping(1)
        actor ! Ping(2)

        val write = logProbe.expectMsgClass(classOf[Write])
        write.events(0).payload should be("a-1")
        write.events(1).payload should be("a-2")
        logProbe.sender() ! WriteSuccess(Seq(event("a-1", 1L), event("a-2", 2L)), write.correlationId, instanceId)

        evtProbe.expectMsg(("a-1", timestamp(1), timestamp(1), 1))
        evtProbe.expectMsg(("a-2", timestamp(2), timestamp(2), 2))
        cmdProbe.expectMsg(Pong(1))
        cmdProbe.expectMsg(Pong(2))
      }
      "process further commands if persist is aborted by exception in persist handler" in {
        val actor = recoveredEventsourcedActor(stateSync = true)
        actor ! Cmd("a", 2)
        actor ! Cmd("b", 2)

        val write1 = logProbe.expectMsgClass(classOf[Write])
        logProbe.sender() ! WriteSuccess(Seq(event("boom", 1L), event("a-2", 2L)), write1.correlationId, instanceId)

        logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId + 1))
        logProbe.sender() ! LoadSnapshotSuccess(None, instanceId + 1)
        logProbe.expectMsg(Replay(1, Some(actor), instanceId + 1))
        logProbe.sender() ! ReplaySuccess(List(event("a-1", 1L), event("a-2", 2L)), 2L, instanceId + 1)
        logProbe.expectMsg(Replay(3, None, instanceId + 1))
        logProbe.sender() ! ReplaySuccess(Nil, 2L, instanceId + 1)

        val write2 = logProbe.expectMsgClass(classOf[Write])
        write2.events(0).payload should be("b-1")
        write2.events(1).payload should be("b-2")
        logProbe.sender() ! WriteSuccess(Seq(event("b-1", 3L), event("b-2", 4L)), write2.correlationId, instanceId + 1)

        evtProbe.expectMsg(("a-1", timestamp(1), timestamp(1), 1))
        evtProbe.expectMsg(("a-2", timestamp(2), timestamp(2), 2))
        evtProbe.expectMsg(("b-1", timestamp(3), timestamp(3), 3))
        evtProbe.expectMsg(("b-2", timestamp(4), timestamp(4), 4))
      }
      "fix #182" in {
        val probe = TestProbe()
        val actor = recoveredStashingActor(probe.ref, stateSync = true)

        actor ! Cmd("c")
        actor ! "stash-on"
        actor ! Ping(1)
        actor ! "stash-off"
        actor ! Ping(2)
        actor ! "unstash"
        actor ! Ping(3)

        processWrite(1)

        probe.expectMsg("c")
        probe.expectMsg(Pong(2))
        probe.expectMsg(Pong(1))
        probe.expectMsg(Pong(3))
      }
      "support user stash-unstash operations that are within a write" in {
        val probe = TestProbe()
        val actor = recoveredStashingActor(probe.ref, stateSync = true)

        actor ! Cmd("a")
        actor ! "stash-on"
        actor ! Ping(1)
        actor ! "stash-off"
        actor ! Ping(2)
        actor ! "unstash"

        processWrite(1)

        actor ! Cmd("b")

        processWrite(2)

        probe.expectMsg("a")
        probe.expectMsg(Pong(2))
        probe.expectMsg(Pong(1))
        probe.expectMsg("b")
      }
      "support repeated user stash-unstash operations that are within a write" in {
        val probe = TestProbe()
        val actor = recoveredStashingActor(probe.ref, stateSync = true)

        actor ! Cmd("a")
        actor ! "stash-on"
        actor ! Ping(1)
        actor ! "stash-off"
        actor ! Ping(2)
        actor ! "unstash"
        actor ! Ping(3)
        actor ! "stash-on"
        actor ! Ping(4)
        actor ! "stash-off"
        actor ! Ping(5)
        actor ! "unstash"

        processWrite(1)

        actor ! Cmd("b")

        processWrite(2)

        probe.expectMsg("a")
        probe.expectMsg(Pong(2))
        probe.expectMsg(Pong(1))
        probe.expectMsg(Pong(3))
        probe.expectMsg(Pong(5))
        probe.expectMsg(Pong(4))
        probe.expectMsg("b")
      }
      "support user stash-unstash operations that overlap with a write" in {
        val probe = TestProbe()
        val actor = recoveredStashingActor(probe.ref, stateSync = true)

        actor ! Cmd("a")
        actor ! "stash-on"
        actor ! Ping(1)
        actor ! "stash-off"
        actor ! Ping(2)

        processWrite(1)

        actor ! "unstash"
        actor ! Cmd("b")

        processWrite(2)

        probe.expectMsg("a")
        probe.expectMsg(Pong(2))
        probe.expectMsg(Pong(1))
        probe.expectMsg("b")
      }
      "support user stash-unstash operations that span writes" in {
        val probe = TestProbe()
        val actor = recoveredStashingActor(probe.ref, stateSync = true)

        actor ! Cmd("a")
        actor ! "stash-on"
        actor ! Ping(1)
        actor ! "stash-off"
        actor ! Ping(2)

        processWrite(1)

        actor ! Cmd("b")
        actor ! "unstash"

        processWrite(2)

        probe.expectMsg("a")
        probe.expectMsg(Pong(2))
        probe.expectMsg("b")
        probe.expectMsg(Pong(1))
      }
      "support user unstash operations in event handler" in {
        val probe = TestProbe()
        val actor = recoveredStashingActor(probe.ref, stateSync = true)

        actor ! Cmd("a")
        actor ! "stash-on"
        actor ! Ping(1)
        actor ! "stash-off"
        actor ! Ping(2)

        processWrite(1)

        actor ! Cmd("unstash")

        processWrite(2)

        actor ! Cmd("b")

        processWrite(3)

        probe.expectMsg("a")
        probe.expectMsg(Pong(2))
        probe.expectMsg(Pong(1))
        probe.expectMsg("b")
      }
      "support user stash-unstash operations that are within a write where unstash is the last operation" in {
        val probe = TestProbe()
        val actor = recoveredStashingActor(probe.ref, stateSync = true)

        actor ! Cmd("a")
        actor ! "stash-on"
        actor ! Ping(1)
        actor ! "stash-off"
        actor ! Ping(2)
        actor ! "unstash"

        processWrite(1)

        probe.expectMsg("a")
        probe.expectMsg(Pong(2))
        probe.expectMsg(Pong(1))
      }
      "support user stash-unstash operations that are outside a write where unstash is the last operation" in {
        val probe = TestProbe()
        val actor = recoveredStashingActor(probe.ref, stateSync = true)

        actor ! Ping(1)
        actor ! "stash-on"
        actor ! Ping(2)
        actor ! "stash-off"
        actor ! Ping(3)
        actor ! "unstash"

        probe.expectMsg(Pong(1))
        probe.expectMsg(Pong(3))
        probe.expectMsg(Pong(2))
      }
      "support user stash operations under failure conditions" in {
        val probe = TestProbe()
        val actor = recoveredStashingActor(probe.ref, stateSync = true)

        actor ! Cmd("a")
        actor ! "stash-on"
        actor ! Ping(1)
        actor ! "stash-off"
        actor ! "boom"
        actor ! Ping(2)

        processWrite(1)
        probe.expectMsg("a")

        logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId + 1))
        logProbe.sender() ! LoadSnapshotSuccess(None, instanceId + 1)
        logProbe.expectMsg(Replay(1L, Some(actor), instanceId + 1))
        logProbe.sender() ! ReplaySuccess(List(event("a", 1L)), 1L, instanceId + 1)
        logProbe.expectMsg(Replay(2L, None, instanceId + 1))
        logProbe.sender() ! ReplaySuccess(Nil, 1L, instanceId + 1)

        probe.expectMsg("a")
        probe.expectMsg(Pong(1))
        probe.expectMsg(Pong(2))
      }
    }
    "in stateSync = false mode" must {
      "process further commands while persistence is in progress" in {
        val actor = recoveredEventsourcedActor(stateSync = false)
        actor ! Cmd("a", 2)
        actor ! Ping(1)
        val write = logProbe.expectMsgClass(classOf[Write])
        write.events(0).payload should be("a-1")
        write.events(1).payload should be("a-2")
        logProbe.sender() ! WriteSuccess(Seq(event("a-1", 1L), event("a-2", 2L)), write.correlationId, instanceId)

        cmdProbe.expectMsg(Pong(1))
        evtProbe.expectMsg(("a-1", timestamp(1), timestamp(1), 1L))
        evtProbe.expectMsg(("a-2", timestamp(2), timestamp(2), 2))
      }
      "process further commands if persist is aborted by exception in command handler" in {
        val actor = recoveredEventsourcedActor(stateSync = false)
        actor ! Cmd("a", 2)
        actor ! "boom"
        actor ! Cmd("b", 2)

        val write1 = logProbe.expectMsgClass(classOf[Write])
        logProbe.sender() ! WriteSuccess(Seq(event("boom", 1L), event("a-2", 2L)), write1.correlationId, instanceId)

        logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId + 1))
        logProbe.sender() ! LoadSnapshotSuccess(None, instanceId + 1)
        logProbe.expectMsg(Replay(1L, Some(actor), instanceId + 1))
        logProbe.sender() ! ReplaySuccess(List(event("a-1", 1L), event("a-2", 2L)), 2L, instanceId + 1)
        logProbe.expectMsg(Replay(3L, None, instanceId + 1))
        logProbe.sender() ! ReplaySuccess(Nil, 2L, instanceId + 1)

        val write2 = logProbe.expectMsgClass(classOf[Write])
        write2.events(0).payload should be("b-1")
        write2.events(1).payload should be("b-2")
        logProbe.sender() ! WriteSuccess(Seq(event("b-1", 3L), event("b-2", 4L)), write2.correlationId, instanceId + 1)

        evtProbe.expectMsg(("a-1", timestamp(1), timestamp(1), 1))
        evtProbe.expectMsg(("a-2", timestamp(2), timestamp(2), 2))
        evtProbe.expectMsg(("b-1", timestamp(3), timestamp(3), 3))
        evtProbe.expectMsg(("b-2", timestamp(4), timestamp(4), 4))
      }
      "fix #182" in {
        val probe = TestProbe()
        val actor = recoveredStashingActor(probe.ref, stateSync = false)

        actor ! Cmd("c")
        actor ! "stash-on"
        actor ! Ping(1)
        actor ! "stash-off"
        actor ! Ping(2)
        actor ! "unstash"
        actor ! Ping(3)

        processWrite(1)

        probe.expectMsg(Pong(2))
        probe.expectMsg(Pong(1))
        probe.expectMsg(Pong(3))
        probe.expectMsg("c")
      }
      "support user stash-unstash operations that are within a write" in {
        val probe = TestProbe()
        val actor = recoveredStashingActor(probe.ref, stateSync = false)

        actor ! Cmd("a")
        actor ! "stash-on"
        actor ! Ping(1)
        actor ! "stash-off"
        actor ! Ping(2)
        actor ! "unstash"

        processWrite(1)

        actor ! Cmd("b")

        processWrite(2)

        probe.expectMsg(Pong(2))
        probe.expectMsg(Pong(1))
        probe.expectMsg("a")
        probe.expectMsg("b")

      }
      "support user stash-unstash operations that overlap with a write" in {
        val probe = TestProbe()
        val actor = recoveredStashingActor(probe.ref, stateSync = false)

        actor ! Cmd("a")
        actor ! "stash-on"
        actor ! Ping(1)
        actor ! "stash-off"
        actor ! Ping(2)

        processWrite(1)

        actor ! "unstash"
        actor ! Cmd("b")

        processWrite(2)

        probe.expectMsg(Pong(2))
        probe.expectMsg("a")
        probe.expectMsg(Pong(1))
        probe.expectMsg("b")
      }
      "support user stash-unstash operations that span writes" in {
        val probe = TestProbe()
        val actor = recoveredStashingActor(probe.ref, stateSync = false)

        actor ! Cmd("a")
        actor ! "stash-on"
        actor ! Ping(1)
        actor ! "stash-off"
        actor ! Ping(2)

        processWrite(1)

        actor ! Cmd("b")
        actor ! "unstash"

        processWrite(2)

        probe.expectMsg(Pong(2))
        probe.expectMsg("a")
        probe.expectMsg(Pong(1))
        probe.expectMsg("b")
      }
      "support user unstash operations in event handler" in {
        val probe = TestProbe()
        val actor = recoveredStashingActor(probe.ref, stateSync = false)

        actor ! Cmd("a")
        actor ! "stash-on"
        actor ! Ping(1)
        actor ! "stash-off"
        actor ! Ping(2)
        processWrite(1)

        actor ! Cmd("unstash")

        processWrite(2)

        actor ! Cmd("b")

        processWrite(3)

        probe.expectMsg(Pong(2))
        probe.expectMsg("a")
        probe.expectMsg(Pong(1))
        probe.expectMsg("b")
      }
      "support user stash-unstash operations where unstash is the last operation" in {
        val probe = TestProbe()
        val actor = recoveredStashingActor(probe.ref, stateSync = false)

        actor ! "stash-on"
        actor ! Ping(1)
        actor ! "stash-off"
        actor ! Ping(2)
        actor ! "unstash"

        probe.expectMsg(Pong(2))
        probe.expectMsg(Pong(1))
      }
      "support user stash operations under failure conditions" in {
        val probe = TestProbe()
        val actor = recoveredStashingActor(probe.ref, stateSync = false)

        actor ! Cmd("a")
        actor ! "stash-on"
        actor ! Ping(1)
        actor ! "stash-off"
        actor ! "boom"
        actor ! Ping(2)

        processWrite(1) // ignored

        logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId + 1))
        logProbe.sender() ! LoadSnapshotSuccess(None, instanceId + 1)
        logProbe.expectMsg(Replay(1L, Some(actor), instanceId + 1))
        logProbe.sender() ! ReplaySuccess(List(event("a", 1L)), 1L, instanceId + 1)
        logProbe.expectMsg(Replay(2L, None, instanceId + 1))
        logProbe.sender() ! ReplaySuccess(Nil, 1L, instanceId + 1)

        probe.expectMsg("a") // from replay
        probe.expectMsg(Pong(1))
        probe.expectMsg(Pong(2))
      }
    }
    "in any mode" must {
      "handle remote events while persistence is in progress" in {
        val actor = recoveredEventsourcedActor(stateSync = true)
        actor ! Cmd("a", 2)
        val write = logProbe.expectMsgClass(classOf[Write])
        write.events(0).payload should be("a-1")
        write.events(1).payload should be("a-2")

        val eventB1 = DurableEvent("b-1", emitterIdB, None, Set(), 0L, timestamp(0, 1), logIdB, logIdA, 1L)
        val eventB2 = DurableEvent("b-2", emitterIdB, None, Set(), 0L, timestamp(0, 2), logIdB, logIdA, 2L)

        val eventA1 = DurableEvent("a-1", emitterIdA, None, Set(), 0L, timestamp(3, 0), logIdA, logIdA, 3L)
        val eventA2 = DurableEvent("a-2", emitterIdA, None, Set(), 0L, timestamp(4, 0), logIdA, logIdA, 4L)

        actor ! Written(eventB1)
        actor ! Written(eventB2)
        logProbe.sender() ! WriteSuccess(Seq(eventA1, eventA2), write.correlationId, instanceId)

        evtProbe.expectMsg(("b-1", timestamp(0, 1), timestamp(1, 1), 1L))
        evtProbe.expectMsg(("b-2", timestamp(0, 2), timestamp(2, 2), 2L))
        evtProbe.expectMsg(("a-1", timestamp(3, 0), timestamp(3, 2), 3L))
        evtProbe.expectMsg(("a-2", timestamp(4, 0), timestamp(4, 2), 4L))
      }
      "invoke persist handler in correct order" in {
        val actor = recoveredEventsourcedActor(stateSync = true)
        actor ! "test-handler-order"

        val write = logProbe.expectMsgClass(classOf[Write])
        write.events(0).payload should be("a")
        write.events(1).payload should be("b")
        logProbe.sender() ! WriteSuccess(Seq(event("a", 1), event("b", 2)), write.correlationId, instanceId)

        evtProbe.expectMsg(("a", timestamp(1), timestamp(1), 1))
        cmdProbe.expectMsg(("a-1", timestamp(1), timestamp(1), 1))

        evtProbe.expectMsg(("b", timestamp(2), timestamp(2), 2))
        cmdProbe.expectMsg(("b-2", timestamp(2), timestamp(2), 2))
      }
      "additionally invoke onLast handler for multi-persist" in {
        val actor = recoveredEventsourcedActor(stateSync = true)
        actor ! "test-multi-persist"

        val write = logProbe.expectMsgClass(classOf[Write])
        write.events(0).payload should be("a")
        write.events(1).payload should be("b")
        write.events(2).payload should be("c")
        logProbe.sender() ! WriteSuccess(Seq(event("a", 1), event("b", 2), event("c", 3)), write.correlationId, instanceId)

        evtProbe.expectMsg(("a", timestamp(1), timestamp(1), 1))
        cmdProbe.expectMsg(("a", timestamp(1), timestamp(1), 1))

        evtProbe.expectMsg(("b", timestamp(2), timestamp(2), 2))
        cmdProbe.expectMsg(("b", timestamp(2), timestamp(2), 2))

        evtProbe.expectMsg(("c", timestamp(3), timestamp(3), 3))
        cmdProbe.expectMsg(("c", timestamp(3), timestamp(3), 3))
        cmdProbe.expectMsg(("c", timestamp(3), timestamp(3), 3))
      }
      "report failed writes to persist handler" in {
        val actor = recoveredEventsourcedActor(stateSync = true)
        actor ! Cmd("a", 2)

        val write = logProbe.expectMsgClass(classOf[Write])
        val event1 = write.events(0)
        val event2 = write.events(1)
        logProbe.sender() ! WriteFailure(Seq(event1, event2), boom, write.correlationId, instanceId)

        cmdProbe.expectMsg((boom, event1.vectorTimestamp, event1.vectorTimestamp, event1.localSequenceNr))
        cmdProbe.expectMsg((boom, event2.vectorTimestamp, event2.vectorTimestamp, event2.localSequenceNr))
      }
      "not send empty write commands to log" in {
        val actor = recoveredEventsourcedActor(stateSync = true)
        actor ! Ping(1)
        actor ! Cmd("a", 2)
        val write = logProbe.expectMsgClass(classOf[Write])
        write.events(0).payload should be("a-1")
        write.events(1).payload should be("a-2")
      }
      "not update clock if event is not handled" in {
        val actor = recoveredEventsourcedActor(stateSync = true)

        actor ! Written(event2b)
        actor ! Written(event2c.copy(payload = "x"))
        actor ! "status"
        actor ! Written(event2d)

        evtProbe.expectMsg(("b", event2b.vectorTimestamp, timestamp(2, 1), event2b.localSequenceNr))
        cmdProbe.expectMsg(("status", event2b.vectorTimestamp, timestamp(2, 1), event2b.localSequenceNr))
        evtProbe.expectMsg(("d", event2d.vectorTimestamp, timestamp(4, 3), event2d.localSequenceNr))
      }
      "must dispatch unhandled commands to the unhandled method" in {
        val actor = recoveredEventsourcedActor(stateSync = true)
        actor ! "unhandled-command"
        cmdProbe.expectMsg("unhandled-command")
      }
      "ignore duplicate replies from event log" in {
        val actor = recoveredEventsourcedActor(stateSync = true)
        actor.tell(Cmd("a", 1), cmdProbe.ref)

        val write = logProbe.expectMsgClass(classOf[Write])
        val event = write.events(0).copy(localLogId = logIdA, localSequenceNr = 1L)

        logProbe.sender() ! WriteSuccess(Seq(event.copy()), write.correlationId, instanceId)
        evtProbe.expectMsg((event.payload, event.vectorTimestamp, timestamp(1), event.localSequenceNr))
        logProbe.sender() ! WriteSuccess(Seq(event), write.correlationId, instanceId)
        evtProbe.expectNoMsg(timeout)
      }
      "apply Write replies and Written messages in received order" in {
        val probe = TestProbe()
        val actor = recoveredStashingActor(probe.ref, stateSync = false)

        actor ! Cmd("a", 1)
        actor ! Cmd("b", 1)
        actor ! Cmd("c", 1)

        processWrite(1)
        actor ! Written(event("x", 2))
        processWrite(3)
        actor ! Written(event("y", 4))
        processWrite(5)
        actor ! Written(event("z", 6))

        probe.expectMsg("a")
        probe.expectMsg("x")
        probe.expectMsg("b")
        probe.expectMsg("y")
        probe.expectMsg("c")
        probe.expectMsg("z")
      }
      "ignore WriteSuccess duplicates" in {
        val probe = TestProbe()
        val actor = recoveredStashingActor(probe.ref, stateSync = false)

        actor ! Cmd("a", 1)
        actor ! Cmd("b", 1)

        val write1 = logProbe.expectMsgClass(classOf[Write])
        logProbe.sender() ! WriteSuccess(Seq(event(write1.events.head.payload, 1)), write1.correlationId, instanceId)
        logProbe.sender() ! WriteSuccess(Seq(event(write1.events.head.payload, 1)), write1.correlationId, instanceId)

        val write2 = logProbe.expectMsgClass(classOf[Write])
        logProbe.sender() ! WriteSuccess(Seq(event(write2.events.head.payload, 2)), write2.correlationId, instanceId)

        probe.expectMsg("a")
        probe.expectMsg("b")
      }
      "ignore WriteFailure duplicates" in {
        val probe = TestProbe()
        val actor = recoveredStashingActor(probe.ref, stateSync = false)

        val e1 = new Exception("1")
        val e2 = new Exception("2")

        actor ! Cmd("a", 1)
        actor ! Cmd("b", 1)

        val write1 = logProbe.expectMsgClass(classOf[Write])
        logProbe.sender() ! WriteFailure(write1.events, e1, write1.correlationId, instanceId)
        logProbe.sender() ! WriteFailure(write1.events, e1, write1.correlationId, instanceId)

        val write2 = logProbe.expectMsgClass(classOf[Write])
        logProbe.sender() ! WriteFailure(write2.events, e2, write2.correlationId, instanceId)

        probe.expectMsg(e1)
        probe.expectMsg(e2)
      }
    }
  }

  "An EventsourcedActor" must {
    "recover from a snapshot" in {
      val actor = unrecoveredSnapshotActor()
      val snapshot = Snapshot(State(Vector("a", "b")), emitterIdA, event("b", 2), timestamp(2, 4))

      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
      logProbe.sender() ! LoadSnapshotSuccess(Some(snapshot), instanceId)
      logProbe.expectMsg(Replay(3L, Some(actor), instanceId))
      logProbe.sender() ! ReplaySuccess(Nil, 2L, instanceId)
      evtProbe.expectMsg((Vector("a", "b"), timestamp(2), timestamp(2, 4), 2))
    }
    "recover from a snapshot and remaining events" in {
      val actor = unrecoveredSnapshotActor()
      val snapshot = Snapshot(State(Vector("a", "b")), emitterIdA, event("b", 2), timestamp(2, 4))

      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
      logProbe.sender() ! LoadSnapshotSuccess(Some(snapshot), instanceId)
      logProbe.expectMsg(Replay(3L, Some(actor), instanceId))
      logProbe.sender() ! ReplaySuccess(List(event("c", 3L), event("d", 4L)), 4L, instanceId)
      evtProbe.expectMsg((Vector("a", "b"), timestamp(2), timestamp(2, 4), 2))
      evtProbe.expectMsg((Vector("a", "b", "c"), timestamp(3), timestamp(3, 4), 3))
      evtProbe.expectMsg((Vector("a", "b", "c", "d"), timestamp(4), timestamp(4, 4), 4))
    }
    "recover from a snapshot and deliver unconfirmed messages" in {
      val actor = unrecoveredSnapshotActor()
      val unconfirmed = Vector(
        DeliveryAttempt("3", "x", cmdProbe.ref.path),
        DeliveryAttempt("4", "y", cmdProbe.ref.path))
      val snapshot = Snapshot(State(Vector("a", "b")), emitterIdA, event("b", 2), timestamp(2, 4), deliveryAttempts = unconfirmed)

      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
      logProbe.sender() ! LoadSnapshotSuccess(Some(snapshot), instanceId)
      logProbe.expectMsg(Replay(3L, Some(actor), instanceId))
      logProbe.sender() ! ReplaySuccess(Nil, 2L, instanceId)
      evtProbe.expectMsg((Vector("a", "b"), timestamp(2), timestamp(2, 4), 2))
      cmdProbe.expectMsg("x")
      cmdProbe.expectMsg("y")
    }
    "recover from scratch if onSnapshot doesn't handle loaded snapshot" in {
      val actor = unrecoveredSnapshotActor()
      val snapshot = Snapshot("foo", emitterIdA, event("b", 2), timestamp(2, 4))

      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
      logProbe.sender() ! LoadSnapshotSuccess(Some(snapshot), instanceId)
      logProbe.expectMsg(Replay(1L, Some(actor), instanceId))
      logProbe.sender() ! ReplaySuccess(List(event("a", 1L), event("b", 2L)), 2L, instanceId)
      evtProbe.expectMsg((Vector("a"), timestamp(1), timestamp(1), 1))
      evtProbe.expectMsg((Vector("a", "b"), timestamp(2), timestamp(2), 2))
    }
    "save a snapshot" in {
      val event1 = DurableEvent("x", emitterIdB, None, Set(), 0L, timestamp(0, 1), logIdB, logIdA, 1L)
      val event2 = DurableEvent("a", emitterIdA, None, Set(), 0L, timestamp(2, 1), logIdA, logIdA, 2L)
      val event3 = DurableEvent("b", emitterIdA, None, Set(), 0L, timestamp(3, 1), logIdA, logIdA, 3L)

      val actor = recoveredSnapshotActor()
      actor ! Written(event1)
      evtProbe.expectMsg((Vector("x"), timestamp(0, 1), timestamp(1, 1), 1))
      actor ! Cmd("a")
      actor ! Cmd("b")

      val write1 = logProbe.expectMsgClass(classOf[Write])
      logProbe.sender() ! WriteSuccess(Seq(event2), write1.correlationId, instanceId)

      val write2 = logProbe.expectMsgClass(classOf[Write])
      logProbe.sender() ! WriteSuccess(Seq(event3), write2.correlationId, instanceId)

      evtProbe.expectMsg((Vector("x", "a"), timestamp(2, 1), timestamp(2, 1), 2))
      evtProbe.expectMsg((Vector("x", "a", "b"), timestamp(3, 1), timestamp(3, 1), 3))
      actor ! "snap"

      val snapshot = Snapshot(State(Vector("x", "a", "b")), emitterIdA, event3, timestamp(3, 1))
      logProbe.expectMsg(SaveSnapshot(snapshot, system.deadLetters, instanceId))
      logProbe.sender() ! SaveSnapshotSuccess(snapshot.metadata, instanceId)
      cmdProbe.expectMsg(snapshot.metadata)
    }
    "save a snapshot with unconfirmed messages" in {
      val actor = recoveredSnapshotActor()
      actor ! Cmd("a")
      actor ! Cmd("b")
      actor ! Deliver("x")
      actor ! Deliver("y")

      val write1 = logProbe.expectMsgClass(classOf[Write])
      logProbe.sender() ! WriteSuccess(Seq(event("a", 1)), write1.correlationId, instanceId)

      val write2 = logProbe.expectMsgClass(classOf[Write])
      logProbe.sender() ! WriteSuccess(Seq(event("b", 2)), write2.correlationId, instanceId)

      val write3 = logProbe.expectMsgClass(classOf[Write])
      logProbe.sender() ! WriteSuccess(Seq(event(DeliverRequested("x"), 3)), write3.correlationId, instanceId)

      val write4 = logProbe.expectMsgClass(classOf[Write])
      logProbe.sender() ! WriteSuccess(Seq(event(DeliverRequested("y"), 4)), write4.correlationId, instanceId)

      evtProbe.expectMsg((Vector("a"), timestamp(1), timestamp(1), 1))
      evtProbe.expectMsg((Vector("a", "b"), timestamp(2), timestamp(2), 2))
      cmdProbe.expectMsg(("x", timestamp(3), timestamp(3), 3))
      cmdProbe.expectMsg(("y", timestamp(4), timestamp(4), 4))
      actor ! "snap"

      val unconfirmed = Vector(
        DeliveryAttempt("3", ("x", timestamp(3), timestamp(3), 3), cmdProbe.ref.path),
        DeliveryAttempt("4", ("y", timestamp(4), timestamp(4), 4), cmdProbe.ref.path))
      val snapshot = Snapshot(State(Vector("a", "b")), emitterIdA, event(DeliverRequested("y"), 4), timestamp(4), deliveryAttempts = unconfirmed)

      logProbe.expectMsg(SaveSnapshot(snapshot, system.deadLetters, instanceId))
      logProbe.sender() ! SaveSnapshotSuccess(snapshot.metadata, instanceId)
      cmdProbe.expectMsg(snapshot.metadata)
    }
    "not save the same snapshot concurrently" in {
      val actor = recoveredSnapshotActor()
      actor ! "snap"
      actor ! "snap"
      cmdProbe.expectMsgClass(classOf[IllegalStateException])
    }
  }

  "An EventsourcedActor" when {
    "in sharedClockEntry = false mode" must {
      def timestamp(a: Long = 0L, b: Long = 0L) = (a, b) match {
        case (0L, 0L) => VectorTime()
        case (a, 0L)  => VectorTime(emitterIdA -> a)
        case (0L, b)  => VectorTime(emitterIdB -> b)
        case (a, b)   => VectorTime(emitterIdA -> a, emitterIdB -> b)
      }
      "recover from replayed self-emitted events" in {
        val actor = unrecoveredCausalityActor(sharedClockEntry = false)

        val e1 = event1a.copy(vectorTimestamp = timestamp(1, 0), processId = emitterIdA)
        val e2 = event1b.copy(vectorTimestamp = timestamp(2, 0), processId = emitterIdA)

        logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
        logProbe.sender() ! LoadSnapshotSuccess(None, instanceId)
        logProbe.expectMsg(Replay(1L, Some(actor), instanceId))
        logProbe.sender() ! ReplaySuccess(List(e1, e2), e2.localSequenceNr, instanceId)

        evtProbe.expectMsg(("a", e1.vectorTimestamp, e1.vectorTimestamp, e1.localSequenceNr))
        evtProbe.expectMsg(("b", e2.vectorTimestamp, e2.vectorTimestamp, e2.localSequenceNr))
      }
      "recover from replayed self-emitted and remote events" in {
        val actor = unrecoveredCausalityActor(sharedClockEntry = false)

        val e1 = event2a.copy(vectorTimestamp = timestamp(1, 0), processId = emitterIdA, localSequenceNr = 6L)
        val e2 = event2b.copy(vectorTimestamp = timestamp(0, 1), processId = emitterIdB, localSequenceNr = 7L)
        val e3 = event2c.copy(vectorTimestamp = timestamp(0, 2), processId = emitterIdB, localSequenceNr = 8L)
        val e4 = event2d.copy(vectorTimestamp = timestamp(0, 3), processId = emitterIdB, localSequenceNr = 9L)

        logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
        logProbe.sender() ! LoadSnapshotSuccess(None, instanceId)
        logProbe.expectMsg(Replay(1L, Some(actor), instanceId))
        logProbe.sender() ! ReplaySuccess(List(e1, e2, e3, e4), e4.localSequenceNr, instanceId)

        evtProbe.expectMsg(("a", e1.vectorTimestamp, timestamp(1, 0), e1.localSequenceNr))
        evtProbe.expectMsg(("b", e2.vectorTimestamp, timestamp(2, 1), e2.localSequenceNr))
        evtProbe.expectMsg(("c", e3.vectorTimestamp, timestamp(3, 2), e3.localSequenceNr))
        evtProbe.expectMsg(("d", e4.vectorTimestamp, timestamp(4, 3), e4.localSequenceNr))
      }
      "increase local time when persisting an event" in {
        val actor = recoveredCausalityActor(sharedClockEntry = false)

        val e1 = DurableEvent("x", emitterIdB, None, Set(), 0L, timestamp(0, 1), emitterIdB, logIdA, 1L)
        val e2 = DurableEvent("a", emitterIdA, None, Set(), 0L, timestamp(2, 1), emitterIdA)
        val e3 = DurableEvent("b", emitterIdA, None, Set(), 0L, timestamp(3, 1), emitterIdA)

        actor ! Written(e1)
        actor ! Cmd("a")
        actor ! Cmd("b")

        val write1 = logProbe.expectMsgClass(classOf[Write])
        write1.events.head.copy(systemTimestamp = 0L) should be(e2)
        logProbe.sender() ! WriteSuccess(Seq(e2.copy(localLogId = logIdA, localSequenceNr = 2L)), write1.correlationId, instanceId)

        val write2 = logProbe.expectMsgClass(classOf[Write])
        write2.events.head.copy(systemTimestamp = 0L) should be(e3)
        logProbe.sender() ! WriteSuccess(Seq(e3.copy(localLogId = logIdA, localSequenceNr = 3L)), write2.correlationId, instanceId)

        evtProbe.expectMsg(("x", e1.vectorTimestamp, timestamp(1, 1), 1L))
        evtProbe.expectMsg(("a", e2.vectorTimestamp, timestamp(2, 1), 2L))
        evtProbe.expectMsg(("b", e3.vectorTimestamp, timestamp(3, 1), 3L))
      }
    }
  }
}
