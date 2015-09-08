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

import com.rbmhtechnology.eventuate.ConfirmedDelivery._

import scala.util._

object EventsourcedActorSpec {
  import EventsourcedViewSpec._

  case class Cmd(payload: Any, num: Int = 1)
  case class Deliver(payload: Any)
  case class DeliverRequested(payload: Any)
  case class State(state: Vector[String])

  class TestEventsourcedActor(
      val logProbe: ActorRef,
      val dstProbe: ActorRef,
      val errProbe: ActorRef,
      override val stateSync: Boolean) extends EventsourcedActor {

    val id = emitterIdA
    val eventLog = logProbe

    override val onCommand: Receive = {
      case "boom" => throw boom
      case Ping(i) => dstProbe ! Pong(i)
      case "test-handler-order" =>
        persist("a")(r => dstProbe ! ((s"${r.get}-1", lastVectorTimestamp, lastHandledTime, lastSequenceNr)))
        persist("b")(r => dstProbe ! ((s"${r.get}-2", lastVectorTimestamp, lastHandledTime, lastSequenceNr)))
      case "test-multi-persist" =>
        val handler = (r: Try[String]) => dstProbe ! ((r.get, lastHandledTime, lastVectorTimestamp, lastSequenceNr))
        persistN(Seq("a", "b", "c"), handler)(handler)
      case Cmd(p, num) => 1 to num foreach { i =>
        persist(s"${p}-${i}") {
          case Success("boom") => throw boom
          case Success(evt) => dstProbe ! ((evt, lastVectorTimestamp, lastHandledTime, lastSequenceNr))
          case Failure(err) => errProbe ! ((err, lastVectorTimestamp, lastHandledTime, lastSequenceNr))
        }
      }
    }

    override val onEvent: Receive = {
      case "boom" => throw boom
      case evt if evt != "x" => dstProbe ! ((evt, lastVectorTimestamp, lastHandledTime, lastSequenceNr))
    }
  }

  class TestStashingActor(
    val logProbe: ActorRef,
    val dstProbe: ActorRef,
    val errProbe: ActorRef,
    override val stateSync: Boolean) extends EventsourcedActor {

    val id = emitterIdA
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

  class TestSnapshotActor(
    val logProbe: ActorRef,
    val dstProbe: ActorRef,
    val errProbe: ActorRef) extends EventsourcedActor with ConfirmedDelivery {

    val id = emitterIdA
    val eventLog = logProbe

    var state: Vector[String] = Vector.empty

    override val onCommand: Receive = {
      case "boom" =>
        throw boom
      case "snap" =>
        save(State(state)) {
          case Success(md) => dstProbe ! md
          case Failure(err) => errProbe ! err
        }
      case Cmd(p: String, _) =>
        persist(p) {
          case Success(evt) => onEvent(evt)
          case Failure(err) => errProbe ! err
        }
      case Deliver(p) =>
        persist(DeliverRequested(p)) {
          case Success(evt) => onEvent(evt)
          case Failure(err) => errProbe ! err
        }
    }

    override val onEvent: Receive = {
      case evt: String =>
        state = state :+ evt
        dstProbe ! message(state)
      case DeliverRequested(p: String) =>
        deliver(lastSequenceNr.toString, message(p), dstProbe.path)
    }

    override val onSnapshot: Receive = {
      case State(s) =>
        state = s
        dstProbe ! message(s)
    }

    private def message(payload: Any) =
      (payload, lastVectorTimestamp, lastHandledTime, lastSequenceNr)
  }
}

class EventsourcedActorSpec extends EventsourcedViewSpec {
  import EventsourcedViewSpec._
  import EventsourcedActorSpec._
  import EventsourcingProtocol._

  var errProbe: TestProbe = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    errProbe = TestProbe()
  }

  override def unrecoveredActor: ActorRef =
    unrecoveredActor(stateSync = true)

  def unrecoveredActor(stateSync: Boolean): ActorRef =
    system.actorOf(Props(new TestEventsourcedActor(logProbe.ref, dstProbe.ref, errProbe.ref, stateSync)))

  def unrecoveredSnapshotActor(): ActorRef =
    system.actorOf(Props(new TestSnapshotActor(logProbe.ref, dstProbe.ref, errProbe.ref)))

  def recoveredActor(stateSync: Boolean = true): ActorRef = {
    val actor = unrecoveredActor(stateSync)
    logProbe.expectMsg(LoadSnapshot(emitterIdA, actor, instanceId))
    actor ! LoadSnapshotSuccess(None, instanceId)
    logProbe.expectMsg(Replay(1, actor, instanceId))
    actor ! ReplaySuccess(instanceId)
    actor
  }

  def recoveredSnapshotActor(): ActorRef = {
    val actor = system.actorOf(Props(new TestSnapshotActor(logProbe.ref, dstProbe.ref, errProbe.ref)))
    logProbe.expectMsg(LoadSnapshot(emitterIdA, actor, instanceId))
    actor ! LoadSnapshotSuccess(None, instanceId)
    logProbe.expectMsg(Replay(1, actor, instanceId))
    actor ! ReplaySuccess(instanceId)
    actor
  }

  def stashingActor(stateSync: Boolean = true): ActorRef = {
    val actor = system.actorOf(Props(new TestStashingActor(logProbe.ref, dstProbe.ref, errProbe.ref, stateSync)))
    logProbe.expectMsg(LoadSnapshot(emitterIdA, actor, instanceId))
    actor ! LoadSnapshotSuccess(None, instanceId)
    logProbe.expectMsg(Replay(1, actor, instanceId))
    actor ! ReplaySuccess(instanceId)
    actor
  }

  def processWrite(actor: ActorRef, snr: Long): Unit = {
    val write = logProbe.expectMsgClass(classOf[Write])
    actor ! WriteSuccess(event(write.events(0).payload, snr), instanceId)
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

        actor ! WriteSuccess(event("a-1", 1L), instanceId)
        actor ! WriteSuccess(event("a-2", 2L), instanceId)

        dstProbe.expectMsg(("a-1", timestamp(1), timestamp(1), 1))
        dstProbe.expectMsg(("a-2", timestamp(2), timestamp(2), 2))
        dstProbe.expectMsg(Pong(1))
        dstProbe.expectMsg(Pong(2))
      }
      "process further commands if persist is aborted by exception in persist handler" in {
        val actor = recoveredActor(stateSync = true)
        actor ! Cmd("a", 2)
        actor ! Cmd("b", 2)

        val write1 = logProbe.expectMsgClass(classOf[Write])
        actor ! WriteSuccess(event("boom", 1L), instanceId)
        actor ! WriteSuccess(event("a-2", 2L), instanceId)

        logProbe.expectMsg(LoadSnapshot(emitterIdA, actor, instanceId + 1))
        actor ! LoadSnapshotSuccess(None, instanceId + 1)
        logProbe.expectMsg(Replay(1, actor, instanceId + 1))
        actor ! Replaying(event("a-1", 1L), instanceId + 1)
        actor ! Replaying(event("a-2", 2L), instanceId + 1)
        actor ! ReplaySuccess(instanceId + 1)

        val write2 = logProbe.expectMsgClass(classOf[Write])
        write2.events(0).payload should be("b-1")
        write2.events(1).payload should be("b-2")
        actor ! WriteSuccess(event("b-1", 3L), instanceId + 1)
        actor ! WriteSuccess(event("b-2", 4L), instanceId + 1)

        dstProbe.expectMsg(("a-1", timestamp(1), timestamp(1), 1))
        dstProbe.expectMsg(("a-2", timestamp(2), timestamp(2), 2))
        dstProbe.expectMsg(("b-1", timestamp(3), timestamp(3), 3))
        dstProbe.expectMsg(("b-2", timestamp(4), timestamp(4), 4))
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

        logProbe.expectMsg(LoadSnapshot(emitterIdA, actor, instanceId + 1))
        actor ! LoadSnapshotSuccess(None, instanceId + 1)
        logProbe.expectMsg(Replay(1, actor, instanceId + 1))
        actor ! Replaying(event("a-1", 1), instanceId + 1)
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
        actor ! WriteSuccess(event("a-1", 1L), instanceId)
        actor ! WriteSuccess(event("a-2", 2L), instanceId)
        dstProbe.expectMsg(Pong(1))
        dstProbe.expectMsg(("a-1", timestamp(1), timestamp(1), 1L))
        dstProbe.expectMsg(("a-2", timestamp(2), timestamp(2), 2))
      }
      "process further commands if persist is aborted by exception in command handler" in {
        val actor = recoveredActor(stateSync = false)
        actor ! Cmd("a", 2)
        actor ! "boom"
        actor ! Cmd("b", 2)

        val write1 = logProbe.expectMsgClass(classOf[Write])
        actor ! WriteSuccess(event("boom", 1L), instanceId)
        actor ! WriteSuccess(event("a-2", 2L), instanceId)

        logProbe.expectMsg(LoadSnapshot(emitterIdA, actor, instanceId + 1))
        actor ! LoadSnapshotSuccess(None, instanceId + 1)
        logProbe.expectMsg(Replay(1, actor, instanceId + 1))
        actor ! Replaying(event("a-1", 1L), instanceId + 1)
        actor ! Replaying(event("a-2", 2L), instanceId + 1)
        actor ! ReplaySuccess(instanceId + 1)

        val write2 = logProbe.expectMsgClass(classOf[Write])
        write2.events(0).payload should be("b-1")
        write2.events(1).payload should be("b-2")
        actor ! WriteSuccess(event("b-1", 3L), instanceId + 1)
        actor ! WriteSuccess(event("b-2", 4L), instanceId + 1)

        dstProbe.expectMsg(("a-1", timestamp(1), timestamp(1), 1))
        dstProbe.expectMsg(("a-2", timestamp(2), timestamp(2), 2))
        dstProbe.expectMsg(("b-1", timestamp(3), timestamp(3), 3))
        dstProbe.expectMsg(("b-2", timestamp(4), timestamp(4), 4))
      }
    }
    "in any mode" must {
      "handle remote events while persistence is in progress" in {
        val actor = recoveredActor(stateSync = true)
        actor ! Cmd("a", 2)
        val write = logProbe.expectMsgClass(classOf[Write])
        write.events(0).payload should be("a-1")
        write.events(1).payload should be("a-2")

        val eventB1 = DurableEvent("b-1", emitterIdB, None, Set(), 0L, timestamp(0, 1), logIdB, logIdB, logIdA, 1L, 1L, 3L)
        val eventB2 = DurableEvent("b-2", emitterIdB, None, Set(), 0L, timestamp(0, 2), logIdB, logIdB, logIdA, 2L, 2L, 3L)

        val eventA1 = DurableEvent("a-1", emitterIdA, None, Set(), 0L, timestamp(3, 0), logIdA, logIdA, logIdA, 3L, 3L, 0L)
        val eventA2 = DurableEvent("a-2", emitterIdA, None, Set(), 0L, timestamp(4, 0), logIdA, logIdA, logIdA, 4L, 4L, 0L)

        actor ! Written(eventB1)
        actor ! Written(eventB2)
        actor ! WriteSuccess(eventA1, instanceId)
        actor ! WriteSuccess(eventA2, instanceId)

        dstProbe.expectMsg(("b-1", timestamp(0, 1), timestamp(1, 1), 1L))
        dstProbe.expectMsg(("b-2", timestamp(0, 2), timestamp(2, 2), 2L))
        dstProbe.expectMsg(("a-1", timestamp(3, 0), timestamp(3, 2), 3L))
        dstProbe.expectMsg(("a-2", timestamp(4, 0), timestamp(4, 2), 4L))
      }
      "invoke persist handler in correct order" in {
        val actor = recoveredActor(stateSync = true)
        actor ! "test-handler-order"

        val write = logProbe.expectMsgClass(classOf[Write])
        write.events(0).payload should be("a")
        write.events(1).payload should be("b")

        actor ! WriteSuccess(event("a", 1), instanceId)
        actor ! WriteSuccess(event("b", 2), instanceId)

        dstProbe.expectMsg(("a-1", timestamp(1), timestamp(1), 1))
        dstProbe.expectMsg(("b-2", timestamp(2), timestamp(2), 2))
      }
      "additionally invoke onLast handler for multi-persist" in {
        val actor = recoveredActor(stateSync = true)
        actor ! "test-multi-persist"

        val write = logProbe.expectMsgClass(classOf[Write])
        write.events(0).payload should be("a")
        write.events(1).payload should be("b")
        write.events(2).payload should be("c")

        actor ! WriteSuccess(event("a", 1), instanceId)
        actor ! WriteSuccess(event("b", 2), instanceId)
        actor ! WriteSuccess(event("c", 3), instanceId)

        dstProbe.expectMsg(("a", timestamp(1), timestamp(1), 1))
        dstProbe.expectMsg(("b", timestamp(2), timestamp(2), 2))
        dstProbe.expectMsg(("c", timestamp(3), timestamp(3), 3))
        dstProbe.expectMsg(("c", timestamp(3), timestamp(3), 3))
      }
      "report failed writes to persist handler" in {
        val actor = recoveredActor(stateSync = true)
        actor ! Cmd("a", 2)

        val write = logProbe.expectMsgClass(classOf[Write])
        val event1 = write.events(0)
        val event2 = write.events(1)

        actor ! WriteFailure(event1, boom, instanceId)
        actor ! WriteFailure(event2, boom, instanceId)

        errProbe.expectMsg((boom, event1.vectorTimestamp, event1.vectorTimestamp, event1.sequenceNr))
        errProbe.expectMsg((boom, event2.vectorTimestamp, event2.vectorTimestamp, event2.sequenceNr))
      }
      "not send empty write commands to log" in {
        val actor = recoveredActor(stateSync = true)
        actor ! Ping(1)
        actor ! Cmd("a", 2)
        val write = logProbe.expectMsgClass(classOf[Write])
        write.events(0).payload should be("a-1")
        write.events(1).payload should be("a-2")
      }
    }
  }

  "An EventsourcedActor" must {
    "recover from a snapshot" in {
      val actor = unrecoveredSnapshotActor()
      val snapshot = Snapshot(State(Vector("a", "b")), emitterIdA, event("b", 2), timestamp(2, 4))

      logProbe.expectMsg(LoadSnapshot(emitterIdA, actor, instanceId))
      actor ! LoadSnapshotSuccess(Some(snapshot), instanceId)
      logProbe.expectMsg(Replay(3, actor, instanceId))
      actor ! ReplaySuccess(instanceId)
      dstProbe.expectMsg((Vector("a", "b"), timestamp(2), timestamp(2, 4), 2))
    }
    "recover from a snapshot and remaining events" in {
      val actor = unrecoveredSnapshotActor()
      val snapshot = Snapshot(State(Vector("a", "b")), emitterIdA, event("b", 2), timestamp(2, 4))

      logProbe.expectMsg(LoadSnapshot(emitterIdA, actor, instanceId))
      actor ! LoadSnapshotSuccess(Some(snapshot), instanceId)
      logProbe.expectMsg(Replay(3, actor, instanceId))
      actor ! Replaying(event("c", 3), instanceId)
      actor ! Replaying(event("d", 4), instanceId)
      actor ! ReplaySuccess(instanceId)
      dstProbe.expectMsg((Vector("a", "b"), timestamp(2), timestamp(2, 4), 2))
      dstProbe.expectMsg((Vector("a", "b", "c"), timestamp(3), timestamp(3, 4), 3))
      dstProbe.expectMsg((Vector("a", "b", "c", "d"), timestamp(4), timestamp(4, 4), 4))
    }
    "recover from a snapshot and deliver unconfirmed messages" in {
      val actor = unrecoveredSnapshotActor()
      val unconfirmed = Vector(
        DeliveryAttempt("3", "x", dstProbe.ref.path),
        DeliveryAttempt("4", "y", dstProbe.ref.path))
      val snapshot = Snapshot(State(Vector("a", "b")), emitterIdA, event("b", 2), timestamp(2, 4), deliveryAttempts = unconfirmed)

      logProbe.expectMsg(LoadSnapshot(emitterIdA, actor, instanceId))
      actor ! LoadSnapshotSuccess(Some(snapshot), instanceId)
      logProbe.expectMsg(Replay(3, actor, instanceId))
      actor ! ReplaySuccess(instanceId)
      dstProbe.expectMsg((Vector("a", "b"), timestamp(2), timestamp(2, 4), 2))
      dstProbe.expectMsg("x")
      dstProbe.expectMsg("y")
    }
    "recover from scratch if onSnapshot doesn't handle loaded snapshot" in {
      val actor = unrecoveredSnapshotActor()
      val snapshot = Snapshot("foo", emitterIdA, event("b", 2), timestamp(2, 4))

      logProbe.expectMsg(LoadSnapshot(emitterIdA, actor, instanceId))
      actor ! LoadSnapshotSuccess(Some(snapshot), instanceId)
      logProbe.expectMsg(Replay(1, actor, instanceId))
      actor ! Replaying(event("a", 1), instanceId)
      actor ! Replaying(event("b", 2), instanceId)
      actor ! ReplaySuccess(instanceId)
      dstProbe.expectMsg((Vector("a"), timestamp(1), timestamp(1), 1))
      dstProbe.expectMsg((Vector("a", "b"), timestamp(2), timestamp(2), 2))
    }
    "save a snapshot" in {
      val event1 = DurableEvent("x", emitterIdB, None, Set(), 0L, timestamp(0, 1), logIdB, logIdB, logIdA, 1L, 1L, 2L)
      val event2 = DurableEvent("a", emitterIdA, None, Set(), 0L, timestamp(2, 1), logIdA, logIdA, logIdA, 2L, 2L, 0L)
      val event3 = DurableEvent("b", emitterIdA, None, Set(), 0L, timestamp(3, 1), logIdA, logIdA, logIdA, 3L, 3L, 0L)

      val actor = recoveredSnapshotActor()
      actor ! Written(event1)
      dstProbe.expectMsg((Vector("x"), timestamp(0, 1), timestamp(1, 1), 1))
      actor ! Cmd("a")
      actor ! Cmd("b")

      logProbe.expectMsgClass(classOf[Write])
      actor ! WriteSuccess(event2, instanceId)
      logProbe.expectMsgClass(classOf[Write])
      actor ! WriteSuccess(event3, instanceId)

      dstProbe.expectMsg((Vector("x", "a"), timestamp(2, 1), timestamp(2, 1), 2))
      dstProbe.expectMsg((Vector("x", "a", "b"), timestamp(3, 1), timestamp(3, 1), 3))
      actor ! "snap"

      val snapshot = Snapshot(State(Vector("x", "a", "b")), emitterIdA, event3, timestamp(3, 1))
      logProbe.expectMsg(SaveSnapshot(snapshot, system.deadLetters, actor, instanceId))
      actor ! SaveSnapshotSuccess(snapshot.metadata, instanceId)
      dstProbe.expectMsg(snapshot.metadata)
    }
    "save a snapshot with unconfirmed messages" in {
      val actor = recoveredSnapshotActor()
      actor ! Cmd("a")
      actor ! Cmd("b")
      actor ! Deliver("x")
      actor ! Deliver("y")
      logProbe.expectMsgClass(classOf[Write])
      actor ! WriteSuccess(event("a", 1), instanceId)
      logProbe.expectMsgClass(classOf[Write])
      actor ! WriteSuccess(event("b", 2), instanceId)
      logProbe.expectMsgClass(classOf[Write])
      actor ! WriteSuccess(event(DeliverRequested("x"), 3), instanceId)
      logProbe.expectMsgClass(classOf[Write])
      actor ! WriteSuccess(event(DeliverRequested("y"), 4), instanceId)

      dstProbe.expectMsg((Vector("a"), timestamp(1), timestamp(1), 1))
      dstProbe.expectMsg((Vector("a", "b"), timestamp(2), timestamp(2), 2))
      dstProbe.expectMsg(("x", timestamp(3), timestamp(3), 3))
      dstProbe.expectMsg(("y", timestamp(4), timestamp(4), 4))
      actor ! "snap"

      val unconfirmed = Vector(
        DeliveryAttempt("3", ("x", timestamp(3), timestamp(3), 3), dstProbe.ref.path),
        DeliveryAttempt("4", ("y", timestamp(4), timestamp(4), 4), dstProbe.ref.path))
      val snapshot = Snapshot(State(Vector("a", "b")), emitterIdA, event(DeliverRequested("y"), 4), timestamp(4), deliveryAttempts = unconfirmed)

      logProbe.expectMsg(SaveSnapshot(snapshot, system.deadLetters, actor, instanceId))
      actor ! SaveSnapshotSuccess(snapshot.metadata, instanceId)
      dstProbe.expectMsg(snapshot.metadata)
    }
    "not save the same snapshot concurrently" in {
      val actor = recoveredSnapshotActor()
      actor ! "snap"
      actor ! "snap"
      errProbe.expectMsgClass(classOf[IllegalStateException])
    }
  }
}
