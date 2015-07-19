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

import org.scalatest._

import scala.util._

object EventsourcedActorSpec {
  val idA = "A"
  val idB = "B"
  val idC = "C"
  val idD = "D"

  val logId = "log"

  case class Cmd(payload: Any, num: Int = 1)
  case class CmdDelayed(payload: Any)

  case class Deliver(payload: Any)
  case class DeliverRequested(payload: Any)

  case class Ping(i: Int)
  case class Pong(i: Int)

  case class State(state: Vector[String])

  class TestEventsourcedActor(
      val logProbe: ActorRef,
      val dstProbe: ActorRef,
      val errProbe: ActorRef,
      override val stateSync: Boolean) extends EventsourcedActor {

    val id = idA
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

    val id = idA
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

    val id = idA
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
      (payload, currentTime, lastVectorTimestamp, lastSequenceNr)
  }

  def event(payload: Any, sequenceNr: Long, timestamp: VectorTime, emitterId: String): DurableEvent =
    DurableEvent(payload, 0L, timestamp, emitterId, None, Set(), 0L, logId, logId, sequenceNr, sequenceNr)

  def eventA(payload: Any, sequenceNr: Long, timestamp: VectorTime): DurableEvent =
    event(payload, sequenceNr, timestamp, idA)

  def eventB(payload: Any, sequenceNr: Long, timestamp: VectorTime): DurableEvent =
    event(payload, sequenceNr, timestamp, idB)

  def eventC(payload: Any, sequenceNr: Long, timestamp: VectorTime): DurableEvent =
    event(payload, sequenceNr, timestamp, idC)

  def eventD(payload: Any, sequenceNr: Long, timestamp: VectorTime): DurableEvent =
    event(payload, sequenceNr, timestamp, idD)

  def timestampA(timeA: Long): VectorTime =
    VectorTime(idA -> timeA)

  def timestampAB(timeA: Long, timeB: Long): VectorTime =
    VectorTime(idA -> timeA, idB -> timeB)

  def timestampABCD(timeA: Long, timeB: Long, timeC: Long, timeD: Long): VectorTime =
    VectorTime(idA -> timeA, idB -> timeB, idC -> timeC, idD -> timeD)
}

class EventsourcedActorSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  import EventsourcedActorSpec._
  import EventsourcingProtocol._

  var instanceId: Int = _
  var logProbe: TestProbe = _
  var dstProbe: TestProbe = _
  var errProbe: TestProbe = _

  override def beforeEach(): Unit = {
    instanceId = EventsourcedView.instanceIdCounter.get
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
    logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId))
    actor ! LoadSnapshotSuccess(None, instanceId)
    logProbe.expectMsg(Replay(1, actor, instanceId))
    actor ! ReplaySuccess(instanceId)
    actor
  }

  def stashingActor(stateSync: Boolean = true): ActorRef = {
    val actor = system.actorOf(Props(new TestStashingActor(logProbe.ref, dstProbe.ref, errProbe.ref, stateSync)))
    logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId))
    actor ! LoadSnapshotSuccess(None, instanceId)
    logProbe.expectMsg(Replay(1, actor, instanceId))
    actor ! ReplaySuccess(instanceId)
    actor
  }

  def unrecoveredSnapshotActor(): ActorRef =
    system.actorOf(Props(new TestSnapshotActor(logProbe.ref, dstProbe.ref, errProbe.ref)))

  def recoveredSnapshotActor(): ActorRef = {
    val actor = system.actorOf(Props(new TestSnapshotActor(logProbe.ref, dstProbe.ref, errProbe.ref)))
    logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId))
    actor ! LoadSnapshotSuccess(None, instanceId)
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
      logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId))
      actor ! LoadSnapshotSuccess(None, instanceId)
      logProbe.expectMsg(Replay(1, actor, instanceId))
      actor ! Replaying(eventA("a", 1, timestampAB(1, 0)), instanceId)
      actor ! Replaying(eventA("b", 2, timestampAB(2, 0)), instanceId)
      actor ! ReplaySuccess(instanceId)
      dstProbe.expectMsg(("a", timestampAB(1, 0), timestampAB(1, 0), 1))
      dstProbe.expectMsg(("b", timestampAB(2, 0), timestampAB(2, 0), 2))
    }
    "recover from replayed local and remote events" in {
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
      logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId))
      actor ! LoadSnapshotSuccess(None, instanceId)
      logProbe.expectMsg(Replay(1, actor, instanceId))
      actor ! Replaying(eventA("a", 1, timestampAB(1, 0)), instanceId)
      actor ! Replaying(eventA("boom", 2, timestampAB(3, 0)), instanceId)
      actor ! Replaying(eventA("c", 3, timestampAB(2, 0)), instanceId)
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
    "deliver events in causal order (events stored consistent with causal order)" in {
      val actor = recoveredActor()
      actor ! Written(eventB("b", 1, timestampABCD(0, 1, 0, 0)))
      actor ! Written(eventC("c", 2, timestampABCD(0, 1, 2, 0)))
      actor ! Written(eventD("d", 3, timestampABCD(0, 1, 2, 2)))

      dstProbe.expectMsg(("b", timestampABCD(1, 1, 0, 0), timestampABCD(0, 1, 0, 0), 1))
      dstProbe.expectMsg(("c", timestampABCD(2, 1, 2, 0), timestampABCD(0, 1, 2, 0), 2))
      dstProbe.expectMsg(("d", timestampABCD(3, 1, 2, 2), timestampABCD(0, 1, 2, 2), 3))
    }
    "deliver events in causal order (events stashed consistent with causal order)" in {
      val actor = recoveredActor()
      actor ! Written(eventC("c", 1, timestampABCD(0, 1, 2, 0)))
      actor ! Written(eventD("d", 2, timestampABCD(0, 1, 2, 2)))
      actor ! Written(eventB("b", 3, timestampABCD(0, 1, 0, 0)))

      dstProbe.expectMsg(("b", timestampABCD(1, 1, 0, 0), timestampABCD(0, 1, 0, 0), 3))
      dstProbe.expectMsg(("c", timestampABCD(2, 1, 2, 0), timestampABCD(0, 1, 2, 0), 1))
      dstProbe.expectMsg(("d", timestampABCD(3, 1, 2, 2), timestampABCD(0, 1, 2, 2), 2))
    }
    "deliver events in causal order (events stashed inconsistent with causal order)" in {
      val actor = recoveredActor()
      actor ! Written(eventD("d", 1, timestampABCD(0, 1, 2, 2)))
      actor ! Written(eventC("c", 2, timestampABCD(0, 1, 2, 0)))
      actor ! Written(eventB("b", 3, timestampABCD(0, 1, 0, 0)))

      dstProbe.expectMsg(("b", timestampABCD(1, 1, 0, 0), timestampABCD(0, 1, 0, 0), 3))
      dstProbe.expectMsg(("c", timestampABCD(2, 1, 2, 0), timestampABCD(0, 1, 2, 0), 2))
      dstProbe.expectMsg(("d", timestampABCD(3, 1, 2, 2), timestampABCD(0, 1, 2, 2), 1))
    }
    "deliver events in causal order under failure conditions (events stashed consistent with causal order)" in {
      val actor = recoveredActor()
      actor ! Written(eventC("c", 1, timestampABCD(0, 1, 2, 0)))
      actor ! Written(eventD("d", 2, timestampABCD(0, 1, 2, 2)))
      actor ! "boom"
      actor ! Written(eventB("b", 3, timestampABCD(0, 1, 0, 0)))

      logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId + 1))
      actor ! LoadSnapshotSuccess(None, instanceId + 1)

      logProbe.expectMsg(Replay(1, actor, instanceId + 1))
      actor ! Replaying(eventC("c", 1, timestampABCD(0, 1, 2, 0)), instanceId + 1)
      actor ! Replaying(eventD("d", 2, timestampABCD(0, 1, 2, 2)), instanceId + 1)
      actor ! Replaying(eventB("b", 3, timestampABCD(0, 1, 0, 0)), instanceId + 1)
      actor ! ReplaySuccess(instanceId + 1)

      dstProbe.expectMsg(("b", timestampABCD(1, 1, 0, 0), timestampABCD(0, 1, 0, 0), 3))
      dstProbe.expectMsg(("c", timestampABCD(2, 1, 2, 0), timestampABCD(0, 1, 2, 0), 1))
      dstProbe.expectMsg(("d", timestampABCD(3, 1, 2, 2), timestampABCD(0, 1, 2, 2), 2))
    }
    "deliver events in causal order under failure conditions (events stashed inconsistent with causal order)" in {
      val actor = recoveredActor()
      actor ! Written(eventD("d", 1, timestampABCD(0, 1, 2, 2)))
      actor ! Written(eventC("c", 2, timestampABCD(0, 1, 2, 0)))
      actor ! "boom"
      actor ! Written(eventB("b", 3, timestampABCD(0, 1, 0, 0)))

      logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId + 1))
      actor ! LoadSnapshotSuccess(None, instanceId + 1)

      logProbe.expectMsg(Replay(1, actor, instanceId + 1))
      actor ! Replaying(eventD("d", 1, timestampABCD(0, 1, 2, 2)), instanceId + 1)
      actor ! Replaying(eventC("c", 2, timestampABCD(0, 1, 2, 0)), instanceId + 1)
      actor ! Replaying(eventB("b", 3, timestampABCD(0, 1, 0, 0)), instanceId + 1)
      actor ! ReplaySuccess(instanceId + 1)

      dstProbe.expectMsg(("b", timestampABCD(1, 1, 0, 0), timestampABCD(0, 1, 0, 0), 3))
      dstProbe.expectMsg(("c", timestampABCD(2, 1, 2, 0), timestampABCD(0, 1, 2, 0), 2))
      dstProbe.expectMsg(("d", timestampABCD(3, 1, 2, 2), timestampABCD(0, 1, 2, 2), 1))
    }
    "deliver events in causal order during transitions from recovering to recovered" in {
      val actor = unrecoveredActor()
      logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId))

      actor ! LoadSnapshotSuccess(None, instanceId)
      logProbe.expectMsg(Replay(1, actor, instanceId))

      actor ! Replaying(eventD("d", 1, timestampABCD(0, 1, 2, 2)), instanceId)
      actor ! ReplaySuccess(instanceId)
      actor ! Written(eventC("c", 2, timestampABCD(0, 1, 2, 0)))
      actor ! Written(eventB("b", 3, timestampABCD(0, 1, 0, 0)))

      dstProbe.expectMsg(("b", timestampABCD(1, 1, 0, 0), timestampABCD(0, 1, 0, 0), 3))
      dstProbe.expectMsg(("c", timestampABCD(2, 1, 2, 0), timestampABCD(0, 1, 2, 0), 2))
      dstProbe.expectMsg(("d", timestampABCD(3, 1, 2, 2), timestampABCD(0, 1, 2, 2), 1))
    }
    "deliver events in causal order under recovery failure conditions" in {
      val actor = unrecoveredActor()
      logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId))

      actor ! LoadSnapshotSuccess(None, instanceId)
      logProbe.expectMsg(Replay(1, actor, instanceId))

      actor ! Replaying(eventC("boom", 1, timestampABCD(0, 1, 2, 0)), instanceId)
      actor ! Replaying(eventD("d", 2, timestampABCD(0, 1, 2, 2)), instanceId)
      actor ! Replaying(eventB("b", 3, timestampABCD(0, 1, 0, 0)), instanceId)

      logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId + 1))
      actor ! LoadSnapshotSuccess(None, instanceId + 1)

      actor ! Replaying(eventC("c", 1, timestampABCD(0, 1, 2, 0)), instanceId + 1)
      actor ! Replaying(eventD("d", 2, timestampABCD(0, 1, 2, 2)), instanceId + 1)
      actor ! Replaying(eventB("b", 3, timestampABCD(0, 1, 0, 0)), instanceId + 1)

      dstProbe.expectMsg(("b", timestampABCD(1, 1, 0, 0), timestampABCD(0, 1, 0, 0), 3))
      dstProbe.expectMsg(("b", timestampABCD(1, 1, 0, 0), timestampABCD(0, 1, 0, 0), 3))
      dstProbe.expectMsg(("c", timestampABCD(2, 1, 2, 0), timestampABCD(0, 1, 2, 0), 1))
      dstProbe.expectMsg(("d", timestampABCD(3, 1, 2, 2), timestampABCD(0, 1, 2, 2), 2))
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
        logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId + 1))
        actor ! LoadSnapshotSuccess(None, instanceId + 1)
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

        logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId + 1))
        actor ! LoadSnapshotSuccess(None, instanceId + 1)
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
        logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId + 1))
        actor ! LoadSnapshotSuccess(None, instanceId + 1)
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
      "handle remote events while persistence is in progress" in {
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

  "An EventsourcedActor" must {
    "recover from a snapshot" in {
      val actor = unrecoveredSnapshotActor()
      val event = eventA("b", 2, timestampA(2))
      val snapshot = Snapshot(State(Vector("a", "b")), idA, event, event, timestamp = timestampA(2))

      logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId))
      actor ! LoadSnapshotSuccess(Some(snapshot), instanceId)
      logProbe.expectMsg(Replay(3, actor, instanceId))
      actor ! ReplaySuccess(instanceId)
      dstProbe.expectMsg((Vector("a", "b"), timestampA(2), timestampA(2), 2))
    }
    "recover from a snapshot and remaining events" in {
      val actor = unrecoveredSnapshotActor()
      val event = eventA("b", 2, timestampA(2))
      val snapshot = Snapshot(State(Vector("a", "b")), idA, event, event, timestamp = timestampA(2))

      logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId))
      actor ! LoadSnapshotSuccess(Some(snapshot), instanceId)
      logProbe.expectMsg(Replay(3, actor, instanceId))
      actor ! Replaying(eventA("c", 3, timestampA(3)), instanceId)
      actor ! Replaying(eventA("d", 4, timestampA(4)), instanceId)
      actor ! ReplaySuccess(instanceId)
      dstProbe.expectMsg((Vector("a", "b"), timestampA(2), timestampA(2), 2))
      dstProbe.expectMsg((Vector("a", "b", "c"), timestampA(3), timestampA(3), 3))
      dstProbe.expectMsg((Vector("a", "b", "c", "d"), timestampA(4), timestampA(4), 4))
    }
    "recover from a snapshot and deliver unconfirmed messages" in {
      val actor = unrecoveredSnapshotActor()
      val event = eventA("b", 2, timestampA(2))
      val unconfirmed = Vector(
        DeliveryAttempt("3", "x", dstProbe.ref.path),
        DeliveryAttempt("4", "y", dstProbe.ref.path))
      val snapshot = Snapshot(State(Vector("a", "b")), idA, event, event, deliveryAttempts = unconfirmed, timestamp = timestampA(2))

      logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId))
      actor ! LoadSnapshotSuccess(Some(snapshot), instanceId)
      logProbe.expectMsg(Replay(3, actor, instanceId))
      actor ! ReplaySuccess(instanceId)
      dstProbe.expectMsg((Vector("a", "b"), timestampA(2), timestampA(2), 2))
      dstProbe.expectMsg("x")
      dstProbe.expectMsg("y")
    }
    "recover from scratch if onSnapshot doesn't handle loaded snapshot" in {
      val actor = unrecoveredSnapshotActor()
      val event = eventA("b", 2, timestampA(2))
      val snapshot = Snapshot("foo", idA, event, event, timestamp = timestampA(2))

      logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId))
      actor ! LoadSnapshotSuccess(Some(snapshot), instanceId)
      logProbe.expectMsg(Replay(1, actor, instanceId))
      actor ! Replaying(eventA("a", 1, timestampA(1)), instanceId)
      actor ! Replaying(eventA("b", 2, timestampA(2)), instanceId)
      actor ! ReplaySuccess(instanceId)
      dstProbe.expectMsg((Vector("a"), timestampA(1), timestampA(1), 1))
      dstProbe.expectMsg((Vector("a", "b"), timestampA(2), timestampA(2), 2))
    }
    "save a snapshot" in {
      val actor = recoveredSnapshotActor()
      actor ! Cmd("a")
      actor ! Cmd("b")
      val event1 = logProbe.expectMsgClass(classOf[Write]).events(0).copy(targetLogSequenceNr = 1L)
      actor ! WriteSuccess(event1, instanceId)
      val event2 = logProbe.expectMsgClass(classOf[Write]).events(0).copy(targetLogSequenceNr = 2L)
      actor ! WriteSuccess(event2, instanceId)
      dstProbe.expectMsg((Vector("a"), timestampA(1), timestampA(1), 1))
      dstProbe.expectMsg((Vector("a", "b"), timestampA(2), timestampA(2), 2))
      actor ! "snap"

      val snapshot = Snapshot(State(Vector("a", "b")), idA, event2, event2, timestamp = timestampA(2))
      logProbe.expectMsg(SaveSnapshot(snapshot, system.deadLetters, actor, instanceId))
      actor ! SaveSnapshotSuccess(snapshot.metadata, instanceId)
      dstProbe.expectMsg(snapshot.metadata)
    }
    "save a snapshot with an undelivered event" in {
      val actor = recoveredSnapshotActor()
      actor ! Cmd("a")
      actor ! Cmd("b")
      val event1 = logProbe.expectMsgClass(classOf[Write]).events(0).copy(targetLogSequenceNr = 1L)
      actor ! WriteSuccess(event1, instanceId)
      val event2 = logProbe.expectMsgClass(classOf[Write]).events(0).copy(targetLogSequenceNr = 2L)
      actor ! WriteSuccess(event2, instanceId)
      val event3 = eventB(123, 3, timestampAB(0, 3)) // not handled
      actor ! Written(event3)
      dstProbe.expectMsg((Vector("a"), timestampA(1), timestampA(1), 1))
      dstProbe.expectMsg((Vector("a", "b"), timestampA(2), timestampA(2), 2))
      actor ! "snap"

      val snapshot = Snapshot(State(Vector("a", "b")), idA, event3, event2, timestamp = timestampA(2))
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
      val event1 = logProbe.expectMsgClass(classOf[Write]).events(0).copy(targetLogSequenceNr = 1L)
      actor ! WriteSuccess(event1, instanceId)
      val event2 = logProbe.expectMsgClass(classOf[Write]).events(0).copy(targetLogSequenceNr = 2L)
      actor ! WriteSuccess(event2, instanceId)
      val event3 = logProbe.expectMsgClass(classOf[Write]).events(0).copy(targetLogSequenceNr = 3L)
      actor ! WriteSuccess(event3, instanceId)
      val event4 = logProbe.expectMsgClass(classOf[Write]).events(0).copy(targetLogSequenceNr = 4L)
      actor ! WriteSuccess(event4, instanceId)
      dstProbe.expectMsg((Vector("a"), timestampA(1), timestampA(1), 1))
      dstProbe.expectMsg((Vector("a", "b"), timestampA(2), timestampA(2), 2))
      dstProbe.expectMsg(("x", timestampA(3), timestampA(3), 3))
      dstProbe.expectMsg(("y", timestampA(4), timestampA(4), 4))
      actor ! "snap"

      val unconfirmed = Vector(
        DeliveryAttempt("3", ("x", timestampA(3), timestampA(3), 3), dstProbe.ref.path),
        DeliveryAttempt("4", ("y", timestampA(4), timestampA(4), 4), dstProbe.ref.path))
      val snapshot = Snapshot(State(Vector("a", "b")), idA, event4, event4, deliveryAttempts = unconfirmed, timestamp = timestampA(4))

      logProbe.expectMsg(SaveSnapshot(snapshot, system.deadLetters, actor, instanceId))
      actor ! SaveSnapshotSuccess(snapshot.metadata, instanceId)
      dstProbe.expectMsg(snapshot.metadata)
    }
    "save a snapshot with stashed events" in {
      val ed = eventD("d", 1, timestampABCD(0, 1, 2, 2))
      val ec = eventC("c", 3, timestampABCD(0, 1, 2, 0))

      val actor = recoveredSnapshotActor()
      actor ! Cmd("a")
      actor ! Written(ed)

      val ea = logProbe.expectMsgClass(classOf[Write]).events(0).copy(targetLogSequenceNr = 2)
      actor ! WriteSuccess(ea, instanceId)
      actor ! Written(ec)
      actor ! "snap"

      val snapshot = Snapshot(State(Vector("a")), idA, ec, ea, stashedEvents = Vector(ed, ec), timestamp = timestampA(1))

      dstProbe.expectMsg((Vector("a"), timestampA(1), timestampA(1), 2))
      logProbe.expectMsg(SaveSnapshot(snapshot, system.deadLetters, actor, instanceId))

      actor ! SaveSnapshotSuccess(snapshot.metadata, instanceId)
      dstProbe.expectMsg(snapshot.metadata)
    }
    "recover from a snapshot with stashed events" in {
      val actor = unrecoveredSnapshotActor()

      val ed = eventD("d", 1, timestampABCD(0, 1, 2, 2))
      val ea = eventA("a", 2, timestampA(1))
      val ec = eventC("c", 3, timestampABCD(0, 1, 2, 0))
      val eb = eventB("b", 4, timestampABCD(0, 1, 0, 0))

      val snapshot = Snapshot(State(Vector("a")), idA, ec, ea, stashedEvents = Vector(ed, ec), timestamp = timestampA(1))

      logProbe.expectMsg(LoadSnapshot(idA, actor, instanceId))
      actor ! LoadSnapshotSuccess(Some(snapshot), instanceId)

      logProbe.expectMsg(Replay(4, actor, instanceId))
      actor ! Replaying(eb, instanceId)
      actor ! ReplaySuccess(instanceId)

      dstProbe.expectMsg((Vector("a"), timestampA(1), timestampA(1), 2))
      dstProbe.expectMsg((Vector("a", "b"), timestampABCD(2, 1, 0, 0), timestampABCD(0, 1, 0, 0), 4))
      dstProbe.expectMsg((Vector("a", "b", "c"), timestampABCD(3, 1, 2, 0), timestampABCD(0, 1, 2, 0), 3))
      dstProbe.expectMsg((Vector("a", "b", "c", "d"), timestampABCD(4, 1, 2, 2), timestampABCD(0, 1, 2, 2), 1))
    }
    "not save the same snapshot concurrently" in {
      val actor = recoveredSnapshotActor()
      actor ! "snap"
      actor ! "snap"
      errProbe.expectMsgClass(classOf[IllegalStateException])
    }
  }
}
