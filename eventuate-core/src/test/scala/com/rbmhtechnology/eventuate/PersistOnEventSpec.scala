/*
 * Copyright 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.rbmhtechnology.eventuate.EventsourcedViewSpec.{ event => _, _ }
import com.rbmhtechnology.eventuate.PersistOnEvent._

import org.scalatest._

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.util._

object PersistOnEventSpec {
  val timeout = 0.2.seconds

  class TestActor(
    val logProbe: ActorRef,
    val persistProbe: ActorRef,
    val deliverProbe: ActorRef,
    override val stateSync: Boolean) extends EventsourcedActor with PersistOnEvent {

    val id = emitterIdA
    val eventLog = logProbe

    def onCommand = {
      case "snap" =>
        save("foo") {
          case Success(_) =>
          case Failure(e) => throw e
        }
      case "boom" =>
        throw TestException
    }

    def onEvent = {
      case "a" | "boom" =>
        persistOnEvent("a-1")
        persistOnEvent("a-2")
      case "b" =>
        persistOnEvent("c")
        persistOnEvent("c-1")
      case "c" =>
        persistOnEvent("c-2")
      case "x" =>
        persistOnEvent("x-1", Set("14"))
        persistOnEvent("x-2", Set("15"))
      case e @ ("a-1" | "a-2" | "c-1" | "c-2") if !recovering =>
        persistProbe ! e
      case s: String =>
    }

    override def onSnapshot = {
      case "foo" =>
    }

    override private[eventuate] def receiveEvent(event: DurableEvent): Unit = {
      super.receiveEvent(event)
      if (event.payload == "boom") {
        // after restart, there is
        // a PersistOnEventRequest
        // duplicate in the mailbox
        throw TestException
      } else deliverProbe ! unconfirmedRequests
    }
  }

  def event(payload: Any, sequenceNr: Long, persistOnEventEvent: Option[DurableEvent] = None): DurableEvent =
    DurableEvent(payload, emitterIdA, None, Set(), 0L, timestamp(sequenceNr), logIdA, logIdA, sequenceNr, None, persistOnEventEvent.map(_.localSequenceNr), persistOnEventEvent.map(_.id))
}

class PersistOnEventSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  import PersistOnEventSpec._
  import EventsourcingProtocol._

  var instanceId: Int = _
  var logProbe: TestProbe = _
  var persistProbe: TestProbe = _
  var deliverProbe: TestProbe = _

  override def beforeEach(): Unit = {
    instanceId = EventsourcedView.instanceIdCounter.get
    logProbe = TestProbe()
    persistProbe = TestProbe()
    deliverProbe = TestProbe()
  }

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  def unrecoveredTestActor(stateSync: Boolean): ActorRef =
    system.actorOf(Props(new TestActor(logProbe.ref, persistProbe.ref, deliverProbe.ref, stateSync)))

  def recoveredTestActor(stateSync: Boolean): ActorRef =
    processRecover(unrecoveredTestActor(stateSync))

  def processRecover(actor: ActorRef, instanceId: Int = instanceId, events: Seq[DurableEvent] = Seq()): ActorRef = {
    logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
    logProbe.sender() ! LoadSnapshotSuccess(None, instanceId)

    events.lastOption match {
      case Some(event) =>
        logProbe.expectMsg(Replay(1L, Some(actor), instanceId))
        logProbe.sender() ! ReplaySuccess(events, event.localSequenceNr, instanceId)
        logProbe.expectMsg(Replay(event.localSequenceNr + 1L, None, instanceId))
        logProbe.sender() ! ReplaySuccess(Nil, event.localSequenceNr, instanceId)
      case None =>
        logProbe.expectMsg(Replay(1L, Some(actor), instanceId))
        logProbe.sender() ! ReplaySuccess(events, 0L, instanceId)
    }
    actor
  }

  "An EventsourcedActor with PersistOnEvent" must {
    "support persistence in event handler" in {
      val actor = recoveredTestActor(stateSync = true)
      val eventA = event("a", 1L)
      actor ! Written(eventA)

      deliverProbe.expectMsg(Set(1L))

      val write = logProbe.expectMsgClass(classOf[Write])
      write.events(0).persistOnEventId should be(Some(eventA.id))
      write.events(1).persistOnEventId should be(Some(eventA.id))
      logProbe.sender() ! WriteSuccess(Seq(
        event(write.events(0).payload, 2L, Some(eventA)),
        event(write.events(1).payload, 3L, Some(eventA))), write.correlationId, instanceId)

      deliverProbe.expectMsg(Set())
      deliverProbe.expectMsg(Set())

      persistProbe.expectMsg("a-1")
      persistProbe.expectMsg("a-2")
    }
    "support cascading persistence" in {
      val actor = recoveredTestActor(stateSync = true)
      val eventB = event("b", 1L)
      actor ! Written(eventB)

      deliverProbe.expectMsg(Set(1L))

      val write1 = logProbe.expectMsgClass(classOf[Write])

      write1.events(0).persistOnEventId should be(Some(eventB.id))
      write1.events(1).persistOnEventId should be(Some(eventB.id))

      val eventC = event(write1.events(0).payload, 2L, Some(eventB))
      val eventC2 = event(write1.events(1).payload, 3L, Some(eventB))
      logProbe.sender() ! WriteSuccess(Seq(eventC, eventC2), write1.correlationId, instanceId)

      deliverProbe.expectMsg(Set(2L))
      deliverProbe.expectMsg(Set(2L))
      persistProbe.expectMsg("c-1")

      val write2 = logProbe.expectMsgClass(classOf[Write])
      write2.events(0).persistOnEventId should be(Some(eventC.id))
      logProbe.sender() ! WriteSuccess(Seq(event(write2.events(0).payload, 4L, Some(eventC))), write2.correlationId, instanceId)

      deliverProbe.expectMsg(Set())
      persistProbe.expectMsg("c-2")
    }
    "confirm persistence with self-emitted events only" in {
      val actor = recoveredTestActor(stateSync = true)
      val eventA = event("a", 1L)
      actor ! Written(eventA)

      deliverProbe.expectMsg(Set(1L))

      val write = logProbe.expectMsgClass(classOf[Write])

      actor ! Written(event("x-1", 2L, Some(eventA)).copy(emitterId = emitterIdB))
      actor ! Written(event("x-2", 3L, Some(eventA)).copy(emitterId = emitterIdB))

      deliverProbe.expectMsg(Set(1L))
      deliverProbe.expectMsg(Set(1L))

      logProbe.sender() ! WriteSuccess(Seq(
        event(write.events(0).payload, 4L, Some(eventA)),
        event(write.events(1).payload, 5L, Some(eventA))), write.correlationId, instanceId)

      deliverProbe.expectMsg(Set())
      deliverProbe.expectMsg(Set())

      persistProbe.expectMsg("a-1")
      persistProbe.expectMsg("a-2")
    }
    "re-attempt persistence on failed write after restart" in {
      val actor = recoveredTestActor(stateSync = true)
      val eventA = event("a", 1L)
      actor ! Written(eventA)

      deliverProbe.expectMsg(Set(1L))

      val write1 = logProbe.expectMsgClass(classOf[Write])

      write1.events(0).persistOnEventId should be(Some(eventA.id))
      write1.events(1).persistOnEventId should be(Some(eventA.id))

      // application crash and restart
      logProbe.sender() ! WriteFailure(Seq(
        event(write1.events(0).payload, 0L, Some(eventA)),
        event(write1.events(1).payload, 0L, Some(eventA))), TestException, write1.correlationId, instanceId)
      processRecover(actor, instanceId + 1, Seq(event("a", 1L)))

      deliverProbe.expectMsg(Set(1L))

      val write2 = logProbe.expectMsgClass(classOf[Write])

      write2.events(0).persistOnEventId should be(Some(eventA.id))
      write2.events(1).persistOnEventId should be(Some(eventA.id))

      logProbe.sender() ! WriteSuccess(Seq(
        event(write2.events(0).payload, 2L, Some(eventA)),
        event(write2.events(1).payload, 3L, Some(eventA))), write2.correlationId, instanceId + 1)

      deliverProbe.expectMsg(Set())
      deliverProbe.expectMsg(Set())

      persistProbe.expectMsg("a-1")
      persistProbe.expectMsg("a-2")
    }
    "not re-attempt persistence on successful write after restart" in {
      val actor = recoveredTestActor(stateSync = true)
      val eventA = event("a", 1L)
      actor ! Written(eventA)

      deliverProbe.expectMsg(Set(1L))

      val write = logProbe.expectMsgClass(classOf[Write])

      write.events(0).persistOnEventId should be(Some(eventA.id))
      write.events(1).persistOnEventId should be(Some(eventA.id))

      logProbe.sender() ! WriteSuccess(Seq(
        event(write.events(0).payload, 2L, Some(eventA)),
        event(write.events(1).payload, 3L, Some(eventA))), write.correlationId, instanceId)

      deliverProbe.expectMsg(Set())
      deliverProbe.expectMsg(Set())

      persistProbe.expectMsg("a-1")
      persistProbe.expectMsg("a-2")

      actor ! "boom"
      processRecover(actor, instanceId + 1, Seq(
        event("a", 1L),
        event(write.events(0).payload, 2L, Some(eventA)),
        event(write.events(1).payload, 3L, Some(eventA))))

      deliverProbe.expectMsg(Set(1L))
      deliverProbe.expectMsg(Set())
      deliverProbe.expectMsg(Set())

      persistProbe.expectNoMsg(timeout)
      persistProbe.expectNoMsg(timeout)
    }
    "not re-attempt persistence on successful write of events without persistOnEventReference after restart" in {
      val actor = recoveredTestActor(stateSync = true)
      val eventA = event("a", 1L)
      actor ! Written(eventA)

      deliverProbe.expectMsg(Set(1L))

      val write = logProbe.expectMsgClass(classOf[Write])

      write.events(0).persistOnEventId should be(Some(eventA.id))
      write.events(1).persistOnEventId should be(Some(eventA.id))

      val persistedOnA = Seq(
        event(write.events(0).payload, 2L, Some(eventA)).copy(persistOnEventId = None),
        event(write.events(1).payload, 3L, Some(eventA)))
      logProbe.sender() ! WriteSuccess(persistedOnA, write.correlationId, instanceId)

      deliverProbe.expectMsg(Set())
      deliverProbe.expectMsg(Set())

      persistProbe.expectMsg("a-1")
      persistProbe.expectMsg("a-2")

      actor ! "boom"
      processRecover(actor, instanceId + 1, eventA +: persistedOnA)

      deliverProbe.expectMsg(Set(1L))
      deliverProbe.expectMsg(Set())
      deliverProbe.expectMsg(Set())

      persistProbe.expectNoMsg(timeout)
      persistProbe.expectNoMsg(timeout)
    }
    "support idempotent event persistence" in {
      val actor = recoveredTestActor(stateSync = true)
      actor ! Written(event("boom", 1L))

      val eventA = event("a", 1L)
      processRecover(actor, instanceId + 1, Seq(eventA))

      val write = logProbe.expectMsgClass(classOf[Write])
      logProbe.sender() ! WriteSuccess(Seq(
        event(write.events(0).payload, 2L, Some(eventA)),
        event(write.events(1).payload, 3L, Some(eventA))), write.correlationId, instanceId + 1)
      logProbe.expectNoMsg(timeout)
    }
    "save a snapshot with persistOnEvent requests" in {
      val actor = recoveredTestActor(stateSync = false)

      actor ! Written(event("x", 1L))
      logProbe.expectMsgClass(classOf[Write])

      actor ! "snap"

      val save = logProbe.expectMsgClass(classOf[SaveSnapshot])
      val expected = PersistOnEventRequest(1L, Some(EventId("logA", 1L)), Vector(
        PersistOnEventInvocation("x-1", Set("14")),
        PersistOnEventInvocation("x-2", Set("15"))), instanceId)

      save.snapshot.persistOnEventRequests(0) should be(expected)
    }
    "recover from a snapshot with persistOnEvent requests whose execution failed" in {
      val actor = recoveredTestActor(stateSync = false)

      actor ! Written(event("x", 1L))

      val write1 = logProbe.expectMsgClass(classOf[Write])

      actor ! "snap"

      val save = logProbe.expectMsgClass(classOf[SaveSnapshot])

      actor ! "boom"

      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId + 1))
      logProbe.sender() ! LoadSnapshotSuccess(Some(save.snapshot), instanceId + 1)
      logProbe.expectMsg(Replay(2L, Some(actor), instanceId + 1))
      logProbe.sender() ! ReplaySuccess(Nil, 1L, instanceId + 1)

      val write2 = logProbe.expectMsgClass(classOf[Write])
      write1.events should be(write2.events)
    }
    "recover from a snapshot with persistOnEvent requests whose execution succeeded" in {
      val actor = recoveredTestActor(stateSync = false)

      val eventX = event("x", 1L)
      actor ! Written(eventX)

      val write1 = logProbe.expectMsgClass(classOf[Write])
      val written = List(
        event(write1.events(0).payload, 2L, Some(eventX)),
        event(write1.events(1).payload, 3L, Some(eventX)))

      actor ! "snap"

      val save = logProbe.expectMsgClass(classOf[SaveSnapshot])

      actor ! "boom"

      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId + 1))
      logProbe.sender() ! LoadSnapshotSuccess(Some(save.snapshot), instanceId + 1)
      logProbe.expectMsg(Replay(2L, Some(actor), instanceId + 1))
      logProbe.sender() ! ReplaySuccess(written, 3L, instanceId + 1)
      logProbe.expectMsg(Replay(4L, None, instanceId + 1))
      logProbe.sender() ! ReplaySuccess(Nil, 3L, instanceId + 1)
      logProbe.expectNoMsg(timeout)
    }
    "recover from a snapshot with persistOnEvent requests without persistOnEventReferences whose execution succeeded" in {
      val actor = recoveredTestActor(stateSync = false)

      val eventX = event("x", 1L)
      actor ! Written(eventX)

      val write1 = logProbe.expectMsgClass(classOf[Write])
      val written = List(
        event(write1.events(0).payload, 2L, Some(eventX)),
        event(write1.events(1).payload, 3L, Some(eventX)))

      actor ! "snap"

      val save = logProbe.expectMsgClass(classOf[SaveSnapshot])
      val snapshotWithoutReferences = save.snapshot.copy(persistOnEventRequests = save.snapshot.persistOnEventRequests.map(_.copy(persistOnEventId = None)))

      actor ! "boom"

      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId + 1))
      logProbe.sender() ! LoadSnapshotSuccess(Some(snapshotWithoutReferences), instanceId + 1)
      logProbe.expectMsg(Replay(2L, Some(actor), instanceId + 1))
      logProbe.sender() ! ReplaySuccess(written, 3L, instanceId + 1)
      logProbe.expectMsg(Replay(4L, None, instanceId + 1))
      logProbe.sender() ! ReplaySuccess(Nil, 3L, instanceId + 1)
      logProbe.expectNoMsg(timeout)
    }
    "be tolerant to changing actor paths across incarnations" in {
      val actor = unrecoveredTestActor(stateSync = false)
      val path = ActorPath.fromString("akka://test/user/invalid")
      val requests = Vector(
        PersistOnEventRequest(3L, Some(EventId("p-2", 2L)), Vector(PersistOnEventInvocation("y", Set())), instanceId),
        PersistOnEventRequest(4L, None, Vector(PersistOnEventInvocation("z", Set())), instanceId))
      val snapshot = Snapshot("foo", emitterIdA, event("x", 2), timestamp(2), 2, persistOnEventRequests = requests)

      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
      logProbe.sender() ! LoadSnapshotSuccess(Some(snapshot), instanceId)
      logProbe.expectMsg(Replay(3L, Some(actor), instanceId))
      logProbe.sender() ! ReplaySuccess(Nil, 2L, instanceId)

      val write1 = logProbe.expectMsgClass(classOf[Write])
      val write2 = logProbe.expectMsgClass(classOf[Write])

      write1.events(0).payload should be("y")
      write2.events(0).payload should be("z")
    }
  }
}
