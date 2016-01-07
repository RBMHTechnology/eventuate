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

import com.rbmhtechnology.eventuate.DurableEvent._
import com.rbmhtechnology.eventuate.EventsourcedViewSpec.{ event => _, _ }

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

    val handler: Handler[String] = {
      case Success(s) => persistProbe ! Success(s)
      case Failure(e) => throw e
    }

    def onCommand = {
      case "boom" =>
        throw boom
    }

    def onEvent = {
      case "a" | "boom" =>
        persistOnEvent("a-1")(handler)
        persistOnEvent("a-2")(handler)
      case "a-n" =>
        persistOnEventN[String](Seq("a-1", "a-2"), onLast = persistProbe ! _)(persistProbe ! _)
      case "b" =>
        persistOnEvent("c")(persistProbe ! _)
      case "c" =>
        persistOnEvent("d")(persistProbe ! _)
      case "e" =>
        persistOnEvent("e-1")(persistProbe ! _)
        persistOnEvent("e-2")(persistProbe ! _)
      case s: String =>
    }

    override def deliveryId(event: DurableEvent): String = event.payload match {
      case "e" => "e"
      case _   => super.deliveryId(event)
    }

    override private[eventuate] def receiveEvent(event: DurableEvent): Unit = {
      super.receiveEvent(event)
      if (event.payload == "boom") {
        // after restart, there is
        // a PersistOnEventRequest
        // duplicate in the mailbox
        throw boom
      } else deliverProbe ! unconfirmed
    }
  }

  def event(payload: Any, sequenceNr: Long, deliveryId: String = UndefinedDeliveryId): DurableEvent =
    DurableEvent(payload, emitterIdA, None, Set(), 0L, timestamp(sequenceNr), logIdA, logIdA, sequenceNr, deliveryId)
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
    "support persistence in event handler (persistOnEvent)" in {
      val actor = recoveredTestActor(stateSync = true)
      actor ! Written(event("a", 1L))

      deliverProbe.expectMsg(Set("1"))

      val write = logProbe.expectMsgClass(classOf[Write])
      write.events(0).deliveryId should be("1")
      write.events(1).deliveryId should be("1")
      logProbe.sender() ! WriteSuccess(Seq(
        event(write.events(0).payload, 2L, "1"),
        event(write.events(1).payload, 3L, "1")), write.correlationId, instanceId)

      deliverProbe.expectMsg(Set())
      deliverProbe.expectMsg(Set())

      persistProbe.expectMsg(Success("a-1"))
      persistProbe.expectMsg(Success("a-2"))
    }
    "support persistence in event handler (persistOnEventN)" in {
      val actor = recoveredTestActor(stateSync = true)
      actor ! Written(event("a-n", 1L))

      deliverProbe.expectMsg(Set("1"))

      val write = logProbe.expectMsgClass(classOf[Write])

      write.events(0).deliveryId should be("1")
      write.events(1).deliveryId should be("1")
      logProbe.sender() ! WriteSuccess(Seq(
        event(write.events(0).payload, 2L, "1"),
        event(write.events(1).payload, 3L, "1")), write.correlationId, instanceId)

      deliverProbe.expectMsg(Set())
      deliverProbe.expectMsg(Set())

      persistProbe.expectMsg(Success("a-1"))
      persistProbe.expectMsg(Success("a-2"))
      persistProbe.expectMsg(Success("a-2")) // onLast handler
    }
    "support cascading persistence" in {
      val actor = recoveredTestActor(stateSync = true)
      actor ! Written(event("b", 1L))

      deliverProbe.expectMsg(Set("1"))

      val write1 = logProbe.expectMsgClass(classOf[Write])
      write1.events(0).deliveryId should be("1")
      logProbe.sender() ! WriteSuccess(Seq(event(write1.events(0).payload, 2L, "1")), write1.correlationId, instanceId)

      deliverProbe.expectMsg(Set("2"))
      persistProbe.expectMsg(Success("c"))

      val write2 = logProbe.expectMsgClass(classOf[Write])
      write2.events(0).deliveryId should be("2")
      logProbe.sender() ! WriteSuccess(Seq(event(write2.events(0).payload, 3L, "2")), write2.correlationId, instanceId)

      deliverProbe.expectMsg(Set())
      persistProbe.expectMsg(Success("d"))
    }
    "confirm persistence with self-emitted events only" in {
      val actor = recoveredTestActor(stateSync = true)
      actor ! Written(event("a", 1L))

      deliverProbe.expectMsg(Set("1"))

      val write = logProbe.expectMsgClass(classOf[Write])

      actor ! Written(event("x-1", 2L, "1").copy(emitterId = emitterIdB))
      actor ! Written(event("x-2", 3L, "1").copy(emitterId = emitterIdB))

      deliverProbe.expectMsg(Set("1"))
      deliverProbe.expectMsg(Set("1"))

      logProbe.sender() ! WriteSuccess(Seq(
        event(write.events(0).payload, 4L, "1"),
        event(write.events(1).payload, 5L, "1")), write.correlationId, instanceId)

      deliverProbe.expectMsg(Set())
      deliverProbe.expectMsg(Set())

      persistProbe.expectMsg(Success("a-1"))
      persistProbe.expectMsg(Success("a-2"))
    }
    "support custom delivery ids" in {
      val actor = recoveredTestActor(stateSync = true)
      actor ! Written(event("e", 1L))

      deliverProbe.expectMsg(Set("e"))

      val write = logProbe.expectMsgClass(classOf[Write])

      write.events(0).deliveryId should be("e")
      write.events(1).deliveryId should be("e")
    }
    "re-attempt persistence on failed write after restart" in {
      val actor = recoveredTestActor(stateSync = true)
      actor ! Written(event("a", 1L))

      deliverProbe.expectMsg(Set("1"))

      val write1 = logProbe.expectMsgClass(classOf[Write])

      write1.events(0).deliveryId should be("1")
      write1.events(1).deliveryId should be("1")

      // application crash and restart
      logProbe.sender() ! WriteFailure(Seq(
        event(write1.events(0).payload, 0L, "1"),
        event(write1.events(1).payload, 0L, "1")), boom, write1.correlationId, instanceId)
      processRecover(actor, instanceId + 1, Seq(event("a", 1L)))

      deliverProbe.expectMsg(Set("1"))

      val write2 = logProbe.expectMsgClass(classOf[Write])

      write2.events(0).deliveryId should be("1")
      write2.events(1).deliveryId should be("1")

      logProbe.sender() ! WriteSuccess(Seq(
        event(write2.events(0).payload, 2L, "1"),
        event(write2.events(1).payload, 3L, "1")), write2.correlationId, instanceId + 1)

      deliverProbe.expectMsg(Set())
      deliverProbe.expectMsg(Set())

      persistProbe.expectMsg(Success("a-1"))
      persistProbe.expectMsg(Success("a-2"))
    }
    "not re-attempt persistence on successful write after restart" in {
      val actor = recoveredTestActor(stateSync = true)
      actor ! Written(event("a", 1L))

      deliverProbe.expectMsg(Set("1"))

      val write = logProbe.expectMsgClass(classOf[Write])

      write.events(0).deliveryId should be("1")
      write.events(1).deliveryId should be("1")

      logProbe.sender() ! WriteSuccess(Seq(
        event(write.events(0).payload, 2L, "1"),
        event(write.events(1).payload, 3L, "1")), write.correlationId, instanceId)

      deliverProbe.expectMsg(Set())
      deliverProbe.expectMsg(Set())

      persistProbe.expectMsg(Success("a-1"))
      persistProbe.expectMsg(Success("a-2"))

      actor ! "boom"
      processRecover(actor, instanceId + 1, Seq(
        event("a", 1L),
        event(write.events(0).payload, 2L, "1"),
        event(write.events(1).payload, 3L, "1")))

      deliverProbe.expectMsg(Set("1"))
      deliverProbe.expectMsg(Set())
      deliverProbe.expectMsg(Set())

      persistProbe.expectNoMsg(timeout)
      persistProbe.expectNoMsg(timeout)
    }
    "support idempotent event persistence" in {
      val actor = recoveredTestActor(stateSync = true)
      actor ! Written(event("boom", 1L))

      processRecover(actor, instanceId + 1, Seq(event("a", 1L)))

      val write = logProbe.expectMsgClass(classOf[Write])
      logProbe.sender() ! WriteSuccess(Seq(
        event(write.events(0).payload, 2L, "1"),
        event(write.events(1).payload, 3L, "1")), write.correlationId, instanceId + 1)
      logProbe.expectNoMsg(timeout)
    }
  }
}
