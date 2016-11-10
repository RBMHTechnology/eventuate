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

import org.scalatest._

import scala.util._

object EventsourcedActorIntegrationSpec {
  case class Cmd(payloads: String*)
  case class State(state: Vector[String])
  case object UnhandledEvent

  class ReplyActor(val id: String, val eventLog: ActorRef) extends EventsourcedActor {
    override def onCommand = {
      case "reply-success" => persist("okay") {
        case Success(r) => sender() ! r
        case Failure(_) => sender() ! "unexpected failure"
      }
      case "reply-failure" => persist("boom") {
        case Success(_) => sender() ! "unexpected success"
        case Failure(e) => sender() ! e.getMessage
      }
    }

    override def onEvent = {
      case evt: String =>
    }
  }

  class BatchActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    override def onCommand = {
      case "boom" =>
        throw IntegrationTestException
      case Cmd(ps @ _*) =>
        ps.foreach { s =>
          persist(s) {
            case Success(r) => sender() ! r
            case Failure(e) => sender() ! e.getMessage
          }
        }
    }

    override def onEvent = {
      case evt: String => if (recovering) probe ! evt
    }
  }

  class AccActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    var acc: Vector[String] = Vector.empty

    override def onCommand = {
      case "get-acc" => sender() ! acc
      case s: String => persist(s) {
        case Success(r) =>
        case Failure(e) => throw e
      }
    }

    override def onEvent = {
      case s: String =>
        acc = acc :+ s
        if (acc.size == 4) probe ! acc
    }
  }

  class ConfirmedDeliveryActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor with ConfirmedDelivery {
    override def onCommand = {
      case "boom"          => throw IntegrationTestException
      case "end"           => probe ! "end"
      case "cmd-1"         => persist("evt-1")(_ => probe ! "out-1")
      case "cmd-2"         => persist("evt-2")(r => ())
      case "cmd-2-confirm" => persistConfirmation("evt-2-confirm", "2")(r => ())
    }

    override def onEvent = {
      case "evt-2" => deliver("2", "out-2", probe.path)
    }
  }

  class ConditionalActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor with ConditionalRequests {
    override def onCommand = {
      case "persist"      => persist("a")(r => probe ! r.get)
      case "persist-mute" => persist("a")(_ => ())
      case other          => probe ! other
    }

    override def onEvent = {
      case "a" =>
    }
  }

  class ConditionalView(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedView with ConditionalRequests {
    override def onCommand = {
      case other => probe ! other
    }

    override def onEvent = {
      case "a" =>
    }
  }

  case class CollabCmd(to: String)
  case class CollabEvt(to: String, from: String)

  class CollabActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    var initialized = false

    override def onCommand = {
      case CollabCmd(to) =>
        persist(CollabEvt(to, id))(_ => ())
        initialized = true
    }

    override def onEvent = {
      case evt @ CollabEvt(`id`, from) =>
        if (initialized) probe ! lastVectorTimestamp else self ! CollabCmd(from)
    }
  }

  case class Route(s: String, destinations: Set[String])

  class RouteeActor(
    override val id: String,
    override val aggregateId: Option[String],
    override val eventLog: ActorRef,
    probe: ActorRef) extends EventsourcedActor {

    override def onCommand = {
      case Route(s, destinations) => persist(s, destinations) {
        case Success(s) =>
        case Failure(e) => throw e
      }
    }

    override def onEvent = {
      case s: String => probe ! s
    }
  }

  class SnapshotActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    var state: Vector[String] = Vector.empty

    override def onCommand = {
      case "boom" =>
        throw IntegrationTestException
      case "snap" => save(State(state)) {
        case Success(m) => sender() ! m.sequenceNr
        case Failure(e) => throw e
      }
      case s: String => persist(s) {
        case Success(r) =>
        case Failure(e) => throw e
      }
      case UnhandledEvent => persist(UnhandledEvent) {
        case Success(r) =>
        case Failure(e) => throw e
      }
    }

    override def onEvent = {
      case s: String =>
        state = state :+ s
        probe ! state
    }

    override def onSnapshot = {
      case State(s) =>
        this.state = s
        probe ! state
    }
  }

  class SnapshotView(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedView {
    var state: Vector[String] = Vector.empty

    override def onCommand = {
      case "boom" =>
        throw IntegrationTestException
      case "snap" => save(State(state)) {
        case Success(m) => sender() ! m.sequenceNr
        case Failure(e) => throw e
      }
    }

    override def onEvent = {
      case s: String =>
        state = state :+ s"v-${s}"
        probe ! state
      case UnhandledEvent =>
        // in constrast to the `SnapshotActor` the `SnapshotView` actor should react on the `UnhandledEvent`
        // so we can wait for that event during the test run
        probe ! UnhandledEvent
    }

    override def onSnapshot = {
      case State(s) =>
        this.state = s
        probe ! state
    }
  }

  class ChunkedReplayActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    var state: Vector[String] = Vector.empty

    override def replayBatchSize: Int = 2

    override def onCommand = {
      case "boom" =>
        throw IntegrationTestException
      case "state" =>
        probe ! state
      case s: String =>
        persist(s) {
          case Success(r) =>
          case Failure(e) => throw e
        }
    }

    override def onEvent = {
      case s: String => state = state :+ s
    }
  }
}

trait EventsourcedActorIntegrationSpec extends TestKitBase with WordSpecLike with Matchers with SingleLocationSpec {
  import EventsourcedActorIntegrationSpec._

  var probe: TestProbe = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    probe = TestProbe()
  }

  "An EventsourcedActor" can {
    "preserve the command sender when invoking the persist handler on success" in {
      val actor = system.actorOf(Props(new ReplyActor("1", log)))
      actor.tell("reply-success", probe.ref)
      probe.expectMsg("okay")
    }
    "preserve the command sender when invoking the persist handler on failure" in {
      val sdrProbe = TestProbe()
      val actor = system.actorOf(Props(new ReplyActor("1", log)))
      actor.tell("reply-failure", probe.ref)
      probe.expectMsg("boom")
    }
    "persist multiple events per command as atomic batch" in {
      val actor = system.actorOf(Props(new BatchActor("1", log, probe.ref)))
      actor.tell(Cmd("a", "boom", "c"), probe.ref)
      probe.expectMsg("boom")
      probe.expectMsg("boom")
      probe.expectMsg("boom")
      actor.tell(Cmd("x", "y"), probe.ref)
      probe.expectMsg("x")
      probe.expectMsg("y")
      actor ! "boom"
      probe.expectMsg("x")
      probe.expectMsg("y")
    }
    "consume events from other actors (via event log)" in {
      val probe1 = TestProbe()
      val probe2 = TestProbe()
      val probe3 = TestProbe()

      val actor1 = system.actorOf(Props(new AccActor("1", log, probe1.ref)))
      val actor2 = system.actorOf(Props(new AccActor("2", log, probe2.ref)))
      val actor3 = system.actorOf(Props(new AccActor("3", log, probe3.ref)))

      actor1 ! "a"
      actor2 ! "b"
      actor1 ! "boom"
      actor1 ! "c"
      actor3 ! "d"

      val r1 = probe1.expectMsgClass(classOf[Vector[String]])
      val r2 = probe2.expectMsgClass(classOf[Vector[String]])
      val r3 = probe3.expectMsgClass(classOf[Vector[String]])

      val expected = Vector("a", "b", "c", "d")

      // check content
      r1.sorted should be(expected)
      r2.sorted should be(expected)
      r3.sorted should be(expected)

      // check ordering
      r1 should be(r2)
      r1 should be(r3)
    }
    "produce commands to other actors (at-most-once)" in {
      val actor = system.actorOf(Props(new ConfirmedDeliveryActor("1", log, probe.ref)))
      actor ! "cmd-1"
      probe.expectMsg("out-1")
      actor ! "boom"
      actor ! "end"
      probe.expectMsg("end")
    }
    "produce commands to other actors (at-least-once)" in {
      val actor = system.actorOf(Props(new ConfirmedDeliveryActor("1", log, probe.ref)))
      actor ! "cmd-2"
      probe.expectMsg("out-2")
      actor ! "boom"
      actor ! "end"
      probe.expectMsg("out-2")
      probe.expectMsg("end")
      actor ! "cmd-2-confirm"
      actor ! "boom"
      actor ! "end"
      probe.expectMsg("end")
    }
    "route events to custom destinations" in {
      val probe1 = TestProbe()
      val probe2 = TestProbe()
      val probe3 = TestProbe()

      val a1r1 = system.actorOf(Props(new RouteeActor("1", Some("a1"), log, probe1.ref)))
      val a1r2 = system.actorOf(Props(new RouteeActor("2", Some("a1"), log, probe2.ref)))
      val a2r1 = system.actorOf(Props(new RouteeActor("3", Some("a2"), log, probe3.ref)))

      a1r1 ! Route("x", Set())

      probe1.expectMsg("x")
      probe2.expectMsg("x")

      a1r1 ! Route("y", Set("a2"))

      probe1.expectMsg("y")
      probe2.expectMsg("y")
      probe3.expectMsg("y")
    }
  }

  "Eventsourced actors and views" must {
    "support conditional request processing" in {
      val act1Props = Props(new ConditionalActor("1", log, probe.ref))
      val act2Props = Props(new ConditionalActor("2", log, probe.ref))
      val viewProps = Props(new ConditionalView("3", log, probe.ref))

      val act1 = system.actorOf(act1Props, "act1")
      val act2 = system.actorOf(act2Props, "act2")
      val view = system.actorOf(viewProps, "view")

      val condition = VectorTime(logId -> 3L)

      view ! ConditionalRequest(condition, "delayed")
      act1 ! ConditionalRequest(condition, "delayed-1")
      act2 ! ConditionalRequest(condition, "delayed-2")

      act1 ! "persist"
      act1 ! "persist"
      act1 ! "persist-mute"

      probe.expectMsg("a")
      probe.expectMsg("a")
      probe.expectMsgAllOf("delayed-1", "delayed-2", "delayed")

      // make sure that conditions are also met after recovery
      system.actorOf(viewProps) ! ConditionalRequest(condition, "delayed")
      system.actorOf(act1Props) ! ConditionalRequest(condition, "delayed-1")
      system.actorOf(act2Props) ! ConditionalRequest(condition, "delayed-2")

      probe.expectMsgAllOf("delayed-1", "delayed-2", "delayed")
    }
    "support snapshots" in {
      val actorProbe = TestProbe()
      val viewProbe = TestProbe()

      val actor = system.actorOf(Props(new SnapshotActor("1", log, actorProbe.ref)))
      val view = system.actorOf(Props(new SnapshotView("2", log, viewProbe.ref)))

      actor ! "a"
      actor ! "b"
      actor ! UnhandledEvent
      actor ! UnhandledEvent

      actorProbe.expectMsg(Vector("a"))
      actorProbe.expectMsg(Vector("a", "b"))

      viewProbe.expectMsg(Vector("v-a"))
      viewProbe.expectMsg(Vector("v-a", "v-b"))
      viewProbe.expectMsg(UnhandledEvent)
      viewProbe.expectMsg(UnhandledEvent)

      actor.tell("snap", actorProbe.ref)
      view.tell("snap", viewProbe.ref)

      // although the `SnapshotActor` handled only events up to sequence number 2
      // we expect the snapshot to be saved until all 4 'processed' events
      actorProbe.expectMsg(4)
      viewProbe.expectMsg(4)

      actor ! "c"

      actorProbe.expectMsg(Vector("a", "b", "c"))
      viewProbe.expectMsg(Vector("v-a", "v-b", "v-c"))

      actor ! "boom"
      view ! "boom"

      actorProbe.expectMsg(Vector("a", "b"))
      actorProbe.expectMsg(Vector("a", "b", "c"))

      viewProbe.expectMsg(Vector("v-a", "v-b"))
      viewProbe.expectMsg(Vector("v-a", "v-b", "v-c"))
    }
    "support batch event replay" in {
      val probe = TestProbe()
      val actor = system.actorOf(Props(new ChunkedReplayActor("1", log, probe.ref)))

      val messages = 1.to(10).map(i => s"m-$i")

      messages.foreach(actor ! _)

      actor ! "state"
      probe.expectMsg(messages)

      actor ! "boom"
      actor ! "state"
      probe.expectMsg(messages)
    }
  }
}
