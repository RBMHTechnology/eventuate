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

import com.rbmhtechnology.eventuate.log.EventLogLifecycleCassandra
import com.rbmhtechnology.eventuate.log.EventLogLifecycleLeveldb

import org.scalatest._

object EventsourcedActorIntegrationSpec {
  case class Cmd(payloads: String*)
  case class State(state: Vector[String])

  class SampleActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    override val onCommand: Receive = {
      case "reply-success" => persist("okay") {
        case Success(r) => sender() ! r
        case Failure(_) => sender() ! "unexpected failure"
      }
      case "reply-failure" => persist("boom") {
        case Success(_) => sender() ! "unexpected success"
        case Failure(e) => sender() ! e.getMessage
      }
      case "boom" =>
        throw boom
      case Cmd(ps @ _*) =>
        ps.foreach { s =>
          persist(s) {
            case Success(r) => sender() ! r
            case Failure(e) => sender() ! e.getMessage
          }
        }
    }

    override val onEvent: Receive = {
      case evt: String => probe ! evt
    }
  }
  
  class AccActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    var acc: Vector[String] = Vector.empty

    override val onCommand: Receive = {
      case "get-acc" => sender() ! acc
      case s: String => persist(s) {
        case Success(r) => onEvent(r)
        case Failure(e) => throw e
      }
    }

    override val onEvent: Receive = {
      case s: String =>
        acc = acc :+ s
        if (acc.size == 4) probe ! acc
    }
  }

  class ConfirmedDeliveryActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor with ConfirmedDelivery {
    override val onCommand: Receive = {
      case "boom" => throw boom
      case "end" => probe ! "end"
      case "cmd-1" => persist("evt-1")(_ => probe ! "out-1")
      case "cmd-2" => persist("evt-2")(r => onEvent(r.get))
      case "cmd-2-confirm" => persist("evt-2-confirm")(r => onEvent(r.get))
    }

    override val onEvent: Receive = {
      case "evt-2" => deliver("2", "out-2", probe.path)
      case "evt-2-confirm" => confirm("2")
    }
  }

  class ConditionalActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    override val onCommand: Receive = {
      case "persist"      => persist("a")(r => probe ! r.get)
      case "persist-mute" => persist("a")(_ => ())
      case other          => probe ! other
    }

    override val onEvent: Receive = {
      case "a" =>
    }
  }

  class ConditionalView(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedView {
    override val onCommand: Receive = {
      case other => probe ! other
    }

    override val onEvent: Receive = {
      case "a" =>
    }
  }

  case class CollabCmd(to: String)
  case class CollabEvt(to: String, from: String)

  class CollabActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    var initialized = false

    override val onCommand: Receive = {
      case CollabCmd(to) =>
        persist(CollabEvt(to, id))(_ => ())
        initialized = true
    }

    override val onEvent: Receive = {
      case evt @ CollabEvt(`id`, from) =>
        if (initialized) probe ! lastVectorTimestamp else self ! CollabCmd(from)
    }
  }

  case class Route(s: String, destinations: Set[String])

  class RouteeActor(override val id: String,
                    override val aggregateId: Option[String],
                    override val eventLog: ActorRef,
                    probe: ActorRef) extends EventsourcedActor {

    override val onCommand: Receive = {
      case Route(s, destinations) => persist(s, destinations) {
        case Success(s) => onEvent(s)
        case Failure(e) => throw e
      }
    }

    override val onEvent: Receive = {
      case s: String => probe ! s
    }
  }

  class SnapshotActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    var state: Vector[String] = Vector.empty

    override val onCommand: Receive = {
      case "boom" =>
        throw boom
      case "snap" => save(State(state)) {
        case Success(m) => sender() ! m.sequenceNr
        case Failure(e) => throw e
      }
      case s: String => persist(s) {
        case Success(r) => onEvent(r)
        case Failure(e) => throw e
      }
    }

    override val onEvent: Receive = {
      case s: String =>
        state = state :+ s
        probe ! state
    }

    override val onSnapshot: Receive = {
      case State(s) =>
        this.state = s
        probe ! state
    }
  }

  class SnapshotView(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedView {
    var state: Vector[String] = Vector.empty

    override val onCommand: Receive = {
      case "boom" =>
        throw boom
      case "snap" => save(State(state)) {
        case Success(m) => sender() ! m.sequenceNr
        case Failure(e) => throw e
      }
    }

    override val onEvent: Receive = {
      case s: String =>
        state = state :+ s"v-${s}"
        probe ! state
    }

    override val onSnapshot: Receive = {
      case State(s) =>
        this.state = s
        probe ! state
    }
  }
}

abstract class EventsourcedActorIntegrationSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterEach {
  import EventsourcedActorIntegrationSpec._

  def log: ActorRef
  def logId: String

  var probe: TestProbe = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    probe = TestProbe()
  }
  
  "An EventsourcedActor" can {
    "preserve the command sender when invoking the persist handler on success" in {
      val actor = system.actorOf(Props(new SampleActor("1", log, probe.ref)))
      actor.tell("reply-success", probe.ref)
      probe.expectMsg("okay")
    }
    "preserve the command sender when invoking the persist handler on failure" in {
      val actor = system.actorOf(Props(new SampleActor("1", log, probe.ref)))
      actor.tell("reply-failure", probe.ref)
      probe.expectMsg("boom")
    }
    "persist multiple events per command as atomic batch" in {
      val actor = system.actorOf(Props(new SampleActor("1", log, probe.ref)))
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
    "support conditional command processing" in {
      val act1Props = Props(new ConditionalActor("1", log, probe.ref))
      val act2Props = Props(new ConditionalActor("2", log, probe.ref))
      val viewProps = Props(new ConditionalView("3", log, probe.ref))

      val act1 = system.actorOf(act1Props, "act1")
      val act2 = system.actorOf(act2Props, "act2")
      val view = system.actorOf(viewProps, "view")

      val condition = VectorTime(logId -> 3L)

      view ! ConditionalCommand(condition, "delayed")
      act1 ! ConditionalCommand(condition, "delayed-1")
      act2 ! ConditionalCommand(condition, "delayed-2")

      act1 ! "persist"
      act1 ! "persist"
      act1 ! "persist-mute"

      probe.expectMsg("a")
      probe.expectMsg("a")
      probe.expectMsgAllOf("delayed-1", "delayed-2", "delayed")

      // make sure that conditions are also met after recovery
      system.actorOf(viewProps) ! ConditionalCommand(condition, "delayed")
      system.actorOf(act1Props) ! ConditionalCommand(condition, "delayed-1")
      system.actorOf(act2Props) ! ConditionalCommand(condition, "delayed-2")

      probe.expectMsgAllOf("delayed-1", "delayed-2", "delayed")
    }
    "support snapshots" in {
      val actorProbe = TestProbe()
      val viewProbe = TestProbe()

      val actor = system.actorOf(Props(new SnapshotActor("1", log, actorProbe.ref)))
      val view = system.actorOf(Props(new SnapshotView("2", log, viewProbe.ref)))

      actor ! "a"
      actor ! "b"

      actorProbe.expectMsg(Vector("a"))
      actorProbe.expectMsg(Vector("a", "b"))

      viewProbe.expectMsg(Vector("v-a"))
      viewProbe.expectMsg(Vector("v-a", "v-b"))

      actor.tell("snap", actorProbe.ref)
      view.tell("snap", viewProbe.ref)

      actorProbe.expectMsg(2)
      viewProbe.expectMsg(2)

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
  }
}

class EventsourcedActorIntegrationSpecLeveldb extends EventsourcedActorIntegrationSpec with EventLogLifecycleLeveldb {
  override def batching = false
}

class EventsourcedActorIntegrationSpecCassandra extends EventsourcedActorIntegrationSpec with EventLogLifecycleCassandra {
  override def batching = false
}