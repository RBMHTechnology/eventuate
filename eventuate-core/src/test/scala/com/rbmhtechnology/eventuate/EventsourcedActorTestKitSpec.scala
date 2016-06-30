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
import akka.pattern.ask
import akka.testkit._
import com.rbmhtechnology.eventuate._
import org.scalatest._
import org.scalatest.exceptions.TestFailedException

import scala.util._

case class Ping(i: Int)

case class Pong(i: Int)

case class Request(s: String)

case class RequestWithPersistence(s: String, persistenceFirst: Boolean)

case class Response(s: String)

case class PersistSlowly(remaining: Seq[Int])

class SimpleActor(log: ActorRef) extends EventsourcedActor with PersistOnEvent {

  override def id: String = "simple-actor"

  override def eventLog: ActorRef = log

  var secretState = 0

  override def onEvent: Receive = {
    case p: Pong =>
      secretState = p.i

    case Ping(i) =>
      persistOnEvent(Pong(i))
  }

  override def onCommand: Receive = {
    case Ping(i) =>
      persist(Pong(i)) {
        case Success(e)  =>
        case Failure(ex) =>
      }

    case Request(request) =>
      sender ! Response(request)

    case RequestWithPersistence(request, false) =>
      sender ! Response(request)
      persist(Response(request)) {
        case Success(e)  =>
        case Failure(ex) =>
      }

    case RequestWithPersistence(request, true) =>
      persist(Response(request)) {
        case Success(e)  =>
        case Failure(ex) =>
      }
      sender ! Response(request)

    case PersistSlowly(remaining) =>
      if (remaining.nonEmpty) {
        self ! PersistSlowly(remaining.tail)
        persist(Pong(remaining.head)) {
          case Success(e)  =>

          case Failure(ex) =>
        }

      }
  }
}

class MySimpleActorTestActor(log: ActorRef) extends SimpleActor(log) with ExplicitOnEventCall {
  override def onCommand: Receive = super.onCommand orElse persistRecieve
}

/**
 * fixme: move to reasonable package.
 */
class EventsourcedActorTestKitSpec extends TestKit(EventSourcedActorTestKit.actorSystem)
  with EventSourcedActorTestKit
  with GivenWhenThen {

  feature("behaviour onCommand is testable") {
    scenario("test persisted events after call") {
      Given("an actor")
      val actor = initEventSourcedActor(new SimpleActor(eventlog))

      Then("persisted events can be tested")
      // tell actor something
      actor ! Ping(2)
      expectEvent(Pong(2))

      // expect event processes communication between event log and actor correctly and is ready for next call.
      actor ! Ping(3)
      expectEvent(Pong(3))

    }

    scenario("events are stashed and can be expected after multiple commands") {

      val actor = initEventSourcedActor(new SimpleActor(eventlog))

      actor ! Ping(2)
      actor ! Ping(3)
      expectEvent(Pong(2))
      expectEvent(Pong(3))
    }

    scenario("test ask pattern") {
      // implicit execution context and timeout are provided by ActorTest
      val actor = initEventSourcedActor(new SimpleActor(eventlog))

      val response = actor ? Request("test request")
      response.map {
        case Response(s) =>
          assert(s === "test request")
      }
    }

    scenario("test ask pattern + event persistence - persistence first") {
      val actor = initEventSourcedActor(new SimpleActor(eventlog))

      val response = actor ? RequestWithPersistence("test request", true)

      expectCommand(response, Response("test request"))
      expectEvent(Response("test request"))

      // order of querying results is irrelevant
      val response2 = actor ? RequestWithPersistence("test request2", true)

      expectEvent(Response("test request2"))
      expectCommand(response2, Response("test request2"))
    }

    scenario("test ask pattern + event persistence persistence last") {
      val actor = initEventSourcedActor(new SimpleActor(eventlog))

      val response = actor ? RequestWithPersistence("test request", false)

      expectCommand(response, Response("test request"))
      expectEvent(Response("test request"))

      // order of querying results is irrelevant

      val response2 = actor ? RequestWithPersistence("test request2", false)

      expectEvent(Response("test request2"))

      expectCommand(response2, Response("test request2"))
    }

    scenario("expecting command in ask pattern can actually fail") {
      val actor = initEventSourcedActor(new SimpleActor(eventlog))

      val response = actor ? RequestWithPersistence("request", false)

      assert(Try(expectCommand(response, Response("incorrect answer"))).isInstanceOf[Failure[TestFailedException]])
    }

    scenario("persisting slowly - delegates remaining and events are expectable") {
      val actor = initEventSourcedActor(new SimpleActor(eventlog))

      actor ! PersistSlowly(1 to 10)

      expectEvent(Pong(1))
      expectEvent(Pong(2))
      expectEvent(Pong(3))
      expectEvent(Pong(4))
      expectEvent(Pong(5))
      expectEvent(Pong(6))
      expectEvent(Pong(7))
      expectEvent(Pong(8))
      expectEvent(Pong(9))
      expectEvent(Pong(10))

      // which is equivalent to:
      actor ! PersistSlowly(1 to 10)
      expectEventsOneAtATime((1 to 10) map Pong)
    }
  }

  feature("eventlog can be cleaned and queried") {
    scenario("popping single events") {
      val actor = initEventSourcedActor(new SimpleActor(eventlog))

      import com.rbmhtechnology.eventuate.EventsourcedActor
      actor ! PersistSlowly(1 to 10)

      expectEvent(Pong(1))
      expectEvent(Pong(2))
      popFromEventLog() // pop Pong(3) next is Pong(4)
      expectEvent(Pong(4))
      expectEvent(Pong(5))
      expectEvent(Pong(6))
      popFromEventLog() // pop Pong(7) next is Pong(8)
      expectEvent(Pong(8))
      expectEvent(Pong(9))
      expectEvent(Pong(10))

    }

    scenario("querying full eventlog") {
      val actor = initEventSourcedActor(new SimpleActor(eventlog))

      actor ! PersistSlowly(1 to 10)

      expectEvent(Pong(1))
      expectEvent(Pong(2))
      assert(queryAllEvents() === (3 to 10).map(Pong)) // assert remainings are Pong(3) to Pong(10)
      actor ! PersistSlowly(1 to 10)

      // assert former remaining are dropped, thus when querying again we get Pong(1)
      expectEvent(Pong(1))
    }

    scenario("querying single events") {
      val actor = initEventSourcedActor(new SimpleActor(eventlog))

      actor ! PersistSlowly(1 to 10)

      expectEvent(Pong(1))
      expectEvent(Pong(2))
      assert(queryNextEvent() === Seq(Pong(3))) // queried Pong(3) next is Pong(4)
      expectEvent(Pong(4))
      expectEvent(Pong(5))
      expectEvent(Pong(6))
      assert(queryNextEvent() === Seq(Pong(7))) // queried Pong(7) next is Pong(8)
      expectEvent(Pong(8))
      expectEvent(Pong(9))
      expectEvent(Pong(10))

    }

    scenario("cleaning eventlog") {
      val actor = initEventSourcedActor(new SimpleActor(eventlog))

      actor ! PersistSlowly(1 to 10)

      expectEvent(Pong(1))
      expectEvent(Pong(2))

      cleanEventLog() // drop Pong(3) to Pong(10)
      actor ! PersistSlowly(1 to 10)

      // thus when querying again we get Pong(1)
      expectEvent(Pong(1))
    }
  }

  feature("behaviour onEvent is testable") {
    scenario("state can be accessed") {
      Given("testable actor")
      val actor = initEventSourcedActorAsTestActorRef(new MySimpleActorTestActor(eventlog))
      // nothing is persisted onEvent(Pong)
      callOnEvent(actor, Pong(3))
      // but actor's state changed
      assert(actor.underlyingActor.secretState == 3)
    }

    scenario("event log can be queried") {
      val actor = initEventSourcedActorAsTestActorRef(new MySimpleActorTestActor(eventlog))

      val event = Pong(2121)

      callOnEvent(actor, event)
      callOnEvent(actor, event)

      val event2 = Ping(12345)
      callOnEvent(actor, event2)
      expectEvent(Pong(12345))
    }

    scenario("a bunch of actors communicate with event log") {
      Given("")
      val actor1 = initEventSourcedActor(new SimpleActor(eventlog))
      val actor2 = initEventSourcedActor(new SimpleActor(eventlog))
      val actor3 = initEventSourcedActor(new SimpleActor(eventlog))
      val actor4 = initEventSourcedActor(new SimpleActor(eventlog))
      val actor5 = initEventSourcedActor(new SimpleActor(eventlog))
      val actor6 = initEventSourcedActor(new SimpleActor(eventlog))
      val actor7 = initEventSourcedActorAsTestActorRef(new MySimpleActorTestActor(eventlog))

      actor1 ! Ping(1)
      expectEvent(Pong(1))
      actor2 ! Ping(2)
      expectEvent(Pong(2))
      actor3 ! Ping(3)
      expectEvent(Pong(3))
      actor4 ! Ping(4)
      expectEvent(Pong(4))
      actor5 ! Ping(5)
      expectEvent(Pong(5))
      actor6 ! Ping(6)
      expectEvent(Pong(6))
      actor7 ! Ping(7)
      expectEvent(Pong(7))
    }
  }
}
