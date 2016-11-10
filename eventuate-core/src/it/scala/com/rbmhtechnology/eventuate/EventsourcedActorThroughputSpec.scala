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

import scala.concurrent.duration._
import scala.util._

import akka.actor._
import akka.testkit._

import org.scalatest._

object EventsourcedActorThroughputSpec {
  class Writer1(val id: String, val eventLog: ActorRef, override val stateSync: Boolean, probe: ActorRef) extends EventsourcedActor {
    var startTime: Long = 0L
    var stopTime: Long = 0L
    var num: Int = 0

    def onCommand = {
      case "stats" =>
        probe ! s"${(1000.0 * 1000 * 1000 * num) / (stopTime - startTime)} events/sec"
      case s: String => persist(s) {
        case Success(e) =>
        case Failure(e) => throw e
      }
    }

    def onEvent = {
      case "start" =>
        startTime = System.nanoTime()
      case "stop" =>
        stopTime = System.nanoTime()
        probe ! num
      case s: String =>
        num += 1
    }
  }

  class Writer2(val id: String, val eventLog: ActorRef, collector: ActorRef) extends EventsourcedActor {
    def onCommand = {
      case s: String =>
        persist(s) {
          case Success(e) => collector ! e
          case Failure(e) => throw e
        }
    }

    def onEvent = {
      case "ignore" =>
    }
  }

  class Collector(expectedReceives: Int, probe: ActorRef) extends Actor {
    var startTime: Long = 0L
    var stopTime: Long = 0L
    var num: Int = 0

    def receive = {
      case "stats" =>
        probe ! s"${(1000.0 * 1000 * 1000 * num) / (stopTime - startTime)} events/sec"
      case s: String if num == 0 =>
        startTime = System.nanoTime()
        num += 1
      case s: String =>
        num += 1
        if (num == expectedReceives) {
          stopTime = System.nanoTime()
          probe ! num
        }
    }
  }
}

trait EventsourcedActorThroughputSpec extends TestKitBase with WordSpecLike with Matchers with SingleLocationSpec {
  import EventsourcedActorThroughputSpec._

  var probe: TestProbe = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    probe = TestProbe()
  }

  val num = 1000
  val timeout = 60.seconds
  val events = (1 to num).map(i => s"e-${i}")

  def run(actor: ActorRef): Unit = {
    actor ! "start"
    events.foreach(actor ! _)
    actor ! "stop"

    probe.expectMsg(timeout, num)
    actor ! "stats"
    println(probe.receiveOne(timeout))

  }

  "An EventsourcedActor" when {
    "configured with stateSync = true" should {
      "have some acceptable write throughput (batching layer has no effect)" in {
        run(system.actorOf(Props(new Writer1("p", log, stateSync = true, probe.ref))))
      }
    }
  }

  "An EventsourcedActor" when {
    "configured with stateSync = false" should {
      "have some acceptable write throughput (batching layer has some effect)" in {
        run(system.actorOf(Props(new Writer1("p", log, stateSync = false, probe.ref))))
      }
    }
  }

  "Several EventsourcedActors" when {
    "configured with stateSync = true" should {
      "have some reasonable overall write throughput (batching layer has some effects)" in {
        val probe = TestProbe()

        val numActors = 10
        val numWrites = numActors * num

        val collector = system.actorOf(Props(new Collector(numWrites, probe.ref)))
        val actors = 1 to numActors map { i => system.actorOf(Props(new Writer2(s"p-${i}", log, collector))) }

        for {
          e <- events
          a <- actors
        } { a ! e }

        probe.expectMsg(timeout, numWrites)
        collector ! "stats"
        println(probe.receiveOne(timeout))
      }
    }
  }
}
