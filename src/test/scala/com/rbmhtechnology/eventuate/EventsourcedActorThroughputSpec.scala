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

import scala.concurrent.duration._
import scala.util._

import akka.actor._
import akka.testkit._

import com.rbmhtechnology.eventuate.log.EventLogSupport
import com.typesafe.config.ConfigFactory

import org.scalatest._

object EventsourcedActorThroughputSpec {
  val config = ConfigFactory.parseString("log.leveldb.dir = target/test")

  class ThroughputTestActor(val processId: String, val eventLog: ActorRef, override val sync: Boolean, probe: ActorRef) extends EventsourcedActor {
    var startTime: Long = 0L
    var stopTime: Long = 0L
    var num: Int = 0

    def onCommand = {
      case "stats" =>
        probe ! s"${(1000.0 * 1000 * 1000 * num) / (stopTime - startTime) } events/sec"
      case s: String => persist(s) {
        case Success(e) => onEvent(e)
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
}

import EventsourcedActorThroughputSpec._

class EventsourcedActorThroughputSpec extends TestKit(ActorSystem("test", config)) with WordSpecLike with Matchers with EventLogSupport {
  var probe: TestProbe = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    probe = TestProbe()
  }

  val num = 10000
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


  "An EventsourcedActor" should {
    "have some reasonable write throughput (actor sync = true)" in {
      run(system.actorOf(Props(new ThroughputTestActor("p", log, sync = true, probe.ref))))
    }

    "have some reasonable write throughput (actor sync = false)" in {
      run(system.actorOf(Props(new ThroughputTestActor("p", log, sync = false, probe.ref))))
    }
  }
}
