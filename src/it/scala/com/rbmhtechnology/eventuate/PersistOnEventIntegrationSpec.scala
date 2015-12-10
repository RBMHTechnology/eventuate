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

import com.rbmhtechnology.eventuate.log.EventLogLifecycleCassandra
import com.rbmhtechnology.eventuate.log.EventLogLifecycleLeveldb

import org.scalatest._

object PersistOnEventIntegrationSpec {
  case class Ping(num: Int)
  case class Pong(num: Int)

  class PingActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor with PersistOnEvent {
    override def onCommand = {
      case "serve" => persist(Ping(1))(Handler.empty)
    }
    override def onEvent = {
      case Pong(10) => probe ! "done"
      case Pong(i)  => persistOnEvent(Ping(i + 1))(Handler.empty)
    }
  }

  class PongActor(val id: String, val eventLog: ActorRef) extends EventsourcedActor with PersistOnEvent {
    override def onCommand = {
      case _ =>
    }
    override def onEvent = {
      case Ping(i) => persistOnEvent(Pong(i))(Handler.empty)
    }
  }
}

abstract class PersistOnEventIntegrationSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterEach {
  import PersistOnEventIntegrationSpec._

  def log: ActorRef
  def logId: String

  var probe: TestProbe = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    probe = TestProbe()
  }

  "Two event-sourced actors" can {
    "play event ping pong" in {
      val probe = TestProbe()
      val pingActor = system.actorOf(Props(new PingActor("ping", log, probe.ref)))
      val pongActor = system.actorOf(Props(new PongActor("pong", log)))

      pingActor ! "serve"
      probe.expectMsg("done")
    }
  }
}

class PersistOnEventIntegrationSpecLeveldb extends PersistOnEventIntegrationSpec with EventLogLifecycleLeveldb
class PersistOnEventIntegrationSpecCassandra extends PersistOnEventIntegrationSpec with EventLogLifecycleCassandra