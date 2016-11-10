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
import akka.remote.testkit._
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit.TestProbe

import com.rbmhtechnology.eventuate.EventsourcedView.Handler
import com.typesafe.config._

class BasicPersistOnEventConfig(providerConfig: Config) extends MultiNodeReplicationConfig {
  val nodeA = role("nodeA")
  val nodeB = role("nodeB")

  testTransport(on = true)

  setConfig(ConfigFactory.parseString("eventuate.log.replication.remote-read-timeout = 2s").withFallback(providerConfig))
}

object BasicPersistOnEventSpec {
  case class Ping(num: Int)
  case class Pong(num: Int)

  class PingActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor with PersistOnEvent {
    override def onCommand = {
      case Ping(i) => persist(Ping(i))(Handler.empty)
    }
    override def onEvent = {
      case p @ Pong(10) => probe ! p
      case p @ Pong(5)  => probe ! p
      case p @ Ping(6)  => probe ! p
      case Pong(i)      => persistOnEvent(Ping(i + 1))
    }
  }

  class PongActor(val id: String, val eventLog: ActorRef) extends EventsourcedActor with PersistOnEvent {
    override def onCommand = {
      case _ =>
    }
    override def onEvent = {
      case Ping(i) => persistOnEvent(Pong(i))
    }
  }
}

abstract class BasicPersistOnEventSpec(config: BasicPersistOnEventConfig) extends MultiNodeSpec(config) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {
  import BasicPersistOnEventSpec._
  import config._

  muteDeadLetters(classOf[AnyRef])(system)

  def initialParticipants: Int =
    roles.size

  "Event-sourced actors" when {
    "located at different locations" can {
      "play partition-tolerant event-driven ping-pong" in {
        val probe = TestProbe()

        runOn(nodeA) {
          val endpoint = createEndpoint(nodeA.name, Set(node(nodeB).address.toReplicationConnection))
          val pingActor = system.actorOf(Props(new PingActor("ping", endpoint.log, probe.ref)))

          pingActor ! Ping(1)
          probe.expectMsg(Pong(5))

          testConductor.blackhole(nodeA, nodeB, Direction.Both).await

          // partitioned from PongActor
          pingActor ! Ping(6)
          probe.expectMsg(Ping(6))

          testConductor.passThrough(nodeA, nodeB, Direction.Both).await

          probe.expectMsg(Pong(10))
        }

        runOn(nodeB) {
          val endpoint = createEndpoint(nodeB.name, Set(node(nodeA).address.toReplicationConnection))
          system.actorOf(Props(new PongActor("pong", endpoint.log)))
        }

        enterBarrier("finish")
      }
    }
  }
}
