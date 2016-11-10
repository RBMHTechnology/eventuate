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

import com.rbmhtechnology.eventuate.ReplicationProtocol.ReplicationEndpointInfo
import com.typesafe.config._

import scala.util._

class BasicCausalityConfig(providerConfig: Config) extends MultiNodeReplicationConfig {
  val nodeA = role("nodeA")
  val nodeB = role("nodeB")

  testTransport(on = true)

  setConfig(ConfigFactory.parseString("eventuate.log.replication.remote-read-timeout = 2s").withFallback(providerConfig))
}

object BasicCausalitySpec {
  class ReplicatedActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    def onCommand = {
      case s: String => persist(s) {
        case Success(e) =>
        case Failure(e) => throw e
      }
    }

    def onEvent = {
      case s: String => probe ! ((s, lastVectorTimestamp, currentVersion))
    }
  }
}

abstract class BasicCausalitySpec(config: BasicCausalityConfig) extends MultiNodeSpec(config) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {
  import BasicCausalitySpec._
  import config._

  val logIdA = ReplicationEndpointInfo.logId(nodeA.name, logName)
  val logIdB = ReplicationEndpointInfo.logId(nodeB.name, logName)

  muteDeadLetters(classOf[AnyRef])(system)

  def initialParticipants: Int =
    roles.size

  def vectorTime(a: Long, b: Long) = (a, b) match {
    case (0L, 0L) => VectorTime()
    case (a, 0L)  => VectorTime(logIdA -> a)
    case (0L, b)  => VectorTime(logIdB -> b)
    case (a, b)   => VectorTime(logIdA -> a, logIdB -> b)
  }

  "Event-sourced actors" when {
    "located at different locations" can {
      "track causality" in {
        val probe = TestProbe()

        runOn(nodeA) {
          val endpoint = createEndpoint(nodeA.name, Set(node(nodeB).address.toReplicationConnection))
          val actor = system.actorOf(Props(new ReplicatedActor("pa", endpoint.log, probe.ref)))

          probe.expectMsg(("x", vectorTime(0, 1), vectorTime(0, 1)))

          actor ! "y"
          probe.expectMsg(("y", vectorTime(2, 1), vectorTime(2, 1)))

          enterBarrier("reply")
          testConductor.blackhole(nodeA, nodeB, Direction.Both).await
          enterBarrier("broken")

          actor ! "z1"
          probe.expectMsg(("z1", vectorTime(3, 1), vectorTime(3, 1)))

          enterBarrier("repair")
          testConductor.passThrough(nodeA, nodeB, Direction.Both).await

          probe.expectMsg(("z2", vectorTime(2, 3), vectorTime(3, 3)))
        }

        runOn(nodeB) {
          val endpoint = createEndpoint(nodeB.name, Set(node(nodeA).address.toReplicationConnection))
          val actor = system.actorOf(Props(new ReplicatedActor("pb", endpoint.log, probe.ref)))

          actor ! "x"
          probe.expectMsg(("x", vectorTime(0, 1), vectorTime(0, 1)))
          probe.expectMsg(("y", vectorTime(2, 1), vectorTime(2, 1)))

          enterBarrier("reply")
          enterBarrier("broken")

          actor ! "z2"
          probe.expectMsg(("z2", vectorTime(2, 3), vectorTime(2, 3)))

          enterBarrier("repair")

          probe.expectMsg(("z1", vectorTime(3, 1), vectorTime(3, 3)))
        }

        enterBarrier("finish")
      }
    }
  }
}
