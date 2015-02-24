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
import akka.remote.testkit._
import akka.testkit.TestProbe

import com.typesafe.config.ConfigFactory

import scala.collection.immutable.Seq
import scala.util._

class BasicReplicationSpecMultiJvmNode1 extends BasicReplicationSpec
class BasicReplicationSpecMultiJvmNode2 extends BasicReplicationSpec
class BasicReplicationSpecMultiJvmNode3 extends BasicReplicationSpec

object BasicReplicationConfig extends MultiNodeConfig {
  val nodeA = role("nodeA")
  val nodeB = role("nodeB")
  val nodeC = role("nodeC")

  commonConfig(ConfigFactory.parseString(
    s"""
      |akka.loglevel = "ERROR"
      |akka.test.single-expect-default = 10s
      |log.replication.transfer-batch-size-max = 3
      |log.replication.transfer-retry-interval = 1s
      |log.replication.connect-retry-interval = 1s
      |log.replication.failure-detection-limit = 60s
    """.stripMargin))
}

object BasicReplicationSpec {
  class ReplicatedActor(val processId: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    def onCommand = {
      case s: String => persist(s) {
        case Success(e) => onEvent(e)
        case Failure(e) => throw e
      }
    }

    def onEvent = {
      case s: String => probe ! s
    }
  }
}

class BasicReplicationSpec extends MultiNodeSpec(BasicReplicationConfig) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {
  import BasicReplicationConfig._
  import BasicReplicationSpec._
  import ReplicationEndpoint._

  def initialParticipants: Int =
    roles.size

  def assertPartialOrder[A](events: Seq[A], sample: A*): Unit = {
    val indices = sample.map(events.indexOf)
    assert(indices == indices.sorted)
  }

  muteDeadLetters(classOf[AnyRef])(system)

  "Event log replication" must {
    "replicate all events by default" in {
      val probe = TestProbe()

      runOn(nodeA) {
        val endpoint = createEndpoint(nodeA.name, Seq(node(nodeB).address.toReplicationConnection))
        val actor = system.actorOf(Props(new ReplicatedActor("pa", endpoint.logs(DefaultLogName), probe.ref)))

        actor ! ("A1")
        actor ! ("A2")
      }

      runOn(nodeB) {
        val endpoint = createEndpoint(nodeB.name, Seq(
          node(nodeA).address.toReplicationConnection,
          node(nodeC).address.toReplicationConnection))
        val actor = system.actorOf(Props(new ReplicatedActor("pb", endpoint.logs(DefaultLogName), probe.ref)))

        actor ! ("B1")
        actor ! ("B2")
      }

      runOn(nodeC) {
        val endpoint = createEndpoint(nodeC.name, Seq(node(nodeB).address.toReplicationConnection))
        val actor = system.actorOf(Props(new ReplicatedActor("pc", endpoint.logs(DefaultLogName), probe.ref)))

        actor ! ("C1")
        actor ! ("C2")
      }

      val actual = probe.expectMsgAllOf("A1", "A2", "B1", "B2", "C1", "C2")

      assertPartialOrder(actual, "A1", "A2")
      assertPartialOrder(actual, "B1", "B2")
      assertPartialOrder(actual, "C1", "C2")

      enterBarrier("finish")
    }
  }
}
