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

class FilteredReplicationSpecMultiJvmNode1 extends FilteredReplicationSpec
class FilteredReplicationSpecMultiJvmNode2 extends FilteredReplicationSpec

object FilteredReplicationConfig extends MultiNodeConfig {
  val nodeA = role("nodeA")
  val nodeB = role("nodeB")

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

object FilteredReplicationSpec {
  class PayloadEqualityFilter(payload: String) extends ReplicationFilter {
    override def apply(event: DurableEvent): Boolean = {
      event.payload == payload
    }
  }

  class ReplicatedActor(val replicaId: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
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

class FilteredReplicationSpec extends MultiNodeSpec(FilteredReplicationConfig) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {
  import FilteredReplicationConfig._
  import FilteredReplicationSpec._
  import ReplicationEndpoint._

  def initialParticipants: Int =
    roles.size

  muteDeadLetters(classOf[AnyRef])(system)

  "Event log replication" must {
    "replicate events based on filter criteria" in {
      val probe = TestProbe()

      runOn(nodeA) {
        val connection = node(nodeB).address.toReplicationConnection.copy(filters = Map(DefaultLogName -> new PayloadEqualityFilter("B2")))
        val endpoint = createEndpoint(nodeA.name, Seq(connection))
        val actor = system.actorOf(Props(new ReplicatedActor("pa", endpoint.logs(DefaultLogName), probe.ref)))

        actor ! ("A1")
        actor ! ("A2")
        actor ! ("A3")

        probe.expectMsgAllOf("A1", "A2", "A3", "B2")
      }

      runOn(nodeB) {
        val connection = node(nodeA).address.toReplicationConnection.copy(filters = Map(DefaultLogName -> new PayloadEqualityFilter("A2")))
        val endpoint = createEndpoint(nodeB.name, Seq(connection))
        val actor = system.actorOf(Props(new ReplicatedActor("pb", endpoint.logs(DefaultLogName), probe.ref)))

        actor ! ("B1")
        actor ! ("B2")
        actor ! ("B3")

        probe.expectMsgAllOf("B1", "B2", "B3", "A2")
      }

      enterBarrier("finish")
    }
  }
}
