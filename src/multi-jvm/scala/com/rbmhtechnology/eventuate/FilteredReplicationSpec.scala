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

import scala.util._

class FilteredReplicationSpecLeveldb extends FilteredReplicationSpec with MultiNodeSupportLeveldb
class FilteredReplicationSpecLeveldbMultiJvmNode1 extends FilteredReplicationSpecLeveldb
class FilteredReplicationSpecLeveldbMultiJvmNode2 extends FilteredReplicationSpecLeveldb

class FilteredReplicationSpecCassandra extends FilteredReplicationSpec with MultiNodeSupportCassandra {
  override def logName = "fr"
}
class FilteredReplicationSpecCassandraMultiJvmNode1 extends FilteredReplicationSpecCassandra
class FilteredReplicationSpecCassandraMultiJvmNode2 extends FilteredReplicationSpecCassandra

object FilteredReplicationConfig extends MultiNodeConfig {
  val nodeA = role("nodeA")
  val nodeB = role("nodeB")

  commonConfig(MultiNodeReplicationConfig.create())
}

object FilteredReplicationSpec {
  class PayloadEqualityFilter(payload: String) extends ReplicationFilter {
    override def apply(event: DurableEvent): Boolean = {
      event.payload == payload
    }
  }

  class ReplicatedActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    val onCommand: Receive = {
      case s: String => persist(s) {
        case Success(e) => onEvent(e)
        case Failure(e) => throw e
      }
    }

    val onEvent: Receive = {
      case s: String => probe ! s
    }
  }
}

abstract class FilteredReplicationSpec extends MultiNodeSpec(FilteredReplicationConfig) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {
  import FilteredReplicationConfig._
  import FilteredReplicationSpec._

  def initialParticipants: Int =
    roles.size

  muteDeadLetters(classOf[AnyRef])(system)

  "Event log replication" must {
    "replicate events based on filter criteria" in {
      val probe = TestProbe()

      runOn(nodeA) {
        val connection = node(nodeB).address.toReplicationConnection.copy(filters = Map(logName -> new PayloadEqualityFilter("B2")))
        val endpoint = createEndpoint(nodeA.name, Set(connection))
        val actor = system.actorOf(Props(new ReplicatedActor("pa", endpoint.log, probe.ref)))

        actor ! ("A1")
        actor ! ("A2")
        actor ! ("A3")

        probe.expectMsgAllOf("A1", "A2", "A3", "B2")
      }

      runOn(nodeB) {
        val connection = node(nodeA).address.toReplicationConnection.copy(filters = Map(logName -> new PayloadEqualityFilter("A2")))
        val endpoint = createEndpoint(nodeB.name, Set(connection))
        val actor = system.actorOf(Props(new ReplicatedActor("pb", endpoint.log, probe.ref)))

        actor ! ("B1")
        actor ! ("B2")
        actor ! ("B3")

        probe.expectMsgAllOf("B1", "B2", "B3", "A2")
      }

      enterBarrier("finish")
    }
  }
}
