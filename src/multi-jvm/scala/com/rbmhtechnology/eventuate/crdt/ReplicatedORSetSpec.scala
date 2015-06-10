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

package com.rbmhtechnology.eventuate.crdt

import akka.actor._
import akka.remote.testconductor.RoleName
import akka.remote.testkit._
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit.TestProbe

import com.rbmhtechnology.eventuate._

class ReplicatedORSetSpecLeveldb extends ReplicatedORSetSpec with MultiNodeSupportLeveldb
class ReplicatedORSetSpecLeveldbMultiJvmNode1 extends ReplicatedORSetSpecLeveldb
class ReplicatedORSetSpecLeveldbMultiJvmNode2 extends ReplicatedORSetSpecLeveldb

class ReplicatedORSetSpecCassandra extends ReplicatedORSetSpec with MultiNodeSupportCassandra {
  override def logName = "ros"
}
class ReplicatedORSetSpecCassandraMultiJvmNode1 extends ReplicatedORSetSpecCassandra
class ReplicatedORSetSpecCassandraMultiJvmNode2 extends ReplicatedORSetSpecCassandra

object ReplicatedORSetConfig extends MultiNodeConfig {
  val nodeA = role("nodeA")
  val nodeB = role("nodeB")

  testTransport(on = true)

  commonConfig(MultiNodeReplicationConfig.create(
    """
      |eventuate.log.replication.batch-size-max = 200
      |eventuate.log.replication.read-timeout = 2s
    """.stripMargin))
}

abstract class ReplicatedORSetSpec extends MultiNodeSpec(ReplicatedORSetConfig) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {
  import BasicReplicationConfig._

  def initialParticipants: Int =
    roles.size

  muteDeadLetters(classOf[AnyRef])(system)

  "A replicated ORSet" must {
    "converge" in {
      val probe = TestProbe()

      runOn(nodeA) {
        val endpoint = createEndpoint(nodeA.name, Set(node(nodeB).address.toReplicationConnection))
        val service = new ORSetService[Int]("A", endpoint.log) {
          override private[crdt] def onChange(crdt: ORSet[Int]): Unit = probe.ref ! crdt.value
        }

        service.add("x", 1)
        probe.expectMsg(Set(1))
        probe.expectMsg(Set(1, 2))

        // network partition
        testConductor.blackhole(nodeA, nodeB, Direction.Both).await
        enterBarrier("broken")

        // this is concurrent to service.remove("x", 1) on node B
        service.add("x", 1)
        probe.expectMsg(Set(1, 2))

        enterBarrier("repair")
        testConductor.passThrough(nodeA, nodeB, Direction.Both).await

        probe.expectMsg(Set(1, 2))
        service.remove("x", 2)
        probe.expectMsg(Set(1))
      }

      runOn(nodeB) {
        val endpoint = createEndpoint(nodeB.name, Set(node(nodeA).address.toReplicationConnection))
        val service = new ORSetService[Int]("B", endpoint.log) {
          override private[crdt] def onChange(crdt: ORSet[Int]): Unit = probe.ref ! crdt.value
        }

        probe.expectMsg(Set(1))
        service.add("x", 2)
        probe.expectMsg(Set(1, 2))

        enterBarrier("broken")

        // this is concurrent to service.add("x", 1) on node A
        service.remove("x", 1)
        probe.expectMsg(Set(2))

        enterBarrier("repair")

        // add has precedence over (concurrent) remove
        probe.expectMsg(Set(1, 2))
        probe.expectMsg(Set(1))
      }

      enterBarrier("finish")
    }
  }
}
