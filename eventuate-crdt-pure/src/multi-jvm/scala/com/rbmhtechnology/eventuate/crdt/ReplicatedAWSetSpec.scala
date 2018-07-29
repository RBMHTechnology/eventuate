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

package com.rbmhtechnology.eventuate.crdt.pure

import akka.actor._
import akka.remote.testkit._
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.crdt.pure.AWSetService.AWSet
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.Operation
import com.typesafe.config.ConfigFactory

class ReplicatedAWSetSpecLeveldb extends ReplicatedAWSetSpec with MultiNodeSupportLeveldb
class ReplicatedAWSetSpecLeveldbMultiJvmNode1 extends ReplicatedAWSetSpecLeveldb
class ReplicatedAWSetSpecLeveldbMultiJvmNode2 extends ReplicatedAWSetSpecLeveldb

object ReplicatedORSetConfig extends MultiNodeReplicationConfig {
  val nodeA = role("nodeA")
  val nodeB = role("nodeB")

  val customConfig = ConfigFactory.parseString(
    """
      |eventuate.log.write-batch-size = 200
      |eventuate.log.replication.remote-read-timeout = 2s
    """.stripMargin)

  testTransport(on = true)

  setConfig(customConfig.withFallback(MultiNodeConfigLeveldb.providerConfig))
}

abstract class ReplicatedAWSetSpec extends MultiNodeSpec(ReplicatedORSetConfig) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {
  import ReplicatedORSetConfig._
  import CRDTTestDSL.AWSetCRDT

  def initialParticipants: Int =
    roles.size

  muteDeadLetters(classOf[AnyRef])(system)

  "A replicated AWSet" must {
    "converge" in {
      val probe = TestProbe()

      runOn(nodeA) {
        val endpoint = createEndpoint(nodeA.name, Set(node(nodeB).address.toReplicationConnection))
        val service = new AWSetService[Int]("A", endpoint.log) {
          override private[crdt] def onChange(crdt: AWSet[Int], operation: Option[Operation]): Unit = probe.ref ! ops.value(crdt)
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
        val service = new AWSetService[Int]("B", endpoint.log) {
          override private[crdt] def onChange(crdt: AWSet[Int], operation: Option[Operation]): Unit = probe.ref ! ops.value(crdt)
        }

        service.value("x")
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
