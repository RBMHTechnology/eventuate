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

import akka.remote.testkit.{MultiNodeSpec, MultiNodeConfig}
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit.TestProbe

class FailureDetectionSpecLeveldb extends FailureDetectionSpec with MultiNodeSupportLeveldb
class FailureDetectionSpecLeveldbMultiJvmNode1 extends FailureDetectionSpecLeveldb
class FailureDetectionSpecLeveldbMultiJvmNode2 extends FailureDetectionSpecLeveldb

class FailureDetectionSpecCassandra extends FailureDetectionSpec with MultiNodeSupportCassandra {
  override def logName = "fd"
}
class FailureDetectionSpecCassandraMultiJvmNode1 extends FailureDetectionSpecCassandra
class FailureDetectionSpecCassandraMultiJvmNode2 extends FailureDetectionSpecCassandra

object FailureDetectionConfig extends MultiNodeConfig {
  val nodeA = role("nodeA")
  val nodeB = role("nodeB")

  testTransport(on = true)

  commonConfig(MultiNodeReplicationConfig.create("eventuate.log.replication.failure-detection-limit = 3s"))
}

abstract class FailureDetectionSpec extends MultiNodeSpec(FailureDetectionConfig) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {
  import FailureDetectionConfig._
  import ReplicationEndpoint._

  def initialParticipants: Int =
    roles.size

  muteDeadLetters(classOf[AnyRef])(system)

  "Event log replication" must {
    "detect replication server availability" in {
      val probeAvailable1 = new TestProbe(system)
      val probeAvailable2 = new TestProbe(system)
      val probeUnavailable = new TestProbe(system)

      system.eventStream.subscribe(probeAvailable1.ref, classOf[Available])
      system.eventStream.subscribe(probeUnavailable.ref, classOf[Unavailable])

      enterBarrier("subscribe")

      runOn(nodeA) {
        createEndpoint(nodeA.name, Set(node(nodeB).address.toReplicationConnection))
        probeAvailable1.expectMsg(Available(nodeB.name, logName))

        enterBarrier("connected")
        testConductor.blackhole(nodeA, nodeB, Direction.Both).await
        probeUnavailable.expectMsg(Unavailable(nodeB.name, logName))
        system.eventStream.subscribe(probeAvailable2.ref, classOf[Available])

        enterBarrier("repair")
        testConductor.passThrough(nodeA, nodeB, Direction.Both).await
        probeAvailable2.expectMsg(Available(nodeB.name, logName))
      }

      runOn(nodeB) {
        createEndpoint(nodeB.name, Set(node(nodeA).address.toReplicationConnection))
        probeAvailable1.expectMsg(Available(nodeA.name, logName))

        enterBarrier("connected")
        probeUnavailable.expectMsg(Unavailable(nodeA.name, logName))
        system.eventStream.subscribe(probeAvailable2.ref, classOf[Available])

        enterBarrier("repair")
        probeAvailable2.expectMsg(Available(nodeA.name, logName))
      }

     enterBarrier("finish")
    }
  }
}