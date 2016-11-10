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

import akka.remote.testkit.MultiNodeSpec
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit.TestProbe

import com.rbmhtechnology.eventuate.ReplicationProtocol.ReplicationReadTimeoutException

import com.typesafe.config._

class FailureDetectionConfig(providerConfig: Config) extends MultiNodeReplicationConfig {
  val nodeA = role("nodeA")
  val nodeB = role("nodeB")

  testTransport(on = true)

  val customConfig = ConfigFactory.parseString("""
    |eventuate.log.replication.remote-read-timeout = 1s
    |eventuate.log.replication.failure-detection-limit = 10s
  """.stripMargin)

  setConfig(customConfig.withFallback(providerConfig))
}

abstract class FailureDetectionSpec(config: FailureDetectionConfig) extends MultiNodeSpec(config) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {
  import ReplicationEndpoint._
  import config._

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
        probeUnavailable.expectMsgPF() {
          case Unavailable(nodeB.name, logName, causes) if causes.nonEmpty => causes.head shouldBe a[ReplicationReadTimeoutException]
        }
        system.eventStream.subscribe(probeAvailable2.ref, classOf[Available])

        enterBarrier("repair")
        testConductor.passThrough(nodeA, nodeB, Direction.Both).await
        probeAvailable2.expectMsg(Available(nodeB.name, logName))
      }

      runOn(nodeB) {
        createEndpoint(nodeB.name, Set(node(nodeA).address.toReplicationConnection))
        probeAvailable1.expectMsg(Available(nodeA.name, logName))

        enterBarrier("connected")
        probeUnavailable.expectMsgPF() {
          case Unavailable(nodeA.name, logName, causes) if causes.nonEmpty => causes.head shouldBe a[ReplicationReadTimeoutException]
        }
        system.eventStream.subscribe(probeAvailable2.ref, classOf[Available])

        enterBarrier("repair")
        probeAvailable2.expectMsg(Available(nodeA.name, logName))
      }

      enterBarrier("finish")
    }
  }
}
