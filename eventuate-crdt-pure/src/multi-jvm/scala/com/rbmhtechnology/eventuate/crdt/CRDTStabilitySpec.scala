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

import akka.actor.ActorRef
import akka.remote.transport.ThrottlerTransportAdapter.Direction._
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.EventuateMultiNodeSpec
import com.rbmhtechnology.eventuate.EventuateMultiNodeSpecConfig
import com.rbmhtechnology.eventuate.EventuateNodeTest
import com.rbmhtechnology.eventuate.MultiNodeConfigLeveldb
import com.rbmhtechnology.eventuate.MultiNodeReplicationConfig
import com.rbmhtechnology.eventuate.MultiNodeSupportLeveldb
import com.rbmhtechnology.eventuate.ReplicationEndpoint
import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.crdt.pure.AWSetService.AWSet
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.Operation
import com.rbmhtechnology.eventuate.crdt.pure.StabilityProtocol.TCStable
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

class CRDTStabilitySpecLeveldb extends CRDTStabilitySpec(new CRDTStabilitySpecConfig(MultiNodeConfigLeveldb.providerConfig)) with MultiNodeSupportLeveldb
class CRDTStabilitySpecLeveldbMultiJvmNode1 extends CRDTStabilitySpecLeveldb
class CRDTStabilitySpecLeveldbMultiJvmNode2 extends CRDTStabilitySpecLeveldb
class CRDTStabilitySpecLeveldbMultiJvmNode3 extends CRDTStabilitySpecLeveldb
class CRDTStabilitySpecLeveldbMultiJvmNode4 extends CRDTStabilitySpecLeveldb

class CRDTStabilitySpecConfig(providerConfig: Config) extends EventuateMultiNodeSpecConfig with MultiNodeReplicationConfig {

  val logName = "stabilityLog"

  def stabilityConfig(serviceId: String, localPartition: String, partitions: Set[String]) = ConfigFactory.parseString(
    s"""
       |eventuate.crdt.stability.$serviceId.partitions = [ ${partitions.mkString(",")} ]
       |eventuate.crdt.stability.$serviceId.local-partition = $localPartition
     """.stripMargin)

  val nodeA = endpointNode("nodeA", logName, Set("nodeB", "nodeC"))
  val nodeB = endpointNode("nodeB", logName, Set("nodeA", "nodeD"))
  val nodeC = endpointNode("nodeC", logName, Set("nodeA"))
  val nodeD = endpointNode("nodeD", logName, Set("nodeB"))

  def nodeServiceId(node: EventuateNodeTest) = s"awset-${node.endpointId}"

  // C is part of the replicated EventLog but is not considered for stability. This may cause errors if CRDT ops are added from C.
  val partitions = Set(nodeA, nodeB, nodeD)

  val nodesConfig = partitions.map(p => {
    val serviceId = nodeServiceId(p)
    val localPartition = p.partitionName(logName).get
    val otherPartitions = (partitions - p).map(_.partitionName(logName).get)
    (p.role, stabilityConfig(serviceId, localPartition, otherPartitions))
  })

  testTransport(on = true)

  setConfig(providerConfig)

  setNodesConfig(nodesConfig)

}

/**
 * This test VectorTime stability for CRDTs in a EventLog replicated among 4 locations, A,B,C & D.
 *
 *      A - B
 *     /    \
 *    C      D
 *
 * C is neither considered for stability nor configurated for receive TCStable messages. Not being considered for stability
 * means that other locations won't await for events from C to emit TCStable messages, so we must assure that it doesn't
 * add operations for the configured CRDTService, or the CRDT state could be potentially wrong.
 * C is partitioned from the cluster until the end to probe that it doesn't affect stabilization on the other nodes.
 *
 * D is the first to emit TCStable messages after have received events from B that are in the causal future of those emitted by A.
 *
 */
abstract class CRDTStabilitySpec(config: CRDTStabilitySpecConfig) extends EventuateMultiNodeSpec(config) {

  import Implicits._
  import config._
  import scala.concurrent.duration._

  override val logName = config.logName

  def initialParticipants: Int = roles.size

  case class StableCRDT(vt: TCStable, value: Set[String], nonStableOps: Set[String])

  def stable(entry: (EventuateNodeTest, Long), entries: (EventuateNodeTest, Long)*) = TCStable(VectorTime((entries :+ entry).map { case (e, l) => (e.partitionName(config.logName).get, l) }: _*))

  val awset1 = "awset1"

  def add(value: String)(implicit service: AWSetService[String], changeProbe: TestProbe) = {
    service.add(awset1, value)
    changeProbe.expectMsg(value)
  }

  def tcstable(a: Long = 0, b: Long = 0, d: Long = -1) = TCStable(VectorTime(
    Seq(nodeA, nodeB, nodeD).map(_.partitionName(logName).get).zip(Seq(a, b, d)).filterNot(_._2 == -1L).toMap
  ))

  def createService(serviceId: String, log: ActorRef, stableProbe: TestProbe, changeProbe: TestProbe): AWSetService[String] = new AWSetService[String](serviceId, log) {
    override private[crdt] def onStable(crdt: AWSet[String], stable: StabilityProtocol.TCStable): Unit = {
      stableProbe.ref ! StableCRDT(stable, ops.value(crdt), crdt.polog.log.map(_.value.asInstanceOf[AddOp].entry.toString))
    }

    override private[crdt] def onChange(crdt: AWSet[String], operation: Option[Operation]): Unit = {
      operation.foreach(changeProbe.ref ! _.asInstanceOf[AddOp].entry)
    }
  }

  val initialize = (serviceId: String) => (e: ReplicationEndpoint) => {
    val stableProbe = TestProbe()
    val changeProbe = TestProbe()
    val service = createService(serviceId, e.log, stableProbe, changeProbe)
    (stableProbe, changeProbe, service)
  }

  "Pure op based CRDT" must {
    val brokenA = "brokenA"
    val brokenC = "brokenC"
    val repairedA = "repairedA"
    val repairedC = "repairedC"

    "receive TCStable" in {

      nodeA.runWith(initialize(nodeServiceId(nodeA))) {
        case (e, (stableProbe, changeProbe, service)) =>
          implicit val (c, s) = (changeProbe, service)
          testConductor.blackhole(nodeA, nodeC, Both).await
          enterBarrier(brokenC)

          add("a1") // (A -> 1)
          add("a2") // (A -> 2)
          changeProbe.expectMsg("b1") // (A -> 2, B -> 3)
          stableProbe.expectNoMessage(3.seconds)

          testConductor.blackhole(nodeA, nodeB, Both).await
          enterBarrier(brokenA)
          stableProbe.expectNoMessage(3.seconds)
          testConductor.passThrough(nodeA, nodeB, Both).await
          enterBarrier(repairedA)

          changeProbe.expectMsg("d1") // (A -> 2, B -> 3, D -> 4)
          stableProbe.expectMsg(StableCRDT(tcstable(a = 2, b = 3, d = 0), Set("a1", "a2", "b1", "d1"), Set("d1")))

          testConductor.passThrough(nodeA, nodeC, Both).await
          enterBarrier(repairedC)

          service.save(awset1).await

          val service2 = createService(nodeServiceId(nodeA), e.log, stableProbe, changeProbe)
          service2.value(awset1).await should be(Set("a1", "a2", "b1", "d1"))

          changeProbe.expectNoMessage(3.seconds)
          stableProbe.expectNoMessage(3.seconds)

      }

      nodeB.runWith(initialize(nodeServiceId(nodeB))) {
        case (_, (stableProbe, changeProbe, service)) =>
          implicit val (c, s) = (changeProbe, service)
          enterBarrier(brokenC)

          service.value(awset1) // wakeup crdt!
          changeProbe.expectMsg("a1") // (A -> 1)
          changeProbe.expectMsg("a2") // (A -> 2)

          add("b1") // (A -> 2, B -> 1)
          stableProbe.expectNoMessage(3.seconds)

          enterBarrier(brokenA)

          changeProbe.expectMsg("d1") // (A -> 2, B -> 3, D -> 4)
          stableProbe.expectMsg(StableCRDT(tcstable(a = 2, b = 0, d = 0), Set("a1", "a2", "b1", "d1"), Set("b1", "d1")))

          enterBarrier(repairedA, repairedC)

      }
      nodeC.runWith(initialize(nodeServiceId(nodeC))) {
        case (_, (stableProbe, changeProbe, service)) =>
          service.value(awset1)
          enterBarrier(brokenC)
          stableProbe.expectNoMessage(3.seconds)

          enterBarrier(brokenA, repairedA, repairedC)

          changeProbe.expectMsg("a1")
          changeProbe.expectMsg("a2")
          changeProbe.expectMsg("b1")
          changeProbe.expectMsg("d1")
          stableProbe.expectNoMessage(3.seconds)
      }
      nodeD.runWith(initialize(nodeServiceId(nodeD))) {
        case (_, (stableProbe, changeProbe, service)) =>
          implicit val (c, s) = (changeProbe, service)

          enterBarrier(brokenC)
          service.value(awset1) // wakeup crdt!
          changeProbe.expectMsg("a1") // (A -> 1)
          changeProbe.expectMsg("a2") // (A -> 2)
          changeProbe.expectMsg("b1") // (A -> 2, B -> 3)
          stableProbe.expectMsg(StableCRDT(tcstable(a = 2, b = 0), Set("b1", "a1", "a2"), Set("b1")))

          enterBarrier(brokenA)
          add("d1")
          enterBarrier(repairedA)
          stableProbe.expectNoMessage(3.seconds)

          enterBarrier(repairedC)

      }

    }
  }

}
