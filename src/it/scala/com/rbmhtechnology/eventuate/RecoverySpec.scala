/*
 * Copyright (C) 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
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

import java.io.File

import akka.actor._
import akka.pattern.ask
import akka.testkit.TestProbe
import akka.util.Timeout

import com.rbmhtechnology.eventuate.log._
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLogSettings
import com.rbmhtechnology.eventuate.utilities._

import org.apache.commons.io.FileUtils
import org.scalatest._

import scala.concurrent.duration._

object RecoverySpecLeveldb {
  class ConvergenceView(val id: String, val eventLog: ActorRef, expectedSize: Int, probe: ActorRef) extends EventsourcedView {
    var state: Set[String] = Set()

    def onCommand = {
      case _ =>
    }

    def onEvent = {
      case s: String =>
        state += s
        if (state.size == expectedSize) probe ! state
    }
  }

  val config =
    """
      |eventuate.log.replication.retry-delay = 1s
      |eventuate.log.replication.remote-read-timeout = 2s
      |eventuate.disaster-recovery.remote-operation-retry-max = 10
      |eventuate.disaster-recovery.remote-operation-retry-delay = 1s
      |eventuate.disaster-recovery.remote-operation-timeout = 1s
    """.stripMargin

  def rootDirectory(target: ReplicationTarget): File =
    new File(new LeveldbEventLogSettings(target.endpoint.system.settings.config).rootDir)

  def logDirectory(target: ReplicationTarget): File = {
    implicit val timeout = Timeout(3.seconds)
    target.log.ask("dir").mapTo[File].await
  }
}

class RecoverySpecLeveldb extends WordSpec with Matchers with ReplicationNodeRegistry with EventLogCleanupLeveldb {
  import ReplicationIntegrationSpec.replicationConnection
  import RecoverySpecLeveldb._

  val logFactory: String => Props = id => EventLogLifecycleLeveldb.TestEventLog.props(id, batching = true)

  var ctr: Int = 0

  override def beforeEach(): Unit =
    ctr += 1

  def config =
    ReplicationConfig.create()

  def nodeId(node: String): String =
    s"${node}_${ctr}"

  def node(nodeName: String, logNames: Set[String], port: Int, connections: Set[ReplicationConnection], customConfig: String = "", activate: Boolean = false): ReplicationNode =
    register(new ReplicationNode(nodeId(nodeName), logNames, logFactory, connections, port = port, customConfig = RecoverySpecLeveldb.config + customConfig, activate = activate))

  def assertConvergence(expected: Set[String], nodes: ReplicationNode *): Unit = {
    val probes = nodes.map { node =>
      val probe = new TestProbe(node.system)
      node.system.actorOf(Props(new ConvergenceView(s"p-${node.id}", node.logs("L1"), expected.size, probe.ref)))
      probe
    }
    probes.foreach(_.expectMsg(expected))
  }

  "Replication endpoint recovery" must {
    "disallow activation of endpoint during and after recovery" in {
      node("B", Set("L1"), 2553, Set(replicationConnection(2552))).endpoint.activate()
      val endpoint = node("A", Set("L1"), 2552, Set(replicationConnection(2553))).endpoint

      val recovery = endpoint.recover()

      an [IllegalStateException] shouldBe thrownBy(endpoint.activate())
      recovery.await
      an [IllegalStateException] shouldBe thrownBy(endpoint.activate())
    }
    "fail when connected endpoint is unavailable" in {
      val endpoint = node("A", Set("L1"), 2552, Set(replicationConnection(2553)),
        customConfig = "eventuate.disaster-recovery.remote-operation-retry-max = 0")
        .endpoint

      an [Exception] shouldBe thrownBy(endpoint.recover().await)
    }
    "succeed normally if the endpoint was healthy (but not convergent yet)" in {
      def newNodeA = node("A", Set("L1"), 2552, Set(replicationConnection(2553)))
      val nodeA = newNodeA
      val nodeB = node("B", Set("L1"), 2553, Set(replicationConnection(2552)))

      val targetA = nodeA.endpoint.target("L1")
      val targetB = nodeB.endpoint.target("L1")

      write(targetA, List("a1", "a2"))
      write(targetB, List("b1", "b2"))
      replicate(targetA, targetB, 1)
      replicate(targetB, targetA, 1)

      nodeA.terminate().await
      val restartedA = newNodeA

      nodeB.endpoint.activate()
      restartedA.endpoint.recover().await

      assertConvergence(Set("a1", "a2", "b1", "b2"), restartedA, nodeB)
    }
    "repair inconsistencies of an endpoint that has lost all events" in {
      val nodeA = node("A", Set("L1"), 2552, Set(replicationConnection(2555)))
      val nodeB = node("B", Set("L1"), 2553, Set(replicationConnection(2555)))
      val nodeC = node("C", Set("L1"), 2554, Set(replicationConnection(2555)))
      def nodeD = node("D", Set("L1"), 2555, Set(replicationConnection(2552), replicationConnection(2553), replicationConnection(2554)))

      val nodeD1 = nodeD

      val targetA = nodeA.endpoint.target("L1")
      val targetB = nodeB.endpoint.target("L1")
      val targetC = nodeC.endpoint.target("L1")
      val targetD1 = nodeD1.endpoint.target("L1")

      val logDirD = logDirectory(targetD1)

      write(targetA, List("a"))
      replicate(targetA, targetD1)
      replicate(targetD1, targetA)

      write(targetB, List("b"))
      write(targetC, List("c"))
      replicate(targetB, targetD1)
      replicate(targetC, targetD1)
      replicate(targetD1, targetB)
      replicate(targetD1, targetC)

      write(targetD1, List("d"))
      replicate(targetD1, targetC)

      // what a disaster ...
      nodeD1.terminate().await
      FileUtils.deleteDirectory(logDirD)

      nodeA.endpoint.activate()
      nodeB.endpoint.activate()
      nodeC.endpoint.activate()

      // start node D again (no backup available)
      val nodeD2 = nodeD

      nodeD2.endpoint.recover().await
      // disclose bug #152 (writing new events is allowed after successful recovery)
      write(nodeD2.endpoint.target("L1"), List("d1"))

      assertConvergence(Set("a", "b", "c", "d", "d1"), nodeA, nodeB, nodeC, nodeD2)
    }
    "repair inconsistencies if recovery was stopped during event recovery and restarted" in {
      def newNodeA = node("A", Set("L1"), 2552, Set(replicationConnection(2553)), customConfig = "eventuate.log.write-batch-size = 1")
      val nodeA = newNodeA
      val nodeB = node("B", Set("L1"), 2553, Set(replicationConnection(2552)))

      nodeA.endpoint.activate()
      nodeB.endpoint.activate()
      val targetA = nodeA.endpoint.target("L1")
      val logDirA = logDirectory(targetA)
      val targetB = nodeB.endpoint.target("L1")

      val as = (0 to 5).map("A" + _)
      val bs = (0 to 5).map("B" + _)
      val all = as.toSet ++ bs.toSet
      write(targetA, as)
      write(targetB, bs)
      assertConvergence(all, nodeA, nodeB)

      nodeA.terminate().await
      FileUtils.deleteDirectory(logDirA)

      val restartedA = newNodeA
      restartedA.endpoint.recover()
      restartedA.eventListener("L1").waitForMessage("A1")

      restartedA.terminate().await

      val restartedA2 = newNodeA
      restartedA2.endpoint.recover().await

      assertConvergence(all, restartedA2, nodeB)
    }
    "repair inconsistencies of an endpoint that has lost all events but has been partially recovered from a storage backup" in {
      val nodeA = node("A", Set("L1"), 2552, Set(replicationConnection(2555)))
      val nodeB = node("B", Set("L1"), 2553, Set(replicationConnection(2555)))
      val nodeC = node("C", Set("L1"), 2554, Set(replicationConnection(2555)))
      def nodeD = node("D", Set("L1"), 2555, Set(replicationConnection(2552), replicationConnection(2553), replicationConnection(2554)))

      val nodeD1 = nodeD

      val targetA = nodeA.endpoint.target("L1")
      val targetB = nodeB.endpoint.target("L1")
      val targetC = nodeC.endpoint.target("L1")
      val targetD1 = nodeD1.endpoint.target("L1")

      val rootDirD = rootDirectory(targetD1)
      val logDirD = logDirectory(targetD1)
      val bckDirD = new File(rootDirD, "backup")

      write(targetA, List("a"))
      replicate(targetA, targetD1)
      replicate(targetD1, targetA)

      write(targetB, List("b"))
      write(targetC, List("c"))
      replicate(targetB, targetD1)

      nodeD1.terminate().await
      FileUtils.copyDirectory(logDirD, bckDirD)

      val nodeD2 = nodeD
      val targetD2 = nodeD2.endpoint.target("L1")

      replicate(targetC, targetD2)
      replicate(targetD2, targetB)
      replicate(targetD2, targetC)

      write(targetD2, List("d"))
      replicate(targetD2, targetC)

      // what a disaster ...
      nodeD2.terminate().await
      FileUtils.deleteDirectory(logDirD)

      // install a backup
      FileUtils.copyDirectory(bckDirD, logDirD)

      nodeA.endpoint.activate()
      nodeB.endpoint.activate()
      nodeC.endpoint.activate()

      // start node D again (with backup available)
      val nodeD3 = nodeD

      nodeD3.endpoint.recover().await

      assertConvergence(Set("a", "b", "c", "d"), nodeA, nodeB, nodeC, nodeD3)
    }
  }

  "A replication endpoint" must {
    "not allow concurrent recoveries" in {
      val nodeA = node("A", Set("L1"), 2552, Set(replicationConnection(2553)))

      nodeA.endpoint.recover()
      intercept[IllegalStateException] {
        nodeA.endpoint.recover().await
      }
    }
    "not allow concurrent recovery and activation" in {
      val nodeA = node("A", Set("L1"), 2552, Set(replicationConnection(2553)))

      nodeA.endpoint.recover()
      intercept[IllegalStateException] {
        nodeA.endpoint.activate()
      }
    }
    "not allow activated endpoints to be recovered" in {
      val nodeA = node("A", Set("L1"), 2552, Set(replicationConnection(2553)))

      nodeA.endpoint.activate()
      intercept[IllegalStateException] {
        nodeA.endpoint.recover().await
      }
    }
    "not allow multiple activations" in {
      val nodeA = node("A", Set("L1"), 2552, Set(replicationConnection(2553)))

      nodeA.endpoint.activate()
      intercept[IllegalStateException] {
        nodeA.endpoint.activate()
      }
    }
  }
}
