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

import java.io.File

import akka.actor._
import akka.pattern.ask
import akka.testkit.TestProbe
import akka.util.Timeout
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLogSettings
import com.rbmhtechnology.eventuate.utilities._
import com.typesafe.config.ConfigFactory
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

  val config = ConfigFactory.parseString(
    """
      |eventuate.log.replication.retry-delay = 1s
      |eventuate.log.replication.remote-read-timeout = 2s
      |eventuate.log.recovery.remote-operation-retry-max = 10
      |eventuate.log.recovery.remote-operation-retry-delay = 1s
      |eventuate.log.recovery.remote-operation-timeout = 1s
    """.stripMargin)

  def rootDirectory(target: ReplicationTarget): File =
    new File(new LeveldbEventLogSettings(target.endpoint.system.settings.config).rootDir)

  def logDirectory(target: ReplicationTarget): File = {
    implicit val timeout = Timeout(3.seconds)
    target.log.ask("dir").mapTo[File].await
  }
}

class RecoverySpecLeveldb extends WordSpec with Matchers with MultiLocationSpecLeveldb {
  import ReplicationIntegrationSpec.replicationConnection
  import RecoverySpecLeveldb._

  val customPort: Int =
    2555

  override val logFactory: String => Props =
    id => SingleLocationSpecLeveldb.TestEventLog.props(id, batching = true)

  def assertConvergence(expected: Set[String], endpoints: ReplicationEndpoint *): Unit = {
    val probes = endpoints.map { endpoint =>
      val probe = new TestProbe(endpoint.system)
      endpoint.system.actorOf(Props(new ConvergenceView(s"p-${endpoint.id}", endpoint.logs("L1"), expected.size, probe.ref)))
      probe
    }
    probes.foreach(_.expectMsg(expected))
  }

  "Replication endpoint recovery" must {
    "disallow activation of endpoint during and after recovery" in {
      val locationA = location("A", customConfig = RecoverySpecLeveldb.config)
      val locationB = location("B", customConfig = RecoverySpecLeveldb.config)

      val endpointA = locationA.endpoint(Set("L1"), Set(replicationConnection(locationB.port)), activate = false)
      val endpointB = locationB.endpoint(Set("L1"), Set(replicationConnection(locationA.port)))

      val recovery = endpointA.recover()

      an [IllegalStateException] shouldBe thrownBy(endpointA.activate())
      recovery.await
      an [IllegalStateException] shouldBe thrownBy(endpointA.activate())
    }
    "fail when connected endpoint is unavailable" in {
      val locationA = location("A", customConfig = ConfigFactory.parseString("eventuate.log.recovery.remote-operation-retry-max = 0").withFallback(RecoverySpecLeveldb.config))
      val endpointA = locationA.endpoint(Set("L1"), Set(replicationConnection(customPort)), activate = false)

      val recoveryException = intercept[RecoveryException] {
        endpointA.recover().await
      }

      recoveryException.partialUpdate should be(false)
    }
    "succeed normally if the endpoint was healthy (but not convergent yet)" in {
      val locationB = location("B", customConfig = RecoverySpecLeveldb.config)
      def newLocationA = location("A", customConfig = RecoverySpecLeveldb.config, customPort = customPort)
      val locationA1 = newLocationA

      val endpointB = locationB.endpoint(Set("L1"), Set(replicationConnection(locationA1.port)), activate = false)
      def newEndpointA(l: Location) = l.endpoint(Set("L1"), Set(replicationConnection(locationB.port)), activate = false)
      val endpointA1 = newEndpointA(locationA1)

      val targetA = endpointA1.target("L1")
      val targetB = endpointB.target("L1")

      write(targetA, List("a1", "a2"))
      write(targetB, List("b1", "b2"))
      replicate(targetA, targetB, 1)
      replicate(targetB, targetA, 1)

      locationA1.terminate().await

      val locationA2 = newLocationA
      val endpointA2 = newEndpointA(locationA2)

      endpointB.activate()
      endpointA2.recover().await

      assertConvergence(Set("a1", "a2", "b1", "b2"), endpointA2, endpointB)
    }
    "repair inconsistencies of an endpoint that has lost all events" in {
      val locationA = location("A", customConfig = RecoverySpecLeveldb.config)
      val locationB = location("B", customConfig = RecoverySpecLeveldb.config)
      val locationC = location("C", customConfig = RecoverySpecLeveldb.config)
      def newLocationD = location("D", customConfig = RecoverySpecLeveldb.config, customPort = customPort)
      val locationD1 = newLocationD

      val endpointA = locationA.endpoint(Set("L1"), Set(replicationConnection(locationD1.port)), activate = false)
      val endpointB = locationB.endpoint(Set("L1"), Set(replicationConnection(locationD1.port)), activate = false)
      val endpointC = locationC.endpoint(Set("L1"), Set(replicationConnection(locationD1.port)), activate = false)
      def newEndpointD(l: Location) = l.endpoint(Set("L1"), Set(replicationConnection(locationA.port), replicationConnection(locationB.port), replicationConnection(locationC.port)), activate = false)
      val endpointD1 = newEndpointD(locationD1)

      val targetA = endpointA.target("L1")
      val targetB = endpointB.target("L1")
      val targetC = endpointC.target("L1")
      val targetD1 = endpointD1.target("L1")

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
      locationD1.terminate().await
      FileUtils.deleteDirectory(logDirD)

      endpointA.activate()
      endpointB.activate()
      endpointC.activate()

      // start node D again (no backup available)
      val locationD2 = newLocationD
      val endpointD2 = newEndpointD(locationD2)

      endpointD2.recover().await
      // disclose bug #152 (writing new events is allowed after successful recovery)
      write(endpointD2.target("L1"), List("d1"))

      assertConvergence(Set("a", "b", "c", "d", "d1"), endpointA, endpointB, endpointC, endpointD2)
    }
    "repair inconsistencies of an endpoint that has lost all events but has been partially recovered from a storage backup" in {
      val locationA = location("A", customConfig = RecoverySpecLeveldb.config)
      val locationB = location("B", customConfig = RecoverySpecLeveldb.config)
      val locationC = location("C", customConfig = RecoverySpecLeveldb.config)
      def newLocationD = location("D", customConfig = RecoverySpecLeveldb.config, customPort = customPort)
      val locationD1 = newLocationD

      val endpointA = locationA.endpoint(Set("L1"), Set(replicationConnection(locationD1.port)), activate = false)
      val endpointB = locationB.endpoint(Set("L1"), Set(replicationConnection(locationD1.port)), activate = false)
      val endpointC = locationC.endpoint(Set("L1"), Set(replicationConnection(locationD1.port)), activate = false)
      def newEndpointD(l: Location) = l.endpoint(Set("L1"), Set(replicationConnection(locationA.port), replicationConnection(locationB.port), replicationConnection(locationC.port)), activate = false)
      val endpointD1 = newEndpointD(locationD1)

      val targetA = endpointA.target("L1")
      val targetB = endpointB.target("L1")
      val targetC = endpointC.target("L1")
      val targetD1 = endpointD1.target("L1")

      val rootDirD = rootDirectory(targetD1)
      val logDirD = logDirectory(targetD1)
      val bckDirD = new File(rootDirD, "backup")

      write(targetA, List("a"))
      replicate(targetA, targetD1)
      replicate(targetD1, targetA)

      write(targetB, List("b"))
      write(targetC, List("c"))
      replicate(targetB, targetD1)

      locationD1.terminate().await
      FileUtils.copyDirectory(logDirD, bckDirD)

      val locationD2 = newLocationD
      val endpointD2 = newEndpointD(locationD2)
      val targetD2 = endpointD2.target("L1")

      replicate(targetC, targetD2)
      replicate(targetD2, targetB)
      replicate(targetD2, targetC)

      write(targetD2, List("d"))
      replicate(targetD2, targetC)

      // what a disaster ...
      locationD2.terminate().await
      FileUtils.deleteDirectory(logDirD)

      // install a backup
      FileUtils.copyDirectory(bckDirD, logDirD)

      endpointA.activate()
      endpointB.activate()
      endpointC.activate()

      // start node D again (with backup available)
      val locationD3 = newLocationD
      val endpointD3 = newEndpointD(locationD3)

      endpointD3.recover().await

      assertConvergence(Set("a", "b", "c", "d"), endpointA, endpointB, endpointC, endpointD3)
    }
    "repair inconsistencies if recovery was stopped during event recovery and restarted" in {
      val config = ConfigFactory.parseString("eventuate.log.write-batch-size = 1").withFallback(RecoverySpecLeveldb.config)

      val locationB = location("B", customConfig = config)
      def newLocationA = location("A", customConfig = config, customPort = customPort)
      val locationA1 = newLocationA

      val endpointB = locationB.endpoint(Set("L1"), Set(replicationConnection(locationA1.port)), activate = true)
      def newEndpointA(l: Location) = l.endpoint(Set("L1"), Set(replicationConnection(locationB.port)), activate = false)
      val endpointA1 = newEndpointA(locationA1)

      val targetA = endpointA1.target("L1")
      val logDirA = logDirectory(targetA)
      val targetB = endpointB.target("L1")

      val as = (0 to 5).map("A" + _)
      val bs = (0 to 5).map("B" + _)
      val all = as.toSet ++ bs.toSet

      endpointA1.activate()

      write(targetA, as)
      write(targetB, bs)
      assertConvergence(all, endpointA1, endpointB)

      locationA1.terminate().await
      FileUtils.deleteDirectory(logDirA)

      val locationA2 = newLocationA
      val endpointA2 = newEndpointA(locationA2)

      endpointA2.recover()
      locationA2.listener(endpointA2.logs("L1")).waitForMessage("A1")
      locationA2.terminate().await

      val locationA3 = newLocationA
      val endpointA3 = newEndpointA(locationA3)

      endpointA3.recover().await

      assertConvergence(all, endpointA3, endpointB)
    }
  }

  "A replication endpoint" must {
    def createEndpoint: ReplicationEndpoint =
      location("A", customConfig = RecoverySpecLeveldb.config).endpoint(Set("L1"), Set(replicationConnection(customPort)), activate = false)

    "not allow concurrent recoveries" in {
      val endpoint = createEndpoint

      endpoint.recover()
      intercept[IllegalStateException] {
        endpoint.recover().await
      }
    }
    "not allow concurrent recovery and activation" in {
      val endpoint = createEndpoint

      endpoint.recover()
      intercept[IllegalStateException] {
        endpoint.activate()
      }
    }
    "not allow activated endpoints to be recovered" in {
      val endpoint = createEndpoint

      endpoint.activate()
      intercept[IllegalStateException] {
        endpoint.recover().await
      }
    }
    "not allow multiple activations" in {
      val endpoint = createEndpoint

      endpoint.activate()
      intercept[IllegalStateException] {
        endpoint.activate()
      }
    }
  }
}
