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
import scala.concurrent.duration._
import scala.util._

class BasicReplicationThroughputSpecMultiJvmNode1 extends BasicReplicationThroughputSpec
class BasicReplicationThroughputSpecMultiJvmNode2 extends BasicReplicationThroughputSpec
class BasicReplicationThroughputSpecMultiJvmNode3 extends BasicReplicationThroughputSpec
class BasicReplicationThroughputSpecMultiJvmNode4 extends BasicReplicationThroughputSpec
class BasicReplicationThroughputSpecMultiJvmNode5 extends BasicReplicationThroughputSpec
class BasicReplicationThroughputSpecMultiJvmNode6 extends BasicReplicationThroughputSpec

object BasicReplicationThroughputConfig extends MultiNodeConfig {
  val nodeA = role("nodeA")
  val nodeB = role("nodeB")
  val nodeC = role("nodeC")
  val nodeD = role("nodeD")
  val nodeE = role("nodeE")
  val nodeF = role("nodeF")

  commonConfig(ConfigFactory.parseString(
    s"""
      |akka.loglevel = "ERROR"
      |akka.remote.netty.tcp.maximum-frame-size = 512000b
      |akka.test.single-expect-default = 10s
      |akka.testconductor.barrier-timeout = 60s
      |log.replication.transfer-batch-size-max = 2000
      |log.replication.transfer-retry-interval = 10s
      |log.replication.connect-retry-interval = 1s
      |log.replication.failure-detection-limit = 60s
    """.stripMargin))
}

object BasicReplicationThroughputSpec {
  class ReplicatedActor(val processId: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    override val stateSync = false

    var events: Vector[String] = Vector.empty
    var startTime: Long = 0L
    var stopTime: Long = 0L

    def onCommand = {
      case "stats" =>
        probe ! s"${(1000.0 * 1000 * 1000 * events.size) / (stopTime - startTime) } events/sec"
      case s: String => persist(s) {
        case Success(e) => onEvent(e)
        case Failure(e) => throw e
      }
    }

    def onEvent = {
      case "start" =>
        startTime = System.nanoTime()
      case "stop" =>
        stopTime = System.nanoTime()
        probe ! events
      case s: String =>
        events = events :+ s
    }
  }
}

class BasicReplicationThroughputSpec extends MultiNodeSpec(BasicReplicationThroughputConfig) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {
  import BasicReplicationThroughputConfig._
  import BasicReplicationThroughputSpec._
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

      val num = 5000
      val timeout = 60.seconds
      val expected = (1 to num).toVector.map(i => s"e-${i}")

      var actor: ActorRef = system.deadLetters

      // ---------------------------------------
      //
      //  Topology:
      //
      //  A        E
      //   \      /
      //    C -- D
      //   /      \
      //  B        F
      //
      // ---------------------------------------


      runOn(nodeA) {
        val endpoint = createEndpoint(nodeA.name, Seq(node(nodeC).address.toReplicationConnection))
        actor = system.actorOf(Props(new ReplicatedActor("pa", endpoint.logs(DefaultLogName), probe.ref)))

        actor ! "start"
        expected.foreach(actor ! _)
        actor ! "stop"
      }

      runOn(nodeB) {
        val endpoint = createEndpoint(nodeB.name, Seq(node(nodeC).address.toReplicationConnection))
        actor = system.actorOf(Props(new ReplicatedActor("pb", endpoint.logs(DefaultLogName), probe.ref)))
      }

      runOn(nodeC) {
        val endpoint = createEndpoint(nodeC.name, Seq(
          node(nodeA).address.toReplicationConnection,
          node(nodeB).address.toReplicationConnection,
          node(nodeD).address.toReplicationConnection))
        actor = system.actorOf(Props(new ReplicatedActor("pc", endpoint.logs(DefaultLogName), probe.ref)))
      }

      runOn(nodeD) {
        val endpoint = createEndpoint(nodeD.name, Seq(
          node(nodeC).address.toReplicationConnection,
          node(nodeE).address.toReplicationConnection,
          node(nodeF).address.toReplicationConnection))
        actor = system.actorOf(Props(new ReplicatedActor("pd", endpoint.logs(DefaultLogName), probe.ref)))
      }

      runOn(nodeE) {
        val endpoint = createEndpoint(nodeE.name, Seq(node(nodeD).address.toReplicationConnection))
        actor = system.actorOf(Props(new ReplicatedActor("pe", endpoint.logs(DefaultLogName), probe.ref)))
      }

      runOn(nodeF) {
        val endpoint = createEndpoint(nodeF.name, Seq(node(nodeD).address.toReplicationConnection))
        actor = system.actorOf(Props(new ReplicatedActor("pf", endpoint.logs(DefaultLogName), probe.ref)))
      }

      probe.expectMsg(timeout, expected)
      actor ! "stats"
      println(probe.receiveOne(timeout))

      enterBarrier("finish")

      // Workaround for a LevelDB shutdown issue that causes "pthread lock: Invalid argument"
      Thread.sleep(2000)
    }
  }
}
