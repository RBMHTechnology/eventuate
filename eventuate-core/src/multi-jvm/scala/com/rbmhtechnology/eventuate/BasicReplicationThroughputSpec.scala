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

import akka.actor._
import akka.remote.testkit._
import akka.testkit.TestProbe

import com.typesafe.config._

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.util._

class BasicReplicationThroughputConfig(providerConfig: Config) extends MultiNodeReplicationConfig {
  val nodeA = role("nodeA")
  val nodeB = role("nodeB")
  val nodeC = role("nodeC")
  val nodeD = role("nodeD")
  val nodeE = role("nodeE")
  val nodeF = role("nodeF")

  val customConfig = ConfigFactory.parseString(
    s"""
      |akka.remote.netty.tcp.maximum-frame-size = 1048576b
      |eventuate.log.write-batch-size = 2000
      |eventuate.log.replication.retry-delay = 10s
    """.stripMargin)

  setConfig(customConfig.withFallback(providerConfig))
}

object BasicReplicationThroughputSpec {
  class ReplicatedActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    override val stateSync = false

    var events: Vector[String] = Vector.empty
    var startTime: Long = 0L
    var stopTime: Long = 0L

    def onCommand = {
      case "stats" =>
        probe ! s"${(1000.0 * 1000 * 1000 * events.size) / (stopTime - startTime)} events/sec"
      case s: String => persist(s) {
        case Success(e) =>
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

abstract class BasicReplicationThroughputSpec(config: BasicReplicationThroughputConfig) extends MultiNodeSpec(config) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {
  import BasicReplicationThroughputSpec._
  import config._

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
        val endpoint = createEndpoint(nodeA.name, Set(node(nodeC).address.toReplicationConnection))
        actor = system.actorOf(Props(new ReplicatedActor("pa", endpoint.log, probe.ref)))

        actor ! "start"
        expected.foreach(actor ! _)
        actor ! "stop"
      }

      runOn(nodeB) {
        val endpoint = createEndpoint(nodeB.name, Set(node(nodeC).address.toReplicationConnection))
        actor = system.actorOf(Props(new ReplicatedActor("pb", endpoint.log, probe.ref)))
      }

      runOn(nodeC) {
        val endpoint = createEndpoint(nodeC.name, Set(
          node(nodeA).address.toReplicationConnection,
          node(nodeB).address.toReplicationConnection,
          node(nodeD).address.toReplicationConnection))
        actor = system.actorOf(Props(new ReplicatedActor("pc", endpoint.log, probe.ref)))
      }

      runOn(nodeD) {
        val endpoint = createEndpoint(nodeD.name, Set(
          node(nodeC).address.toReplicationConnection,
          node(nodeE).address.toReplicationConnection,
          node(nodeF).address.toReplicationConnection))
        actor = system.actorOf(Props(new ReplicatedActor("pd", endpoint.log, probe.ref)))
      }

      runOn(nodeE) {
        val endpoint = createEndpoint(nodeE.name, Set(node(nodeD).address.toReplicationConnection))
        actor = system.actorOf(Props(new ReplicatedActor("pe", endpoint.log, probe.ref)))
      }

      runOn(nodeF) {
        val endpoint = createEndpoint(nodeF.name, Set(node(nodeD).address.toReplicationConnection))
        actor = system.actorOf(Props(new ReplicatedActor("pf", endpoint.log, probe.ref)))
      }

      probe.expectMsg(timeout, expected)
      actor ! "stats"
      println(probe.receiveOne(timeout))

      enterBarrier("finish")
    }
  }
}
