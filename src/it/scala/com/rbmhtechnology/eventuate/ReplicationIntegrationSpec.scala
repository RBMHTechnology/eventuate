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
import akka.testkit.TestProbe

import com.rbmhtechnology.eventuate.log._
import com.rbmhtechnology.eventuate.log.cassandra._
import com.rbmhtechnology.eventuate.log.leveldb._

import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest._

import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util._

object ReplicationIntegrationSpec {
  class PayloadEqualityFilter(payload: String) extends ReplicationFilter {
    override def apply(event: DurableEvent): Boolean = {
      event.payload == payload
    }
  }

  class ReplicatedActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    override val stateSync = false

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

  def replicationConnection(port: Int, filters: Map[String, ReplicationFilter] = Map.empty): ReplicationConnection =
    ReplicationConnection("127.0.0.1", port, filters)
}

abstract class ReplicationIntegrationSpec extends WordSpec with Matchers with ReplicationNodeRegistry {
  import ReplicationIntegrationSpec._

  implicit def logFactory: String => Props

  var ctr: Int = 0

  override def beforeEach(): Unit =
    ctr += 1

  def config =
    ReplicationConfig.create()

  def nodeId(node: String): String =
    s"${node}_${ctr}"

  def node(nodeName: String, logNames: Set[String], port: Int, connections: Set[ReplicationConnection]): ReplicationNode =
    register(new ReplicationNode(nodeId(nodeName), logNames, port, connections))

  def assertPartialOrder[A](events: Seq[A], sample: A*): Unit = {
    val indices = sample.map(events.indexOf)
    assert(indices == indices.sorted)
  }

  "Event log replication" must {
    "replicate all events by default" in {
      val nodeA = node("A", Set("L1"), 2552, Set(replicationConnection(2553)))
      val nodeB = node("B", Set("L1"), 2553, Set(replicationConnection(2552), replicationConnection(2554)))
      val nodeC = node("C", Set("L1"), 2554, Set(replicationConnection(2553)))

      val probeA = new TestProbe(nodeA.system)
      val probeB = new TestProbe(nodeB.system)
      val probeC = new TestProbe(nodeC.system)

      val actorA = nodeA.system.actorOf(Props(new ReplicatedActor("pa", nodeA.logs("L1"), probeA.ref)))
      val actorB = nodeB.system.actorOf(Props(new ReplicatedActor("pb", nodeB.logs("L1"), probeB.ref)))
      val actorC = nodeC.system.actorOf(Props(new ReplicatedActor("pc", nodeC.logs("L1"), probeC.ref)))

      actorA ! "a1"
      actorA ! "a2"
      actorA ! "a3"

      actorB ! "b1"
      actorB ! "b2"
      actorB ! "b3"

      actorC ! "c1"
      actorC ! "c2"
      actorC ! "c3"

      val expected = List("a1", "a2", "a3", "b1", "b2", "b3", "c1", "c2", "c3")

      val eventsA = probeA.expectMsgAllOf(expected: _*)
      val eventsB = probeB.expectMsgAllOf(expected: _*)
      val eventsC = probeC.expectMsgAllOf(expected: _*)

      def assertPartialOrderOnAllReplicas(sample: String*): Unit = {
        assertPartialOrder(eventsA, sample)
        assertPartialOrder(eventsB, sample)
        assertPartialOrder(eventsC, sample)
      }

      assertPartialOrderOnAllReplicas("a1", "a2", "a3")
      assertPartialOrderOnAllReplicas("b1", "b2", "b3")
      assertPartialOrderOnAllReplicas("c1", "c2", "c3")
    }
    "replicate events based on filter criteria" in {
      val nodeA = node("A", Set("L1"), 2552, Set(replicationConnection(2553, Map("L1" -> new PayloadEqualityFilter("b2")))))
      val nodeB = node("B", Set("L1"), 2553, Set(replicationConnection(2552, Map("L1" -> new PayloadEqualityFilter("a2")))))

      val probeA = new TestProbe(nodeA.system)
      val probeB = new TestProbe(nodeB.system)

      val actorA = nodeA.system.actorOf(Props(new ReplicatedActor("pa", nodeA.logs("L1"), probeA.ref)))
      val actorB = nodeB.system.actorOf(Props(new ReplicatedActor("pb", nodeB.logs("L1"), probeB.ref)))

      actorA ! "a1"
      actorA ! "a2"
      actorA ! "a3"

      actorB ! "b1"
      actorB ! "b2"
      actorB ! "b3"

      val eventsA = probeA.expectMsgAllOf("a1", "a2", "a3", "b2")
      val eventsB = probeB.expectMsgAllOf("b1", "b2", "b3", "a2")
    }
    "immediately attempt next batch if last replicated batch was not empty" in {
      val nodeA = node("A", Set("L1"), 2552, Set(replicationConnection(2553)))
      val nodeB = node("B", Set("L1"), 2553, Set(replicationConnection(2552)))

      val probeB = new TestProbe(nodeB.system)

      val actorA = nodeA.system.actorOf(Props(new ReplicatedActor("pa", nodeA.logs("L1"), nodeA.system.deadLetters)))
      val actorB = nodeB.system.actorOf(Props(new ReplicatedActor("pb", nodeB.logs("L1"), probeB.ref)))

      val num = 100

      1 to num foreach { i => actorA ! s"a${i}" }
      1 to num foreach { i => probeB.expectMsg(s"a${i}") }
    }
    "detect replication server availability" in {
      import ReplicationEndpoint._

      val nodeA = node("A", Set("L1"), 2552, Set(replicationConnection(2553)))
      val nodeB = node("B", Set("L1"), 2553, Set(replicationConnection(2552)))

      val probeA = new TestProbe(nodeA.system)
      val probeB = new TestProbe(nodeB.system)

      nodeA.system.eventStream.subscribe(probeA.ref, classOf[Available])
      nodeB.system.eventStream.subscribe(probeB.ref, classOf[Available])

      probeA.expectMsg(Available(nodeId("B"), "L1"))
      probeB.expectMsg(Available(nodeId("A"), "L1"))
    }
    "detect replication server unavailability" in {
      import ReplicationEndpoint._

      val nodeA = node("A", Set("L1"), 2552, Set(replicationConnection(2553)))
      val nodeB1 = node("B", Set("L1"), 2553, Set(replicationConnection(2552)))

      val probeAvailable1 = new TestProbe(nodeA.system)
      val probeAvailable2 = new TestProbe(nodeA.system)
      val probeUnavailable = new TestProbe(nodeA.system)

      nodeA.system.eventStream.subscribe(probeAvailable1.ref, classOf[Available])
      nodeA.system.eventStream.subscribe(probeUnavailable.ref, classOf[Unavailable])

      probeAvailable1.expectMsg(Available(nodeId("B"), "L1"))
      Await.result(nodeB1.terminate(), 10.seconds)
      probeUnavailable.expectMsg(Unavailable(nodeId("B"), "L1"))

      // start replication node B again
      node("B", Set("L1"), 2553, Set(replicationConnection(2552)))

      nodeA.system.eventStream.subscribe(probeAvailable2.ref, classOf[Available])
      probeAvailable2.expectMsg(Available(nodeId("B"), "L1"))
    }
    "support multiple logs per replication endpoint" in {
      val logNames = Set("L1", "L2")

      val nodeA = node("A", logNames, 2552, Set(replicationConnection(2553)))
      val nodeB = node("B", logNames, 2553, Set(replicationConnection(2552)))

      val probeAL1 = new TestProbe(nodeA.system)
      val probeAL2 = new TestProbe(nodeA.system)
      val probeBL1 = new TestProbe(nodeB.system)
      val probeBL2 = new TestProbe(nodeB.system)

      val actorAL1 = nodeA.system.actorOf(Props(new ReplicatedActor("pa1", nodeA.logs("L1"), probeAL1.ref)))
      val actorAL2 = nodeA.system.actorOf(Props(new ReplicatedActor("pa2", nodeA.logs("L2"), probeAL2.ref)))
      val actorBL1 = nodeB.system.actorOf(Props(new ReplicatedActor("pb1", nodeB.logs("L1"), probeBL1.ref)))
      val actorBL2 = nodeB.system.actorOf(Props(new ReplicatedActor("pb2", nodeB.logs("L2"), probeBL2.ref)))

      actorAL1 ! "a"
      actorBL1 ! "b"

      actorAL2 ! "x"
      actorBL2 ! "y"

      probeAL1.expectMsgAllOf("a", "b")
      probeBL1.expectMsgAllOf("a", "b")

      probeAL2.expectMsgAllOf("x", "y")
      probeBL2.expectMsgAllOf("x", "y")
    }
    "replicate only logs with matching names" in {
      val logNamesA = Set("L1", "L2")
      val logNamesB = Set("L2", "L3")

      val nodeA = node("A", logNamesA, 2552, Set(replicationConnection(2553)))
      val nodeB = node("B", logNamesB, 2553, Set(replicationConnection(2552)))

      val probeAL1 = new TestProbe(nodeA.system)
      val probeAL2 = new TestProbe(nodeA.system)
      val probeBL2 = new TestProbe(nodeB.system)
      val probeBL3 = new TestProbe(nodeB.system)

      val actorAL1 = nodeA.system.actorOf(Props(new ReplicatedActor("pa1", nodeA.logs("L1"), probeAL1.ref)))
      val actorAL2 = nodeA.system.actorOf(Props(new ReplicatedActor("pa2", nodeA.logs("L2"), probeAL2.ref)))
      val actorBL2 = nodeB.system.actorOf(Props(new ReplicatedActor("pb2", nodeB.logs("L2"), probeBL2.ref)))
      val actorBL3 = nodeB.system.actorOf(Props(new ReplicatedActor("pb3", nodeB.logs("L3"), probeBL3.ref)))

      actorAL1 ! "a"
      actorAL2 ! "b"

      actorBL2 ! "c"
      actorBL3 ! "d"

      probeAL1.expectMsgAllOf("a")
      probeAL2.expectMsgAllOf("b", "c")

      probeBL2.expectMsgAllOf("b", "c")
      probeBL3.expectMsgAllOf("d")
    }
  }
}

class ReplicationIntegrationSpecLeveldb extends ReplicationIntegrationSpec with EventLogCleanupLeveldb {
  override val logFactory: String => Props = id => LeveldbEventLog.props(id)
}

class ReplicationIntegrationSpecCassandra extends ReplicationIntegrationSpec with EventLogCleanupCassandra {
  override val logFactory: String => Props = id => CassandraEventLog.props(id)

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(60000)
  }

  override def node(nodeName: String, logNames: Set[String], port: Int, connections: Set[ReplicationConnection]): ReplicationNode = {
    val node = super.node(nodeName, logNames, port, connections)
    Cassandra(node.system) // enforce keyspace/schema setup
    node
  }
}
