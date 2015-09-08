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
import com.rbmhtechnology.eventuate.log.EventLogSpec._
import com.rbmhtechnology.eventuate.log.cassandra.Cassandra

import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest._

import scala.collection.immutable.Seq
import scala.util._

object CausalityIntegrationSpec {
  class ReplicatedActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    val onCommand: Receive = {
      case s: String => persist(s) {
        case Success(e) => onEvent(e)
        case Failure(e) => throw e
      }
    }

    val onEvent: Receive = {
      case s: String if s.startsWith("z") =>
        probe ! s
      case s: String =>
        probe ! ((s, lastVectorTimestamp, lastHandledTime))
    }
  }

  class Collaborator(val id: String, val eventLog: ActorRef, handles: Set[String], probe: ActorRef) extends EventsourcedActor {
    val onCommand: Receive = {
      case s: String => persist(s) {
        case Success(e) => onEvent(e)
        case Failure(e) => throw e
      }
    }

    val onEvent: Receive = {
      case s: String if handles.contains(s) =>
        probe ! ((s, lastVectorTimestamp, lastHandledTime))
    }
  }

  def replicationConnection(port: Int, filters: Map[String, ReplicationFilter] = Map.empty): ReplicationConnection =
    ReplicationConnection("127.0.0.1", port, filters)
}

abstract class CausalityIntegrationSpec extends WordSpec with Matchers with ReplicationNodeRegistry {
  import CausalityIntegrationSpec._

  implicit def logFactory: String => Props

  var ctr: Int = 0

  override def beforeEach(): Unit = {
    ctr += 1
  }

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

  "Eventuate" must {
    "track causality with plausible clocks" in {
      val logName = "L1"

      val nodeA = node("A", Set(logName), 2552, Set(replicationConnection(2553)))
      val nodeB = node("B", Set(logName), 2553, Set(replicationConnection(2552)))

      val logA = nodeA.logs(logName)
      val logB = nodeB.logs(logName)

      val logIdA = nodeA.endpoint.logId(logName)
      val logIdB = nodeB.endpoint.logId(logName)

      val probeA1 = new TestProbe(nodeA.system)
      val probeA2 = new TestProbe(nodeA.system)
      val probeA3 = new TestProbe(nodeA.system)
      val probeB = new TestProbe(nodeB.system)

      val actorA1 = nodeA.system.actorOf(Props(new Collaborator("pa1", logA, Set("e1", "e2", "e5"), probeA1.ref)))
      val actorA2 = nodeA.system.actorOf(Props(new Collaborator("pa2", logA, Set("e3", "e5", "e6"), probeA2.ref)))
      val actorA3 = nodeA.system.actorOf(Props(new Collaborator("pa3", logA, Set("e4"), probeA2.ref)))
      val actorB = nodeB.system.actorOf(Props(new Collaborator("pb", logB, Set("e1", "e6"), probeB.ref)))

      def vectorTime(a: Long, b: Long) = (a, b) match {
        case (0L, 0L) => VectorTime()
        case (a,  0L) => VectorTime(logIdA -> a)
        case (0L,  b) => VectorTime(logIdB -> b)
        case (a,   b) => VectorTime(logIdA -> a, logIdB -> b)
      }

      actorB ! "e1"
      probeA1.expectMsg(("e1", vectorTime(0, 1), vectorTime(1, 1)))
      probeB.expectMsg(("e1", vectorTime(0, 1), vectorTime(0, 1)))

      actorA1 ! "e2"
      probeA1.expectMsg(("e2", vectorTime(2, 1), vectorTime(2, 1)))

      actorA2 ! "e3"
      probeA2.expectMsg(("e3", vectorTime(3, 0), vectorTime(3, 0)))

      actorA3 ! "e4"
      probeA2.expectMsg(("e4", vectorTime(4, 0), vectorTime(4, 0)))

      actorA1 ! "e5"
      probeA1.expectMsg(("e5", vectorTime(5, 1), vectorTime(5, 1)))
      probeA2.expectMsg(("e5", vectorTime(5, 1), vectorTime(5, 1)))

      actorA2 ! "e6"
      probeA2.expectMsg(("e6", vectorTime(6, 1), vectorTime(6, 1)))
      probeB.expectMsg(("e6", vectorTime(6, 1), vectorTime(6, 6)))

      // -----------------------------------------------------------
      //  Please note:
      //  - e2 <-> e3 (because e1 -> e2 and e1 <-> e3)
      //  - e3 <-> e4 (but plausible clocks reports e3 -> e4)
      // -----------------------------------------------------------
    }
  }
}

class CausalityIntegrationSpecLeveldb extends CausalityIntegrationSpec with EventLogCleanupLeveldb {
  override val logFactory: String => Props = id => EventLogLifecycleLeveldb.TestEventLog.props(id, batching = true)
}

class CausalityIntegrationSpecCassandra extends CausalityIntegrationSpec with EventLogCleanupCassandra {
  override val logFactory: String => Props = id => EventLogLifecycleCassandra.TestEventLog.props(id, batching = true)

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
