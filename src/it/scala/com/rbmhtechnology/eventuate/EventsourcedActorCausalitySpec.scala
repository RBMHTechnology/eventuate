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
import com.rbmhtechnology.eventuate.log.cassandra.Cassandra

import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest._

import scala.collection.immutable.Seq
import scala.util._

object EventsourcedActorCausalitySpec {
  class Collaborator(val id: String, val eventLog: ActorRef, override val sharedClockEntry: Boolean, handles: Set[String], probe: ActorRef) extends EventsourcedActor {
    val onCommand: Receive = {
      case s: String => persist(s) {
        case Success(e) => onEvent(e)
        case Failure(e) => throw e
      }
    }

    val onEvent: Receive = {
      case s: String if handles.contains(s) =>
        probe ! ((s, lastVectorTimestamp, currentTime))
    }
  }
}

abstract class EventsourcedActorCausalitySpec extends WordSpec with Matchers with ReplicationNodeRegistry {
  import ReplicationIntegrationSpec.replicationConnection
  import EventsourcedActorCausalitySpec._

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

  "Event-sourced actors" when {
    "located at different locations" can {
      "track causality by default (sharedClockEntry = true)" in {
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

        val actorA1 = nodeA.system.actorOf(Props(new Collaborator("pa1", logA, sharedClockEntry = true, Set("e1", "e2", "e5"), probeA1.ref)))
        val actorA2 = nodeA.system.actorOf(Props(new Collaborator("pa2", logA, sharedClockEntry = true, Set("e3", "e5", "e6"), probeA2.ref)))
        val actorA3 = nodeA.system.actorOf(Props(new Collaborator("pa3", logA, sharedClockEntry = true, Set("e4"), probeA3.ref)))
        val actorB = nodeB.system.actorOf(Props(new Collaborator("pb", logB, sharedClockEntry = true, Set("e1", "e6"), probeB.ref)))

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
        probeA3.expectMsg(("e4", vectorTime(4, 0), vectorTime(4, 0)))

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
    "located at the same location" can {
      "track causality if enabled (sharedClockEntry = false)" in {
        val logName = "L1"
        val logNode = this.node("A", Set(logName), 2552, Set())
        val log = logNode.logs(logName)
        val logId = logNode.endpoint.logId(logName)

        val actorIdA = "PA"
        val actorIdB = "PB"
        val actorIdC = "PC"

        val probeA = new TestProbe(logNode.system)
        val probeB = new TestProbe(logNode.system)
        val probeC = new TestProbe(logNode.system)

        val actorA = logNode.system.actorOf(Props(new Collaborator(actorIdA, log, sharedClockEntry = false, Set("e1", "e3"), probeA.ref)))
        val actorB = logNode.system.actorOf(Props(new Collaborator(actorIdB, log, sharedClockEntry = false, Set("e2", "e3"), probeB.ref)))
        val actorC = logNode.system.actorOf(Props(new Collaborator(actorIdC, log, sharedClockEntry = true, Set("e1", "e2", "e3"), probeC.ref)))

        actorA ! "e1"
        probeA.expectMsg(("e1", VectorTime(actorIdA -> 1L), VectorTime(actorIdA -> 1L)))
        probeC.expectMsg(("e1", VectorTime(actorIdA -> 1L), VectorTime(actorIdA -> 1L, logId -> 1L)))

        actorB ! "e2"
        probeB.expectMsg(("e2", VectorTime(actorIdB -> 1L), VectorTime(actorIdB -> 1L)))
        probeC.expectMsg(("e2", VectorTime(actorIdB -> 1L), VectorTime(actorIdA -> 1L, actorIdB -> 1L, logId -> 2L)))

        actorC ! "e3"
        probeA.expectMsg(("e3", VectorTime(actorIdA -> 1L, actorIdB -> 1L, logId -> 3L), VectorTime(actorIdA -> 2L, actorIdB -> 1L, logId -> 3L)))
        probeB.expectMsg(("e3", VectorTime(actorIdA -> 1L, actorIdB -> 1L, logId -> 3L), VectorTime(actorIdA -> 1L, actorIdB -> 2L, logId -> 3L)))
        probeC.expectMsg(("e3", VectorTime(actorIdA -> 1L, actorIdB -> 1L, logId -> 3L), VectorTime(actorIdA -> 1L, actorIdB -> 1L, logId -> 3L)))
      }
    }
  }
}

class EventsourcedActorCausalitySpecLeveldb extends EventsourcedActorCausalitySpec with EventLogCleanupLeveldb {
  override val logFactory: String => Props = id => EventLogLifecycleLeveldb.TestEventLog.props(id, batching = true)
}

class EventsourcedActorCausalitySpecCassandra extends EventsourcedActorCausalitySpec with EventLogCleanupCassandra {
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
