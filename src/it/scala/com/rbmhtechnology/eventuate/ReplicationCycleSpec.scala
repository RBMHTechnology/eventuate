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

abstract class ReplicationCycleSpec extends WordSpec with Matchers with ReplicationNodeRegistry {
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
    register(new ReplicationNode(nodeId(nodeName), logNames, port, connections, "eventuate.log.replication.batch-size-max = 50"))

  def testReplication(nodeA: ReplicationNode, nodeB: ReplicationNode, nodeC: ReplicationNode): Unit = {
    val probeA = new TestProbe(nodeA.system)
    val probeB = new TestProbe(nodeB.system)
    val probeC = new TestProbe(nodeC.system)

    val actorA = nodeA.system.actorOf(Props(new ReplicatedActor("pa", nodeA.logs("L1"), probeA.ref)))
    val actorB = nodeB.system.actorOf(Props(new ReplicatedActor("pb", nodeB.logs("L1"), probeB.ref)))
    val actorC = nodeB.system.actorOf(Props(new ReplicatedActor("pc", nodeC.logs("L1"), probeC.ref)))

    actorA ! "a1"
    actorB ! "b1"
    actorC ! "c1"

    val expected = List("a1", "b1", "c1")

    val eventsA = probeA.expectMsgAllOf(expected: _*)
    val eventsB = probeB.expectMsgAllOf(expected: _*)
    val eventsC = probeC.expectMsgAllOf(expected: _*)

    val num = 100
    val expectedA = 2 to num map { i => s"a$i"}
    val expectedB = 2 to num map { i => s"b$i"}
    val expectedC = 2 to num map { i => s"c$i"}
    val all = expectedA ++ expectedB ++ expectedC

    expectedA.foreach(s => actorA ! s)
    expectedB.foreach(s => actorB ! s)
    expectedC.foreach(s => actorC ! s)

    probeA.expectMsgAllOf(all: _*)
    probeB.expectMsgAllOf(all: _*)
    probeC.expectMsgAllOf(all: _*)
  }

  "Event log replication" must {
    "support bi-directional cyclic replication networks" in {
      testReplication(
        node("A", Set("L1"), 2552, Set(replicationConnection(2553), replicationConnection(2554))),
        node("B", Set("L1"), 2553, Set(replicationConnection(2552), replicationConnection(2554))),
        node("C", Set("L1"), 2554, Set(replicationConnection(2552), replicationConnection(2553))))

    }
    "support uni-directional cyclic replication networks" in {
      testReplication(
        node("A", Set("L1"), 2552, Set(replicationConnection(2553))),
        node("B", Set("L1"), 2553, Set(replicationConnection(2554))),
        node("C", Set("L1"), 2554, Set(replicationConnection(2552))))
    }
  }
}

class ReplicationCycleSpecLeveldb extends ReplicationCycleSpec with EventLogCleanupLeveldb {
  override val logFactory: String => Props = id => LeveldbEventLog.props(id)
}

class ReplicationCycleSpecCassandra extends ReplicationCycleSpec with EventLogCleanupCassandra {
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
