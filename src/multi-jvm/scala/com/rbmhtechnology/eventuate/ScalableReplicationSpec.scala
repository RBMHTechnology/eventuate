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
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.ReplicationEndpoint.Available

import scala.collection.immutable.Seq
import scala.util._

class ScalableReplicationSpecLeveldb extends ScalableReplicationSpec with MultiNodeSupportLeveldb
class ScalableReplicationSpecLeveldbMultiJvmNode1 extends ScalableReplicationSpecLeveldb
class ScalableReplicationSpecLeveldbMultiJvmNode2 extends ScalableReplicationSpecLeveldb
class ScalableReplicationSpecLeveldbMultiJvmNode3 extends ScalableReplicationSpecLeveldb

class ScalableReplicationSpecCassandra extends ScalableReplicationSpec with MultiNodeSupportCassandra {
  override def logName = "sr"
}
class ScalableReplicationSpecCassandraMultiJvmNode1 extends ScalableReplicationSpecCassandra
class ScalableReplicationSpecCassandraMultiJvmNode2 extends ScalableReplicationSpecCassandra
class ScalableReplicationSpecCassandraMultiJvmNode3 extends ScalableReplicationSpecCassandra

object ScalableReplicationConfig extends MultiNodeConfig {
  val nodeA = role("nodeA")
  val nodeB = role("nodeB")
  val nodeC = role("nodeC")

  testTransport(on = true)

  commonConfig(MultiNodeReplicationConfig.create("eventuate.log.replication.read-timeout = 1s"))
}

object ScalableReplicationSpec {
  class ReplicatedActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
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
}

abstract class ScalableReplicationSpec extends MultiNodeSpec(ScalableReplicationConfig) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {
  import ScalableReplicationConfig._
  import ScalableReplicationSpec._

  def initialParticipants: Int =
    roles.size

  def assertPartialOrder[A](events: Seq[A], sample: A*): Unit = {
    val indices = sample.map(events.indexOf)
    assert(indices == indices.sorted)
  }

  muteDeadLetters(classOf[AnyRef])(system)

  "Scalable event replication" must {
    "allow causal delivery" in {
      val filter = Map(logName -> NonReplicatedFilter)

      runOn(nodeA) {
        val availableProbe = TestProbe()
        val messageProbe = TestProbe()

        system.eventStream.subscribe(availableProbe.ref, classOf[Available])

        val endpoint = createEndpoint(nodeA.name, Set(
          node(nodeB).address.toReplicationConnection.copy(filters = filter),
          node(nodeC).address.toReplicationConnection.copy(filters = filter)))

        availableProbe.expectMsgAllOf(
          Available(nodeB.name, logName),
          Available(nodeC.name, logName))

        val actor = system.actorOf(Props(new ReplicatedActor("pa", endpoint.log, messageProbe.ref)))

        testConductor.blackhole(nodeA, nodeC, Direction.Both).await
        actor ! ("A")

        Thread.sleep(2000)
        testConductor.passThrough(nodeA, nodeC, Direction.Both).await
      }

      runOn(nodeB) {
        val messageProbe = TestProbe()
        val endpoint = createEndpoint(nodeB.name, Set(
          node(nodeA).address.toReplicationConnection.copy(filters = filter),
          node(nodeC).address.toReplicationConnection.copy(filters = filter)))
        val actor = system.actorOf(Props(new ReplicatedActor("pb", endpoint.log, messageProbe.ref)))

        messageProbe.expectMsg("A")
        actor ! ("B")
      }

      runOn(nodeC) {
        val messageProbe = TestProbe()
        val endpoint = createEndpoint(nodeC.name, Set(
          node(nodeA).address.toReplicationConnection.copy(filters = filter),
          node(nodeB).address.toReplicationConnection.copy(filters = filter)))
        val actor = system.actorOf(Props(new ReplicatedActor("pc", endpoint.log, messageProbe.ref)))

        messageProbe.expectMsg("A")
        messageProbe.expectMsg("B")
      }
      enterBarrier("finish")
    }
  }
}