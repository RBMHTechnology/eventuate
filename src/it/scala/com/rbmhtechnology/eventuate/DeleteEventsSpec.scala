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

import akka.actor._

import com.rbmhtechnology.eventuate.ReplicationIntegrationSpec.replicationConnection
import com.rbmhtechnology.eventuate.log.EventLogCleanupLeveldb
import com.rbmhtechnology.eventuate.log.EventLogLifecycleLeveldb.TestEventLog
import com.rbmhtechnology.eventuate.utilities.AwaitHelper

import org.scalatest.Matchers
import org.scalatest.WordSpec

import scala.util.Failure
import scala.util.Success

object DeleteEventsSpecLeveldb {
  val L1 = "L1"

  val portA = 2552
  val connectedToA = replicationConnection(portA)

  val portB = 2553
  val connectedToB = replicationConnection(portB)

  val portC = 2554
  val connectedToC = replicationConnection(portC)

  class Emitter(locationId: String, val eventLog: ActorRef) extends EventsourcedActor with ActorLogging {

    override def id = s"${locationId}_Emitter"

    override def onEvent = Actor.emptyBehavior

    override def onCommand = {
      case msg => persist(msg) {
        case Success(_) =>
        case Failure(ex) => log.error(ex, s"Error when persisting $msg in test")
      }
    }
  }

  def emitter(node: ReplicationNode, logName: String) =
    node.system.actorOf(Props(new Emitter(node.id, node.logs(logName))))
}

class DeleteEventsSpecLeveldb extends WordSpec with Matchers with ReplicationNodeRegistry with EventLogCleanupLeveldb {
  import DeleteEventsSpecLeveldb._

  implicit val logFactory: String => Props = id => TestEventLog.props(id, batching = true)

  private var ctr: Int = 0

  override def beforeEach(): Unit =
    ctr += 1

  def config =
    ReplicationConfig.create()

  def nodeId(node: String): String =
    s"${node}_${ctr}"

  def node(nodeName: String, logNames: Set[String], port: Int, connections: Set[ReplicationConnection], customConfig: String = "", activate: Boolean = false): ReplicationNode =
    register(new ReplicationNode(nodeId(nodeName), logNames, port, connections, customConfig = RecoverySpecLeveldb.config + customConfig, activate = activate))

  "Deleting events" must {
    "not replay deleted events on restart" in {
      val nodeA = newNodeA(Set(L1))
      val emitterA = emitter(nodeA, L1)
      val listenerA = nodeA.eventListener(L1)

      (0 to 5).foreach(emitterA ! _)
      listenerA.waitForMessage(5)

      nodeA.endpoint.delete(L1, 3, Set.empty).await shouldBe 3

      nodeA.terminate().await

      val restartedA = newNodeA(Set(L1))
      restartedA.eventListener(L1).expectMsgAllOf(3 to 5: _*)
    }
  }

  "Conditionally deleting events" must {
    "keep event available for corresponding remote log" in {
      val nodeA = newNodeA(Set(L1), Set(connectedToB, connectedToC))
      val nodeB = newNodeB(Set(L1), Set(connectedToA))
      val nodeC = newNodeC(Set(L1), Set(connectedToA))
      val emitterA = emitter(nodeA, L1)
      val listenerA = nodeA.eventListener(L1)
      val listenerB = nodeB.eventListener(L1)
      val listenerC = nodeC.eventListener(L1)

      (0 to 5).foreach(emitterA ! _)
      listenerA.waitForMessage(5)

      nodeA.endpoint.delete(L1, 3, Set(nodeB.endpoint.id, nodeC.endpoint.id)).await shouldBe 3

      nodeA.endpoint.activate()
      nodeB.endpoint.activate()
      listenerB.expectMsgAllOf(0 to 5: _*)

      nodeC.endpoint.activate()
      listenerC.expectMsgAllOf(0 to 5: _*)
    }
  }

  def newNodeA(logNames: Set[String], connections: Set[ReplicationConnection] = Set.empty) =
    node("A", logNames, portA, connections)

  def newNodeB(logNames: Set[String], connections: Set[ReplicationConnection] = Set.empty) =
    node("B", logNames, portB, connections)

  def newNodeC(logNames: Set[String], connections: Set[ReplicationConnection] = Set.empty) =
    node("C", logNames, portC, connections)
}
