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

import scala.collection.immutable.Seq
import scala.util._

import akka.actor._
import akka.testkit.TestProbe

import org.scalatest._

object ReplicationProtocolIntegrationSpec {
  class PayloadEqualityFilter(payload: String) extends ReplicationFilter {
    override def apply(event: DurableEvent): Boolean = {
      event.payload == payload
    }
  }

  class TestActor1(val processId: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    def onCommand = {
      case s: String => persist(s) {
        case Success(e) => onEvent(e)
        case Failure(e) => throw e
      }
    }

    def onEvent = {
      case s: String => probe ! s
    }
  }

  def localConnection(port: Int, filter: Option[ReplicationFilter] = None): ReplicationConnection =
    ReplicationConnection("127.0.0.1", port, filter)
}

class ReplicationProtocolIntegrationSpec extends WordSpec with Matchers with BeforeAndAfterEach {
  import ReplicationProtocolIntegrationSpec._

  var nodes: List[ReplicationNode] = Nil

  override protected def afterEach(): Unit = {
    nodes.foreach(_.shutdown())
    nodes.foreach(_.awaitTermination())
    nodes.foreach(_.cleanup())
  }

  def register(node: ReplicationNode): ReplicationNode = {
    nodes = node :: nodes
    node
  }

  def assertPartialOrder[A](events: Seq[A], sample: A*): Unit = {
    val indices = sample.map(events.indexOf)
    assert(indices == indices.sorted)
  }

  "Event log replication" must {
    "replicate all events by default" in {
      val nodeA = register(new ReplicationNode("A", 2552, List(localConnection(2553))))
      val nodeB = register(new ReplicationNode("B", 2553, List(localConnection(2552), localConnection(2554))))
      val nodeC = register(new ReplicationNode("C", 2554, List(localConnection(2553))))

      val probeA = new TestProbe(nodeA.system)
      val probeB = new TestProbe(nodeB.system)
      val probeC = new TestProbe(nodeC.system)

      val actorA = nodeA.system.actorOf(Props(new TestActor1("pa", nodeA.log, probeA.ref)))
      val actorB = nodeB.system.actorOf(Props(new TestActor1("pb", nodeB.log, probeB.ref)))
      val actorC = nodeB.system.actorOf(Props(new TestActor1("pc", nodeC.log, probeC.ref)))

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
      val nodeA = register(new ReplicationNode("A", 2552, List(localConnection(2553, Some(new PayloadEqualityFilter("b2"))))))
      val nodeB = register(new ReplicationNode("B", 2553, List(localConnection(2552, Some(new PayloadEqualityFilter("a2"))))))

      val probeA = new TestProbe(nodeA.system)
      val probeB = new TestProbe(nodeB.system)

      val actorA = nodeA.system.actorOf(Props(new TestActor1("pa", nodeA.log, probeA.ref)))
      val actorB = nodeB.system.actorOf(Props(new TestActor1("pb", nodeB.log, probeB.ref)))

      actorA ! "a1"
      actorA ! "a2"
      actorA ! "a3"

      actorB ! "b1"
      actorB ! "b2"
      actorB ! "b3"

      val eventsA = probeA.expectMsgAllOf("a1", "a2", "a3", "b2")
      val eventsB = probeB.expectMsgAllOf("b1", "b2", "b3", "a2")
    }
    "immediately attempt next batch if last replicated batch had maximum size" in {
      val nodeA = register(new ReplicationNode("A", 2552, List(localConnection(2553))))
      val nodeB = register(new ReplicationNode("C", 2553, List(localConnection(2552))))

      val probeB = new TestProbe(nodeB.system)

      val actorA = nodeA.system.actorOf(Props(new TestActor1("pa", nodeA.log, nodeA.system.deadLetters)))
      val actorB = nodeB.system.actorOf(Props(new TestActor1("pb", nodeB.log, probeB.ref)))

      val num = 100

      1 to num foreach { i => actorA ! s"a${i}" }
      1 to num foreach { i => probeB.expectMsg(s"a${i}") }
    }
  }
}
