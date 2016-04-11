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
import akka.testkit.TestProbe

import com.rbmhtechnology.eventuate.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.ReplicationProtocol._

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

    def onCommand = {
      case s: String => persist(s) {
        case Success(e) =>
        case Failure(e) => throw e
      }
    }

    def onEvent = {
      case s: String => probe ! s
    }
  }

  def replicationConnection(port: Int, filters: Map[String, ReplicationFilter] = Map.empty): ReplicationConnection =
    ReplicationConnection("127.0.0.1", port, filters)
}

trait ReplicationIntegrationSpec extends WordSpec with Matchers with MultiLocationSpec {
  import ReplicationIntegrationSpec._

  def customPort: Int

  def assertPartialOrder[A](events: Seq[A], sample: A*): Unit = {
    val indices = sample.map(events.indexOf)
    assert(indices == indices.sorted)
  }

  "Event log replication" must {
    "replicate all events by default" in {
      val locationA = location("A")
      val locationB = location("B")
      val locationC = location("C")

      val endpointA = locationA.endpoint(Set("L1"), Set(replicationConnection(locationB.port)))
      val endpointB = locationB.endpoint(Set("L1"), Set(replicationConnection(locationA.port), replicationConnection(locationC.port)))
      val endpointC = locationC.endpoint(Set("L1"), Set(replicationConnection(locationB.port)))

      val actorA = locationA.system.actorOf(Props(new ReplicatedActor("pa", endpointA.logs("L1"), locationA.probe.ref)))
      val actorB = locationB.system.actorOf(Props(new ReplicatedActor("pb", endpointB.logs("L1"), locationB.probe.ref)))
      val actorC = locationC.system.actorOf(Props(new ReplicatedActor("pc", endpointC.logs("L1"), locationC.probe.ref)))

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

      val eventsA = locationA.probe.expectMsgAllOf(expected: _*)
      val eventsB = locationB.probe.expectMsgAllOf(expected: _*)
      val eventsC = locationC.probe.expectMsgAllOf(expected: _*)

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
      val locationA = location("A")
      val locationB = location("B")

      val endpointA = locationA.endpoint(Set("L1"), Set(replicationConnection(locationB.port, Map("L1" -> new PayloadEqualityFilter("b2")))))
      val endpointB = locationB.endpoint(Set("L1"), Set(replicationConnection(locationA.port, Map("L1" -> new PayloadEqualityFilter("a2")))))

      val actorA = locationA.system.actorOf(Props(new ReplicatedActor("pa", endpointA.logs("L1"), locationA.probe.ref)))
      val actorB = locationB.system.actorOf(Props(new ReplicatedActor("pb", endpointB.logs("L1"), locationB.probe.ref)))

      actorA ! "a1"
      actorA ! "a2"
      actorA ! "a3"

      actorB ! "b1"
      actorB ! "b2"
      actorB ! "b3"

      val eventsA = locationA.probe.expectMsgAllOf("a1", "a2", "a3", "b2")
      val eventsB = locationB.probe.expectMsgAllOf("b1", "b2", "b3", "a2")
    }
    "immediately attempt next batch if last replicated batch was not empty" in {
      val locationA = location("A")
      val locationB = location("B")

      val endpointA = locationA.endpoint(Set("L1"), Set(replicationConnection(locationB.port)))
      val endpointB = locationB.endpoint(Set("L1"), Set(replicationConnection(locationA.port)))

      val actorA = locationA.system.actorOf(Props(new ReplicatedActor("pa", endpointA.logs("L1"), locationA.system.deadLetters)))
      val actorB = locationB.system.actorOf(Props(new ReplicatedActor("pb", endpointB.logs("L1"), locationB.probe.ref)))

      val num = 100

      1 to num foreach { i => actorA ! s"a${i}" }
      1 to num foreach { i => locationB.probe.expectMsg(s"a${i}") }
    }
    "detect replication server availability" in {
      import ReplicationEndpoint._

      val locationA = location("A")
      val locationB = location("B")

      val endpointA = locationA.endpoint(Set("L1"), Set(replicationConnection(locationB.port)))
      val endpointB = locationB.endpoint(Set("L1"), Set(replicationConnection(locationA.port)))

      endpointA.system.eventStream.subscribe(locationA.probe.ref, classOf[Available])
      endpointB.system.eventStream.subscribe(locationB.probe.ref, classOf[Available])

      locationA.probe.expectMsg(Available(endpointB.id, "L1"))
      locationB.probe.expectMsg(Available(endpointA.id, "L1"))
    }
    "detect replication server unavailability" in {
      import ReplicationEndpoint._

      val locationA = location("A")
      val locationB1 = location("B", customPort = customPort)

      val endpointA = locationA.endpoint(Set("L1"), Set(replicationConnection(locationB1.port)))
      val endpointB1 = locationB1.endpoint(Set("L1"), Set(replicationConnection(locationA.port)))

      val probeAvailable1 = new TestProbe(locationA.system)
      val probeAvailable2 = new TestProbe(locationA.system)
      val probeUnavailable = new TestProbe(locationA.system)

      locationA.system.eventStream.subscribe(probeAvailable1.ref, classOf[Available])
      locationA.system.eventStream.subscribe(probeUnavailable.ref, classOf[Unavailable])

      probeAvailable1.expectMsg(Available(endpointB1.id, "L1"))
      Await.result(locationB1.terminate(), 10.seconds)
      probeUnavailable.expectMsg(Unavailable(endpointB1.id, "L1"))

      val locationB2 = location("B", customPort = customPort)
      val endpointB2 = locationB2.endpoint(Set("L1"), Set(replicationConnection(locationA.port)))

      locationA.system.eventStream.subscribe(probeAvailable2.ref, classOf[Available])
      probeAvailable2.expectMsg(Available(endpointB2.id, "L1"))
    }
    "support multiple logs per replication endpoint" in {
      val logNames = Set("L1", "L2")

      val locationA = location("A")
      val locationB = location("B")

      val endpointA = locationA.endpoint(logNames, Set(replicationConnection(locationB.port)))
      val endpointB = locationB.endpoint(logNames, Set(replicationConnection(locationA.port)))

      val probeAL1 = new TestProbe(locationA.system)
      val probeAL2 = new TestProbe(locationA.system)
      val probeBL1 = new TestProbe(locationB.system)
      val probeBL2 = new TestProbe(locationB.system)

      val actorAL1 = locationA.system.actorOf(Props(new ReplicatedActor("pa1", endpointA.logs("L1"), probeAL1.ref)))
      val actorAL2 = locationA.system.actorOf(Props(new ReplicatedActor("pa2", endpointA.logs("L2"), probeAL2.ref)))
      val actorBL1 = locationB.system.actorOf(Props(new ReplicatedActor("pb1", endpointB.logs("L1"), probeBL1.ref)))
      val actorBL2 = locationB.system.actorOf(Props(new ReplicatedActor("pb2", endpointB.logs("L2"), probeBL2.ref)))

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

      val locationA = location("A")
      val locationB = location("B")

      val endpointA = locationA.endpoint(logNamesA, Set(replicationConnection(locationB.port)))
      val endpointB = locationB.endpoint(logNamesB, Set(replicationConnection(locationA.port)))

      val probeAL1 = new TestProbe(locationA.system)
      val probeAL2 = new TestProbe(locationA.system)
      val probeBL2 = new TestProbe(locationB.system)
      val probeBL3 = new TestProbe(locationB.system)

      val actorAL1 = locationA.system.actorOf(Props(new ReplicatedActor("pa1", endpointA.logs("L1"), probeAL1.ref)))
      val actorAL2 = locationA.system.actorOf(Props(new ReplicatedActor("pa2", endpointA.logs("L2"), probeAL2.ref)))
      val actorBL2 = locationB.system.actorOf(Props(new ReplicatedActor("pb2", endpointB.logs("L2"), probeBL2.ref)))
      val actorBL3 = locationB.system.actorOf(Props(new ReplicatedActor("pb3", endpointB.logs("L3"), probeBL3.ref)))

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

  "A replication endpoint" must {
    "accept replication reads from endpoints with same application name and same version" in {
      testVersionedReads("test", ApplicationVersion("1.0"), "test", ApplicationVersion("1.0")).expectMsgType[ReplicationReadSuccess]
    }
    "accept replication reads from endpoints with same application name and higher version" in {
      testVersionedReads("test", ApplicationVersion("1.0"), "test", ApplicationVersion("1.1")).expectMsgType[ReplicationReadSuccess]
    }
    "reject replication reads from endpoints with same application name and lower version" in {
      testVersionedReads("test", ApplicationVersion("1.0"), "test", ApplicationVersion("0.9")).expectMsg(ReplicationReadEnvelopeIncompatible(ApplicationVersion("1.0")))
    }
    "accept replication reads from endpoints with different application name and same version" in {
      testVersionedReads("test", ApplicationVersion("1.0"), "other", ApplicationVersion("1.0")).expectMsgType[ReplicationReadSuccess]
    }
    "accept replication reads from endpoints with different application name and higher version" in {
      testVersionedReads("test", ApplicationVersion("1.0"), "other", ApplicationVersion("1.1")).expectMsgType[ReplicationReadSuccess]
    }
    "accept replication reads from endpoints with different application name and lower version" in {
      testVersionedReads("test", ApplicationVersion("1.0"), "other", ApplicationVersion("0.9")).expectMsgType[ReplicationReadSuccess]
    }
  }

  def testVersionedReads(
    sourceApplicationName: String,
    sourceApplicationVersion: ApplicationVersion,
    targetApplicationName: String,
    targetApplicationVersion: ApplicationVersion): TestProbe = {

    val locationA = location("A")
    val endpointA = locationA.endpoint(Set("L1"), Set(), sourceApplicationName, sourceApplicationVersion)

    val message = ReplicationRead(1, Int.MaxValue, Int.MaxValue, NoFilter, DurableEvent.UndefinedLogId, locationA.system.deadLetters, VectorTime())
    val envelope = ReplicationReadEnvelope(message, "L1", targetApplicationName, targetApplicationVersion)

    endpointA.acceptor.tell(envelope, locationA.probe.ref)
    locationA.probe
  }
}
