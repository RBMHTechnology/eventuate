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

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import akka.actor._
import akka.serialization.Serializer
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.EndpointFilters.sourceFilters
import com.rbmhtechnology.eventuate.EndpointFilters.targetOverwritesSourceFilters
import com.rbmhtechnology.eventuate.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.ReplicationProtocol.ReplicationEndpointInfo.logId
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.serializer.DurableEventSerializerWithBinaryPayload
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest._

import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util._
import scala.util.control.Exception.ultimately

object ReplicationIntegrationSpec {
  class JavaSerializerWithManifest(system: ExtendedActorSystem) extends Serializer {
    override def includeManifest: Boolean = true

    override val identifier: Int = 647345238

    def toBinary(o: AnyRef): Array[Byte] = {
      val bos = new ByteArrayOutputStream
      val out = new ObjectOutputStream(bos)
      ultimately(out.close())(out.writeObject(o))
      bos.toByteArray
    }

    def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef = {
      val in = new ObjectInputStream(new ByteArrayInputStream(bytes))
      ultimately(in.close())(in.readObject())
    }
  }

  def javaSerializerWithManifestFor(types: Class[_]*): Config = {
    ConfigFactory.parseString(
      s"""
        |akka.actor.serializers.java-serializer-with-manifest = "com.rbmhtechnology.eventuate.ReplicationIntegrationSpec$$JavaSerializerWithManifest"
        |akka.actor.serialization-bindings {
        |  ${types.map(t => s"""  "${t.getName}" = java-serializer-with-manifest""").mkString("\n")}
        |}
      """.stripMargin)
  }

  class PayloadEqualityFilter(payload: String) extends ReplicationFilter {
    override def apply(event: DurableEvent): Boolean = {
      event.payload == payload
    }
  }

  class PayloadInequalityFilter(payload: String) extends ReplicationFilter {
    override def apply(event: DurableEvent): Boolean = {
      event.payload != payload
    }
  }

  class ReplicatedActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    override val stateSync = false

    def onCommand = {
      case s => persist(s) {
        case Success(e) =>
        case Failure(e) => throw e
      }
    }

    def onEvent = {
      case s => probe ! s
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
    "replicate events based on remote filter criteria" in {
      val locationA = location("A")
      val locationB = location("B")

      val endpointA = locationA.endpoint(Set("L1"), Set(replicationConnection(locationB.port, filters = Map("L1" -> new PayloadEqualityFilter("b2")))))
      val endpointB = locationB.endpoint(Set("L1"), Set(replicationConnection(locationA.port, filters = Map("L1" -> new PayloadEqualityFilter("a2")))))

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
    "replicate events based on local filter criteria" in {
      val locationA = location("A")
      val locationB = location("B")

      val endpointA = locationA.endpoint(Set("L1"), Set(replicationConnection(locationB.port)),
        sourceFilters(Map("L1" -> new PayloadEqualityFilter("a2"))))
      val endpointB = locationB.endpoint(Set("L1"), Set(replicationConnection(locationA.port)),
        targetOverwritesSourceFilters(
          Map(logId(locationA.id, "L1") -> new PayloadInequalityFilter("b2")),
          Map("L1" -> new PayloadEqualityFilter("b2"))
        ))

      val actorA = locationA.system.actorOf(Props(new ReplicatedActor("pa", endpointA.logs("L1"), locationA.probe.ref)))
      val actorB = locationB.system.actorOf(Props(new ReplicatedActor("pb", endpointB.logs("L1"), locationB.probe.ref)))

      actorA ! "a1"
      actorA ! "a2"
      actorA ! "a3"

      actorB ! "b1"
      actorB ! "b2"
      actorB ! "b3"

      val eventsA = locationA.probe.expectMsgAllOf("a1", "a2", "a3", "b1", "b3")
      val eventsB = locationB.probe.expectMsgAllOf("b1", "b2", "b3", "a2")
    }
    "replicate events based on local and remote filter criteria" in {
      val locationA = location("A")
      val locationB = location("B")

      val endpointA = locationA.endpoint(Set("L1"), Set(replicationConnection(locationB.port, filters = Map("L1" -> new PayloadInequalityFilter("b1")))), sourceFilters(Map("L1" -> new PayloadInequalityFilter("a2"))))
      val endpointB = locationB.endpoint(Set("L1"), Set(replicationConnection(locationA.port, filters = Map("L1" -> new PayloadInequalityFilter("a1")))), sourceFilters(Map("L1" -> new PayloadInequalityFilter("b2"))))

      val actorA = locationA.system.actorOf(Props(new ReplicatedActor("pa", endpointA.logs("L1"), locationA.probe.ref)))
      val actorB = locationB.system.actorOf(Props(new ReplicatedActor("pb", endpointB.logs("L1"), locationB.probe.ref)))

      actorA ! "a1"
      actorA ! "a2"
      actorA ! "a3"

      actorB ! "b1"
      actorB ! "b2"
      actorB ! "b3"

      val eventsA = locationA.probe.expectMsgAllOf("a1", "a2", "a3", "b3")
      val eventsB = locationB.probe.expectMsgAllOf("b1", "b2", "b3", "a3")
    }
    "replicate events according to BinaryPayload based local filters" in {
      val locationA = location(
        "A",
        customConfig = javaSerializerWithManifestFor(classOf[String], classOf[Integer]))
      val locationB = location(
        "B",
        customConfig = ConfigFactory.parseString(
          s"akka.actor.serializers.eventuate-durable-event = ${classOf[DurableEventSerializerWithBinaryPayload].getName}"))
      val locationC = location(
        "C",
        customConfig = javaSerializerWithManifestFor(classOf[String], classOf[Integer]))

      val endpointA =
        locationA.endpoint(Set("L1"), Set(replicationConnection(locationB.port)))
      locationB.endpoint(
        Set("L1"),
        Set(replicationConnection(locationA.port), replicationConnection(locationC.port)),
        sourceFilters(Map("L1" -> BinaryPayloadManifestFilter(".*String".r))))
      val endpointC =
        locationC.endpoint(Set("L1"), Set(replicationConnection(locationB.port)))

      val actorA = locationA.system.actorOf(Props(new ReplicatedActor("pa", endpointA.logs("L1"), locationA.probe.ref)))
      locationC.system.actorOf(Props(new ReplicatedActor("pc", endpointC.logs("L1"), locationC.probe.ref)))

      actorA ! "a1"
      actorA ! 2
      actorA ! "a2"

      locationC.probe.expectMsgAllOf("a1", "a2")
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
      probeUnavailable.expectMsgPF() {
        case Unavailable(endpointB1.id, "L1", causes) if causes.nonEmpty => causes.head shouldBe a[ReplicationReadTimeoutException]
      }

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
      testVersionedReads("test", ApplicationVersion("1.0"), "test", ApplicationVersion("0.9")).expectMsg(ReplicationReadFailure(IncompatibleApplicationVersionException(locationId("A"), ApplicationVersion("1.0"), ApplicationVersion("0.9")), DurableEvent.UndefinedLogId))
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
    val endpointA = locationA.endpoint(Set("L1"), Set(), applicationName = sourceApplicationName, applicationVersion = sourceApplicationVersion)

    val message = ReplicationRead(1, Int.MaxValue, Int.MaxValue, NoFilter, DurableEvent.UndefinedLogId, locationA.system.deadLetters, VectorTime())
    val envelope = ReplicationReadEnvelope(message, "L1", targetApplicationName, targetApplicationVersion)

    endpointA.acceptor.tell(envelope, locationA.probe.ref)
    locationA.probe
  }
}
