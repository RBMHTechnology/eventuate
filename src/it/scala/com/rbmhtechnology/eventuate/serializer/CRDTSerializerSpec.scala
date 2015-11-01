package com.rbmhtechnology.eventuate.serializer

import akka.serialization.SerializationExtension
import akka.testkit.TestProbe

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.crdt._
import com.rbmhtechnology.eventuate.serializer.DurableEventSerializerSpec.ExamplePayload

import org.scalatest._

object CRDTSerializerSpec {
  def orSet(payload: ExamplePayload) =
    ORSet[ExamplePayload].add(payload, VectorTime("s" -> 17L))

  def mvRegister(payload: ExamplePayload) =
    MVRegister[ExamplePayload].set(payload, VectorTime("s" -> 18L), 18, "e1")

  def lwwRegister(payload: ExamplePayload) =
    LWWRegister[ExamplePayload].set(payload, VectorTime("s" -> 19L), 19, "e2")
}

class CRDTSerializerSpec extends WordSpec with Matchers with BeforeAndAfterAll {
  import DurableEventSerializerSpec.ExamplePayload
  import CRDTSerializerSpec._

  val config =
    """
      |akka.actor.serializers {
      |  eventuate-test = "com.rbmhtechnology.eventuate.serializer.DurableEventSerializerSpec$ExamplePayloadSerializer"
      |}
      |akka.actor.serialization-bindings {
      |  "com.rbmhtechnology.eventuate.serializer.DurableEventSerializerSpec$ExamplePayload" = eventuate-test
      |}
    """.stripMargin

  val support = new SerializerSpecSupport(
    ReplicationConfig.create(2552),
    ReplicationConfig.create(2553, config))

  override def afterAll(): Unit =
    support.shutdown()

  import support._

  "A CRDTSerializer" must {
    "support ORSet serialization with default payload serialization" in {
      val probe = new TestProbe(system1)
      val serialization = SerializationExtension(system1)

      val initial = orSet(ExamplePayload("foo", "bar"))
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[ORSet[_]]).get should be(expected)
    }
    "support ORSet serialization with custom payload serialization" in {
      val probe = new TestProbe(system2)
      val serialization = SerializationExtension(system2)

      val initial = orSet(ExamplePayload("foo", "bar"))
      val expected = orSet(ExamplePayload("bar", "foo"))

      serialization.deserialize(serialization.serialize(initial).get, classOf[ORSet[_]]).get should be(expected)
    }
    "support MVRegister serialization with default payload serialization" in {
      val probe = new TestProbe(system1)
      val serialization = SerializationExtension(system1)

      val initial = mvRegister(ExamplePayload("foo", "bar"))
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[MVRegister[_]]).get should be(expected)
    }
    "support MVRegister serialization with custom payload serialization" in {
      val probe = new TestProbe(system2)
      val serialization = SerializationExtension(system2)

      val initial = mvRegister(ExamplePayload("foo", "bar"))
      val expected = mvRegister(ExamplePayload("bar", "foo"))

      serialization.deserialize(serialization.serialize(initial).get, classOf[MVRegister[_]]).get should be(expected)
    }
    "support LWWRegister serialization with default payload serialization" in {
      val probe = new TestProbe(system1)
      val serialization = SerializationExtension(system1)

      val initial = lwwRegister(ExamplePayload("foo", "bar"))
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[LWWRegister[_]]).get should be(expected)
    }
    "support LWWRegister serialization with custom payload serialization" in {
      val probe = new TestProbe(system2)
      val serialization = SerializationExtension(system2)

      val initial = lwwRegister(ExamplePayload("foo", "bar"))
      val expected = lwwRegister(ExamplePayload("bar", "foo"))

      serialization.deserialize(serialization.serialize(initial).get, classOf[LWWRegister[_]]).get should be(expected)
    }
  }
}
