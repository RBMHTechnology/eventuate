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

package com.rbmhtechnology.eventuate.serializer

import akka.actor._
import akka.serialization.SerializerWithStringManifest
import akka.serialization.Serializer
import com.rbmhtechnology.eventuate._
import com.typesafe.config.ConfigFactory
import org.scalatest._

object DurableEventSerializerSpec {
  case class ExamplePayload(foo: String, bar: String)

  val serializerConfig = ConfigFactory.parseString(
    """
      |akka.actor.serializers {
      |  eventuate-test = "com.rbmhtechnology.eventuate.serializer.DurableEventSerializerSpec$ExamplePayloadSerializer"
      |}
      |akka.actor.serialization-bindings {
      |  "com.rbmhtechnology.eventuate.serializer.DurableEventSerializerSpec$ExamplePayload" = eventuate-test
      |}
    """.stripMargin)

  val serializerWithStringManifestConfig = ConfigFactory.parseString(
    """
      |akka.actor.serializers {
      |  eventuate-test = "com.rbmhtechnology.eventuate.serializer.DurableEventSerializerSpec$ExamplePayloadSerializerWithStringManifest"
      |}
      |akka.actor.serialization-bindings {
      |  "com.rbmhtechnology.eventuate.serializer.DurableEventSerializerSpec$ExamplePayload" = eventuate-test
      |}
    """.stripMargin)

  val binaryPayloadSerializerConfig = ConfigFactory.parseString(
    s"akka.actor.serializers.eventuate-durable-event = ${classOf[DurableEventSerializerWithBinaryPayload].getName}")

  /**
   * Swaps `foo` and `bar` of `ExamplePayload`.
   */
  trait SwappingExamplePayloadSerializer {
    def toBinary(o: AnyRef): Array[Byte] = o match {
      case ExamplePayload(foo, bar) => s"${foo}-${bar}".getBytes("UTF-8")
    }

    def _fromBinary(bytes: Array[Byte]): AnyRef = {
      val s = new String(bytes, "UTF-8").split("-")
      ExamplePayload(s(1), s(0))
    }
  }

  class ExamplePayloadSerializer(system: ExtendedActorSystem) extends Serializer with SwappingExamplePayloadSerializer {
    val ExamplePayloadClass = classOf[ExamplePayload]

    override def identifier: Int = ExamplePayloadSerializer.serializerId
    override def includeManifest: Boolean = true

    override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest.get match {
      case ExamplePayloadClass => _fromBinary(bytes)
    }
  }

  object ExamplePayloadSerializer {
    val serializerId = 44085
  }

  class ExamplePayloadSerializerWithStringManifest(system: ExtendedActorSystem) extends SerializerWithStringManifest with SwappingExamplePayloadSerializer {
    import ExamplePayloadSerializerWithStringManifest._

    override def identifier: Int = serializerId

    override def manifest(o: AnyRef) = StringManifest

    override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
      case StringManifest => _fromBinary(bytes)
    }
  }

  object ExamplePayloadSerializerWithStringManifest {
    val serializerId = 44084
    val StringManifest = "manifest"
  }

  val event = DurableEvent(
    payload = ExamplePayload("foo", "bar"),
    emitterId = "r",
    emitterAggregateId = Some("a"),
    customDestinationAggregateIds = Set("x", "y"),
    systemTimestamp = 2L,
    vectorTimestamp = VectorTime("p1" -> 1L, "p2" -> 2L),
    processId = "p4",
    localLogId = "p3",
    localSequenceNr = 17L,
    deliveryId = Some("x"),
    persistOnEventSequenceNr = Some(12L))
}

class DurableEventSerializerSpec extends WordSpec with Matchers with Inside with BeforeAndAfterAll {
  import DurableEventSerializerSpec._

  val context = new SerializationContext(
    MultiLocationConfig.create(),
    MultiLocationConfig.create(customConfig = serializerConfig),
    MultiLocationConfig.create(customConfig = serializerWithStringManifestConfig),
    MultiLocationConfig.create(customConfig = binaryPayloadSerializerConfig))

  val Seq(defaultSerialization, swappingSerialization, swappingSerializationWithStringManigest, binaryPayloadSerialization) = context.serializations

  override def afterAll(): Unit =
    context.shutdown()

  "A DurableEventSerializer" must {
    "support default payload serialization" in {
      val expected = event

      defaultSerialization.deserialize(defaultSerialization.serialize(event).get, classOf[DurableEvent]).get should be(expected)
    }
    "support custom payload serialization" in Seq(swappingSerialization, swappingSerializationWithStringManigest).foreach { serialization =>
      val expected = event.copy(ExamplePayload("bar", "foo"))

      serialization.deserialize(serialization.serialize(event).get, classOf[DurableEvent]).get should be(expected)
    }
  }

  "A DurableEventSerializerWithBinaryPayload" must {
    "deserialize from a custom serializer into a BinaryPayload" in {
      val bytes = swappingSerialization.serialize(event).get

      inside(binaryPayloadSerialization.deserialize(bytes, classOf[DurableEvent]).get.payload) {
        case BinaryPayload(_, serializerId, manifest, isStringManifest) =>
          serializerId should be(ExamplePayloadSerializer.serializerId)
          manifest should be(Some(classOf[ExamplePayload].getName))
          isStringManifest should be(false)
      }
    }
    "deserialize from a custom serializer with string manifest into a BinaryPayload" in {
      val bytes = swappingSerializationWithStringManigest.serialize(event).get

      inside(binaryPayloadSerialization.deserialize(bytes, classOf[DurableEvent]).get.payload) {
        case BinaryPayload(_, serializerId, manifest, isStringManifest) =>
          serializerId should be(ExamplePayloadSerializerWithStringManifest.serializerId)
          manifest should be(Some(ExamplePayloadSerializerWithStringManifest.StringManifest))
          isStringManifest should be(true)
      }
    }
    "serialize into custom source serializer compatible byte array" in Seq(swappingSerialization, swappingSerializationWithStringManigest).foreach { serialization =>
      val bytes = serialization.serialize(event).get

      val binaryPayloadBytes = binaryPayloadSerialization.serialize(binaryPayloadSerialization.deserialize(bytes, classOf[DurableEvent]).get).get

      serialization.deserialize(binaryPayloadBytes, classOf[DurableEvent]).get should be(event.copy(ExamplePayload("bar", "foo")))
    }
  }
}
