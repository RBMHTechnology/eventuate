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

package com.rbmhtechnology.eventuate.serializer

import akka.actor._
import akka.serialization.{SerializationExtension, Serializer}

import com.rbmhtechnology.eventuate._

import org.scalatest._

object DurableEventSerializerSpec {
  case class ExamplePayload(foo: String, bar: String)

  /**
   * Swaps `foo` and `bar` of `ExamplePayload`.
   */
  class ExamplePayloadSerializer(system: ExtendedActorSystem) extends Serializer {
    val ExamplePayloadClass = classOf[ExamplePayload]

    override def identifier: Int = 44085
    override def includeManifest: Boolean = true

    override def toBinary(o: AnyRef): Array[Byte] = o match {
      case ExamplePayload(foo, bar) => s"${foo}-${bar}".getBytes("UTF-8")
    }

    override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest.get match {
      case ExamplePayloadClass =>
        val s = new String(bytes, "UTF-8").split("-")
        ExamplePayload(s(1), s(0))
    }
  }

  val event = DurableEvent(
    payload = ExamplePayload("foo", "bar"),
    emitterId = "r",
    emitterAggregateId = Some("a"),
    customDestinationAggregateIds = Set("x", "y"),
    systemTimestamp = 2L,
    vectorTimestamp = VectorTime("p1" -> 1L, "p2" -> 2L),
    processId = "p4",
    sourceLogId = "p3",
    targetLogId = "p4",
    sourceLogSequenceNr = 17L,
    targetLogSequenceNr = 18L,
    sourceLogReadPosition = 22L)
}

class DurableEventSerializerSpec extends WordSpec with Matchers with BeforeAndAfterAll {
  import DurableEventSerializerSpec._

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

  "A DurableEventSerializer" must {
    "support default payload serialization" in {
      val serialization = SerializationExtension(system1)
      val expected = event

      serialization.deserialize(serialization.serialize(event).get, classOf[DurableEvent]).get should be(expected)
    }
    "support custom payload serialization" in {
      val serialization = SerializationExtension(system2)
      val expected = event.copy(ExamplePayload("bar", "foo"))

      serialization.deserialize(serialization.serialize(event).get, classOf[DurableEvent]).get should be(expected)
    }
  }
}
