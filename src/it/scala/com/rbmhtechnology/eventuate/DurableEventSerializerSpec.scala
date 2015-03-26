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
import akka.serialization.{SerializationExtension, Serializer}
import com.typesafe.config.ConfigFactory
import org.scalatest._

import scala.concurrent.duration._

object DurableEventSerializerSpec {
  val config = ConfigFactory.parseString(
    """
      |akka.actor.serializers {
      |  eventuate-test = "com.rbmhtechnology.eventuate.DurableEventSerializerSpec$ExamplePayloadSerializer"
      |}
      |
      |akka.actor.serialization-bindings {
      |  "com.rbmhtechnology.eventuate.DurableEventSerializerSpec$ExamplePayload" = eventuate-test
      |}
      |
      |akka.remote.netty.tcp.port=2553
    """.stripMargin)

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
}

class DurableEventSerializerSpec extends WordSpec with Matchers with BeforeAndAfterAll {
  import com.rbmhtechnology.eventuate.DurableEventSerializerSpec._

  private val event = DurableEvent(
    payload = ExamplePayload("foo", "bar"),
    systemTimestamp = 1L,
    vectorTimestamp = VectorTime("A" -> 1L, "B" -> 2L),
    emitterReplicaId = "r",
    emitterAggregateId = Some("a"),
    customRoutingDestinations = Set("x", "y"),
    sourceLogId = "X",
    targetLogId = "Y",
    sourceLogSequenceNr = 17L,
    targetLogSequenceNr = 18L)

  private var defaultSystem: ActorSystem = _
  private var customSystem: ActorSystem = _

  override protected def beforeAll(): Unit = {
    defaultSystem = ActorSystem("test-default")
    customSystem = ActorSystem("test-custom", config)

  }

  override protected def afterAll(): Unit = {
    defaultSystem.shutdown()
    customSystem.shutdown()

    defaultSystem.awaitTermination(10.seconds)
    customSystem.awaitTermination(10.seconds)
  }

  "A DurableEventSerializer" must {
    "support default payload serialization" in {
      val serialization = SerializationExtension(defaultSystem)
      val expected = event

      serialization.deserialize(serialization.serialize(event).get, classOf[DurableEvent]).get should be(expected)
    }
    "support custom payload serialization" in {
      val serialization = SerializationExtension(customSystem)
      val expected = event.copy(ExamplePayload("bar", "foo"))

      serialization.deserialize(serialization.serialize(event).get, classOf[DurableEvent]).get should be(expected)
    }
  }
}
