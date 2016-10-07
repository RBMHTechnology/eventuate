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

package com.rbmhtechnology.eventuate.adapter.vertx

import akka.actor.ActorSystem
import akka.serialization.{Serializer, SerializerWithStringManifest}
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec
import org.scalatest.{BeforeAndAfterEach, MustMatchers, WordSpecLike}

object AkkaSerializationMessageCodecSpec {

  implicit class MessageCodecExtension[A, B](c: MessageCodec[A, B]) {
    def encodeAndDecode(o: A, b: Buffer = Buffer.buffer(), pos: Int = 0): B = {
      c.encodeToWire(b, o)
      c.decodeFromWire(pos, b)
    }
  }

  case class TypeWithDefaultSerialization(payload: String)

  class TypeWithCustomSerializer(val payload: String) {
    def canEqual(other: Any): Boolean = other.isInstanceOf[TypeWithCustomSerializer]

    override def equals(other: Any): Boolean = other match {
      case that: TypeWithCustomSerializer =>
        (that canEqual this) &&
          payload == that.payload
      case _ => false
    }
  }

  class TypeWithCustomStringManifestSerializer(val payload: String) {
    def canEqual(other: Any): Boolean = other.isInstanceOf[TypeWithCustomStringManifestSerializer]

    override def equals(other: Any): Boolean = other match {
      case that: TypeWithCustomStringManifestSerializer =>
        (that canEqual this) &&
          payload == that.payload
      case _ => false
    }
  }

  class CustomSerializer extends Serializer {
    override def identifier: Int = 11223344

    override def includeManifest: Boolean =
      true

    override def toBinary(o: AnyRef): Array[Byte] = o match {
      case c: TypeWithCustomSerializer => c.payload.getBytes
      case _ => Array.empty
    }

    override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
      case Some(c) if c == classOf[TypeWithCustomSerializer] => new TypeWithCustomSerializer(new String(bytes))
      case _ => "invalid type "
    }
  }

  class CustomStringManifestSerializer extends SerializerWithStringManifest {
    override def identifier: Int = 22334455

    override def manifest(o: AnyRef): String = o match {
      case c: TypeWithCustomStringManifestSerializer => "custom-type"
      case _ => "unknown"
    }

    override def toBinary(o: AnyRef): Array[Byte] = o match {
      case c: TypeWithCustomStringManifestSerializer => c.payload.getBytes
      case _ => Array.empty
    }

    override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
      case "custom-type" => new TypeWithCustomStringManifestSerializer(new String(bytes))
      case _ => "invalid type "
    }
  }

  val Config = ConfigFactory.parseString(
    """
      | akka.actor {
      |   serializers {
      |     custom-simple = "com.rbmhtechnology.eventuate.adapter.vertx.AkkaSerializationMessageCodecSpec$CustomSerializer"
      |     custom-stringManifest = "com.rbmhtechnology.eventuate.adapter.vertx.AkkaSerializationMessageCodecSpec$CustomStringManifestSerializer"
      |   }
      |   serialization-bindings {
      |     "com.rbmhtechnology.eventuate.adapter.vertx.AkkaSerializationMessageCodecSpec$TypeWithCustomSerializer" = custom-simple
      |     "com.rbmhtechnology.eventuate.adapter.vertx.AkkaSerializationMessageCodecSpec$TypeWithCustomStringManifestSerializer" = custom-stringManifest
      |   }
      | }
    """.stripMargin)
}

class AkkaSerializationMessageCodecSpec extends TestKit(ActorSystem("test", AkkaSerializationMessageCodecSpec.Config))
  with WordSpecLike with MustMatchers with BeforeAndAfterEach with StopSystemAfterAll {

  import AkkaSerializationMessageCodecSpec._

  var codec: MessageCodec[AnyRef, AnyRef] = _

  override def beforeEach(): Unit = {
    codec = AkkaSerializationMessageCodec("test-codec")
  }

  "An AkkaSerializationMessageCodec" must {
    "encode and decode a simple data type" in {
      codec.encodeAndDecode("test-value") mustBe "test-value"
    }
    "encode and decode a data type supported by Akka default serialization" in {
      val o = TypeWithDefaultSerialization("content")

      codec.encodeAndDecode(o) mustBe o
    }
    "encode and decode a data type configured with a custom serializer" in {
      val o = new TypeWithCustomSerializer("content")

      codec.encodeAndDecode(o) mustBe o
    }
    "encode and decode a data type configured with a custom string-manifest serializer" in {
      val o = new TypeWithCustomStringManifestSerializer("content")

      codec.encodeAndDecode(o) mustBe o
    }
    "encode and decode from the correct position in the underlying buffer" in {
      val initial = "initial".getBytes()
      val b = Buffer.buffer(initial)

      codec.encodeAndDecode("test-value", b, initial.length) mustBe "test-value"
    }
  }
}
