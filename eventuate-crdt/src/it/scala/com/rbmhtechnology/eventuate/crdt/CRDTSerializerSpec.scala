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

package com.rbmhtechnology.eventuate.crdt

import akka.serialization.SerializationExtension
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.crdt.CRDTService.ValueUpdated
import com.rbmhtechnology.eventuate.serializer.DurableEventSerializerSpec._
import com.rbmhtechnology.eventuate.serializer.SerializationContext
import org.scalatest._

object CRDTSerializerSpec {
  def orSet(payload: ExamplePayload) =
    ORSet[ExamplePayload].add(payload, VectorTime("s" -> 17L))

  def orCart(key: ExamplePayload) =
    ORCart[ExamplePayload].add(key, 3, VectorTime("s" -> 17L))

  def orCartEntry(key: ExamplePayload) =
    ORCartEntry(key, 4)

  def rgArray(value: ExamplePayload) =
    RGArray[ExamplePayload].insert(value, Position(1, "s"))

  def rgArrayVertex(value: ExamplePayload) =
    Vertex[ExamplePayload](value, Position(20, "s"), isTombstoned = false)

  def mvRegister(payload: ExamplePayload) =
    MVRegister[ExamplePayload].assign(payload, VectorTime("s" -> 18L), 18, "e1")

  def lwwRegister(payload: ExamplePayload) =
    LWWRegister[ExamplePayload].assign(payload, VectorTime("s" -> 19L), 19, "e2")

  def removeOp(payload: ExamplePayload): RemoveOp =
    RemoveOp(payload, Set(VectorTime("s" -> 19L), VectorTime("t" -> 20L)))

  def insertOp(payload: ExamplePayload): InsertOp =
    InsertOp(Position(19, "s"), payload, Some(20))
}

class CRDTSerializerSpec extends WordSpec with Matchers with BeforeAndAfterAll {
  import CRDTSerializerSpec._

  val context = new SerializationContext(
    MultiLocationConfig.create(),
    MultiLocationConfig.create(customConfig = serializerConfig),
    MultiLocationConfig.create(customConfig = serializerWithStringManifestConfig))

  override def afterAll(): Unit =
    context.shutdown()

  import context._

  "A CRDTSerializer" must {
    "support ORSet serialization with default payload serialization" in {
      val serialization = SerializationExtension(systems(0))

      val initial = orSet(ExamplePayload("foo", "bar"))
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[ORSet[_]]).get should be(expected)
    }
    "support ORCart serialization with default key serialization" in {
      val serialization = SerializationExtension(systems(0))

      val initial = orCart(ExamplePayload("foo", "bar"))
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[ORCart[_]]).get should be(expected)
    }
    "support RGArray serialization with default key serialization" in {
      val serialization = SerializationExtension(systems(0))

      val initial = rgArray(ExamplePayload("foo", "bar"))
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[RGArray[_]]).get should be(expected)
    }
    "support ORCartEntry serialization with default key serialization" in {
      val serialization = SerializationExtension(systems(0))

      val initial = orCartEntry(ExamplePayload("foo", "bar"))
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[ORCartEntry[_]]).get should be(expected)
    }
    "support RGArray Vertex serialization with default value serialization" in {
      val serialization = SerializationExtension(systems(0))

      val initial = rgArrayVertex(ExamplePayload("foo", "bar"))
      val expected = rgArrayVertex(ExamplePayload("bar", "foo"))

      serialization.deserialize(serialization.serialize(initial).get, classOf[Vertex[_]]).get should be(expected)
    }
    "support ORSet serialization with custom payload serialization" in serializations.tail.foreach { serialization =>
      val initial = orSet(ExamplePayload("foo", "bar"))
      val expected = orSet(ExamplePayload("bar", "foo"))

      serialization.deserialize(serialization.serialize(initial).get, classOf[ORSet[_]]).get should be(expected)
    }
    "support ORCart serialization with custom key serialization" in serializations.tail.foreach { serialization =>
      val initial = orCart(ExamplePayload("foo", "bar"))
      val expected = orCart(ExamplePayload("bar", "foo"))

      serialization.deserialize(serialization.serialize(initial).get, classOf[ORCart[_]]).get should be(expected)
    }
    "support RGArray serialization with default key serialization" in serializations.tail.foreach { serialization =>
      val initial = rgArray(ExamplePayload("foo", "bar"))
      val expected = orCart(ExamplePayload("foo", "bar"))

      serialization.deserialize(serialization.serialize(initial).get, classOf[RGArray[_]]).get should be(expected)
    }
    "support ORCartEntry serialization with custom key serialization" in serializations.tail.foreach { serialization =>
      val initial = orCartEntry(ExamplePayload("foo", "bar"))
      val expected = orCartEntry(ExamplePayload("bar", "foo"))

      serialization.deserialize(serialization.serialize(initial).get, classOf[ORCartEntry[_]]).get should be(expected)
    }
    "support RGArray Vertex serialization with custom value serialization" in serializations.tail.foreach { serialization =>
      val initial = rgArrayVertex(ExamplePayload("foo", "bar"))
      val expected = rgArrayVertex(ExamplePayload("bar", "foo"))

      serialization.deserialize(serialization.serialize(initial).get, classOf[Vertex[_]]).get should be(expected)
    }
    "support MVRegister serialization with default payload serialization" in {
      val initial = mvRegister(ExamplePayload("foo", "bar"))
      val expected = initial

      serializations(0).deserialize(serializations(0).serialize(initial).get, classOf[MVRegister[_]]).get should be(expected)
    }
    "support MVRegister serialization with custom payload serialization" in serializations.tail.foreach { serialization =>
      val initial = mvRegister(ExamplePayload("foo", "bar"))
      val expected = mvRegister(ExamplePayload("bar", "foo"))

      serialization.deserialize(serialization.serialize(initial).get, classOf[MVRegister[_]]).get should be(expected)
    }
    "support LWWRegister serialization with default payload serialization" in {
      val initial = lwwRegister(ExamplePayload("foo", "bar"))
      val expected = initial

      serializations(0).deserialize(serializations(0).serialize(initial).get, classOf[LWWRegister[_]]).get should be(expected)
    }
    "support LWWRegister serialization with custom payload serialization" in serializations.tail.foreach { serialization =>
      val initial = lwwRegister(ExamplePayload("foo", "bar"))
      val expected = lwwRegister(ExamplePayload("bar", "foo"))

      serialization.deserialize(serialization.serialize(initial).get, classOf[LWWRegister[_]]).get should be(expected)
    }
    "support UpdateOp serialization with default payload serialization" in {
      val serialization = SerializationExtension(systems(0))

      val initial = UpdateOp(17)
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[UpdateOp]).get should be(expected)
    }
    "support ValueUpdated serialization with default payload serialization" in {
      val serialization = SerializationExtension(systems(0))

      val initial = ValueUpdated(AssignOp(ExamplePayload("foo", "bar")))
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[ValueUpdated]).get should be(expected)
    }
    "support ValueUpdated serialization with custom payload serialization" in serializations.tail.foreach { serialization =>
      val initial = ValueUpdated(AssignOp(ExamplePayload("foo", "bar")))
      val expected = ValueUpdated(AssignOp(ExamplePayload("bar", "foo")))

      serialization.deserialize(serialization.serialize(initial).get, classOf[ValueUpdated]).get should be(expected)
    }
    "support SetOp serialization with default payload serialization" in {
      val serialization = SerializationExtension(systems(0))

      val initial = AssignOp(ExamplePayload("foo", "bar"))
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[AssignOp]).get should be(expected)
    }
    "support SetOp serialization with custom payload serialization" in serializations.tail.foreach { serialization =>
      val initial = AssignOp(ExamplePayload("foo", "bar"))
      val expected = AssignOp(ExamplePayload("bar", "foo"))

      serialization.deserialize(serialization.serialize(initial).get, classOf[AssignOp]).get should be(expected)
    }
    "support AddOp serialization with default payload serialization" in {
      val serialization = SerializationExtension(systems(0))

      val initial = AddOp(ExamplePayload("foo", "bar"))
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[AddOp]).get should be(expected)
    }
    "support AddOp serialization with custom payload serialization" in serializations.tail.foreach { serialization =>
      val initial = AddOp(ExamplePayload("foo", "bar"))
      val expected = AddOp(ExamplePayload("bar", "foo"))

      serialization.deserialize(serialization.serialize(initial).get, classOf[AddOp]).get should be(expected)
    }
    "support RemoveOp serialization with default payload serialization" in {
      val serialization = SerializationExtension(systems(0))

      val initial = removeOp(ExamplePayload("foo", "bar"))
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[RemoveOp]).get should be(expected)
    }
    "support RemoveOp serialization with custom payload serialization" in serializations.tail.foreach { serialization =>
      val initial = removeOp(ExamplePayload("foo", "bar"))
      val expected = removeOp(ExamplePayload("bar", "foo"))

      serialization.deserialize(serialization.serialize(initial).get, classOf[RemoveOp]).get should be(expected)
    }

    "support InsertOp serialization with default payload serialization" in {
      val serialization = SerializationExtension(systems(0))

      val initial = insertOp(ExamplePayload("foo", "bar"))
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[DeleteOp]).get should be(expected)
    }

    "support InsertOp serialization with custom payload serialization" in serializations.tail.foreach { serialization =>

      val initial = insertOp(ExamplePayload("foo", "bar"))
      val expected = insertOp(ExamplePayload("bar", "foo"))

      serialization.deserialize(serialization.serialize(initial).get, classOf[DeleteOp]).get should be(expected)
    }

    "support DeleteOp serialization" in {
      val serialization = SerializationExtension(systems(0))

      val initial = DeleteOp(Position(20, "s"))
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[DeleteOp]).get should be(expected)
    }
  }
}
