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

package com.rbmhtechnology.eventuate.crdt.pure

import akka.serialization.SerializationExtension
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.crdt.pure.CRDTService.ValueUpdated
import com.rbmhtechnology.eventuate.serializer.DurableEventSerializerSpec._
import com.rbmhtechnology.eventuate.serializer.SerializationContext
import org.scalatest._

object CRDTSerializerSpec {
  def crdt[A](payloads: List[Any], state: A) = {
    val log = payloads
      .zipWithIndex
      .map { case (v, i) => Versioned(v, VectorTime("s" -> i.toLong)) }.toSet
    CRDT(POLog(log), state)
  }

  def crdt[B](payload: Any, state: B) =
    CRDT(POLog(Set(Versioned(payload, VectorTime("s" -> 17L)))), state)

  def orCartEntry(key: ExamplePayload) =
    AWCartEntry(key, 4)

  def removeOp(payload: ExamplePayload): RemoveOp =
    RemoveOp(payload)
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
    "support AWSet serialization with default payload serialization" in {
      val serialization = SerializationExtension(systems(0))

      val initial = crdt(ExamplePayload("foo", "bar"), Set(ExamplePayload("pay1", "pay2")))
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[CRDT[_]]).get should be(expected)
    }
    "support AWSet serialization with custom payload serialization" in serializations.tail.foreach { serialization =>
      val initial = crdt(ExamplePayload("foo", "bar"), Set(ExamplePayload("pay1", "pay2")))
      val expected = crdt(ExamplePayload("bar", "foo"), Set(ExamplePayload("pay1", "pay2")))

      serialization.deserialize(serialization.serialize(initial).get, classOf[CRDT[_]]).get should be(expected)
    }
    "support AWCartEntry serialization with default key serialization" in {
      val serialization = SerializationExtension(systems(0))

      val initial = orCartEntry(ExamplePayload("foo", "bar"))
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[AWCartEntry[_]]).get should be(expected)
    }
    "support AWCartEntry serialization with custom key serialization" in serializations.tail.foreach { serialization =>
      val initial = orCartEntry(ExamplePayload("foo", "bar"))
      val expected = orCartEntry(ExamplePayload("bar", "foo"))

      serialization.deserialize(serialization.serialize(initial).get, classOf[AWCartEntry[_]]).get should be(expected)
    }
    "support SimpleCRDT serialization with default payload serialization" in {
      val initial = crdt(ExamplePayload("foo", "bar"), Set(AddOp(ExamplePayload("pay1", "pay2"))))
      val expected = initial

      serializations(0).deserialize(serializations(0).serialize(initial).get, classOf[CRDT[_]]).get should be(expected)
    }
    "support SimpleCRDT serialization with custom payload serialization" in serializations.tail.foreach { serialization =>
      val initial = crdt(ExamplePayload("foo", "bar"), Set(AddOp(ExamplePayload("pay1", "pay2"))))
      val expected = crdt(ExamplePayload("bar", "foo"), Set(AddOp(ExamplePayload("pay1", "pay2"))))

      serialization.deserialize(serialization.serialize(initial).get, classOf[CRDT[_]]).get should be(expected)
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
    "support Clear serialization with default payload serialization" in {
      val serialization = SerializationExtension(systems(0))

      serialization.deserialize(serialization.serialize(ClearOp).get, ClearOp.getClass).get should be(ClearOp)
    }
    "support Clear serialization with custom payload serialization" in serializations.tail.foreach { serialization =>

      serialization.deserialize(serialization.serialize(ClearOp).get, ClearOp.getClass).get should be(ClearOp)
    }
  }
}
