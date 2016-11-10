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

import akka.actor.ActorPath
import akka.serialization.SerializationExtension
import akka.testkit.TestProbe

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.ConfirmedDelivery._
import com.rbmhtechnology.eventuate.PersistOnEvent._
import com.rbmhtechnology.eventuate.log.EventLogClock
import com.rbmhtechnology.eventuate.serializer.DurableEventSerializerSpec.serializerConfig
import com.rbmhtechnology.eventuate.serializer.DurableEventSerializerSpec.serializerWithStringManifestConfig
import com.rbmhtechnology.eventuate.serializer.DurableEventSerializerSpec.{ event, ExamplePayload }

import org.scalatest._

object SnapshotSerializerSpec {
  implicit class ConcurrentVersionsTreeHelper(tree: ConcurrentVersionsTree[ExamplePayload, String]) {
    def nodeTuples = tree.nodes.map { node => (node.versioned, node.rejected) }
  }

  val clock =
    EventLogClock(sequenceNr = 17L, versionVector = VectorTime("A" -> 77L))

  def last(payload: Any) =
    event.copy(payload = payload)

  def deliveryAttempts(payload: Any, destination: ActorPath) = Vector(
    DeliveryAttempt("3", payload, destination),
    DeliveryAttempt("4", payload, destination))

  def persistOnEventRequests(payload: Any) = Vector(
    PersistOnEventRequest(7L, Vector(PersistOnEventInvocation(payload, Set("a"))), 17),
    PersistOnEventRequest(8L, Vector(PersistOnEventInvocation(payload, Set("b"))), 17))

  def snapshot(payload: Any, destination: ActorPath) =
    Snapshot(payload, "x", last(payload), vectorTime(17, 18), event.localSequenceNr,
      deliveryAttempts(payload, destination),
      persistOnEventRequests(payload))

  def vectorTime(t1: Long, t2: Long): VectorTime =
    VectorTime("p1" -> t1, "p2" -> t2)
}

class SnapshotSerializerSpec extends WordSpec with Matchers with BeforeAndAfterAll {
  import DurableEventSerializerSpec.ExamplePayload
  import SnapshotSerializerSpec._

  val context = new SerializationContext(
    MultiLocationConfig.create(),
    MultiLocationConfig.create(customConfig = serializerConfig),
    MultiLocationConfig.create(customConfig = serializerWithStringManifestConfig))

  override def afterAll(): Unit =
    context.shutdown()

  import context._

  "A SnapshotSerializer" must {
    "support snapshot serialization with default payload serialization" in {
      val probe = new TestProbe(systems(0))
      val serialization = SerializationExtension(systems(0))

      val initial = snapshot(ExamplePayload("foo", "bar"), probe.ref.path)
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[Snapshot]).get should be(expected)
    }
    "support snapshot serialization with custom payload serialization" in systems.tail.foreach { system =>
      val probe = new TestProbe(system)
      val serialization = SerializationExtension(system)

      val initial = snapshot(ExamplePayload("foo", "bar"), probe.ref.path)
      val expected = snapshot(ExamplePayload("bar", "foo"), probe.ref.path)

      serialization.deserialize(serialization.serialize(initial).get, classOf[Snapshot]).get should be(expected)
    }
    "support ConcurrentVersionsTree serialization with default node payload serialization" in {
      val initial = ConcurrentVersionsTree[ExamplePayload, String](ExamplePayload("a", "x"))((s, a) => s.copy(foo = a))
        .update("b", vectorTime(1, 0), 17L, "cb")
        .update("c", vectorTime(2, 0), 18L, "cc")
        .update("d", vectorTime(1, 1), 18L, "cd")
        .resolve(vectorTime(2, 0), vectorTime(3, 1))
      val expected = initial
      val actual = serializations(0).deserialize(serializations(0).serialize(initial).get, classOf[ConcurrentVersionsTree[ExamplePayload, String]]).get

      actual.nodeTuples should be(expected.nodeTuples)
    }
    "support ConcurrentVersionsTree serialization with custom node payload serialization" in serializations.tail.foreach { serialization =>
      val initial = ConcurrentVersionsTree[ExamplePayload, String](ExamplePayload("a", "x"))((s, a) => s.copy(foo = a))
        .update("b", vectorTime(1, 0), 17L, "cb")
        .update("c", vectorTime(2, 0), 18L, "cc")
        .update("d", vectorTime(1, 1), 19L, "cd")
        .resolve(vectorTime(2, 0), vectorTime(3, 1))
      val expected = ConcurrentVersionsTree[ExamplePayload, String](ExamplePayload("x", "a"))((s, a) => s.copy(bar = a))
        .update("b", vectorTime(1, 0), 17L, "cb")
        .update("c", vectorTime(2, 0), 18L, "cc")
        .update("d", vectorTime(1, 1), 19L, "cd")
        .resolve(vectorTime(2, 0), vectorTime(3, 1))
      val actual = serialization.deserialize(serialization.serialize(initial).get, classOf[ConcurrentVersionsTree[ExamplePayload, String]]).get

      actual.nodeTuples should be(expected.nodeTuples)
    }
    "support event log clock serialization" in {
      val initial = clock
      val expected = initial

      serializations(0).deserialize(serializations(0).serialize(initial).get, classOf[EventLogClock]).get should be(expected)
    }
  }
}
