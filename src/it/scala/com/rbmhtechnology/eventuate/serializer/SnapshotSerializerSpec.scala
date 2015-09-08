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

import akka.actor.ActorPath
import akka.serialization.SerializationExtension
import akka.testkit.TestProbe

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.ConfirmedDelivery.DeliveryAttempt
import com.rbmhtechnology.eventuate.serializer.DurableEventSerializerSpec.{event, ExamplePayload}

import org.scalatest._

object SnapshotSerializerSpec {
  implicit class ConcurrentVersionsTreeHelper(tree: ConcurrentVersionsTree[ExamplePayload, String]) {
    def nodeTuples = tree.nodes.map { node => (node.versioned, node.rejected) }
  }

  def last(payload: Any) =
    event.copy(payload = payload)

  def unconfirmed(payload: Any, destination: ActorPath) = Vector(
    DeliveryAttempt("3", payload, destination),
    DeliveryAttempt("4", payload, destination))

  def snapshot(payload: Any, destination: ActorPath) =
    Snapshot(payload, "x", last(payload), vectorTime(17, 18), unconfirmed(payload, destination))

  def vectorTime(t1: Long, t2: Long): VectorTime =
    VectorTime("p1" -> t1, "p2" -> t2)
}

class SnapshotSerializerSpec extends WordSpec with Matchers with BeforeAndAfterAll {
  import DurableEventSerializerSpec.ExamplePayload
  import SnapshotSerializerSpec._

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

  "A SnapshotSerializer" must {
    "support snapshot serialization with default payload serialization" in {
      val probe = new TestProbe(system1)
      val serialization = SerializationExtension(system1)

      val initial = snapshot(ExamplePayload("foo", "bar"), probe.ref.path)
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[Snapshot]).get should be(expected)
    }
    "support snapshot serialization with custom payload serialization" in {
      val probe = new TestProbe(system2)
      val serialization = SerializationExtension(system2)

      val initial = snapshot(ExamplePayload("foo", "bar"), probe.ref.path)
      val expected = snapshot(ExamplePayload("bar", "foo"), probe.ref.path)

      serialization.deserialize(serialization.serialize(initial).get, classOf[Snapshot]).get should be(expected)
    }
    "support ConcurrentVersionsTree serialization with default node payload serialization" in {
      val serialization = SerializationExtension(system1)
      val initial = ConcurrentVersionsTree[ExamplePayload, String](ExamplePayload("a", "x"))((s, a) => s.copy(foo = a))
        .update("b", vectorTime(1, 0), "cb")
        .update("c", vectorTime(2, 0), "cc")
        .update("d", vectorTime(1, 1), "cd")
        .resolve(vectorTime(2, 0), vectorTime(3, 1))
      val expected = initial
      val actual = serialization.deserialize(serialization.serialize(initial).get, classOf[ConcurrentVersionsTree[ExamplePayload, String]]).get

      actual.nodeTuples should be(expected.nodeTuples)
    }
    "support ConcurrentVersionsTree serialization with custom node payload serialization" in {
      val serialization = SerializationExtension(system2)
      val initial = ConcurrentVersionsTree[ExamplePayload, String](ExamplePayload("a", "x"))((s, a) => s.copy(foo = a))
        .update("b", vectorTime(1, 0), "cb")
        .update("c", vectorTime(2, 0), "cc")
        .update("d", vectorTime(1, 1), "cd")
        .resolve(vectorTime(2, 0), vectorTime(3, 1))
      val expected = ConcurrentVersionsTree[ExamplePayload, String](ExamplePayload("x", "a"))((s, a) => s.copy(bar = a))
        .update("b", vectorTime(1, 0), "cb")
        .update("c", vectorTime(2, 0), "cc")
        .update("d", vectorTime(1, 1), "cd")
        .resolve(vectorTime(2, 0), vectorTime(3, 1))
      val actual = serialization.deserialize(serialization.serialize(initial).get, classOf[ConcurrentVersionsTree[ExamplePayload, String]]).get

      actual.nodeTuples should be(expected.nodeTuples)
    }
  }
}
