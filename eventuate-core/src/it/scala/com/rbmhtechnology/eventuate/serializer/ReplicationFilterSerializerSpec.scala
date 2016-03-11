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
import akka.serialization.Serializer
import akka.serialization.SerializerWithStringManifest
import akka.testkit.TestProbe

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.ReplicationFilter._
import com.rbmhtechnology.eventuate.serializer.SerializationContext.ReceiverActor
import com.rbmhtechnology.eventuate.serializer.SerializationContext.SenderActor
import com.typesafe.config.ConfigFactory

import org.scalatest._

object ReplicationFilterSerializerSpec {
  case class ProcessIdFilter(processId: String) extends ReplicationFilter {
    override def apply(event: DurableEvent): Boolean = event.processId == processId
  }

  case class ExampleFilter(num: Int) extends ReplicationFilter {
    def apply(event: DurableEvent): Boolean = num == 1
  }

  val serializerConfig = ConfigFactory.parseString(
    """
      |akka.actor.serializers {
      |  eventuate-test = "com.rbmhtechnology.eventuate.serializer.ReplicationFilterSerializerSpec$ExampleFilterSerializer"
      |}
      |akka.actor.serialization-bindings {
      |  "com.rbmhtechnology.eventuate.serializer.ReplicationFilterSerializerSpec$ExampleFilter" = eventuate-test
      |}
    """.stripMargin)

  val serializerWithStringManifestConfig = ConfigFactory.parseString(
    """
      |akka.actor.serializers {
      |  eventuate-test = "com.rbmhtechnology.eventuate.serializer.ReplicationFilterSerializerSpec$ExampleFilterSerializerWithStringManifest"
      |}
      |akka.actor.serialization-bindings {
      |  "com.rbmhtechnology.eventuate.serializer.ReplicationFilterSerializerSpec$ExampleFilter" = eventuate-test
      |}
    """.stripMargin)

  /**
   * Increments `ExampleFilter.num` by 1 during deserialization.
   */
  trait IncrementingExampleFilterSerializer {
    def toBinary(o: AnyRef): Array[Byte] = o match {
      case ExampleFilter(num) => num.toString.getBytes("UTF-8")
    }

    def _fromBinary(bytes: Array[Byte]): AnyRef =
      ExampleFilter(new String(bytes, "UTF-8").toInt + 1)
  }

  class ExampleFilterSerializer(system: ExtendedActorSystem) extends Serializer with IncrementingExampleFilterSerializer {
    val ExampleFilterClass = classOf[ExampleFilter]

    override def identifier: Int = 44086
    override def includeManifest: Boolean = true

    override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest.get match {
      case ExampleFilterClass => _fromBinary(bytes)
    }
  }

  val StringManifest = "manifest"

  class ExampleFilterSerializerWithStringManifest(system: ExtendedActorSystem) extends SerializerWithStringManifest with IncrementingExampleFilterSerializer {

    override def identifier: Int = 44087

    override def manifest(o: AnyRef) = StringManifest

    override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
      case StringManifest => _fromBinary(bytes)
    }
  }

  def filter1(custom: ReplicationFilter = ProcessIdFilter("d")) = AndFilter(List(
    ProcessIdFilter("a"),
    ProcessIdFilter("b"),
    NoFilter,
    OrFilter(List(
      ProcessIdFilter("c"),
      custom
    ))
  ))

  def filter2(custom: ReplicationFilter = ProcessIdFilter("d")) = OrFilter(List(
    ProcessIdFilter("a"),
    ProcessIdFilter("b"),
    NoFilter,
    AndFilter(List(
      ProcessIdFilter("c"),
      custom
    ))
  ))

  val filter3 = ProcessIdFilter("a")
}

class ReplicationFilterSerializerSpec extends WordSpec with Matchers with BeforeAndAfterAll {
  import ReplicationFilterSerializerSpec._

  val context = new SerializationContext(
    MultiLocationConfig.create(),
    MultiLocationConfig.create(customConfig = serializerConfig),
    MultiLocationConfig.create(customConfig = serializerWithStringManifestConfig))

  override def afterAll(): Unit =
    context.shutdown()

  import context._

  val receiverProbe = new TestProbe(systems(1))
  val receiverActor = systems(1).actorOf(Props(new ReceiverActor(receiverProbe.ref)), "receiver")
  val senderActor = systems(0).actorOf(Props(new SenderActor(systems(0).actorSelection(s"akka.tcp://test-system-2@127.0.0.1:${ports(1)}/user/receiver"))))

  "A ReplicationFilterSerializer" must {
    "serialize replication filter trees with an and-filter root" in {
      serializations(0).deserialize(serializations(0).serialize(filter1()).get, classOf[AndFilter]).get should be(filter1())
    }
    "serialize replication filter trees with an or-filter root" in {
      serializations(0).deserialize(serializations(0).serialize(filter2()).get, classOf[OrFilter]).get should be(filter2())
    }
    "serialize NoFilter" in {
      serializations(0).deserialize(serializations(0).serialize(NoFilter).get, NoFilter.getClass).get should be(NoFilter)
    }
    "serialize custom filters" in {
      serializations(0).deserialize(serializations(0).serialize(filter3).get, classOf[ProcessIdFilter]).get should be(filter3)
      serializations(1).deserialize(serializations(1).serialize(ExampleFilter(1)).get, classOf[ExampleFilter]).get should be(ExampleFilter(2))
    }
    "serialize replication filter trees with an and-filter root and a custom filter serialization" in serializations.tail.foreach { serialization =>
      serialization.deserialize(serialization.serialize(filter1(ExampleFilter(1))).get, classOf[AndFilter]).get should be(filter1(ExampleFilter(2)))
    }
    "serialize replication filter trees with an or-filter root and a custom filter serialization" in serializations.tail.foreach { serialization =>
      serialization.deserialize(serialization.serialize(filter2(ExampleFilter(1))).get, classOf[OrFilter]).get should be(filter2(ExampleFilter(2)))
    }
    "support remoting of replication filter trees with an and-filter root" in {
      senderActor ! filter1()
      receiverProbe.expectMsg(filter1())
    }
    "support remoting of replication filter trees with an or-filter root" in {
      senderActor ! filter2()
      receiverProbe.expectMsg(filter2())
    }
    "support remoting of exclusion filters" in {
      senderActor ! filter3
      receiverProbe.expectMsg(filter3)
    }
  }
}
