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
import akka.serialization.Serializer

import com.rbmhtechnology.eventuate.ReplicationFilter.AndFilter
import com.rbmhtechnology.eventuate.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.ReplicationFilter.OrFilter

import com.rbmhtechnology.eventuate._

import org.scalatest._

object ReplicationFilterSerializerSpec {
  case class ProcessIdFilter(processId: String) extends ReplicationFilter {
    override def apply(event: DurableEvent): Boolean = event.processId == processId
  }

  case class ExampleFilter(num: Int) extends ReplicationFilter {
    def apply(event: DurableEvent): Boolean = num == 1
  }

  /**
   * Increments `ExampleFilter.num` by 1 during deserialization.
   */
  class ExampleFilterSerializer(system: ExtendedActorSystem) extends Serializer {
    val ExampleFilterClass = classOf[ExampleFilter]

    override def identifier: Int = 44086
    override def includeManifest: Boolean = true

    override def toBinary(o: AnyRef): Array[Byte] = o match {
      case ExampleFilter(num) => num.toString.getBytes("UTF-8")
    }

    override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest.get match {
      case ExampleFilterClass => ExampleFilter(new String(bytes, "UTF-8").toInt + 1)
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

  val config =
    """
      |akka.actor.serializers {
      |  eventuate-test = "com.rbmhtechnology.eventuate.serializer.ReplicationFilterSerializerSpec$ExampleFilterSerializer"
      |}
      |akka.actor.serialization-bindings {
      |  "com.rbmhtechnology.eventuate.serializer.ReplicationFilterSerializerSpec$ExampleFilter" = eventuate-test
      |}
    """.stripMargin

  val support = new SerializerSpecSupport(
    ReplicationConfig.create(2552),
    ReplicationConfig.create(2553, config))

  override def afterAll(): Unit =
    support.shutdown()

  import support._

  "A ReplicationFilterSerializer" must {
    "serialize replication filter trees with an and-filter root" in {
      serialization1.deserialize(serialization1.serialize(filter1()).get, classOf[AndFilter]).get should be(filter1())
    }
    "serialize replication filter trees with an or-filter root" in {
      serialization1.deserialize(serialization1.serialize(filter2()).get, classOf[OrFilter]).get should be(filter2())
    }
    "serialize NoFilter" in {
      serialization1.deserialize(serialization1.serialize(NoFilter).get, NoFilter.getClass).get should be(NoFilter)
    }
    "serialize custom filters" in {
      serialization1.deserialize(serialization1.serialize(filter3).get, classOf[ProcessIdFilter]).get should be(filter3)
      serialization2.deserialize(serialization2.serialize(ExampleFilter(1)).get, classOf[ExampleFilter]).get should be(ExampleFilter(2))
    }
    "serialize replication filter trees with an and-filter root and a custom leaf" in {
      serialization2.deserialize(serialization2.serialize(filter1(ExampleFilter(1))).get, classOf[AndFilter]).get should be(filter1(ExampleFilter(2)))
    }
    "serialize replication filter trees with an or-filter root and a custom leaf" in {
      serialization2.deserialize(serialization2.serialize(filter2(ExampleFilter(1))).get, classOf[OrFilter]).get should be(filter2(ExampleFilter(2)))
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
