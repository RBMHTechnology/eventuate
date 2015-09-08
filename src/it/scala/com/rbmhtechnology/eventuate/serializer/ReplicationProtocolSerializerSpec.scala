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

import akka.actor.ActorRef
import akka.testkit.TestProbe

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.ReplicationProtocol._

import org.scalatest._

object ReplicationProtocolSerializerSpec {
  import ReplicationFilterSerializerSpec._

  val getReplicationEndpointInfoSuccess =
    GetReplicationEndpointInfoSuccess(ReplicationEndpointInfo("A", Set("B", "C")))

  val replicationRead1 =
    ReplicationRead(17L, 10, filter1(), "A")

  val replicationRead2 =
    ReplicationRead(17L, 10, filter3, "A")

  val replicationReadSuccess =
    ReplicationReadSuccess(List(
      DurableEvent("a", "r1"),
      DurableEvent("b", "r2")), 27L, "B")

  val replicationReadFailure =
    ReplicationReadFailure("test", "B")

  def subscribeReplicator(replicator: ActorRef) =
    SubscribeReplicator("A", "B", replicator, filter1())
}

class ReplicationProtocolSerializerSpec extends WordSpec with Matchers with BeforeAndAfterAll {
  import ReplicationProtocolSerializerSpec._

  val support = new SerializerSpecSupport(
    ReplicationConfig.create(2552),
    ReplicationConfig.create(2553))

  override def afterAll(): Unit =
    support.shutdown()

  import support._

  "A ReplicationProtocolSerializer" must {
    "serialize GetReplicationEndpointInfo messages" in {
      serialization1.deserialize(serialization1.serialize(GetReplicationEndpointInfo).get, GetReplicationEndpointInfo.getClass).get should be(GetReplicationEndpointInfo)
    }
    "serialize GetReplicationEndpointInfoSuccess messages" in {
      serialization1.deserialize(serialization1.serialize(getReplicationEndpointInfoSuccess).get, classOf[GetReplicationEndpointInfoSuccess]).get should be(getReplicationEndpointInfoSuccess)
    }
    "serialize ReplicationRead messages" in {
      serialization1.deserialize(serialization1.serialize(replicationRead1).get, classOf[ReplicationRead]).get should be(replicationRead1)
      serialization1.deserialize(serialization1.serialize(replicationRead2).get, classOf[ReplicationRead]).get should be(replicationRead2)
    }
    "serialize ReplicationReadSuccess messages" in {
      serialization1.deserialize(serialization1.serialize(replicationReadSuccess).get, classOf[ReplicationReadSuccess]).get should be(replicationReadSuccess)
    }
    "serialize ReplicationReadFailure messages" in {
      serialization1.deserialize(serialization1.serialize(replicationReadFailure).get, classOf[ReplicationReadFailure]).get should be(replicationReadFailure)
    }
    "serialize SubscribeReplicator messages" in {
      val replicator = new TestProbe(system1).ref
      serialization1.deserialize(serialization1.serialize(subscribeReplicator(replicator)).get, classOf[SubscribeReplicator]).get should be(subscribeReplicator(replicator))
    }
    "support remoting of GetReplicationEndpointInfo messages" in {
      senderActor ! GetReplicationEndpointInfo
      receiverProbe.expectMsg(GetReplicationEndpointInfo)
    }
    "support remoting of GetReplicationEndpointInfoSuccess messages" in {
      senderActor ! getReplicationEndpointInfoSuccess
      receiverProbe.expectMsg(getReplicationEndpointInfoSuccess)
    }
    "support remoting of ReplicationRead messages" in {
      senderActor ! replicationRead1
      receiverProbe.expectMsg(replicationRead1)
      senderActor ! replicationRead2
      receiverProbe.expectMsg(replicationRead2)
    }
    "support remoting of ReplicationReadSuccess messages" in {
      senderActor ! replicationReadSuccess
      receiverProbe.expectMsg(replicationReadSuccess)
    }
    "support remoting of ReplicationReadFailure messages" in {
      senderActor ! replicationReadFailure
      receiverProbe.expectMsg(replicationReadFailure)
    }
    "support remoting of SubscribeReplicator messages" in {
      val replicator = new TestProbe(system2).ref
      senderActor ! subscribeReplicator(replicator)
      receiverProbe.expectMsg(subscribeReplicator(replicator))
    }
  }
}
