/*
 * Copyright (C) 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
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
import akka.actor.Props
import akka.testkit.TestProbe

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.serializer.SerializationContext.ReceiverActor
import com.rbmhtechnology.eventuate.serializer.SerializationContext.SenderActor

import org.scalatest._

object ReplicationProtocolSerializerSpec {
  import ReplicationFilterSerializerSpec._

  val getReplicationEndpointInfoSuccess =
    GetReplicationEndpointInfoSuccess(ReplicationEndpointInfo("A", Set(LogInfo("B", 0l), LogInfo("C", 10l))))

  val replicationReadSuccess =
    ReplicationReadSuccess(List(
      DurableEvent("a", "r1"),
      DurableEvent("b", "r2")), 27L, "B", VectorTime("X" -> 4L))

  val replicationReadFailure =
    ReplicationReadFailure("test", "B")

  def replicationRead1(r: ActorRef) =
    ReplicationRead(17L, 10, filter1(), "A", r, VectorTime("X" -> 12L))

  def replicationRead2(r: ActorRef) =
    ReplicationRead(18L, 11, filter3, "B", r, VectorTime("Y" -> 13L))

  def replicationReadEnvelope(r: ReplicationRead): ReplicationReadEnvelope =
    ReplicationReadEnvelope(r, "X", "myapp", ApplicationVersion(2, 1))

  val replicationReadEnvelopeIncompatible: ReplicationReadEnvelopeIncompatible =
    ReplicationReadEnvelopeIncompatible(ApplicationVersion(2, 1))
}

class ReplicationProtocolSerializerSpec extends WordSpec with Matchers with BeforeAndAfterAll {
  import ReplicationProtocolSerializerSpec._

  val context = new SerializationContext(
    LocationConfig.create(),
    LocationConfig.create())

  override def afterAll(): Unit =
    context.shutdown()

  import context._

  val receiverProbe = new TestProbe(systems(1))
  val receiverActor = systems(1).actorOf(Props(new ReceiverActor(receiverProbe.ref)), "receiver")
  val senderActor = systems(0).actorOf(Props(new SenderActor(systems(0).actorSelection(s"akka.tcp://test-system-2@127.0.0.1:${ports(1)}/user/receiver"))))

  val dl1 = systems(0).deadLetters
  val dl2 = systems(1).deadLetters

  "A ReplicationProtocolSerializer" must {
    "serialize GetReplicationEndpointInfo messages" in {
      serializations(0).deserialize(serializations(0).serialize(GetReplicationEndpointInfo).get, GetReplicationEndpointInfo.getClass).get should be(GetReplicationEndpointInfo)
    }
    "serialize GetReplicationEndpointInfoSuccess messages" in {
      serializations(0).deserialize(serializations(0).serialize(getReplicationEndpointInfoSuccess).get, classOf[GetReplicationEndpointInfoSuccess]).get should be(getReplicationEndpointInfoSuccess)
    }
    "serialize ReplicationReadEnvelope messages" in {
      serializations(0).deserialize(serializations(0).serialize(replicationReadEnvelope(replicationRead1(dl1))).get, classOf[ReplicationReadEnvelope]).get should be(replicationReadEnvelope(replicationRead1(dl1)))
      serializations(0).deserialize(serializations(0).serialize(replicationReadEnvelope(replicationRead2(dl2))).get, classOf[ReplicationReadEnvelope]).get should be(replicationReadEnvelope(replicationRead2(dl2)))
    }
    "serialize ReplicationReadEnvelopeIncompatible messages" in {
      serializations(0).deserialize(serializations(0).serialize(replicationReadEnvelopeIncompatible).get, classOf[ReplicationReadEnvelopeIncompatible]).get should be(replicationReadEnvelopeIncompatible)
    }
    "serialize ReplicationRead messages" in {
      serializations(0).deserialize(serializations(0).serialize(replicationRead1(dl1)).get, classOf[ReplicationRead]).get should be(replicationRead1(dl1))
      serializations(0).deserialize(serializations(0).serialize(replicationRead2(dl2)).get, classOf[ReplicationRead]).get should be(replicationRead2(dl2))
    }
    "serialize ReplicationReadSuccess messages" in {
      serializations(0).deserialize(serializations(0).serialize(replicationReadSuccess).get, classOf[ReplicationReadSuccess]).get should be(replicationReadSuccess)
    }
    "serialize ReplicationReadFailure messages" in {
      serializations(0).deserialize(serializations(0).serialize(replicationReadFailure).get, classOf[ReplicationReadFailure]).get should be(replicationReadFailure)
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
      senderActor ! replicationRead1(dl1)
      receiverProbe.expectMsgPF() { case r: ReplicationRead => r.copy(replicator = null) should be(replicationRead1(null)) }
      senderActor ! replicationRead2(dl1)
      receiverProbe.expectMsgPF() { case r: ReplicationRead => r.copy(replicator = null) should be(replicationRead2(null)) }
    }
    "support remoting of ReplicationReadSuccess messages" in {
      senderActor ! replicationReadSuccess
      receiverProbe.expectMsg(replicationReadSuccess)
    }
    "support remoting of ReplicationReadFailure messages" in {
      senderActor ! replicationReadFailure
      receiverProbe.expectMsg(replicationReadFailure)
    }
    "support remoting of ReplicationReadEnvelopeIncompatible messages" in {
      senderActor ! replicationReadEnvelopeIncompatible
      receiverProbe.expectMsg(replicationReadEnvelopeIncompatible)
    }
  }
}
