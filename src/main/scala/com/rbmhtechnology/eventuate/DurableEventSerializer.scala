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
import akka.serialization._

import com.google.protobuf.ByteString
import com.rbmhtechnology.eventuate.DurableEventFormats._

import scala.collection.JavaConverters._
import scala.language.existentials

/**
 * A [[https://developers.google.com/protocol-buffers/ Protocol Buffers]] based serializer for [[DurableEvent]]s.
 * Serialization of `DurableEvent`'s `payload` is delegated to a serializer that is configured with Akka's
 * [[http://doc.akka.io/docs/akka/2.3.9/scala/serialization.html serialization extension]] mechanism.
 */
class DurableEventSerializer(system: ExtendedActorSystem) extends Serializer {
  val DurableEventClass = classOf[DurableEvent]

  override def identifier: Int = 22563
  override def includeManifest: Boolean = true

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case m: DurableEvent => durableEventMessageBuilder(m).build().toByteArray
    case _               => throw new IllegalArgumentException(s"can't serialize object of type ${o.getClass}")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
    case None => durableEvent(DurableEventMessage.parseFrom(bytes))
    case Some(c) => c match {
      case DurableEventClass => durableEvent(DurableEventMessage.parseFrom(bytes))
      case _                 => throw new IllegalArgumentException(s"can't deserialize object of type ${c}")
    }
  }

  // --------------------------------------------------------------------------------
  //  toBinary helpers
  // --------------------------------------------------------------------------------

  private def durableEventMessageBuilder(durableEvent: DurableEvent): DurableEventMessage.Builder = {
    val builder = DurableEventMessage.newBuilder
    builder.setPayload(payloadMessageBuilder(durableEvent.payload.asInstanceOf[AnyRef]))
    builder.setSystemTimestamp(durableEvent.systemTimestamp)
    builder.setVectorTimestamp(vectorTimeMessageBuilder(durableEvent.vectorTimestamp))
    builder.setEmitterReplicaId(durableEvent.emitterReplicaId)
    builder.setSourceLogId(durableEvent.sourceLogId)
    builder.setTargetLogId(durableEvent.targetLogId)
    builder.setSourceLogSequenceNr(durableEvent.sourceLogSequenceNr)
    builder.setTargetLogSequenceNr(durableEvent.targetLogSequenceNr)

    durableEvent.emitterAggregateId.foreach { id =>
      builder.setEmitterAggregateId(id)
    }

    durableEvent.customRoutingDestinations.foreach { dest =>
      builder.addCustomRoutingDestinations(dest)
    }

    builder
  }

  private def payloadMessageBuilder(payload: AnyRef) = {
    val serializer = SerializationExtension(system).findSerializerFor(payload)
    val builder = PayloadMessage.newBuilder()

    if (serializer.includeManifest)
      builder.setPayloadManifest(ByteString.copyFromUtf8(payload.getClass.getName))

    builder.setPayload(ByteString.copyFrom(serializer.toBinary(payload)))
    builder.setSerializerId(serializer.identifier)
    builder
  }

  private def vectorTimeMessageBuilder(vectorTime: VectorTime): VectorTimeMessage.Builder = {
    val builder = VectorTimeMessage.newBuilder
    vectorTime.value.foreach { entry =>
      builder.addEntries(VectorTimeEntryMessage.newBuilder
        .setProcessId(entry._1)
        .setLogicalTime(entry._2))
    }
    builder
  }

  // --------------------------------------------------------------------------------
  //  fromBinary helpers
  // --------------------------------------------------------------------------------

  private def durableEvent(durableEventMessage: DurableEventMessage): DurableEvent = {
    val emitterAggregateId: Option[String] =
      if (durableEventMessage.hasEmitterAggregateId) Some(durableEventMessage.getEmitterAggregateId) else None

    val customRoutingDestinations = durableEventMessage.getCustomRoutingDestinationsList.iterator().asScala.foldLeft(Set.empty[String]) {
      case (result, dest) => result + dest
    }

    DurableEvent(
      payload = payload(durableEventMessage.getPayload),
      systemTimestamp = durableEventMessage.getSystemTimestamp,
      vectorTimestamp = vectorTime(durableEventMessage.getVectorTimestamp),
      emitterReplicaId = durableEventMessage.getEmitterReplicaId,
      emitterAggregateId = emitterAggregateId,
      customRoutingDestinations = customRoutingDestinations,
      sourceLogId = durableEventMessage.getSourceLogId,
      targetLogId = durableEventMessage.getTargetLogId,
      sourceLogSequenceNr = durableEventMessage.getSourceLogSequenceNr,
      targetLogSequenceNr = durableEventMessage.getTargetLogSequenceNr)
  }

  private def payload(payloadMessage: PayloadMessage): Any = {
    val payloadClass = if (payloadMessage.hasPayloadManifest)
      Some(system.dynamicAccess.getClassFor[AnyRef](payloadMessage.getPayloadManifest.toStringUtf8).get) else None

    SerializationExtension(system).deserialize(
      payloadMessage.getPayload.toByteArray,
      payloadMessage.getSerializerId,
      payloadClass).get
  }

  private def vectorTime(vectorTimeMessage: VectorTimeMessage): VectorTime = {
    VectorTime(vectorTimeMessage.getEntriesList.iterator.asScala.foldLeft(Map.empty[String, Long]) {
      case (result, entry) => result.updated(entry.getProcessId, entry.getLogicalTime)
    })
  }
}
