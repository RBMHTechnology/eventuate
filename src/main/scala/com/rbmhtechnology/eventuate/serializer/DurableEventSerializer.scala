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
import akka.serialization._

import com.google.protobuf.ByteString
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.serializer.DurableEventFormats._

import scala.collection.JavaConverters._
import scala.collection.immutable.VectorBuilder
import scala.language.existentials

/**
 * A [[https://developers.google.com/protocol-buffers/ Protocol Buffers]] based serializer for [[DurableEvent]]s.
 * Serialization of `DurableEvent`'s `payload` is delegated to a serializer that is configured with Akka's
 * [[http://doc.akka.io/docs/akka/2.3.9/scala/serialization.html serialization extension]] mechanism.
 */
class DurableEventSerializer(system: ExtendedActorSystem) extends Serializer {
  val DurableEventClass = classOf[DurableEvent]
  val DurableEventBatchClass = classOf[DurableEventBatch]

  override def identifier: Int = 22563
  override def includeManifest: Boolean = true

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case m: DurableEvent =>
      durableEventFormatBuilder(m).build().toByteArray
    case m: DurableEventBatch =>
      durableEventBatchFormatBuilder(m).build().toByteArray
    case _ =>
      throw new IllegalArgumentException(s"can't serialize object of type ${o.getClass}")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
    case None =>
      durableEvent(DurableEventFormat.parseFrom(bytes))
    case Some(c) => c match {
      case DurableEventClass =>
        durableEvent(DurableEventFormat.parseFrom(bytes))
      case DurableEventBatchClass =>
        durableEventBatch(DurableEventBatchFormat.parseFrom(bytes))
      case _ =>
        throw new IllegalArgumentException(s"can't deserialize object of type ${c}")
    }
  }

  // --------------------------------------------------------------------------------
  //  toBinary helpers
  // --------------------------------------------------------------------------------

  private def durableEventBatchFormatBuilder(durableEventBatch: DurableEventBatch): DurableEventBatchFormat.Builder = {
    val builder = DurableEventBatchFormat.newBuilder()
    durableEventBatch.events.foreach(event => builder.addEvents(durableEventFormatBuilder(event)))
    durableEventBatch.sourceLogId.foreach(builder.setSourceLogId)
    durableEventBatch.lastSourceLogSequenceNrRead.foreach(builder.setLastSourceLogSequenceNrRead)
    builder
  }

  private[eventuate] def durableEventFormatBuilder(durableEvent: DurableEvent): DurableEventFormat.Builder = {
    val builder = DurableEventFormat.newBuilder
    builder.setPayload(payloadFormatBuilder(durableEvent.payload.asInstanceOf[AnyRef]))
    builder.setSystemTimestamp(durableEvent.systemTimestamp)
    builder.setVectorTimestamp(vectorTimeFormatBuilder(durableEvent.vectorTimestamp))
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

  private def payloadFormatBuilder(payload: AnyRef) = {
    val serializer = SerializationExtension(system).findSerializerFor(payload)
    val builder = PayloadFormat.newBuilder()

    if (serializer.includeManifest)
      builder.setPayloadManifest(ByteString.copyFromUtf8(payload.getClass.getName))

    builder.setPayload(ByteString.copyFrom(serializer.toBinary(payload)))
    builder.setSerializerId(serializer.identifier)
    builder
  }

  private def vectorTimeFormatBuilder(vectorTime: VectorTime): VectorTimeFormat.Builder = {
    val builder = VectorTimeFormat.newBuilder
    vectorTime.value.foreach { entry =>
      builder.addEntries(VectorTimeEntryFormat.newBuilder
        .setProcessId(entry._1)
        .setLogicalTime(entry._2))
    }
    builder
  }

  // --------------------------------------------------------------------------------
  //  fromBinary helpers
  // --------------------------------------------------------------------------------

  private def durableEventBatch(durableEventBatchFormat: DurableEventBatchFormat): DurableEventBatch = {
    val builder = new VectorBuilder[DurableEvent]

    durableEventBatchFormat.getEventsList.iterator().asScala.foreach { durableEventFormat =>
      builder += durableEvent(durableEventFormat)
    }

    DurableEventBatch(
      builder.result(),
      if (durableEventBatchFormat.hasSourceLogId) Some(durableEventBatchFormat.getSourceLogId) else None,
      if (durableEventBatchFormat.hasLastSourceLogSequenceNrRead) Some(durableEventBatchFormat.getLastSourceLogSequenceNrRead) else None)
  }

  private[eventuate] def durableEvent(durableEventFormat: DurableEventFormat): DurableEvent = {
    val emitterAggregateId: Option[String] =
      if (durableEventFormat.hasEmitterAggregateId) Some(durableEventFormat.getEmitterAggregateId) else None

    val customRoutingDestinations = durableEventFormat.getCustomRoutingDestinationsList.iterator().asScala.foldLeft(Set.empty[String]) {
      case (result, dest) => result + dest
    }

    DurableEvent(
      payload = payload(durableEventFormat.getPayload),
      systemTimestamp = durableEventFormat.getSystemTimestamp,
      vectorTimestamp = vectorTime(durableEventFormat.getVectorTimestamp),
      emitterReplicaId = durableEventFormat.getEmitterReplicaId,
      emitterAggregateId = emitterAggregateId,
      customRoutingDestinations = customRoutingDestinations,
      sourceLogId = durableEventFormat.getSourceLogId,
      targetLogId = durableEventFormat.getTargetLogId,
      sourceLogSequenceNr = durableEventFormat.getSourceLogSequenceNr,
      targetLogSequenceNr = durableEventFormat.getTargetLogSequenceNr)
  }

  private def payload(payloadFormat: PayloadFormat): Any = {
    val payloadClass = if (payloadFormat.hasPayloadManifest)
      Some(system.dynamicAccess.getClassFor[AnyRef](payloadFormat.getPayloadManifest.toStringUtf8).get) else None

    SerializationExtension(system).deserialize(
      payloadFormat.getPayload.toByteArray,
      payloadFormat.getSerializerId,
      payloadClass).get
  }

  private def vectorTime(vectorTimeFormat: VectorTimeFormat): VectorTime = {
    VectorTime(vectorTimeFormat.getEntriesList.iterator.asScala.foldLeft(Map.empty[String, Long]) {
      case (result, entry) => result.updated(entry.getProcessId, entry.getLogicalTime)
    })
  }
}
