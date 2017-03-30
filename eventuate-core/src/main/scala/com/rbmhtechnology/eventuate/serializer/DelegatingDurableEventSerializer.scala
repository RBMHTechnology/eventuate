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
import akka.serialization._
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.serializer.DurableEventFormats._

import scala.collection.JavaConverters._
import scala.language.existentials

/**
 * A [[https://developers.google.com/protocol-buffers/ Protocol Buffers]] based serializer for [[DurableEvent]]s.
 * Serialization of `DurableEvent`'s `payload` is delegated to a serializer that is configured with Akka's
 * [[http://doc.akka.io/docs/akka/2.3.9/scala/serialization.html serialization extension]] mechanism.
 *
 * This serializer is configured by default (in `reference.conf`) for [[DurableEvent]]s
 */
class DelegatingDurableEventSerializer(system: ExtendedActorSystem)
  extends DurableEventSerializer(system, new DelegatingPayloadSerializer(system))

/**
 * A [[https://developers.google.com/protocol-buffers/ Protocol Buffers]] based serializer for [[DurableEvent]]s.
 * Serialization of `DurableEvent`'s `payload` is delegated to [[BinaryPayloadSerializer]].
 *
 * To use this serializer the default config provided by `reference.conf` has to be overwritten in an
 * `application.conf` like follows:
 *
 * {{{
 *   akka.actor.serializers.eventuate-durable-event = com.rbmhtechnology.eventuate.serializer.DurableEventSerializerWithBinaryPayload
 * }}}
 *
 * This serializer can be useful in scenarios where an Eventuate application is just used for
 * event forwarding (through replication) and only handles the payload of events
 * based on their metadata (like the manifest).
 */
class DurableEventSerializerWithBinaryPayload(system: ExtendedActorSystem)
  extends DurableEventSerializer(system, new BinaryPayloadSerializer(system))

abstract class DurableEventSerializer(
  system: ExtendedActorSystem,
  payloadSerializer: PayloadSerializer) extends Serializer {

  val commonSerializer = new CommonSerializer(system)

  val DurableEventClass = classOf[DurableEvent]

  override def identifier: Int = 22563
  override def includeManifest: Boolean = true

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case m: DurableEvent =>
      durableEventFormatBuilder(m).build().toByteArray
    case _ =>
      throw new IllegalArgumentException(s"can't serialize object of type ${o.getClass}")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
    case None =>
      durableEvent(DurableEventFormat.parseFrom(bytes))
    case Some(c) => c match {
      case DurableEventClass =>
        durableEvent(DurableEventFormat.parseFrom(bytes))
      case _ =>
        throw new IllegalArgumentException(s"can't deserialize object of type ${c}")
    }
  }

  // --------------------------------------------------------------------------------
  //  toBinary helpers
  // --------------------------------------------------------------------------------

  def durableEventFormatBuilder(durableEvent: DurableEvent): DurableEventFormat.Builder = {
    val builder = DurableEventFormat.newBuilder
    builder.setPayload(payloadSerializer.payloadFormatBuilder(durableEvent.payload.asInstanceOf[AnyRef]))
    builder.setEmitterId(durableEvent.emitterId)
    builder.setSystemTimestamp(durableEvent.systemTimestamp)
    builder.setVectorTimestamp(commonSerializer.vectorTimeFormatBuilder(durableEvent.vectorTimestamp))
    builder.setProcessId(durableEvent.processId)
    builder.setLocalLogId(durableEvent.localLogId)
    builder.setLocalSequenceNr(durableEvent.localSequenceNr)

    durableEvent.deliveryId.foreach { deliveryId =>
      builder.setDeliveryId(deliveryId)
    }

    durableEvent.persistOnEventSequenceNr.foreach { persistOnEventSequenceNr =>
      builder.setPersistOnEventSequenceNr(persistOnEventSequenceNr)
    }
    durableEvent.persistOnEventId.foreach { persistOnEventId =>
      builder.setPersistOnEventId(eventIdFormatBuilder(persistOnEventId))
    }

    durableEvent.emitterAggregateId.foreach { id =>
      builder.setEmitterAggregateId(id)
    }

    durableEvent.customDestinationAggregateIds.foreach { dest =>
      builder.addCustomDestinationAggregateIds(dest)
    }

    builder
  }

  def eventIdFormatBuilder(eventId: EventId) = {
    val builder = EventIdFormat.newBuilder()
    builder.setProcessId(eventId.processId)
    builder.setSequenceNr(eventId.sequenceNr)
    builder
  }

  // --------------------------------------------------------------------------------
  //  fromBinary helpers
  // --------------------------------------------------------------------------------

  def durableEvent(durableEventFormat: DurableEventFormat): DurableEvent = {
    val emitterAggregateId: Option[String] =
      if (durableEventFormat.hasEmitterAggregateId) Some(durableEventFormat.getEmitterAggregateId) else None

    val customDestinationAggregateIds = durableEventFormat.getCustomDestinationAggregateIdsList.iterator.asScala.foldLeft(Set.empty[String]) {
      case (result, dest) => result + dest
    }

    val deliveryId = if (durableEventFormat.hasDeliveryId) Some(durableEventFormat.getDeliveryId) else None
    val persistOnEventSequenceNr = if (durableEventFormat.hasPersistOnEventSequenceNr) Some(durableEventFormat.getPersistOnEventSequenceNr) else None
    val persistOnEventId = if (durableEventFormat.hasPersistOnEventId) Some(eventId(durableEventFormat.getPersistOnEventId)) else None

    DurableEvent(
      payload = payloadSerializer.payload(durableEventFormat.getPayload),
      emitterId = durableEventFormat.getEmitterId,
      emitterAggregateId = emitterAggregateId,
      customDestinationAggregateIds = customDestinationAggregateIds,
      systemTimestamp = durableEventFormat.getSystemTimestamp,
      vectorTimestamp = commonSerializer.vectorTime(durableEventFormat.getVectorTimestamp),
      processId = durableEventFormat.getProcessId,
      localLogId = durableEventFormat.getLocalLogId,
      localSequenceNr = durableEventFormat.getLocalSequenceNr,
      deliveryId = deliveryId,
      persistOnEventSequenceNr = persistOnEventSequenceNr,
      persistOnEventId = persistOnEventId)
  }

  def eventId(eventId: EventIdFormat) =
    EventId(eventId.getProcessId, eventId.getSequenceNr)
}
