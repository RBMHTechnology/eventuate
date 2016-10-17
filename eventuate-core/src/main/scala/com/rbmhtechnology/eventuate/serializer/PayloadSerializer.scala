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

import akka.actor.ExtendedActorSystem
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest
import com.google.protobuf.ByteString
import com.rbmhtechnology.eventuate.BinaryPayload
import com.rbmhtechnology.eventuate.serializer.CommonFormats.PayloadFormat

/**
 * Interface of a serializer converting between payloads and `PayloadFormat`.
 */
trait PayloadSerializer {
  def payloadFormatBuilder(payload: AnyRef): PayloadFormat.Builder
  def payload(payloadFormat: PayloadFormat): AnyRef
}

/**
 * A [[PayloadSerializer]] delegating to a serializer that is configured with Akka's
 * [[http://doc.akka.io/docs/akka/2.3.9/scala/serialization.html serialization extension]] mechanism.
 */
class DelegatingPayloadSerializer(system: ExtendedActorSystem) extends PayloadSerializer {

  override def payloadFormatBuilder(payload: AnyRef): PayloadFormat.Builder = {
    val serializer = SerializationExtension(system).findSerializerFor(payload)
    val builder = PayloadFormat.newBuilder()

    if (serializer.includeManifest) {
      val stringManifest = serializer match {
        case serializerWithStringManifest: SerializerWithStringManifest =>
          builder.setIsStringManifest(true)
          serializerWithStringManifest.manifest(payload)
        case _ =>
          payload.getClass.getName
      }
      builder.setPayloadManifest(stringManifest)
    }

    builder.setPayload(ByteString.copyFrom(serializer.toBinary(payload)))
    builder.setSerializerId(serializer.identifier)
    builder
  }

  override def payload(payloadFormat: PayloadFormat): AnyRef = {
    val deserialized = if (payloadFormat.getIsStringManifest) {
      SerializationExtension(system).deserialize(
        payloadFormat.getPayload.toByteArray,
        payloadFormat.getSerializerId,
        payloadFormat.getPayloadManifest)
    } else if (payloadFormat.hasPayloadManifest) {
      val manifestClass = system.dynamicAccess.getClassFor[AnyRef](payloadFormat.getPayloadManifest).get
      SerializationExtension(system).deserialize(
        payloadFormat.getPayload.toByteArray,
        manifestClass)
    } else {
      SerializationExtension(system).deserialize(
        payloadFormat.getPayload.toByteArray,
        payloadFormat.getSerializerId,
        None)
    }
    deserialized.get
  }
}

/**
 * A [[PayloadSerializer]] converting between `BinaryPayloads` and `PayloadFormats`.
 */
class BinaryPayloadSerializer(system: ExtendedActorSystem) extends PayloadSerializer {

  override def payloadFormatBuilder(payload: AnyRef): PayloadFormat.Builder = {
    val binaryPayload = payload.asInstanceOf[BinaryPayload]
    val builder = PayloadFormat.newBuilder()
      .setPayload(binaryPayload.bytes)
      .setSerializerId(binaryPayload.serializerId)
      .setIsStringManifest(binaryPayload.isStringManifest)
    binaryPayload.manifest.foreach(builder.setPayloadManifest)
    builder
  }

  override def payload(payloadFormat: PayloadFormat): AnyRef = {
    BinaryPayload(
      payloadFormat.getPayload,
      payloadFormat.getSerializerId,
      if (payloadFormat.hasPayloadManifest) Some(payloadFormat.getPayloadManifest) else None,
      payloadFormat.getIsStringManifest)
  }
}
