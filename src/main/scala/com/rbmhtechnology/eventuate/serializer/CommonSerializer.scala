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

import akka.actor.ExtendedActorSystem
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest

import com.google.protobuf.ByteString
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.serializer.CommonFormats._

import scala.collection.JavaConverters._

class CommonSerializer(system: ExtendedActorSystem) {
  // --------------------------------------------------------------------------------
  //  toBinary helpers
  // --------------------------------------------------------------------------------

  def payloadFormatBuilder(payload: AnyRef) = {
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

  def vectorTimeFormatBuilder(vectorTime: VectorTime): VectorTimeFormat.Builder = {
    val builder = VectorTimeFormat.newBuilder
    vectorTime.value.foreach { entry =>
      builder.addEntries(VectorTimeEntryFormat.newBuilder
        .setProcessId(entry._1)
        .setLogicalTime(entry._2))
    }
    builder
  }

  def versionedFormatBuilder(versioned: Versioned[_]): VersionedFormat.Builder = {
    val builder = VersionedFormat.newBuilder
    builder.setPayload(payloadFormatBuilder(versioned.value.asInstanceOf[AnyRef]))
    builder.setVectorTimestamp(vectorTimeFormatBuilder(versioned.vectorTimestamp))
    builder.setSystemTimestamp(versioned.systemTimestamp)
    builder.setCreator(versioned.creator)
  }

  // --------------------------------------------------------------------------------
  //  fromBinary helpers
  // --------------------------------------------------------------------------------

  def payload(payloadFormat: PayloadFormat): Any = {
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

  def vectorTime(vectorTimeFormat: VectorTimeFormat): VectorTime = {
    VectorTime(vectorTimeFormat.getEntriesList.iterator.asScala.foldLeft(Map.empty[String, Long]) {
      case (result, entry) => result.updated(entry.getProcessId, entry.getLogicalTime)
    })
  }

  def versioned(versionedFormat: VersionedFormat): Versioned[Any] = {
    Versioned[Any](
      payload(versionedFormat.getPayload),
      vectorTime(versionedFormat.getVectorTimestamp),
      versionedFormat.getSystemTimestamp,
      versionedFormat.getCreator)
  }
}
