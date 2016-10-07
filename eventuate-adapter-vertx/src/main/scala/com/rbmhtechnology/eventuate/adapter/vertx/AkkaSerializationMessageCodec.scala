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

package com.rbmhtechnology.eventuate.adapter.vertx

import akka.actor.{ ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider }
import com.rbmhtechnology.eventuate.serializer.CommonFormats.PayloadFormat
import com.rbmhtechnology.eventuate.serializer.CommonSerializer
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec

object AkkaSerializationMessageCodec {
  val Name = "akka-serialization-message-codec"

  def apply(name: String)(implicit system: ActorSystem): MessageCodec[AnyRef, AnyRef] =
    new AkkaSerializationMessageCodec(name)

  def apply(clazz: Class[_])(implicit system: ActorSystem): MessageCodec[AnyRef, AnyRef] =
    new AkkaSerializationMessageCodec(s"${AkkaSerializationMessageCodec.Name}-${clazz.getName}")
}

class AkkaSerializationMessageCodec(override val name: String)(implicit system: ActorSystem) extends MessageCodec[AnyRef, AnyRef] {

  val serializer = PayloadSerializationExtension(system)

  override def transform(o: AnyRef): AnyRef =
    o

  override def encodeToWire(buffer: Buffer, o: AnyRef): Unit = {
    val payload = serializer.toBinary(o)
    buffer.appendInt(payload.length)
    buffer.appendBytes(payload)
  }

  override def decodeFromWire(pos: Int, buffer: Buffer): AnyRef = {
    val payloadLength = buffer.getInt(pos)
    val payload = buffer.getBytes(pos + Integer.BYTES, pos + Integer.BYTES + payloadLength)
    serializer.fromBinary(payload).asInstanceOf[AnyRef]
  }

  override def systemCodecID(): Byte = -1
}

object PayloadSerializationExtension extends ExtensionId[PayloadSerializationExtension] with ExtensionIdProvider {

  override def lookup = PayloadSerializationExtension

  override def createExtension(system: ExtendedActorSystem): PayloadSerializationExtension =
    new PayloadSerializationExtension(system)

  override def get(system: ActorSystem): PayloadSerializationExtension =
    super.get(system)
}

class PayloadSerializationExtension(system: ExtendedActorSystem) extends Extension {

  val serializer = new CommonSerializer(system)

  def toBinary(o: AnyRef): Array[Byte] =
    serializer.payloadFormatBuilder(o).build().toByteArray

  def fromBinary(b: Array[Byte]): Any =
    serializer.payload(PayloadFormat.parseFrom(b))
}
