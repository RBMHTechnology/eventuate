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

import akka.actor.ExtendedActorSystem
import akka.serialization._

import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.serializer.ReplicationProtocolFormats._

import scala.collection.JavaConverters._
import scala.collection.immutable.VectorBuilder

class ReplicationProtocolSerializer(system: ExtendedActorSystem) extends Serializer {
  val eventSerializer = new DurableEventSerializer(system)
  val filterSerializer = new ReplicationFilterSerializer(system)

  val GetReplicationEndpointInfoClass = GetReplicationEndpointInfo.getClass
  val GetReplicationEndpointInfoSuccessClass = classOf[GetReplicationEndpointInfoSuccess]
  val ReplicationReadEnvelopeClass = classOf[ReplicationReadEnvelope]
  val ReplicationReadClass = classOf[ReplicationRead]
  val ReplicationReadSuccessClass = classOf[ReplicationReadSuccess]
  val ReplicationReadFailureClass = classOf[ReplicationReadFailure]
  val ReplicationDueClass = ReplicationDue.getClass

  override def identifier: Int = 22565
  override def includeManifest: Boolean = true

  override def toBinary(o: AnyRef): Array[Byte] = {
    o match {
      case GetReplicationEndpointInfo =>
        GetReplicationEndpointInfoFormat.newBuilder().build().toByteArray
      case m: GetReplicationEndpointInfoSuccess =>
        getReplicationEndpointInfoSuccessFormatBuilder(m).build().toByteArray
      case m: ReplicationReadEnvelope =>
        replicationReadEnvelopeFormatBuilder(m).build().toByteArray
      case m: ReplicationRead =>
        replicationReadFormatBuilder(m).build().toByteArray
      case m: ReplicationReadSuccess =>
        replicationReadSuccessFormatBuilder(m).build().toByteArray
      case m: ReplicationReadFailure =>
        replicationReadFailureFormatBuilder(m).build().toByteArray
      case ReplicationDue =>
        ReplicationDueFormat.newBuilder().build().toByteArray
      case _ =>
        throw new IllegalArgumentException(s"can't serialize object of type ${o.getClass}")
    }
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
    case None => throw new IllegalArgumentException("manifest required")
    case Some(clazz) => clazz match {
      case GetReplicationEndpointInfoClass =>
        GetReplicationEndpointInfo
      case GetReplicationEndpointInfoSuccessClass =>
        getReplicationEndpointInfoSuccess(GetReplicationEndpointInfoSuccessFormat.parseFrom(bytes))
      case ReplicationReadEnvelopeClass =>
        replicationReadEnvelope(ReplicationReadEnvelopeFormat.parseFrom(bytes))
      case ReplicationReadClass =>
        replicationRead(ReplicationReadFormat.parseFrom(bytes))
      case ReplicationReadSuccessClass =>
        replicationReadSuccess(ReplicationReadSuccessFormat.parseFrom(bytes))
      case ReplicationReadFailureClass =>
        replicationReadFailure(ReplicationReadFailureFormat.parseFrom(bytes))
      case ReplicationDueClass =>
        ReplicationDue
      case _ =>
        throw new IllegalArgumentException(s"can't deserialize object of type ${clazz}")
    }
  }

  // --------------------------------------------------------------------------------
  //  toBinary helpers
  // --------------------------------------------------------------------------------

  private def replicationReadFailureFormatBuilder(message: ReplicationReadFailure): ReplicationReadFailureFormat.Builder = {
    val builder = ReplicationReadFailureFormat.newBuilder()
    builder.setCause(message.cause)
    builder.setTargetLogId(message.targetLogId)
    builder
  }

  private def replicationReadSuccessFormatBuilder(message: ReplicationReadSuccess): ReplicationReadSuccessFormat.Builder = {
    val builder = ReplicationReadSuccessFormat.newBuilder()
    message.events.foreach(event => builder.addEvents(eventSerializer.durableEventFormatBuilder(event)))
    builder.setReplicationProgress(message.replicationProgress)
    builder.setTargetLogId(message.targetLogId)
    builder.setCurrentSourceVectorTime(eventSerializer.vectorTimeFormatBuilder(message.currentSourceVectorTime))
    builder
  }

  private def replicationReadFormatBuilder(message: ReplicationRead): ReplicationReadFormat.Builder = {
    val builder = ReplicationReadFormat.newBuilder()
    builder.setFromSequenceNr(message.fromSequenceNr)
    builder.setMaxNumEvents(message.maxNumEvents)
    builder.setFilter(filterSerializer.filterTreeFormatBuilder(message.filter))
    builder.setTargetLogId(message.targetLogId)
    builder.setReplicator(Serialization.serializedActorPath(message.replicator))
    builder.setCurrentTargetVectorTime(eventSerializer.vectorTimeFormatBuilder(message.currentTargetVectorTime))
    builder
  }

  private def replicationReadEnvelopeFormatBuilder(message: ReplicationReadEnvelope): ReplicationReadEnvelopeFormat.Builder = {
    val builder = ReplicationReadEnvelopeFormat.newBuilder()
    builder.setPayload(replicationReadFormatBuilder(message.payload))
    builder.setLogName(message.logName)
    builder
  }

  private def getReplicationEndpointInfoSuccessFormatBuilder(message: GetReplicationEndpointInfoSuccess): GetReplicationEndpointInfoSuccessFormat.Builder =
    GetReplicationEndpointInfoSuccessFormat.newBuilder().setInfo(replicationEndpointInfoFormatBuilder(message.info))

  private def replicationEndpointInfoFormatBuilder(info: ReplicationEndpointInfo): ReplicationEndpointInfoFormat.Builder = {
    val builder = ReplicationEndpointInfoFormat.newBuilder()
    builder.setEndpointId(info.endpointId)
    builder.addAllLogNames(info.logNames.asJava)
    builder
  }

  // --------------------------------------------------------------------------------
  //  fromBinary helpers
  // --------------------------------------------------------------------------------

  private def replicationReadFailure(messageFormat: ReplicationReadFailureFormat): ReplicationReadFailure =
    ReplicationReadFailure(
      messageFormat.getCause,
      messageFormat.getTargetLogId)

  private def replicationReadSuccess(messageFormat: ReplicationReadSuccessFormat): ReplicationReadSuccess = {
    val builder = new VectorBuilder[DurableEvent]

    messageFormat.getEventsList.iterator().asScala.foreach { eventFormat =>
      builder += eventSerializer.durableEvent(eventFormat)
    }

    ReplicationReadSuccess(
      builder.result(),
      messageFormat.getReplicationProgress,
      messageFormat.getTargetLogId,
      eventSerializer.vectorTime(messageFormat.getCurrentSourceVectorTime))
  }

  private def replicationRead(messageFormat: ReplicationReadFormat): ReplicationRead =
    ReplicationRead(
      messageFormat.getFromSequenceNr,
      messageFormat.getMaxNumEvents,
      filterSerializer.filterTree(messageFormat.getFilter),
      messageFormat.getTargetLogId,
      system.provider.resolveActorRef(messageFormat.getReplicator),
      eventSerializer.vectorTime(messageFormat.getCurrentTargetVectorTime))

  private def replicationReadEnvelope(messageFormat: ReplicationReadEnvelopeFormat): ReplicationReadEnvelope =
    ReplicationReadEnvelope(
      replicationRead(messageFormat.getPayload),
      messageFormat.getLogName)

  private def getReplicationEndpointInfoSuccess(messageFormat: GetReplicationEndpointInfoSuccessFormat) =
    GetReplicationEndpointInfoSuccess(replicationEndpointInfo(messageFormat.getInfo))

  private def replicationEndpointInfo(infoFormat: ReplicationEndpointInfoFormat): ReplicationEndpointInfo = {
    ReplicationEndpointInfo(
      infoFormat.getEndpointId,
      infoFormat.getLogNamesList.asScala.toSet)
  }
}
