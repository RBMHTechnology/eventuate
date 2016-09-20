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
import akka.serialization._
import com.rbmhtechnology.eventuate.ReplicationProtocol.SynchronizeReplicationProgressFailure
import com.rbmhtechnology.eventuate.ReplicationProtocol.SynchronizeReplicationProgressSourceException
import com.rbmhtechnology.eventuate.ReplicationProtocol.SynchronizeReplicationProgressSuccess
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.serializer.ReplicationProtocolFormats._

import scala.collection.JavaConverters._
import scala.collection.breakOut
import scala.collection.immutable.VectorBuilder

class ReplicationProtocolSerializer(system: ExtendedActorSystem) extends Serializer {
  val eventSerializer = new DurableEventSerializer(system)
  val filterSerializer = new ReplicationFilterSerializer(system)

  import eventSerializer.commonSerializer

  val GetReplicationEndpointInfoClass = GetReplicationEndpointInfo.getClass
  val GetReplicationEndpointInfoSuccessClass = classOf[GetReplicationEndpointInfoSuccess]
  val SynchronizeReplicationProgressClass = classOf[SynchronizeReplicationProgress]
  val SynchronizeReplicationProgressSuccessClass = classOf[SynchronizeReplicationProgressSuccess]
  val SynchronizeReplicationProgressFailureClass = classOf[SynchronizeReplicationProgressFailure]
  val SynchronizeReplicationProgressSourceExceptionClass = classOf[SynchronizeReplicationProgressSourceException]
  val ReplicationReadEnvelopeClass = classOf[ReplicationReadEnvelope]
  val ReplicationReadClass = classOf[ReplicationRead]
  val ReplicationReadSuccessClass = classOf[ReplicationReadSuccess]
  val ReplicationReadFailureClass = classOf[ReplicationReadFailure]
  val ReplicationDueClass = ReplicationDue.getClass
  val ReplicationReadSourceExceptionClass = classOf[ReplicationReadSourceException]
  val IncompatibleApplicationVersionExceptionClass = classOf[IncompatibleApplicationVersionException]

  override def identifier: Int = 22565
  override def includeManifest: Boolean = true

  override def toBinary(o: AnyRef): Array[Byte] = {
    o match {
      case GetReplicationEndpointInfo =>
        GetReplicationEndpointInfoFormat.newBuilder().build().toByteArray
      case m: GetReplicationEndpointInfoSuccess =>
        getReplicationEndpointInfoSuccessFormatBuilder(m).build().toByteArray
      case m: SynchronizeReplicationProgress =>
        synchronizeReplicationProgressFormatBuilder(m).build().toByteArray
      case m: SynchronizeReplicationProgressSuccess =>
        synchronizeReplicationProgressSuccessFormatBuilder(m).build().toByteArray
      case m: SynchronizeReplicationProgressFailure =>
        synchronizeReplicationProgressFailureFormatBuilder(m).build().toByteArray
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
      case m: ReplicationReadSourceException =>
        replicationReadSourceExceptionFormatBuilder(m).build().toByteArray
      case m: IncompatibleApplicationVersionException =>
        incompatibleApplicationVersionExceptionFormatBuilder(m).build().toByteArray
      case m: SynchronizeReplicationProgressSourceException =>
        synchronizeReplicationProgressSourceExceptionFormatBuilder(m).build().toByteArray
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
      case SynchronizeReplicationProgressClass =>
        synchronizeReplicationProgress(SynchronizeReplicationProgressFormat.parseFrom(bytes))
      case SynchronizeReplicationProgressSuccessClass =>
        synchronizeReplicationProgressSuccess(SynchronizeReplicationProgressSuccessFormat.parseFrom(bytes))
      case SynchronizeReplicationProgressFailureClass =>
        synchronizeReplicationProgressFailure(SynchronizeReplicationProgressFailureFormat.parseFrom(bytes))
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
      case ReplicationReadSourceExceptionClass =>
        replicationReadSourceException(ReplicationReadSourceExceptionFormat.parseFrom(bytes))
      case IncompatibleApplicationVersionExceptionClass =>
        incompatibleApplicationVersionException(IncompatibleApplicationVersionExceptionFormat.parseFrom(bytes))
      case SynchronizeReplicationProgressSourceExceptionClass =>
        synchronizeReplicationProgressSourceException(SynchronizeReplicationProgressSourceExceptionFormat.parseFrom(bytes))
      case _ =>
        throw new IllegalArgumentException(s"can't deserialize object of type ${clazz}")
    }
  }

  // --------------------------------------------------------------------------------
  //  toBinary helpers
  // --------------------------------------------------------------------------------

  private def replicationReadFailureFormatBuilder(message: ReplicationReadFailure): ReplicationReadFailureFormat.Builder = {
    val builder = ReplicationReadFailureFormat.newBuilder()
    builder.setCause(commonSerializer.payloadFormatBuilder(message.cause))
    builder.setTargetLogId(message.targetLogId)
    builder
  }

  private def replicationReadSuccessFormatBuilder(message: ReplicationReadSuccess): ReplicationReadSuccessFormat.Builder = {
    val builder = ReplicationReadSuccessFormat.newBuilder()
    message.events.foreach(event => builder.addEvents(eventSerializer.durableEventFormatBuilder(event)))
    builder.setFromSequenceNr(message.fromSequenceNr)
    builder.setReplicationProgress(message.replicationProgress)
    builder.setTargetLogId(message.targetLogId)
    builder.setCurrentSourceVersionVector(commonSerializer.vectorTimeFormatBuilder(message.currentSourceVersionVector))
    builder
  }

  private def replicationReadFormatBuilder(message: ReplicationRead): ReplicationReadFormat.Builder = {
    val builder = ReplicationReadFormat.newBuilder()
    builder.setFromSequenceNr(message.fromSequenceNr)
    builder.setMax(message.max)
    builder.setScanLimit(message.scanLimit)
    builder.setFilter(filterSerializer.filterTreeFormatBuilder(message.filter))
    builder.setTargetLogId(message.targetLogId)
    builder.setReplicator(Serialization.serializedActorPath(message.replicator))
    builder.setCurrentTargetVersionVector(commonSerializer.vectorTimeFormatBuilder(message.currentTargetVersionVector))
    builder
  }

  private def replicationReadEnvelopeFormatBuilder(message: ReplicationReadEnvelope): ReplicationReadEnvelopeFormat.Builder = {
    val builder = ReplicationReadEnvelopeFormat.newBuilder()
    builder.setPayload(replicationReadFormatBuilder(message.payload))
    builder.setLogName(message.logName)
    builder.setTargetApplicationName(message.targetApplicationName)
    builder.setTargetApplicationVersion(applicationVersionFormatBuilder(message.targetApplicationVersion))
    builder
  }

  private def getReplicationEndpointInfoSuccessFormatBuilder(message: GetReplicationEndpointInfoSuccess): GetReplicationEndpointInfoSuccessFormat.Builder =
    GetReplicationEndpointInfoSuccessFormat.newBuilder().setInfo(replicationEndpointInfoFormatBuilder(message.info))

  private def synchronizeReplicationProgressFormatBuilder(message: SynchronizeReplicationProgress): SynchronizeReplicationProgressFormat.Builder =
    SynchronizeReplicationProgressFormat.newBuilder().setInfo(replicationEndpointInfoFormatBuilder(message.info))

  private def synchronizeReplicationProgressSuccessFormatBuilder(message: SynchronizeReplicationProgressSuccess): SynchronizeReplicationProgressSuccessFormat.Builder =
    SynchronizeReplicationProgressSuccessFormat.newBuilder().setInfo(replicationEndpointInfoFormatBuilder(message.info))

  private def synchronizeReplicationProgressFailureFormatBuilder(message: SynchronizeReplicationProgressFailure): SynchronizeReplicationProgressFailureFormat.Builder =
    SynchronizeReplicationProgressFailureFormat.newBuilder().setCause(commonSerializer.payloadFormatBuilder(message.cause))

  private def replicationEndpointInfoFormatBuilder(info: ReplicationEndpointInfo): ReplicationEndpointInfoFormat.Builder = {
    val builder = ReplicationEndpointInfoFormat.newBuilder()
    builder.setEndpointId(info.endpointId)
    info.logSequenceNrs.foreach(logInfo => builder.addLogInfos(logInfoFormatBuilder(logInfo)))
    builder
  }

  private def logInfoFormatBuilder(info: (String, Long)): LogInfoFormat.Builder = {
    val builder = LogInfoFormat.newBuilder()
    builder.setLogName(info._1)
    builder.setSequenceNr(info._2)
    builder
  }

  private def applicationVersionFormatBuilder(applicationVersion: ApplicationVersion): ApplicationVersionFormat.Builder = {
    val builder = ApplicationVersionFormat.newBuilder()
    builder.setMajor(applicationVersion.major)
    builder.setMinor(applicationVersion.minor)
    builder
  }

  private def incompatibleApplicationVersionExceptionFormatBuilder(exception: IncompatibleApplicationVersionException): IncompatibleApplicationVersionExceptionFormat.Builder = {
    val builder = IncompatibleApplicationVersionExceptionFormat.newBuilder()
    builder.setSourceEndpointId(exception.sourceEndpointId)
    builder.setSourceApplicationVersion(applicationVersionFormatBuilder(exception.sourceApplicationVersion))
    builder.setTargetApplicationVersion(applicationVersionFormatBuilder(exception.targetApplicationVersion))
    builder
  }

  private def replicationReadSourceExceptionFormatBuilder(exception: ReplicationReadSourceException): ReplicationReadSourceExceptionFormat.Builder = {
    val builder = ReplicationReadSourceExceptionFormat.newBuilder()
    builder.setMessage(exception.getMessage)
    builder
  }

  private def synchronizeReplicationProgressSourceExceptionFormatBuilder(ex: SynchronizeReplicationProgressSourceException): SynchronizeReplicationProgressSourceExceptionFormat.Builder =
    SynchronizeReplicationProgressSourceExceptionFormat.newBuilder().setMessage(ex.message)

  // --------------------------------------------------------------------------------
  //  fromBinary helpers
  // --------------------------------------------------------------------------------

  private def replicationReadFailure(messageFormat: ReplicationReadFailureFormat): ReplicationReadFailure =
    ReplicationReadFailure(
      commonSerializer.payload(messageFormat.getCause).asInstanceOf[ReplicationReadException],
      messageFormat.getTargetLogId)

  private def replicationReadSuccess(messageFormat: ReplicationReadSuccessFormat): ReplicationReadSuccess = {
    val builder = new VectorBuilder[DurableEvent]

    messageFormat.getEventsList.iterator.asScala.foreach { eventFormat =>
      builder += eventSerializer.durableEvent(eventFormat)
    }

    ReplicationReadSuccess(
      builder.result(),
      messageFormat.getFromSequenceNr,
      messageFormat.getReplicationProgress,
      messageFormat.getTargetLogId,
      commonSerializer.vectorTime(messageFormat.getCurrentSourceVersionVector))
  }

  private def replicationRead(messageFormat: ReplicationReadFormat): ReplicationRead =
    ReplicationRead(
      messageFormat.getFromSequenceNr,
      messageFormat.getMax,
      messageFormat.getScanLimit,
      filterSerializer.filterTree(messageFormat.getFilter),
      messageFormat.getTargetLogId,
      system.provider.resolveActorRef(messageFormat.getReplicator),
      commonSerializer.vectorTime(messageFormat.getCurrentTargetVersionVector))

  private def replicationReadEnvelope(messageFormat: ReplicationReadEnvelopeFormat): ReplicationReadEnvelope =
    ReplicationReadEnvelope(
      replicationRead(messageFormat.getPayload),
      messageFormat.getLogName,
      messageFormat.getTargetApplicationName,
      applicationVersion(messageFormat.getTargetApplicationVersion))

  private def getReplicationEndpointInfoSuccess(messageFormat: GetReplicationEndpointInfoSuccessFormat) =
    GetReplicationEndpointInfoSuccess(replicationEndpointInfo(messageFormat.getInfo))

  private def synchronizeReplicationProgress(messageFormat: SynchronizeReplicationProgressFormat) =
    SynchronizeReplicationProgress(replicationEndpointInfo(messageFormat.getInfo))

  private def synchronizeReplicationProgressSuccess(messageFormat: SynchronizeReplicationProgressSuccessFormat) =
    SynchronizeReplicationProgressSuccess(replicationEndpointInfo(messageFormat.getInfo))

  private def synchronizeReplicationProgressFailure(messageFormat: SynchronizeReplicationProgressFailureFormat): SynchronizeReplicationProgressFailure =
    SynchronizeReplicationProgressFailure(commonSerializer.payload(messageFormat.getCause).asInstanceOf[SynchronizeReplicationProgressException])

  private def replicationEndpointInfo(infoFormat: ReplicationEndpointInfoFormat): ReplicationEndpointInfo = {
    ReplicationEndpointInfo(
      infoFormat.getEndpointId,
      infoFormat.getLogInfosList.asScala.map(logInfo)(breakOut))
  }

  private def logInfo(infoFormat: LogInfoFormat): (String, Long) =
    infoFormat.getLogName -> infoFormat.getSequenceNr

  private def applicationVersion(applicationVersionFormat: ApplicationVersionFormat): ApplicationVersion =
    ApplicationVersion(applicationVersionFormat.getMajor, applicationVersionFormat.getMinor)

  private def incompatibleApplicationVersionException(exceptionFormat: IncompatibleApplicationVersionExceptionFormat): IncompatibleApplicationVersionException =
    IncompatibleApplicationVersionException(
      exceptionFormat.getSourceEndpointId,
      applicationVersion(exceptionFormat.getSourceApplicationVersion),
      applicationVersion(exceptionFormat.getTargetApplicationVersion))

  private def replicationReadSourceException(exceptionFormat: ReplicationReadSourceExceptionFormat): ReplicationReadSourceException =
    ReplicationReadSourceException(exceptionFormat.getMessage)

  private def synchronizeReplicationProgressSourceException(exceptionFormat: SynchronizeReplicationProgressSourceExceptionFormat): SynchronizeReplicationProgressSourceException =
    SynchronizeReplicationProgressSourceException(exceptionFormat.getMessage)
}
