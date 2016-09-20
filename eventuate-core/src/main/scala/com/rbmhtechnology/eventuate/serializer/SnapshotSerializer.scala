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
import akka.serialization.Serializer

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.ConfirmedDelivery._
import com.rbmhtechnology.eventuate.PersistOnEvent._
import com.rbmhtechnology.eventuate.log.EventLogClock
import com.rbmhtechnology.eventuate.serializer.SnapshotFormats._

import scala.collection.JavaConverters._
import scala.collection.immutable.VectorBuilder

class SnapshotSerializer(system: ExtendedActorSystem) extends Serializer {
  val eventSerializer = new DurableEventSerializer(system)

  import eventSerializer.commonSerializer

  val SnapshotClass = classOf[Snapshot]
  val ConcurrentVersionsTreeClass = classOf[ConcurrentVersionsTree[_, _]]
  val ClockClass = classOf[EventLogClock]

  override def identifier: Int = 22566
  override def includeManifest: Boolean = true

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case s: Snapshot =>
      snapshotFormatBuilder(s).build().toByteArray
    case t: ConcurrentVersionsTree[_, _] =>
      concurrentVersionsTreeFormat(t).build().toByteArray
    case c: EventLogClock =>
      eventLogClockFormatBuilder(c).build().toByteArray
    case _ =>
      throw new IllegalArgumentException(s"can't serialize object of type ${o.getClass}")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
    case None => throw new IllegalArgumentException("manifest required")
    case Some(clazz) => clazz match {
      case SnapshotClass =>
        snapshot(SnapshotFormat.parseFrom(bytes))
      case ConcurrentVersionsTreeClass =>
        concurrentVersionsTree(ConcurrentVersionsTreeFormat.parseFrom(bytes))
      case ClockClass =>
        eventLogClock(EventLogClockFormat.parseFrom(bytes))
      case _ =>
        throw new IllegalArgumentException(s"can't deserialize object of type ${clazz}")
    }
  }

  // --------------------------------------------------------------------------------
  //  toBinary helpers
  // --------------------------------------------------------------------------------

  private def snapshotFormatBuilder(snapshot: Snapshot): SnapshotFormat.Builder = {
    val builder = SnapshotFormat.newBuilder
    builder.setPayload(commonSerializer.payloadFormatBuilder(snapshot.payload.asInstanceOf[AnyRef]))
    builder.setEmitterId(snapshot.emitterId)
    builder.setLastEvent(eventSerializer.durableEventFormatBuilder(snapshot.lastEvent))
    builder.setCurrentTime(commonSerializer.vectorTimeFormatBuilder(snapshot.currentTime))
    builder.setSequenceNr(snapshot.sequenceNr)

    snapshot.deliveryAttempts.foreach { da =>
      builder.addDeliveryAttempts(deliveryAttemptFormatBuilder(da))
    }

    snapshot.persistOnEventRequests.foreach { pr =>
      builder.addPersistOnEventRequests(persistOnEventRequestFormatBuilder(pr))
    }

    builder
  }

  private def deliveryAttemptFormatBuilder(deliveryAttempt: DeliveryAttempt): DeliveryAttemptFormat.Builder = {
    val builder = DeliveryAttemptFormat.newBuilder
    builder.setDeliveryId(deliveryAttempt.deliveryId)
    builder.setMessage(commonSerializer.payloadFormatBuilder(deliveryAttempt.message.asInstanceOf[AnyRef]))
    builder.setDestination(deliveryAttempt.destination.toSerializationFormat)
    builder
  }

  private def persistOnEventRequestFormatBuilder(persistOnEventRequest: PersistOnEventRequest): PersistOnEventRequestFormat.Builder = {
    val builder = PersistOnEventRequestFormat.newBuilder
    builder.setPersistOnEventSequenceNr(persistOnEventRequest.persistOnEventSequenceNr)
    builder.setInstanceId(persistOnEventRequest.instanceId)

    persistOnEventRequest.invocations.foreach { invocation =>
      builder.addInvocation(persistOnEventInvocationFormatBuilder(invocation))
    }

    builder
  }

  private def persistOnEventInvocationFormatBuilder(persistOnEventInvocation: PersistOnEventInvocation): PersistOnEventInvocationFormat.Builder = {
    val builder = PersistOnEventInvocationFormat.newBuilder
    builder.setEvent(commonSerializer.payloadFormatBuilder(persistOnEventInvocation.event.asInstanceOf[AnyRef]))

    persistOnEventInvocation.customDestinationAggregateIds.foreach { dest =>
      builder.addCustomDestinationAggregateIds(dest)
    }

    builder
  }

  private def concurrentVersionsTreeFormat(tree: ConcurrentVersionsTree[_, _]): ConcurrentVersionsTreeFormat.Builder = {
    val builder = ConcurrentVersionsTreeFormat.newBuilder
    builder.setOwner(tree.owner)
    builder.setRoot(concurrentVersionsTreeNodeFormat(tree.root))
    builder
  }

  // TODO: make tail recursive or create a trampolined version
  private def concurrentVersionsTreeNodeFormat(node: ConcurrentVersionsTree.Node[_]): ConcurrentVersionsTreeNodeFormat.Builder = {
    val builder = ConcurrentVersionsTreeNodeFormat.newBuilder()
    builder.setVersioned(commonSerializer.versionedFormatBuilder(node.versioned))
    builder.setRejected(node.rejected)

    node.children.foreach { child =>
      builder.addChildren(concurrentVersionsTreeNodeFormat(child))
    }

    builder
  }

  private def eventLogClockFormatBuilder(clock: EventLogClock): EventLogClockFormat.Builder = {
    val builder = EventLogClockFormat.newBuilder
    builder.setSequenceNr(clock.sequenceNr)
    builder.setVersionVector(commonSerializer.vectorTimeFormatBuilder(clock.versionVector))
    builder
  }

  // --------------------------------------------------------------------------------
  //  fromBinary helpers
  // --------------------------------------------------------------------------------

  private def snapshot(snapshotFormat: SnapshotFormat): Snapshot = {
    val deliveryAttemptsBuilder = new VectorBuilder[DeliveryAttempt]
    val persistOnEventRequestsBuilder = new VectorBuilder[PersistOnEventRequest]

    snapshotFormat.getDeliveryAttemptsList.iterator.asScala.foreach { daf =>
      deliveryAttemptsBuilder += deliveryAttempt(daf)
    }

    snapshotFormat.getPersistOnEventRequestsList.iterator.asScala.foreach { prf =>
      persistOnEventRequestsBuilder += persistOnEventRequest(prf)
    }

    val durableEvent = eventSerializer.durableEvent(snapshotFormat.getLastEvent)
    val sequenceNr =
      if (snapshotFormat.hasSequenceNr) snapshotFormat.getSequenceNr
      else durableEvent.localSequenceNr

    Snapshot(
      commonSerializer.payload(snapshotFormat.getPayload),
      snapshotFormat.getEmitterId,
      durableEvent,
      commonSerializer.vectorTime(snapshotFormat.getCurrentTime),
      sequenceNr,
      deliveryAttemptsBuilder.result(),
      persistOnEventRequestsBuilder.result())
  }

  private def deliveryAttempt(deliveryAttemptFormat: DeliveryAttemptFormat): DeliveryAttempt = {
    DeliveryAttempt(
      deliveryAttemptFormat.getDeliveryId,
      commonSerializer.payload(deliveryAttemptFormat.getMessage),
      ActorPath.fromString(deliveryAttemptFormat.getDestination))
  }

  private def persistOnEventRequest(persistOnEventRequestFormat: PersistOnEventRequestFormat): PersistOnEventRequest = {
    val invocationsBuilder = new VectorBuilder[PersistOnEventInvocation]

    persistOnEventRequestFormat.getInvocationList.iterator.asScala.foreach { pif =>
      invocationsBuilder += persistOnEventInvocation(pif)
    }

    PersistOnEventRequest(
      persistOnEventRequestFormat.getPersistOnEventSequenceNr,
      invocationsBuilder.result(),
      persistOnEventRequestFormat.getInstanceId)
  }

  private def persistOnEventInvocation(persistOnEventInvocationFormat: PersistOnEventInvocationFormat): PersistOnEventInvocation = {
    val customDestinationAggregateIds = persistOnEventInvocationFormat.getCustomDestinationAggregateIdsList.iterator.asScala.foldLeft(Set.empty[String]) {
      case (result, dest) => result + dest
    }

    PersistOnEventInvocation(
      commonSerializer.payload(persistOnEventInvocationFormat.getEvent),
      customDestinationAggregateIds)
  }

  private def concurrentVersionsTree(treeFormat: ConcurrentVersionsTreeFormat): ConcurrentVersionsTree[Any, Any] = {
    new ConcurrentVersionsTree(concurrentVersionsTreeNode(treeFormat.getRoot)).withOwner(treeFormat.getOwner)
  }

  // TODO: make tail recursive or create a trampolined version
  private def concurrentVersionsTreeNode(nodeFormat: ConcurrentVersionsTreeNodeFormat): ConcurrentVersionsTree.Node[Any] = {
    val node = new ConcurrentVersionsTree.Node(commonSerializer.versioned(nodeFormat.getVersioned))
    node.rejected = nodeFormat.getRejected

    nodeFormat.getChildrenList.iterator.asScala.foreach { childFormat =>
      node.addChild(concurrentVersionsTreeNode(childFormat))
    }

    node
  }

  private def eventLogClock(clockFormat: EventLogClockFormat): EventLogClock = {
    EventLogClock(
      sequenceNr = clockFormat.getSequenceNr,
      versionVector = commonSerializer.vectorTime(clockFormat.getVersionVector))
  }
}
