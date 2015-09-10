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
import akka.serialization.Serializer

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.ConfirmedDelivery.DeliveryAttempt
import com.rbmhtechnology.eventuate.serializer.SnapshotFormats._

import scala.collection.JavaConverters._
import scala.collection.immutable.VectorBuilder

class SnapshotSerializer(system: ExtendedActorSystem) extends Serializer {
  val eventSerializer = new DurableEventSerializer(system)

  val SnapshotClass = classOf[Snapshot]
  val ConcurrentVersionsTreeClass = classOf[ConcurrentVersionsTree[_, _]]

  override def identifier: Int = 22566
  override def includeManifest: Boolean = true

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case s: Snapshot =>
      snapshotFormatBuilder(s).build().toByteArray
    case t: ConcurrentVersionsTree[_, _] =>
      concurrentVersionsTreeFormat(t).build().toByteArray
    case _ =>
      throw new IllegalArgumentException(s"can't serialize object of type ${o.getClass}")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
    case None        => throw new IllegalArgumentException("manifest required")
    case Some(clazz) => clazz match {
      case SnapshotClass =>
        snapshot(SnapshotFormat.parseFrom(bytes))
      case ConcurrentVersionsTreeClass =>
        concurrentVersionsTree(ConcurrentVersionsTreeFormat.parseFrom(bytes))
      case _ =>
        throw new IllegalArgumentException(s"can't deserialize object of type ${clazz}")
    }
  }

  // --------------------------------------------------------------------------------
  //  toBinary helpers
  // --------------------------------------------------------------------------------

  private def snapshotFormatBuilder(snapshot: Snapshot): SnapshotFormat.Builder = {
    val builder = SnapshotFormat.newBuilder
    builder.setPayload(eventSerializer.payloadFormatBuilder(snapshot.payload.asInstanceOf[AnyRef]))
    builder.setEmitterId(snapshot.emitterId)
    builder.setLastEvent(eventSerializer.durableEventFormatBuilder(snapshot.lastEvent))
    builder.setCurrentTime(eventSerializer.vectorTimeFormatBuilder(snapshot.currentTime))

    snapshot.deliveryAttempts.foreach { da =>
      builder.addDeliveryAttempts(deliveryAttemptFormatBuilder(da))
    }

    builder
  }

  private def deliveryAttemptFormatBuilder(deliveryAttempt: DeliveryAttempt): DeliveryAttemptFormat.Builder = {
    val builder = DeliveryAttemptFormat.newBuilder
    builder.setDeliveryId(deliveryAttempt.deliveryId)
    builder.setMessage(eventSerializer.payloadFormatBuilder(deliveryAttempt.message.asInstanceOf[AnyRef]))
    builder.setDestination(deliveryAttempt.destination.toSerializationFormat)
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
    builder.setVersioned(versionedFormatBuilder(node.versioned))
    builder.setRejected(node.rejected)

    node.children.foreach { child =>
      builder.addChildren(concurrentVersionsTreeNodeFormat(child))
    }

    builder
  }

  private def versionedFormatBuilder(versioned: Versioned[_]): VersionedFormat.Builder = {
    val builder = VersionedFormat.newBuilder
    builder.setPayload(eventSerializer.payloadFormatBuilder(versioned.value.asInstanceOf[AnyRef]))
    builder.setUpdateTimestamp(eventSerializer.vectorTimeFormatBuilder(versioned.updateTimestamp))
    builder.setCreator(versioned.creator)
  }

  // --------------------------------------------------------------------------------
  //  fromBinary helpers
  // --------------------------------------------------------------------------------

  private def snapshot(snapshotFormat: SnapshotFormat): Snapshot = {
    val builder = new VectorBuilder[DeliveryAttempt]

    snapshotFormat.getDeliveryAttemptsList.asScala.iterator.foreach { daf =>
      builder += deliveryAttempt(daf)
    }

    Snapshot(
      eventSerializer.payload(snapshotFormat.getPayload),
      snapshotFormat.getEmitterId,
      eventSerializer.durableEvent(snapshotFormat.getLastEvent),
      eventSerializer.vectorTime(snapshotFormat.getCurrentTime),
      builder.result())
  }

  private def deliveryAttempt(deliveryAttemptFormat: DeliveryAttemptFormat): DeliveryAttempt = {
    DeliveryAttempt(
      deliveryAttemptFormat.getDeliveryId,
      eventSerializer.payload(deliveryAttemptFormat.getMessage),
      ActorPath.fromString(deliveryAttemptFormat.getDestination))
  }

  private def concurrentVersionsTree(treeFormat: ConcurrentVersionsTreeFormat): ConcurrentVersionsTree[Any, Any] = {
    new ConcurrentVersionsTree(concurrentVersionsTreeNode(treeFormat.getRoot)).withOwner(treeFormat.getOwner)
  }

  // TODO: make tail recursive or create a trampolined version
  private def concurrentVersionsTreeNode(nodeFormat: ConcurrentVersionsTreeNodeFormat): ConcurrentVersionsTree.Node[Any] = {
    val node = new ConcurrentVersionsTree.Node(versioned(nodeFormat.getVersioned))
    node.rejected = nodeFormat.getRejected

    nodeFormat.getChildrenList.iterator().asScala.foreach { childFormat =>
      node.addChild(concurrentVersionsTreeNode(childFormat))
    }

    node
  }

  private def versioned(versionedFormat: VersionedFormat): Versioned[Any] = {
    Versioned[Any](
      eventSerializer.payload(versionedFormat.getPayload),
      eventSerializer.vectorTime(versionedFormat.getUpdateTimestamp),
      versionedFormat.getCreator)
  }
}
