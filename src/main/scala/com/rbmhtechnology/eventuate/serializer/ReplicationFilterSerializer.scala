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
import com.google.protobuf.ByteString

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.serializer.ReplicationFilterFormats._

import scala.collection.JavaConverters._
import scala.language.existentials

class ReplicationFilterSerializer(system: ExtendedActorSystem) extends Serializer {
  import ReplicationFilterTreeFormat.NodeType._

  val ExclusionFilterClass = classOf[SourceLogIdExclusionFilter]
  val AndFilterClass = classOf[AndFilter]
  val OrFilterClass= classOf[OrFilter]

  override def identifier: Int = 22564
  override def includeManifest: Boolean = true

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case f: SourceLogIdExclusionFilter =>
      exclusionFilterFormatBuilder(f).build().toByteArray
    case f: ReplicationFilter =>
      filterTreeFormatBuilder(f).build().toByteArray
    case _ =>
      throw new IllegalArgumentException(s"can't serialize object of type ${o.getClass}")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
    case None        => throw new IllegalArgumentException("manifest required")
    case Some(clazz) => clazz match {
      case ExclusionFilterClass =>
        exclusionFilter(SourceLogIdExclusionFilterFormat.parseFrom(bytes))
      case AndFilterClass | OrFilterClass =>
        filterTree(ReplicationFilterTreeFormat.parseFrom(bytes))
      case _ =>
        throw new IllegalArgumentException(s"can't deserialize object of type ${clazz}")
    }
  }

  // --------------------------------------------------------------------------------
  //  toBinary helpers
  // --------------------------------------------------------------------------------

  def filterTreeFormatBuilder(filterTree: ReplicationFilter): ReplicationFilterTreeFormat.Builder = {
    val builder = ReplicationFilterTreeFormat.newBuilder()
    filterTree match {
      case AndFilter(filters) =>
        builder.setNodeType(AND)
        filters.foreach(filter => builder.addChildren(filterTreeFormatBuilder(filter)))
      case OrFilter(filters) =>
        builder.setNodeType(OR)
        filters.foreach(filter => builder.addChildren(filterTreeFormatBuilder(filter)))
      case filter =>
        builder.setNodeType(LEAF)
        builder.setFilter(filterLeafFormatBuilder(filter))
    }
    builder
  }

  private def filterLeafFormatBuilder(filterLeaf: ReplicationFilter): ReplicationFilterLeafFormat.Builder = {
    val serializer = SerializationExtension(system).findSerializerFor(filterLeaf)
    val builder = ReplicationFilterLeafFormat.newBuilder()

    if (serializer.includeManifest)
      builder.setFilterManifest(ByteString.copyFromUtf8(filterLeaf.getClass.getName))

    builder.setFilter(ByteString.copyFrom(serializer.toBinary(filterLeaf)))
    builder.setSerializerId(serializer.identifier)
    builder
  }

  private def exclusionFilterFormatBuilder(filter: SourceLogIdExclusionFilter): SourceLogIdExclusionFilterFormat.Builder = {
    val builder = SourceLogIdExclusionFilterFormat.newBuilder()

    builder.setSourceLogId(filter.sourceLogId)
    builder
  }

  // --------------------------------------------------------------------------------
  //  fromBinary helpers
  // --------------------------------------------------------------------------------

  def filterTree(filterTreeFormat: ReplicationFilterTreeFormat): ReplicationFilter = {
    filterTreeFormat.getNodeType match {
      case AND => AndFilter(filterTreeFormat.getChildrenList.asScala.map(filterTree).toList)
      case OR => OrFilter(filterTreeFormat.getChildrenList.asScala.map(filterTree).toList)
      case LEAF => filterLeaf(filterTreeFormat.getFilter)
    }
  }

  private def filterLeaf(filterMessage: ReplicationFilterLeafFormat): ReplicationFilter = {
    val filterClass = if (filterMessage.hasFilterManifest)
      Some(system.dynamicAccess.getClassFor[ReplicationFilter](filterMessage.getFilterManifest.toStringUtf8).get) else None

    SerializationExtension(system).deserialize(
      filterMessage.getFilter.toByteArray,
      filterMessage.getSerializerId,
      filterClass).get
  }

  private def exclusionFilter(exclusionFilterFormat: SourceLogIdExclusionFilterFormat): SourceLogIdExclusionFilter = {
    SourceLogIdExclusionFilter(exclusionFilterFormat.getSourceLogId)
  }
}
