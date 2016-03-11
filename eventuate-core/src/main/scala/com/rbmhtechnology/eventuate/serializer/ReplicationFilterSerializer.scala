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

import com.rbmhtechnology.eventuate.ReplicationFilter.AndFilter
import com.rbmhtechnology.eventuate.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.ReplicationFilter.OrFilter

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.serializer.ReplicationFilterFormats._

import scala.collection.JavaConverters._
import scala.language.existentials

class ReplicationFilterSerializer(system: ExtendedActorSystem) extends Serializer {
  import ReplicationFilterTreeFormat.NodeType._

  val commonSerializer = new CommonSerializer(system)

  val AndFilterClass = classOf[AndFilter]
  val OrFilterClass = classOf[OrFilter]
  val NoFilterClass = NoFilter.getClass

  override def identifier: Int = 22564
  override def includeManifest: Boolean = true

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case NoFilter =>
      NoFilterFormat.newBuilder().build().toByteArray
    case f: ReplicationFilter =>
      filterTreeFormatBuilder(f).build().toByteArray
    case _ =>
      throw new IllegalArgumentException(s"can't serialize object of type ${o.getClass}")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
    case None => throw new IllegalArgumentException("manifest required")
    case Some(clazz) => clazz match {
      case NoFilterClass =>
        NoFilter
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
        builder.setFilter(commonSerializer.payloadFormatBuilder(filter))
    }
    builder
  }

  // --------------------------------------------------------------------------------
  //  fromBinary helpers
  // --------------------------------------------------------------------------------

  def filterTree(filterTreeFormat: ReplicationFilterTreeFormat): ReplicationFilter = {
    filterTreeFormat.getNodeType match {
      case AND  => AndFilter(filterTreeFormat.getChildrenList.asScala.map(filterTree).toList)
      case OR   => OrFilter(filterTreeFormat.getChildrenList.asScala.map(filterTree).toList)
      case LEAF => commonSerializer.payload(filterTreeFormat.getFilter).asInstanceOf[ReplicationFilter]
    }
  }
}
