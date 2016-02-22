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

import akka.actor._
import akka.serialization.Serializer

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.crdt._
import com.rbmhtechnology.eventuate.serializer.CRDTFormats._

import scala.collection.JavaConverters._
import scala.collection.immutable.VectorBuilder

class CRDTSerializer(system: ExtendedActorSystem) extends Serializer {
  val commonSerializer = new CommonSerializer(system)

  val MVRegisterClass = classOf[MVRegister[_]]
  val LWWRegisterClass = classOf[LWWRegister[_]]
  val ORSetClass = classOf[ORSet[_]]

  override def identifier: Int = 22567
  override def includeManifest: Boolean = true

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case r: MVRegister[_] =>
      mvRegisterFormatBuilder(r).build().toByteArray
    case r: LWWRegister[_] =>
      lwwRegisterFormatBuilder(r).build().toByteArray
    case s: ORSet[_] =>
      orSetFormatBuilder(s).build().toByteArray
    case _ =>
      throw new IllegalArgumentException(s"can't serialize object of type ${o.getClass}")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
    case None => throw new IllegalArgumentException("manifest required")
    case Some(clazz) => clazz match {
      case MVRegisterClass =>
        mvRegister(MVRegisterFormat.parseFrom(bytes))
      case LWWRegisterClass =>
        lwwRegister(LWWRegisterFormat.parseFrom(bytes))
      case ORSetClass =>
        orSet(ORSetFormat.parseFrom(bytes))
      case _ =>
        throw new IllegalArgumentException(s"can't deserialize object of type ${clazz}")
    }
  }

  // --------------------------------------------------------------------------------
  //  toBinary helpers
  // --------------------------------------------------------------------------------

  private def lwwRegisterFormatBuilder(lwwRegister: LWWRegister[_]): LWWRegisterFormat.Builder = {
    val builder = LWWRegisterFormat.newBuilder
    builder.setMvRegister(mvRegisterFormatBuilder(lwwRegister.mvRegister))
  }

  private def mvRegisterFormatBuilder(mVRegister: MVRegister[_]): MVRegisterFormat.Builder = {
    val builder = MVRegisterFormat.newBuilder

    mVRegister.versioned.foreach { r =>
      builder.addVersioned(commonSerializer.versionedFormatBuilder(r))
    }

    builder
  }

  private def orSetFormatBuilder(orSet: ORSet[_]): ORSetFormat.Builder = {
    val builder = ORSetFormat.newBuilder

    orSet.versionedEntries.foreach { ve =>
      builder.addVersionedEntries(commonSerializer.versionedFormatBuilder(ve))
    }

    builder
  }

  // --------------------------------------------------------------------------------
  //  fromBinary helpers
  // --------------------------------------------------------------------------------

  private def lwwRegister(lwwRegisterFormat: LWWRegisterFormat): LWWRegister[Any] = {
    LWWRegister(mvRegister(lwwRegisterFormat.getMvRegister))
  }

  private def mvRegister(mvRegisterFormat: MVRegisterFormat): MVRegister[Any] = {
    val builder = new VectorBuilder[Versioned[Any]]

    val rs = mvRegisterFormat.getVersionedList.iterator.asScala.foldLeft(Set.empty[Versioned[Any]]) {
      case (acc, r) => acc + commonSerializer.versioned(r)
    }

    MVRegister(rs)
  }

  private def orSet(orSetFormat: ORSetFormat): ORSet[Any] = {
    val builder = new VectorBuilder[Versioned[Any]]

    val ves = orSetFormat.getVersionedEntriesList.iterator.asScala.foldLeft(Set.empty[Versioned[Any]]) {
      case (acc, ve) => acc + commonSerializer.versioned(ve)
    }

    ORSet(ves)
  }
}
