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

package com.rbmhtechnology.eventuate.crdt

import akka.actor._
import akka.serialization.Serializer

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.crdt.CRDTFormats._
import com.rbmhtechnology.eventuate.crdt.CRDTService._
import com.rbmhtechnology.eventuate.serializer.CommonSerializer

import scala.collection.JavaConverters._

class CRDTSerializer(system: ExtendedActorSystem) extends Serializer {
  val commonSerializer = new CommonSerializer(system)

  private val MVRegisterClass = classOf[MVRegister[_]]
  private val LWWRegisterClass = classOf[LWWRegister[_]]
  private val ORSetClass = classOf[ORSet[_]]
  private val ORCartClass = classOf[ORCart[_]]
  private val ORCartEntryClass = classOf[ORCartEntry[_]]
  private val ValueUpdatedClass = classOf[ValueUpdated]
  private val UpdatedOpClass = classOf[UpdateOp]
  private val SetOpClass = classOf[SetOp]
  private val AddOpClass = classOf[AddOp]
  private val RemoveOpClass = classOf[RemoveOp]

  override def identifier: Int = 22567
  override def includeManifest: Boolean = true

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case r: MVRegister[_] =>
      mvRegisterFormatBuilder(r).build().toByteArray
    case r: LWWRegister[_] =>
      lwwRegisterFormatBuilder(r).build().toByteArray
    case s: ORSet[_] =>
      orSetFormatBuilder(s).build().toByteArray
    case s: ORCart[_] =>
      orCartFormatBuilder(s).build().toByteArray
    case s: ORCartEntry[_] =>
      orCartEntryFormatBuilder(s).build().toByteArray
    case v: ValueUpdated =>
      valueUpdatedFormat(v).build().toByteArray
    case o: UpdateOp =>
      updateOpFormatBuilder(o).build().toByteArray
    case o: SetOp =>
      setOpFormatBuilder(o).build().toByteArray
    case o: AddOp =>
      addOpFormatBuilder(o).build().toByteArray
    case o: RemoveOp =>
      removeOpFormatBuilder(o).build().toByteArray
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
      case ORCartClass =>
        orCart(ORCartFormat.parseFrom(bytes))
      case ORCartEntryClass =>
        orCartEntry(ORCartEntryFormat.parseFrom(bytes))
      case ValueUpdatedClass =>
        valueUpdated(ValueUpdatedFormat.parseFrom(bytes))
      case UpdatedOpClass =>
        updateOp(UpdateOpFormat.parseFrom(bytes))
      case SetOpClass =>
        setOp(SetOpFormat.parseFrom(bytes))
      case AddOpClass =>
        addOp(AddOpFormat.parseFrom(bytes))
      case RemoveOpClass =>
        removeOp(RemoveOpFormat.parseFrom(bytes))
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

  private def orCartFormatBuilder(orCart: ORCart[_]): ORCartFormat.Builder =
    ORCartFormat.newBuilder.setOrSet(orSetFormatBuilder(orCart.orSet))

  private def orCartEntryFormatBuilder(orCartEntry: ORCartEntry[_]): ORCartEntryFormat.Builder = {
    val builder = ORCartEntryFormat.newBuilder

    builder.setKey(commonSerializer.payloadFormatBuilder(orCartEntry.key.asInstanceOf[AnyRef]))
    builder.setQuantity(orCartEntry.quantity)
    builder
  }

  private def valueUpdatedFormat(valueUpdated: ValueUpdated): ValueUpdatedFormat.Builder =
    ValueUpdatedFormat.newBuilder.setOperation(commonSerializer.payloadFormatBuilder(valueUpdated.operation.asInstanceOf[AnyRef]))

  private def updateOpFormatBuilder(op: UpdateOp): UpdateOpFormat.Builder =
    UpdateOpFormat.newBuilder.setDelta(commonSerializer.payloadFormatBuilder(op.delta.asInstanceOf[AnyRef]))

  private def setOpFormatBuilder(op: SetOp): SetOpFormat.Builder =
    SetOpFormat.newBuilder.setValue(commonSerializer.payloadFormatBuilder(op.value.asInstanceOf[AnyRef]))

  private def addOpFormatBuilder(op: AddOp): AddOpFormat.Builder =
    AddOpFormat.newBuilder.setEntry(commonSerializer.payloadFormatBuilder(op.entry.asInstanceOf[AnyRef]))

  private def removeOpFormatBuilder(op: RemoveOp): RemoveOpFormat.Builder = {
    val builder = RemoveOpFormat.newBuilder

    builder.setEntry(commonSerializer.payloadFormatBuilder(op.entry.asInstanceOf[AnyRef]))

    op.timestamps.foreach { timestamp =>
      builder.addTimestamps(commonSerializer.vectorTimeFormatBuilder(timestamp))
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
    val rs = mvRegisterFormat.getVersionedList.iterator.asScala.foldLeft(Set.empty[Versioned[Any]]) {
      case (acc, r) => acc + commonSerializer.versioned(r)
    }

    MVRegister(rs)
  }

  private def orSet(orSetFormat: ORSetFormat): ORSet[Any] = {
    val ves = orSetFormat.getVersionedEntriesList.iterator.asScala.foldLeft(Set.empty[Versioned[Any]]) {
      case (acc, ve) => acc + commonSerializer.versioned(ve)
    }

    ORSet(ves)
  }

  private def orCart(orCartFormat: ORCartFormat): ORCart[Any] =
    ORCart(orSet(orCartFormat.getOrSet).asInstanceOf[ORSet[ORCartEntry[Any]]])

  private def orCartEntry(orCartEntryFormat: ORCartEntryFormat): ORCartEntry[Any] =
    ORCartEntry(commonSerializer.payload(orCartEntryFormat.getKey), orCartEntryFormat.getQuantity)

  private def valueUpdated(valueUpdatedFormat: ValueUpdatedFormat): ValueUpdated =
    ValueUpdated(commonSerializer.payload(valueUpdatedFormat.getOperation))

  private def updateOp(opFormat: UpdateOpFormat): UpdateOp =
    UpdateOp(commonSerializer.payload(opFormat.getDelta))

  private def setOp(opFormat: SetOpFormat): SetOp =
    SetOp(commonSerializer.payload(opFormat.getValue))

  private def addOp(opFormat: AddOpFormat): AddOp =
    AddOp(commonSerializer.payload(opFormat.getEntry))

  private def removeOp(opFormat: RemoveOpFormat): RemoveOp = {
    val timestamps = opFormat.getTimestampsList.iterator().asScala.foldLeft(Set.empty[VectorTime]) {
      case (result, timestampFormat) => result + commonSerializer.vectorTime(timestampFormat)
    }

    RemoveOp(commonSerializer.payload(opFormat.getEntry), timestamps)
  }
}
