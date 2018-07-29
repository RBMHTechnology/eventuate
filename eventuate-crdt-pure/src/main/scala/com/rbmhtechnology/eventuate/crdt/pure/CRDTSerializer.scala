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

package com.rbmhtechnology.eventuate.crdt.pure

import akka.actor._
import akka.serialization.Serializer
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.crdt.pure.CRDTFormats._
import com.rbmhtechnology.eventuate.crdt.pure.CRDTService._
import com.rbmhtechnology.eventuate.serializer.CommonSerializer

import scala.collection.JavaConverters._

class CRDTSerializer(system: ExtendedActorSystem) extends Serializer {
  val commonSerializer = new CommonSerializer(system)

  import commonSerializer.payloadSerializer

  private val ValueUpdatedClass = classOf[ValueUpdated]
  private val CRDTClass = classOf[CRDT[_]]
  private val UpdatedOpClass = classOf[UpdateOp]
  private val AssignOpClass = classOf[AssignOp]
  private val AddOpClass = classOf[AddOp]
  private val RemoveOpClass = classOf[RemoveOp]
  private val AWCartEntryClass = classOf[AWCartEntry[_]]
  private val ClearClass = ClearOp.getClass

  override def identifier: Int = 22567

  override def includeManifest: Boolean = true

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case c: CRDT[_] =>
      crdtFormatBuilder(c).build().toByteArray
    case v: ValueUpdated =>
      valueUpdatedFormat(v).build().toByteArray
    case o: UpdateOp =>
      updateOpFormatBuilder(o).build().toByteArray
    case o: AddOp =>
      addOpFormatBuilder(o).build().toByteArray
    case o: RemoveOp =>
      removeOpFormatBuilder(o).build().toByteArray
    case o: AssignOp =>
      assignOpFormatBuilder(o).build().toByteArray
    case e: AWCartEntry[_] =>
      awCartEntryFormatBuilder(e).build().toByteArray
    case ClearOp =>
      ClearFormat.newBuilder().build().toByteArray
    case _ =>
      throw new IllegalArgumentException(s"can't serialize object of type ${o.getClass}")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
    case None => throw new IllegalArgumentException("manifest required")
    case Some(clazz) => clazz match {
      case CRDTClass =>
        crdt(CRDTPureOpFormat.parseFrom(bytes))
      case ValueUpdatedClass =>
        valueUpdated(ValueUpdatedFormat.parseFrom(bytes))
      case UpdatedOpClass =>
        updateOp(UpdateOpFormat.parseFrom(bytes))
      case AssignOpClass =>
        assignOp(AssignOpFormat.parseFrom(bytes))
      case AddOpClass =>
        addOp(AddOpFormat.parseFrom(bytes))
      case RemoveOpClass =>
        removeOp(RemoveOpFormat.parseFrom(bytes))
      case AWCartEntryClass =>
        awCartEntry(AWCartEntryFormat.parseFrom(bytes))
      case ClearClass => ClearOp
      case _ =>
        throw new IllegalArgumentException(s"can't deserialize object of type ${clazz}")
    }
  }

  // --------------------------------------------------------------------------------
  //  toBinary helpers
  // --------------------------------------------------------------------------------

  private def addOpFormatBuilder(op: AddOp): AddOpFormat.Builder =
    AddOpFormat.newBuilder.setEntry(payloadSerializer.payloadFormatBuilder(op.entry.asInstanceOf[AnyRef]))

  private def removeOpFormatBuilder(op: RemoveOp): RemoveOpFormat.Builder =
    RemoveOpFormat.newBuilder.setEntry(payloadSerializer.payloadFormatBuilder(op.entry.asInstanceOf[AnyRef]))

  private def assignOpFormatBuilder(op: AssignOp): AssignOpFormat.Builder =
    AssignOpFormat.newBuilder.setValue(payloadSerializer.payloadFormatBuilder(op.value.asInstanceOf[AnyRef]))

  private def updateOpFormatBuilder(op: UpdateOp): UpdateOpFormat.Builder =
    UpdateOpFormat.newBuilder.setDelta(payloadSerializer.payloadFormatBuilder(op.delta.asInstanceOf[AnyRef]))

  private def valueUpdatedFormat(valueUpdated: ValueUpdated): ValueUpdatedFormat.Builder =
    ValueUpdatedFormat.newBuilder.setOperation(payloadSerializer.payloadFormatBuilder(valueUpdated.operation.asInstanceOf[AnyRef]))

  private def awCartEntryFormatBuilder(orCartEntry: AWCartEntry[_]): AWCartEntryFormat.Builder = {
    val builder = AWCartEntryFormat.newBuilder

    builder.setKey(payloadSerializer.payloadFormatBuilder(orCartEntry.key.asInstanceOf[AnyRef]))
    builder.setQuantity(orCartEntry.quantity)
    builder
  }

  private def pologBuilder(polog: POLog): POLogFormat.Builder = {
    val builder = POLogFormat.newBuilder

    polog.log.foreach { ve =>
      builder.addVersionedEntries(commonSerializer.versionedFormatBuilder(ve))
    }

    builder
  }

  private def crdtFormatBuilder(c: CRDT[_]): CRDTPureOpFormat.Builder = {
    CRDTPureOpFormat.newBuilder.setPolog(pologBuilder(c.polog)).setState(payloadSerializer.payloadFormatBuilder(c.state.asInstanceOf[AnyRef]))
  }

  // --------------------------------------------------------------------------------
  //  fromBinary helpers
  // --------------------------------------------------------------------------------

  private def addOp(opFormat: AddOpFormat): AddOp =
    AddOp(payloadSerializer.payload(opFormat.getEntry))

  private def removeOp(opFormat: RemoveOpFormat): RemoveOp =
    RemoveOp(payloadSerializer.payload(opFormat.getEntry))

  private def assignOp(opFormat: AssignOpFormat): AssignOp =
    AssignOp(payloadSerializer.payload(opFormat.getValue))

  private def updateOp(opFormat: UpdateOpFormat): UpdateOp =
    UpdateOp(payloadSerializer.payload(opFormat.getDelta))

  private def valueUpdated(valueUpdatedFormat: ValueUpdatedFormat): ValueUpdated =
    ValueUpdated(payloadSerializer.payload(valueUpdatedFormat.getOperation))

  private def awCartEntry(orCartEntryFormat: AWCartEntryFormat): AWCartEntry[Any] =
    AWCartEntry(payloadSerializer.payload(orCartEntryFormat.getKey), orCartEntryFormat.getQuantity)

  private def polog(pologFormat: POLogFormat): POLog = {
    val rs = pologFormat.getVersionedEntriesList.iterator.asScala.foldLeft(Set.empty[Versioned[Any]]) {
      case (acc, r) => acc + commonSerializer.versioned(r)
    }

    POLog(rs)
  }

  private def crdt(crdtFormat: CRDTPureOpFormat): CRDT[_] =
    CRDT(polog(crdtFormat.getPolog), payloadSerializer.payload(crdtFormat.getState))

}

