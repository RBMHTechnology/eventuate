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

package com.rbmhtechnology.example.dbreplica.repository

import java.lang.{ Long => JLong }
import java.sql.ResultSet

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.log.EventLogClock

import org.apache.commons.io.IOUtils
import org.hsqldb.jdbc.JDBCBlob
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core._
import org.springframework.stereotype._
import org.springframework.transaction.annotation.Transactional

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.language.implicitConversions

@Repository
class AssetEventRepository @Autowired() (template: JdbcTemplate, system: ActorSystem) {
  import IOUtils._

  private val serialization = SerializationExtension(system)

  def readClock(clock: EventLogClock): EventLogClock =
    findFrom(clock.sequenceNr + 1L).foldLeft(clock)(_ update _)

  def findFrom(sequenceNr: Long): Seq[DurableEvent] =
    template.query("SELECT event FROM AssetEventLog WHERE id >= ? ORDER BY id ASC", Array[AnyRef](sequenceNr: JLong), eventMapper).asScala.toVector

  def findFor(assetId: String): Seq[DurableEvent] =
    template.query("SELECT event FROM AssetEventLog WHERE assetId = ? ORDER BY id ASC", Array[AnyRef](assetId), eventMapper).asScala.toVector

  def insert(assetId: String, event: DurableEvent): Int =
    template.update("INSERT INTO AssetEventLog (id, assetId, event) VALUES (?, ?, ?)", event.localSequenceNr: JLong, assetId, new JDBCBlob(serializeDurableEvent(event)))

  def updateSequenceNr(): Long =
    template.query("CALL NEXT VALUE FOR AssetEventLogSequence", sequenceNrMapper).asScala.head

  private def serializeVectorTime(vectorTime: VectorTime): Array[Byte] =
    serialization.serialize(vectorTime).get

  private def deserializeVectorTime(vectorTimeBytes: Array[Byte]): VectorTime =
    serialization.deserialize(vectorTimeBytes, classOf[VectorTime]).get

  private def serializeDurableEvent(event: DurableEvent): Array[Byte] =
    serialization.serialize(event).get

  private def deserializeDurableEvent(eventBytes: Array[Byte]): DurableEvent =
    serialization.deserialize(eventBytes, classOf[DurableEvent]).get

  private val sequenceNrMapper = new RowMapper[Long] {
    override def mapRow(rs: ResultSet, rowNum: Int): Long =
      rs.getLong(1)
  }

  private val eventMapper = new RowMapper[DurableEvent] {
    override def mapRow(rs: ResultSet, rowNum: Int): DurableEvent =
      deserializeDurableEvent(toByteArray(rs.getBlob(1).getBinaryStream))
  }

  private val versionMapper = new RowMapper[VectorTime] {
    override def mapRow(rs: ResultSet, rowNum: Int): VectorTime =
      deserializeVectorTime(toByteArray(rs.getBlob(1).getBinaryStream))
  }
}

