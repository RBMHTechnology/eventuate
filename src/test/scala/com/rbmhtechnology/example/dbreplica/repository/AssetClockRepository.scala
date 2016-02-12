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

import java.sql.ResultSet

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension

import com.rbmhtechnology.eventuate.VectorTime

import org.apache.commons.io.IOUtils
import org.hsqldb.jdbc.JDBCBlob
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core._
import org.springframework.stereotype.Repository

import scala.collection.JavaConverters._

@Repository
class AssetClockRepository @Autowired() (template: JdbcTemplate, system: ActorSystem) {
  import IOUtils._

  private val serialization = SerializationExtension(system)

  def find(assetId: String): Option[VectorTime] =
    template.query("SELECT * FROM AssetClock WHERE assetId = ? FOR UPDATE", Array[AnyRef](assetId), assetClockMapper).asScala.headOption

  def insert(assetId: String, clock: VectorTime): Int =
    template.update("INSERT INTO AssetClock (assetId, vectorClock) VALUES (?, ?)", assetId, new JDBCBlob(serializeVectorTime(clock)))

  def update(assetId: String, clock: VectorTime): Int =
    template.update("UPDATE AssetClock SET vectorClock = ? WHERE assetId = ?", serializeVectorTime(clock), assetId)

  def delete(assetId: String): Int =
    template.update("DELETE FROM AssetClock WHERE assetId = ?", assetId)

  private def serializeVectorTime(vectorTime: VectorTime): Array[Byte] =
    serialization.serialize(vectorTime).get

  private def deserializeVectorTime(vectorTimeBytes: Array[Byte]): VectorTime =
    serialization.deserialize(vectorTimeBytes, classOf[VectorTime]).get

  private val assetClockMapper = new RowMapper[VectorTime] {
    override def mapRow(rs: ResultSet, rowNum: Int): VectorTime =
      deserializeVectorTime(toByteArray(rs.getBlob("vectorClock").getBinaryStream))
  }
}

