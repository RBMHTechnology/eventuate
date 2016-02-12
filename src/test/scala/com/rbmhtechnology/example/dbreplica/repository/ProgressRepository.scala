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
import java.sql.{ Connection, ResultSet }

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core._
import org.springframework.stereotype.Repository

import scala.collection.JavaConverters._

@Repository
class ProgressRepository @Autowired() (template: JdbcTemplate) {
  def readReplicationProgress(logId: String): Long =
    template.query("SELECT progress FROM ReplicationProgress WHERE logId = ?", Array[AnyRef](logId), ProgressMapper).asScala.headOption.getOrElse(0L)

  def writeReplicationProgress(logId: String, progress: Long): Unit =
    template.execute(new WriteCallback(logId, progress))
}

private object ProgressMapper extends RowMapper[Long] {
  override def mapRow(rs: ResultSet, rowNum: Int): Long =
    rs.getLong("progress")
}

private class WriteCallback(logId: String, progress: Long) extends ConnectionCallback[Unit] {
  override def doInConnection(connection: Connection): Unit = {
    insertOrUpdate(connection, s"SELECT * FROM ReplicationProgress WHERE logId = '$logId'") { rs =>
      rs.updateString("logId", logId)
      rs.updateLong("progress", progress: JLong)
    }
  }

  private def insertOrUpdate(connection: Connection, query: String)(operations: ResultSet => Unit) = {
    val stmt = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)
    val rs = stmt.executeQuery(query)
    if (rs.next()) {
      operations(rs)
      rs.updateRow()
    } else {
      rs.moveToInsertRow()
      operations(rs)
      rs.insertRow()
    }
  }
}

