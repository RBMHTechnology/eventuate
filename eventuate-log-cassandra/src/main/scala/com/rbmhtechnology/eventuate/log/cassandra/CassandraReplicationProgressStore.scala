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

package com.rbmhtechnology.eventuate.log.cassandra

import java.lang.{ Long => JLong }

import com.datastax.driver.core.ResultSetFuture

import scala.collection.JavaConverters._
import scala.concurrent._

private[eventuate] class CassandraReplicationProgressStore(cassandra: Cassandra, logId: String) {
  def readReplicationProgressesAsync(implicit executor: ExecutionContext): Future[Map[String, Long]] =
    cassandra.session.executeAsync(cassandra.preparedReadReplicationProgressesStatement.bind(logId)).map { resultSet =>
      resultSet.iterator().asScala.foldLeft(Map.empty[String, Long]) {
        case (acc, row) => acc + (row.getString("source_log_id") -> row.getLong("source_log_read_pos"))
      }
    }

  def readReplicationProgressAsync(sourceLogId: String)(implicit executor: ExecutionContext): Future[Long] =
    cassandra.session.executeAsync(cassandra.preparedReadReplicationProgressStatement.bind(logId, sourceLogId)).map { resultSet =>
      if (resultSet.isExhausted) 0L else resultSet.one().getLong("source_log_read_pos")
    }

  def writeReplicationProgressesAsync(progresses: Map[String, Long])(implicit executor: ExecutionContext): Future[Unit] =
    Future.sequence(progresses.map(p => listenableFutureToFuture(cassandra.session.executeAsync(cassandra.preparedWriteReplicationProgressStatement.bind(logId, p._1, p._2: JLong))))).map(_ => ())
}
