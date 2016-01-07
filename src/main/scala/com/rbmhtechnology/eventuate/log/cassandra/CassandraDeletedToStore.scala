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

package com.rbmhtechnology.eventuate.log.cassandra

import java.lang.{ Long => JLong }

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

private[eventuate] class CassandraDeletedToStore(cassandra: Cassandra, logId: String) {

  def writeDeletedTo(deletedTo: Long): Unit =
    cassandra.execute(cassandra.preparedWriteDeletedToStatement.bind(logId, deletedTo: JLong), cassandra.settings.writeTimeout)

  def readDeletedToAsync(implicit executor: ExecutionContext): Future[Long] = {
    cassandra.session.executeAsync(cassandra.preparedReadDeletedToStatement.bind(logId)).map { resultSet =>
      if (resultSet.isExhausted) 0L else resultSet.one().getLong("deleted_to")
    }
  }
}
