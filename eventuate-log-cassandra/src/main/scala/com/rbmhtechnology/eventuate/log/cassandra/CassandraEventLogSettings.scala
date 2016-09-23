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

import java.util.concurrent.TimeUnit

import akka.util.Helpers.Requiring
import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.config.Config
import com.rbmhtechnology.eventuate.log._

import scala.concurrent.duration.{ DurationLong, FiniteDuration }

class CassandraEventLogSettings(config: Config) extends EventLogSettings {
  import CassandraEventLogSettings._

  def writeTimeout: Long =
    config.getDuration(WriteTimeout, TimeUnit.MILLISECONDS)

  def writeBatchSize: Int =
    config.getInt(WriteBatchSize)

  def readConsistency: ConsistencyLevel =
    ConsistencyLevel.valueOf(config.getString(CassandraSettings.CassandraReadConsistency))

  def writeConsistency: ConsistencyLevel =
    ConsistencyLevel.valueOf(config.getString(CassandraSettings.CassandraWriteConsistency))

  def writeRetryMax: Int =
    config.getInt(CassandraWriteRetryMax)

  override def partitionSize: Long =
    config.getLong(CassandraPartitionSize)
      .requiring(_ > writeBatchSize,
        s"partition-size must be greater than write-batch-size ($writeBatchSize)")

  val indexUpdateLimit: Int =
    config.getInt(IndexUpdateLimit)

  val initRetryMax: Int =
    config.getInt(InitRetryMax)

  val initRetryDelay: FiniteDuration =
    config.getDuration(InitRetryDelay, TimeUnit.MILLISECONDS).millis

  def deletionRetryDelay: FiniteDuration =
    ???
}

object CassandraEventLogSettings {
  val WriteBatchSize = "write-batch-size"
  val WriteTimeout = "write-timeout"
  val CassandraPartitionSize = "partition-size"
  val CassandraWriteRetryMax = "write-retry-max"
  val InitRetryMax = "init-retry-max"
  val InitRetryDelay = "init-retry-delay"
  val IndexUpdateLimit = "index-update-limit"
}
