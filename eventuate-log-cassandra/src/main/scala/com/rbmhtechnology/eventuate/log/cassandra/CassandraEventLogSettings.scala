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

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import akka.util.Helpers.Requiring

import com.datastax.driver.core.{ Cluster, ConsistencyLevel }
import com.typesafe.config.Config

import com.rbmhtechnology.eventuate.log._

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class CassandraEventLogSpecificSettings(config: Config, eventlog: String)
  extends CassandraEventLogSettings(config) {
  import CassandraEventLogSettings._

  private val getLong = get(config.getLong) _
  private val getInt = get(config.getInt) _
  private val getConsistency = get(key => ConsistencyLevel.valueOf(config.getString(key))) _
  private val getMilliseconds = get(key => config.getDuration(key, TimeUnit.MILLISECONDS)) _

  override val writeBatchSize: Int =
    getInt(WriteBatchSize, super.writeBatchSize)

  override val writeTimeout: Long =
    getMilliseconds(WriteTimeout, super.writeTimeout)

  override val partitionSize: Long =
    getLong(CassandraPartitionSize, super.partitionSize)

  override val writeRetryMax: Int =
    getInt(CassandraWriteRetryMax, super.writeRetryMax)

  override val readConsistency: ConsistencyLevel =
    getConsistency(CassandraReadConsistency, super.readConsistency)

  override val writeConsistency: ConsistencyLevel =
    getConsistency(CassandraWriteConsistency, super.writeConsistency)

  private def get[T](getter: String => T)(key: String, fallback: => T): T = {
    val settingKey = s"eventuate.log.eventlog.$eventlog.$key"
    if (config.hasPath(settingKey)) getter(settingKey) else fallback
  }
}

class CassandraEventLogSettings(config: Config) extends EventLogSettings {
  import CassandraEventLogSettings._

  def writeTimeout: Long =
    config.getDuration(s"eventuate.log.$WriteTimeout", TimeUnit.MILLISECONDS)

  def writeBatchSize: Int =
    config.getInt(s"eventuate.log.$WriteBatchSize")

  def readConsistency: ConsistencyLevel =
    ConsistencyLevel.valueOf(config.getString(s"eventuate.log.$CassandraReadConsistency"))

  def writeConsistency: ConsistencyLevel =
    ConsistencyLevel.valueOf(config.getString(s"eventuate.log.$CassandraWriteConsistency"))

  def writeRetryMax: Int =
    config.getInt(s"eventuate.log.$CassandraWriteRetryMax")

  override def partitionSize: Long =
    config.getLong(s"eventuate.log.$CassandraPartitionSize")
      .requiring(_ > writeBatchSize,
        s"eventuate.log.cassandra.partition-size must be greater than eventuate.log.write-batch-size (${writeBatchSize})")

  val keyspace: String =
    config.getString("eventuate.log.cassandra.keyspace")

  val keyspaceAutoCreate: Boolean =
    config.getBoolean("eventuate.log.cassandra.keyspace-autocreate")

  val replicationFactor: Int =
    config.getInt("eventuate.log.cassandra.replication-factor")

  val tablePrefix: String =
    config.getString("eventuate.log.cassandra.table-prefix")

  val defaultPort: Int =
    config.getInt("eventuate.log.cassandra.default-port")

  val contactPoints =
    getContactPoints(config.getStringList("eventuate.log.cassandra.contact-points").asScala, defaultPort)

  val indexUpdateLimit: Int =
    config.getInt("eventuate.log.cassandra.index-update-limit")

  val initRetryMax: Int =
    config.getInt("eventuate.log.cassandra.init-retry-max")

  val initRetryDelay: FiniteDuration =
    config.getDuration("eventuate.log.cassandra.init-retry-delay", TimeUnit.MILLISECONDS).millis

  def deletionRetryDelay: FiniteDuration =
    ???

  val connectRetryMax: Int =
    config.getInt("eventuate.log.cassandra.connect-retry-max")

  val connectRetryDelay: FiniteDuration =
    config.getDuration("eventuate.log.cassandra.connect-retry-delay", TimeUnit.MILLISECONDS).millis

  val clusterBuilder: Cluster.Builder =
    Cluster.builder.addContactPointsWithPorts(contactPoints.asJava).withCredentials(
      config.getString("eventuate.log.cassandra.username"),
      config.getString("eventuate.log.cassandra.password"))
}

private object CassandraEventLogSettings {
  val WriteBatchSize = "write-batch-size"
  val WriteTimeout = "write-timeout"
  val CassandraPartitionSize = "cassandra.partition-size"
  val CassandraWriteRetryMax = "cassandra.write-retry-max"
  val CassandraReadConsistency = "cassandra.read-consistency"
  val CassandraWriteConsistency = "cassandra.write-consistency"

  def getContactPoints(contactPoints: Seq[String], defaultPort: Int): Seq[InetSocketAddress] = {
    contactPoints match {
      case null | Nil => throw new IllegalArgumentException("a contact point list cannot be empty.")
      case hosts => hosts map {
        ipWithPort =>
          ipWithPort.split(":") match {
            case Array(host, port) => new InetSocketAddress(host, port.toInt)
            case Array(host)       => new InetSocketAddress(host, defaultPort)
            case msg               => throw new IllegalArgumentException(s"a contact point should have the form [host:port] or [host] but was: $msg.")
          }
      }
    }
  }
}
