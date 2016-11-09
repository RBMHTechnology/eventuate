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

class CassandraEventLogSettings(config: Config) extends EventLogSettings {
  import CassandraEventLogSettings._

  val writeTimeout: Long =
    config.getDuration("eventuate.log.write-timeout", TimeUnit.MILLISECONDS)

  val writeBatchSize: Int =
    config.getInt("eventuate.log.write-batch-size")

  val keyspace: String =
    config.getString("eventuate.log.cassandra.keyspace")

  val keyspaceAutoCreate: Boolean =
    config.getBoolean("eventuate.log.cassandra.keyspace-autocreate")

  val replicationFactor: Int =
    config.getInt("eventuate.log.cassandra.replication-factor")

  val tablePrefix: String =
    config.getString("eventuate.log.cassandra.table-prefix")

  val readConsistency: ConsistencyLevel =
    ConsistencyLevel.valueOf(config.getString("eventuate.log.cassandra.read-consistency"))

  val writeConsistency: ConsistencyLevel =
    ConsistencyLevel.valueOf(config.getString("eventuate.log.cassandra.write-consistency"))

  val writeRetryMax: Int =
    config.getInt("eventuate.log.cassandra.write-retry-max")

  val defaultPort: Int =
    config.getInt("eventuate.log.cassandra.default-port")

  val contactPoints =
    getContactPoints(config.getStringList("eventuate.log.cassandra.contact-points").asScala, defaultPort)

  val partitionSize: Long =
    config.getLong("eventuate.log.cassandra.partition-size")
      .requiring(
        _ > writeBatchSize,
        s"eventuate.log.cassandra.partition-size must be greater than eventuate.log.write-batch-size (${writeBatchSize})")

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
