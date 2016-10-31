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

import com.datastax.driver.core.{ Cluster, ConsistencyLevel }
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationLong
import scala.concurrent.duration.FiniteDuration

case class CassandraSettings(config: Config) {
  import CassandraSettings._

  val keyspace: String =
    config.getString("eventuate.cassandra.keyspace")

  val keyspaceAutoCreate: Boolean =
    config.getBoolean("eventuate.cassandra.keyspace-autocreate")

  val replicationFactor: Int =
    config.getInt("eventuate.cassandra.replication-factor")

  val tablePrefix: String =
    config.getString("eventuate.cassandra.table-prefix")

  val defaultPort: Int =
    config.getInt("eventuate.cassandra.default-port")

  val contactPoints =
    getContactPoints(config.getStringList("eventuate.cassandra.contact-points").asScala, defaultPort)

  def readConsistency: ConsistencyLevel =
    ConsistencyLevel.valueOf(config.getString(s"eventuate.cassandra.${CassandraReadConsistency}"))

  def writeConsistency: ConsistencyLevel =
    ConsistencyLevel.valueOf(config.getString(s"eventuate.cassandra.${CassandraWriteConsistency}"))

  val connectRetryMax: Int =
    config.getInt("eventuate.cassandra.connect-retry-max")

  val connectRetryDelay: FiniteDuration =
    config.getDuration("eventuate.cassandra.connect-retry-delay", TimeUnit.MILLISECONDS).millis

  val clusterBuilder: Cluster.Builder =
    Cluster.builder.addContactPointsWithPorts(contactPoints.asJava).withCredentials(
      config.getString("eventuate.cassandra.username"),
      config.getString("eventuate.cassandra.password"))
}

private object CassandraSettings {
  val CassandraReadConsistency = "read-consistency"
  val CassandraWriteConsistency = "write-consistency"

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
