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

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import com.datastax.driver.core.{Cluster, ConsistencyLevel}
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.concurrent.duration._

private[eventuate] class CassandraConfig(config: Config) {
  import CassandraConfig._

  val keyspace: String =
    config.getString("keyspace")

  val keyspaceAutoCreate: Boolean =
    config.getBoolean("keyspace-autocreate")

  val replicationFactor: Int =
    config.getInt("replication-factor")

  val tablePrefix: String =
    config.getString("table-prefix")

  val readConsistency: ConsistencyLevel =
    ConsistencyLevel.valueOf(config.getString("read-consistency"))

  val writeConsistency: ConsistencyLevel =
    ConsistencyLevel.valueOf(config.getString("write-consistency"))

  val defaultPort: Int =
    config.getInt("default-port")

  val contactPoints =
    getContactPoints(config.getStringList("contact-points").asScala, defaultPort)

  val maxResultSetSize: Int =
    config.getInt("max-result-set-size")

  val initRetryBackoff: FiniteDuration =
    config.getDuration("init-retry-backoff", TimeUnit.MILLISECONDS).millis

  val indexUpdateLimit: Int =
    config.getInt("index-update-limit")

  val clusterBuilder: Cluster.Builder =
    Cluster.builder.addContactPointsWithPorts(contactPoints.asJava)

  if (config.hasPath("authentication")) {
    clusterBuilder.withCredentials(
      config.getString("authentication.username"),
      config.getString("authentication.password"))
  }
}

private object CassandraConfig {
  def getContactPoints(contactPoints: Seq[String], defaultPort: Int): Seq[InetSocketAddress] = {
    contactPoints match {
      case null | Nil => throw new IllegalArgumentException("a contact point list cannot be empty.")
      case hosts => hosts map {
        ipWithPort => ipWithPort.split(":") match {
          case Array(host, port) => new InetSocketAddress(host, port.toInt)
          case Array(host) => new InetSocketAddress(host, defaultPort)
          case msg => throw new IllegalArgumentException(s"a contact point should have the form [host:port] or [host] but was: $msg.")
        }
      }
    }
  }
}
