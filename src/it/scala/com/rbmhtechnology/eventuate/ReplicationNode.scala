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

package com.rbmhtechnology.eventuate

import java.io.File

import scala.collection.immutable.Seq

import akka.actor._

import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog
import com.typesafe.config.ConfigFactory

import org.apache.commons.io.FileUtils

class ReplicationNode(nodeId: String, logIds: Set[String], port: Int, connections: Set[ReplicationConnection]) {
  val config = ConfigFactory.parseString(
    s"""
      |akka.remote.netty.tcp.hostname = "127.0.0.1"
      |akka.remote.netty.tcp.port = ${port}
      |akka.test.single-expect-default = 10s
      |
      |log.leveldb.dir = target/logs-system-${nodeId}
      |log.replication.batch-size-max = 3
      |log.replication.retry-interval = 1s
      |log.replication.failure-detection-limit = 3s
    """.stripMargin)

  val system: ActorSystem =
    ActorSystem(ReplicationConnection.DefaultRemoteSystemName, config)

  cleanup()
  storageLocation.mkdirs()

  val endpoint: ReplicationEndpoint =
    new ReplicationEndpoint(nodeId, logIds, id => LeveldbEventLog.props(id), connections)(system)

  def logs: Map[String, ActorRef] =
    endpoint.logs

  def shutdown(): Unit = {
    system.shutdown()
  }

  def awaitTermination(): Unit = {
    system.awaitTermination()
  }

  def cleanup(): Unit = {
    FileUtils.deleteDirectory(storageLocation)
  }

  private def storageLocation: File =
    new File(system.settings.config.getString("log.leveldb.dir"))

  private def localEndpoint(port: Int) =
    s""""127.0.0.1:${port}""""
}
