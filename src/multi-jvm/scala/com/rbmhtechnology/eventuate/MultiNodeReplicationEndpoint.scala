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

import akka.actor.{ActorRef, Address}
import akka.remote.testkit.MultiNodeSpec

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll

import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog

trait MultiNodeReplicationEndpoint extends BeforeAndAfterAll { this: MultiNodeSpec with MultiNodeWordSpec =>
  private val logPrefix = "log"
  private var logId = ""

  def logName: String = {
    val cn = getClass.getSimpleName
    cn.substring(0, cn.lastIndexOf("MultiJvm"))
  }

  def createEndpoint(endpointId: String, connections: Set[ReplicationConnection]): ReplicationEndpoint =
    createEndpoint(endpointId, Set(logName), connections)

  def createEndpoint(endpointId: String, logNames: Set[String], connections: Set[ReplicationConnection]): ReplicationEndpoint = {
    new ReplicationEndpoint(endpointId, logNames, id => { logId = id; LeveldbEventLog.props(id, logPrefix) }, connections)
  }

  implicit class RichAddress(address: Address) {
    def toReplicationConnection: ReplicationConnection =
      ReplicationConnection(address.host.get, address.port.get, address.system)
  }

  implicit class RichReplicationEndpoint(endpoint: ReplicationEndpoint) {
    def log: ActorRef =
      endpoint.logs(logName)
  }

  override def afterAll(): Unit = {
    // get all config data before shutting down node
    val logRootDir = new File(system.settings.config.getString("eventuate.log.leveldb.dir"))
    val logDir = new File(logRootDir, s"${logPrefix}-${logId}")

    // shut down node
    super.afterAll()

    // delete log files
    FileUtils.deleteDirectory(logDir)
  }
}
