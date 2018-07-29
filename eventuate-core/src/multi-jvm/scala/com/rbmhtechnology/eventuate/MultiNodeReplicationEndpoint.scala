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

package com.rbmhtechnology.eventuate

import akka.actor._
import akka.remote.testkit.MultiNodeSpec

import org.scalatest.BeforeAndAfterAll

trait MultiNodeReplicationEndpoint extends BeforeAndAfterAll { this: MultiNodeSpec with MultiNodeWordSpec =>
  def logName: String = {
    val cn = getClass.getSimpleName
    cn.substring(0, cn.lastIndexOf("MultiJvm"))
  }

  def createEndpoint(endpointId: String, connections: Set[ReplicationConnection]): ReplicationEndpoint =
    createEndpoint(endpointId, Set(logName), connections)

  def createEndpoint(endpointId: String, logNames: Set[String], connections: Set[ReplicationConnection], activate: Boolean = true): ReplicationEndpoint = {
    val endpoint = new ReplicationEndpoint(endpointId, logNames, id => logProps(id), connections)
    if (activate) endpoint.activate()
    endpoint
  }

  implicit class RichAddress(address: Address) {
    def toReplicationConnection: ReplicationConnection =
      ReplicationConnection(address.host.get, address.port.get, address.system)
  }

  implicit class RichReplicationEndpoint(endpoint: ReplicationEndpoint) {
    def log: ActorRef =
      endpoint.logs(logName)

    def logId: String =
      endpoint.logId(logName)
  }

  def logProps(logId: String): Props
}
