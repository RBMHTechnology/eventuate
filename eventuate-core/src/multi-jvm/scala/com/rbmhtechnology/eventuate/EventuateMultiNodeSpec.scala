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

import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import com.rbmhtechnology.eventuate.ReplicationProtocol.ReplicationEndpointInfo
import com.typesafe.config.Config

case class EventuateNodeTest(endpointId: String, connections: Set[String], logs: Set[String], role: RoleName, customConfig: Option[Config] = None) {

  def partitionName(logName: String): Option[String] = if (logs.contains(logName)) Some(ReplicationEndpointInfo.logId(endpointId, logName)) else None

}

abstract class EventuateMultiNodeSpecConfig extends MultiNodeConfig {

  def endpointNode(name: String, log: String, connections: Set[String], customConfig: Option[Config] = None) = EventuateNodeTest(name, connections, Set(log), role(name), customConfig)

  def endpointNodeWithLogs(name: String, logs: Set[String], connections: Set[String], customConfig: Option[Config] = None) = EventuateNodeTest(name, connections, logs, role(name), customConfig)

  def setNodesConfig(configs: Set[(RoleName, Config)]): Unit = configs.foreach { case (roleName, config) => nodeConfig(roleName)(config) }

}

abstract class EventuateMultiNodeSpec(config: EventuateMultiNodeSpecConfig) extends MultiNodeSpec(config) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {

  object Implicits {

    import scala.language.implicitConversions

    implicit def toRole(e: EventuateNodeTest) = e.role

    implicit class EnhancedEventuateNodeTest(e: EventuateNodeTest) {
      private def _createEndpoint(activate: Boolean): ReplicationEndpoint = createEndpoint(e.endpointId, e.logs, e.connections.map(c => node(RoleName(c)).address.toReplicationConnection), activate)

      private def _run(thunk: ReplicationEndpoint => Unit, activate: Boolean): Unit =
        thunk(_createEndpoint(activate))

      private def _runWith[T](thunk1: ReplicationEndpoint => T, thunk2: (ReplicationEndpoint, T) => Unit, activate: Boolean): Unit = {
        val endpoint = _createEndpoint(activate)
        val t = thunk1(endpoint)
        thunk2(endpoint, t)
      }

      def run(thunk: ReplicationEndpoint => Unit, activate: Boolean = true) = runOn(e.role)(_run(thunk, activate))

      def runWith[T](thunk1: ReplicationEndpoint => T, activate: Boolean = true)(thunk2: (ReplicationEndpoint, T) => Unit) = runOn(e.role)(_runWith(thunk1, thunk2, activate))

    }

  }

}
