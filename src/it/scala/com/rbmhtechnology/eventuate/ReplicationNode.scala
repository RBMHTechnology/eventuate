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

import akka.actor._

class ReplicationNode(nodeId: String, logNames: Set[String], port: Int, connections: Set[ReplicationConnection])(implicit factory: String => Props) {
  val system: ActorSystem =
    ActorSystem(ReplicationConnection.DefaultRemoteSystemName, ReplicationConfig.create(port))

  val endpoint: ReplicationEndpoint =
    new ReplicationEndpoint(nodeId, logNames, factory, connections)(system)

  def logs: Map[String, ActorRef] =
    endpoint.logs

  def shutdown(): Unit = {
    system.shutdown()
  }

  def awaitTermination(): Unit = {
    system.awaitTermination()
  }

  private def localEndpoint(port: Int) =
    s"127.0.0.1:${port}"
}
