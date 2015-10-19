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

import org.scalatest._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class ReplicationNode(val id: String, logNames: Set[String], port: Int, connections: Set[ReplicationConnection], customConfig: String = "", activate: Boolean = true)(implicit logFactory: String => Props) {
  val system: ActorSystem =
    ActorSystem(ReplicationConnection.DefaultRemoteSystemName, ReplicationConfig.create(port, customConfig))

  val endpoint: ReplicationEndpoint = {
    val endpoint = new ReplicationEndpoint(id, logNames, logFactory, connections)(system)
    if (activate) endpoint.activate()
    endpoint
  }

  def logs: Map[String, ActorRef] =
    endpoint.logs

  def terminate(): Future[Terminated] = {
    system.terminate()
  }

  private def localEndpoint(port: Int) =
    s"127.0.0.1:${port}"
}

trait ReplicationNodeRegistry extends BeforeAndAfterEach { this: Suite =>
  var nodes: List[ReplicationNode] = Nil

  override def afterEach(): Unit =
    nodes.foreach(node => Await.result(node.terminate(), 10.seconds))

  def register(node: ReplicationNode): ReplicationNode = {
    nodes = node :: nodes
    node
  }
}