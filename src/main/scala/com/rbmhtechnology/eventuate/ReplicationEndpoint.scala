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

import java.util.function.{Function => JFunction}

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import akka.actor._

case class ReplicationConnection(host: String, port: Int, filter: Option[ReplicationFilter] = None)

object ReplicationEndpoint {
  case class Address(host: String, port: Int)

  /**
   * Identifies a [[ReplicationEndpoint]] instance.
   *
   * @param uid Globally unique endpoint id.
   * @param iid Instance specific endpoint id. Unique within scope of `uid`.
   */
  case class InstanceId(uid: String, iid: Long) {

    /**
     * Returns `true` if `id` is a new instance (= incarnation) `this`.
     */
    def newIncarnationOf(id: InstanceId): Boolean =
      uid == id.uid && iid != id.iid
  }

  /**
   * Published to the actor system's event stream if a remote endpoint is available.
   */
  case class Available(endpointId: String)

  /**
   * Published to the actor system's event stream if a remote endpoint is unavailable.
   */
  case class Unavailable(endpointId: String)

  private object Address {
    def unapply(s: String): Option[(String, Int)] = {
      val hp = s.split(":")
      Some((hp(0), hp(1).toInt))
    }
  }

  def apply(system: ActorSystem, logFactory: String => Props): ReplicationEndpoint = {
    val connections = system.settings.config.getStringList("log.connections").asScala.toList.map {
      case Address(host, port) => ReplicationConnection(host, port)
    }
    new ReplicationEndpoint(system, logFactory, connections)
  }

  /**
   * Java API.
   */
  def create(system: ActorSystem, logFactory: JFunction[String, Props]) =
    apply(system, id => logFactory.apply(id))
}

class ReplicationEndpoint(system: ActorSystem, logFactory: String => Props, connections: Seq[ReplicationConnection]) {
  import ReplicationEndpoint._

  val config = system.settings.config

  val id: String = config.getString("log.id")
  val instanceId: InstanceId = InstanceId(id, System.currentTimeMillis)

  val log: ActorRef = system.actorOf(logFactory(id))
  val connector = system.actorOf(Props(new ReplicationServerConnector(id, log, instanceId)), ReplicationServerConnector.name)

  connections.foreach {
    case ReplicationConnection(host, port, filter) =>
      val cf = filter match {
        case Some(f) => SourceLogIdExclusionFilter(id).compose(f)
        case None    => SourceLogIdExclusionFilter(id)
      }
      system.actorOf(Props(new ReplicationClientConnector(host, port, cf, log, instanceId)))
  }
}
