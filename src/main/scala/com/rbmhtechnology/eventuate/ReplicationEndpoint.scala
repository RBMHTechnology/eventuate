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

object ReplicationConnection {
  def apply(host: String, port: Int, filters: Map[String, ReplicationFilter]): ReplicationConnection =
    new ReplicationConnection(host, port, filters = filters)

  def apply(host: String, port: Int, filter: Option[ReplicationFilter]): ReplicationConnection = filter match {
    case Some(f) => new ReplicationConnection(host, port, filters = Map(ReplicationEndpoint.DefaultLogName -> f))
    case None    => new ReplicationConnection(host, port)
  }
}

case class ReplicationConnection(host: String, port: Int, protocol: String = "akka.tcp", name: String = "site", filters: Map[String, ReplicationFilter] = Map.empty)

object ReplicationEndpoint {
  /**
   * Default log name that is used if application doesn't specify names.
   */
  val DefaultLogName: String = "default"

  /**
   * Identifies a [[ReplicationEndpoint]] instance.
   *
   * @param uid Globally unique endpoint id.
   * @param iid Instance specific endpoint id. Unique within scope of `uid`.
   */
  case class InstanceId(uid: String, iid: Long) {

    /**
     * Returns `true` if `this` is a new instance (= incarnation) `id`.
     */
    def newIncarnationOf(id: InstanceId): Boolean =
      uid == id.uid && iid != id.iid
  }

  /**
   * Published to the actor system's event stream if a remote log is available.
   */
  case class Available(endpointId: String, logName: String)

  /**
   * Published to the actor system's event stream if a remote log is unavailable.
   */
  case class Unavailable(endpointId: String, logName: String)

  /**
   * Matches a string of format "<hostname>:<port>".
   */
  private object Address {
    def unapply(s: String): Option[(String, Int)] = {
      val hp = s.split(":")
      Some((hp(0), hp(1).toInt))
    }
  }

  def apply(logFactory: String => Props)(implicit system: ActorSystem): ReplicationEndpoint = {
    val config = system.settings.config
    val connections = config.getStringList("endpoint.connections").asScala.toList.map {
      case Address(host, port) => ReplicationConnection(host, port)
    }
    apply(logFactory, connections)
  }

  def apply(logFactory: String => Props, connections: Seq[ReplicationConnection])(implicit system: ActorSystem): ReplicationEndpoint = {
    val config = system.settings.config
    val endpointId = config.getString("endpoint.id")
    new ReplicationEndpoint(endpointId, Set(DefaultLogName), logFactory, connections)(system)
  }

  /**
   * Java API.
   */
  def create(logFactory: JFunction[String, Props], system: ActorSystem) =
    apply(id => logFactory.apply(id))(system)
}

class ReplicationEndpoint(val id: String, logNames: Set[String], logFactory: String => Props, connections: Seq[ReplicationConnection])(implicit system: ActorSystem) {
  import ReplicationEndpoint._

  val instanceId: InstanceId =
    InstanceId(id, System.currentTimeMillis)

  val logs: Map[String, ActorRef] =
    logNames.map(logName => logName -> system.actorOf(logFactory(logId(logName)))).toMap

  val connector: ActorRef =
    system.actorOf(Props(new ReplicationServerConnector(logs, logId, instanceId)), ReplicationServerConnector.name)

  def logId(logName: String): String =
    s"${id}-${logName}"

  connections.foreach {
    case ReplicationConnection(host, port, protocol, name, filters) =>
      var cfs = filters
      logNames.foreach { logName =>
        cfs.get(logName) match {
          case Some(f) => cfs += (logName -> SourceLogIdExclusionFilter(logId(logName)).compose(f))
          case None    => cfs += (logName -> SourceLogIdExclusionFilter(logId(logName)))
        }
      }
      system.actorOf(Props(new ReplicationClientConnector(host, port, protocol, name, logs, cfs, instanceId)))
  }
}
