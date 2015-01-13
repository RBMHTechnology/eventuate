/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
 */

package com.rbmhtechnology.eventuate

import java.util.function.{Function => JFunction}

import scala.collection.JavaConverters._

import akka.actor._

object ReplicationEndpoint {
  case class InstanceId(sid: String, uid: Long) {
    def newIncarnationOf(id: InstanceId): Boolean =
      sid == id.sid && uid != id.uid
  }

  object HostAndPort {
    def unapply(s: String): Option[(String, Int)] = {
      val hp = s.split(":")
      Some((hp(0), hp(1).toInt))
    }
  }

  /**
   * Java API.
   */
  def create(system: ActorSystem, factory: JFunction[String, Props]) =
    new ReplicationEndpoint(system, id => factory.apply(id))
}

class ReplicationEndpoint(system: ActorSystem, factory: String => Props) {
  import ReplicationEndpoint._

  val config = system.settings.config

  val id: String = config.getString("log.id")
  val instanceId: InstanceId = InstanceId(id, System.currentTimeMillis)

  val log: ActorRef = system.actorOf(factory(id))
  val connector = system.actorOf(Props(new ReplicationServerConnector(id, log, instanceId)), ReplicationServerConnector.name)

  config.getStringList("log.connections").asScala.foreach {
    case HostAndPort(h, p) => system.actorOf(Props(new ReplicationClientConnector(h, p, id, log, instanceId)))
  }
}