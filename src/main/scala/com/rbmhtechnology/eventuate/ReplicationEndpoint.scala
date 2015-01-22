/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
 */

package com.rbmhtechnology.eventuate

import java.util.function.{Function => JFunction}

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import akka.actor._

case class ReplicationConnection(host: String, port: Int, filter: Option[ReplicationFilter] = None)

object ReplicationEndpoint {
  case class Address(host: String, port: Int)

  case class InstanceId(sid: String, uid: Long) {
    def newIncarnationOf(id: InstanceId): Boolean =
      sid == id.sid && uid != id.uid
  }


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
  def create(system: ActorSystem, factory: JFunction[String, Props]) =
    apply(system, id => factory.apply(id))
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
