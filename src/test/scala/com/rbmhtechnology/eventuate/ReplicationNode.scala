/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
 */

package com.rbmhtechnology.eventuate

import java.io.File

import scala.collection.immutable.Seq

import akka.actor._

import com.rbmhtechnology.eventuate.log.LeveldbEventLog
import com.typesafe.config.ConfigFactory

import org.apache.commons.io.FileUtils

class ReplicationNode(logId: String, port: Int, connections: Seq[ReplicationConnection]) {
  val config = ConfigFactory.parseString(
    s"""
      |akka.remote.netty.tcp.hostname = "127.0.0.1"
      |akka.remote.netty.tcp.port = ${port}
      |akka.test.single-expect-default = 10s
      |
      |log.id = ${logId}
      |log.leveldb.dir = target/logs-system-${logId}
    """.stripMargin)

  val system: ActorSystem =
    ActorSystem("site", config)

  cleanup()
  storageLocation.mkdirs()

  val endpoint: ReplicationEndpoint =
    new ReplicationEndpoint(system, id => LeveldbEventLog.props(id, "log"), connections)

  def log: ActorRef =
    endpoint.log

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
