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

package com.rbmhtechnology.eventuate.log.kafka

import java.io.File
import java.util.Properties

import org.apache.commons.io.FileUtils
import org.apache.curator.test.TestingServer

class KafkaServer(dir: String) {
  // Kafka config
  val props = new Properties()
  props.setProperty("hostname", "localhost")
  props.setProperty("broker.id", "1")
  props.setProperty("log.dirs", s"${dir}/kafka")
  props.setProperty("zookeeper.connect", "localhost:2181")

  // Start Zookeeper
  val zookeeper = new TestingServer(2181, new File(dir, "zookeeper"))

  // Start Kafka
  val server = new kafka.server.KafkaServer(new kafka.server.KafkaConfig(props))
  server.startup()

  def stop(): Unit = {
    server.shutdown()
    zookeeper.stop()
  }

  def cleanup(): Unit =
    FileUtils.deleteDirectory(new File(dir))
}

object KafkaServer extends App {
  new KafkaServer("target/data")
}