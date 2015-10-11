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

import com.typesafe.config._

object ReplicationConfig {
  def create(port: Int = 2552, customConfig: String = ""): Config = {
    val defaultConfig = ConfigFactory.parseString(
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.enabled-transports = ["akka.remote.netty.tcp"]
         |akka.remote.netty.tcp.hostname = "127.0.0.1"
         |akka.remote.netty.tcp.port = ${port}
         |akka.remote.retry-gate-closed-for = 300ms
         |akka.test.single-expect-default = 20s
         |akka.loglevel = "ERROR"
         |
         |eventuate.snapshot.filesystem.dir = target/test-snapshot
         |
         |eventuate.log.leveldb.dir = target/test-log
         |eventuate.log.leveldb.index-update-limit = 3
         |eventuate.log.cassandra.default-port = 9142
         |eventuate.log.cassandra.index-update-limit = 3
         |eventuate.log.replication.batch-size-max = 3
         |eventuate.log.replication.retry-interval = 1s
         |eventuate.log.replication.failure-detection-limit = 3s
       """.stripMargin)

    ConfigFactory.parseString(customConfig).withFallback(defaultConfig)
  }
}
