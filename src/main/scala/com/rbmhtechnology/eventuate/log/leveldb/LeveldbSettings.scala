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

package com.rbmhtechnology.eventuate.log.leveldb

import akka.actor.ActorSystem
import akka.dispatch.MessageDispatcher

private[eventuate] class LeveldbSettings(system: ActorSystem) {
  implicit val readDispatcher: MessageDispatcher =
    system.dispatchers.lookup("eventuate.log.leveldb.read-dispatcher")

  val rootDir: String =
    system.settings.config.getString("eventuate.log.leveldb.dir")

  val fsync: Boolean =
    system.settings.config.getBoolean("eventuate.log.leveldb.fsync")

  val stateSnapshotLimit: Int =
    system.settings.config.getInt("eventuate.log.leveldb.state-snapshot-limit")
}
