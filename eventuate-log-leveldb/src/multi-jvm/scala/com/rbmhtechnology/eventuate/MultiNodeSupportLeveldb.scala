/*
 * Copyright 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.eventuate

import java.io.File

import akka.actor.Props
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeSpec

import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll

trait MultiNodeSupportLeveldb extends BeforeAndAfterAll { this: MultiNodeSpec with MultiNodeWordSpec =>
  val coordinator = RoleName("nodeA")

  def logProps(logId: String): Props =
    LeveldbEventLog.props(logId)

  override def afterAll(): Unit = {
    // get all config data before shutting down node
    val snapshotRootDir = new File(system.settings.config.getString("eventuate.snapshot.filesystem.dir"))
    val logRootDir = new File(system.settings.config.getString("eventuate.log.leveldb.dir"))

    // shut down node
    super.afterAll()

    // delete log and snapshot files
    if (isNode(coordinator)) {
      FileUtils.deleteDirectory(snapshotRootDir)
      FileUtils.deleteDirectory(logRootDir)
    }
  }
}
