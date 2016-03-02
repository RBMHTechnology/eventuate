/*
 * Copyright (C) 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
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

import java.io.File

import akka.actor._

import com.rbmhtechnology.eventuate.log._
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog
import com.rbmhtechnology.eventuate.utilities.RestarterActor

trait LocationCleanupLeveldb extends LocationCleanup {
  override def storageLocations: List[File] =
    List("eventuate.log.leveldb.dir", "eventuate.snapshot.filesystem.dir").map(s => new File(config.getString(s)))
}

object SingleLocationSpecLeveldb {
  object TestEventLog {
    def props(logId: String, batching: Boolean): Props = {
      val logProps = Props(new TestEventLog(logId)).withDispatcher("eventuate.log.dispatchers.write-dispatcher")
      if (batching) Props(new BatchingLayer(logProps)) else logProps
    }
  }

  class TestEventLog(id: String) extends LeveldbEventLog(id, "log-test") with SingleLocationSpec.TestEventLog {
    override def unhandled(message: Any): Unit = message match {
      case "boom" =>
        throw boom
      case "dir" =>
        sender() ! logDir
      case _ =>
        super.unhandled(message)
    }
  }
}

trait SingleLocationSpecLeveldb extends SingleLocationSpec with LocationCleanupLeveldb {
  import SingleLocationSpecLeveldb._

  private var _log: ActorRef = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    _log = system.actorOf(logProps(logId))
  }

  def log: ActorRef =
    _log

  def logProps(logId: String): Props =
    RestarterActor.props(TestEventLog.props(logId, batching))
}

trait MultiLocationSpecLeveldb extends MultiLocationSpec with LocationCleanupLeveldb {
  override val logFactory: String => Props = id => LeveldbEventLog.props(id)
}
