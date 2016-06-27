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

import akka.actor._

import com.rbmhtechnology.eventuate.log._
import com.rbmhtechnology.eventuate.log.leveldb._
import com.rbmhtechnology.eventuate.utilities.RestarterActor
import com.typesafe.config.ConfigFactory

import scala.collection.immutable.Seq

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

  class TestEventLog(id: String) extends LeveldbEventLog(id, "log-test") with SingleLocationSpec.TestEventLog[LeveldbEventLogState] {
    override def unhandled(message: Any): Unit = message match {
      case "boom" => throw IntegrationTestException
      case "dir"  => sender() ! logDir
      case _      => super.unhandled(message)
    }

    override def write(events: Seq[DurableEvent], partition: Long, clock: EventLogClock): Unit =
      super.write(events.filterNot(_.payload == "gap"), partition, clock)
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

  override val providerConfig = ConfigFactory.parseString(
    s"""
       |eventuate.log.leveldb.dir = target/test-log
       |eventuate.log.leveldb.index-update-limit = 3
       |eventuate.log.leveldb.deletion-retry-delay = 1 ms
     """.stripMargin)
}
