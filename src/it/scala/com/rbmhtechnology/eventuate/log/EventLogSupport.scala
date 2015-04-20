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

package com.rbmhtechnology.eventuate.log

import java.io.File

import scala.collection.immutable.Seq

import akka.actor._
import akka.testkit.TestKit

import org.apache.commons.io.FileUtils
import org.iq80.leveldb.WriteBatch
import org.scalatest._

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog.ReadResult

object EventLogSupport {
  case object GetSequenceNr
  case class GetSequenceNrSuccess(sequenceNr: Long)

  case class SetReplicationProgress(logId: String, progress: Long)
  case class GetReplicationProgress(logId: String)
  case class GetReplicationProgressSuccess(progress: Long)

  class TestEventLog(id: String) extends LeveldbEventLog(id, "log-test") {
    override def replay(from: Long, classifier: Int)(f: (DurableEvent) => Unit): Unit =
      if (from == -1L) throw boom else super.replay(from, classifier)(f)

    override def read(from: Long, max: Int, filter: ReplicationFilter): ReadResult =
      if (from == -1L) throw boom else super.read(from, max, filter)

    override def write(events: Seq[DurableEvent], batch: WriteBatch): Unit = events match {
      case es if es.map(_.payload).contains("boom") => throw boom
      case _ => super.write(events, batch)
    }

    override def unhandled(message: Any): Unit = message match {
      case GetSequenceNr =>
        sender() ! GetSequenceNrSuccess(sequenceNr)
      case GetReplicationProgress(logId) =>
        sender() ! GetReplicationProgressSuccess(replicationProgressMap.readReplicationProgress(logId))
      case SetReplicationProgress(logId, sequenceNr) =>
        withBatch(batch => replicationProgressMap.writeReplicationProgress(logId, sequenceNr, batch))
      case "boom" =>
        throw boom
      case _ =>
        super.unhandled(message)
    }
  }
}

trait EventLogSupport extends BeforeAndAfterAll with BeforeAndAfterEach { this: Suite =>
  import EventLogSupport._

  private var _logCtr: Int = 0
  private var _log: ActorRef = _

  private lazy val storageLocations: List[File] =
    List("eventuate.log.leveldb.dir").map(s => new File(system.settings.config.getString(s)))

  override def beforeEach(): Unit = {
    _logCtr += 1
    _log = system.actorOf(logProps(logId))
  }

  override def beforeAll(): Unit = {
    storageLocations.foreach(FileUtils.deleteDirectory)
    storageLocations.foreach(_.mkdirs())
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    storageLocations.foreach(FileUtils.deleteDirectory)
  }

  def log: ActorRef =
    _log

  def logId: String =
    _logCtr.toString

  def logProps(logId: String): Props =
    Props(new BatchingEventLog(Props(new TestEventLog(logId)).withDispatcher("eventuate.log.leveldb.write-dispatcher")))

  def system: ActorSystem
}

