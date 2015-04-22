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

import akka.actor._
import akka.testkit.TestKit

import com.rbmhtechnology.eventuate._
import org.scalatest._

import scala.collection.immutable.Seq
import scala.concurrent.Future

object KafkaEventLogSupport {
  case object GetSequenceNr
  case class GetSequenceNrSuccess(sequenceNr: Long)

  case class SetReplicationProgress(logId: String, progress: Long)
  case class GetReplicationProgress(logId: String)
  case class GetReplicationProgressSuccess(progress: Long)

  class TestEventLog(id: String) extends KafkaEventLog(id) {
    override def writeAsync(events: Seq[DurableEvent]): Future[Seq[DurableEvent]] = events match {
      case es if es.map(_.payload).contains("boom") => Future.failed(boom)
      case _ => super.writeAsync(events)
    }

    override def replay(from: Long)(f: (DurableEvent) => Unit): Unit =
      if (from == -1L) throw boom else super.replay(from)(f)

    /*
    override def read(from: Long, max: Int, filter: ReplicationFilter): ReadResult =
      if (from == -1L) throw boom else super.read(from, max, filter)
    */

    override def unhandled(message: Any): Unit = message match {
      //case GetSequenceNr =>
      //  sender() ! GetSequenceNrSuccess(sequenceNr)
      //case GetReplicationProgress(logId) =>
      //  sender() ! GetReplicationProgressSuccess(replicationProgressMap.readReplicationProgress(logId))
      //case SetReplicationProgress(logId, sequenceNr) =>
      //  withBatch(batch => replicationProgressMap.writeReplicationProgress(logId, sequenceNr, batch))
      case "boom" =>
        throw boom
      case _ =>
        super.unhandled(message)
    }
  }
}

trait KafkaEventLogSupport extends BeforeAndAfterAll with BeforeAndAfterEach { this: Suite =>
  import KafkaEventLogSupport._

  private var _server: KafkaServer = _
  private var _logCtr: Int = 0
  private var _log: ActorRef = _

  private lazy val storageLocations: List[File] =
    List("eventuate.log.leveldb.dir").map(s => new File(system.settings.config.getString(s)))

  override def beforeEach(): Unit = {
    super.beforeEach()

    _logCtr += 1
    _log = system.actorOf(logProps(logId))
  }

  override def beforeAll(): Unit = {
    _server = new KafkaServer("target/logs")
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    _server.stop()
    _server.cleanup()
  }

  def log: ActorRef =
    _log

  def logId: String =
    _logCtr.toString

  def logProps(logId: String): Props =
    Props(new TestEventLog(logId)).withDispatcher("eventuate.log.kafka.write-dispatcher")

  def system: ActorSystem
}

