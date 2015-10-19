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

import akka.actor._
import akka.pattern.ask
import akka.testkit.{TestProbe, TestKit}
import akka.util.Timeout

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.log.cassandra._
import com.rbmhtechnology.eventuate.log.cassandra.CassandraIndex._
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog.ReadResult
import com.typesafe.config.Config

import org.apache.commons.io.FileUtils
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.iq80.leveldb.WriteBatch
import org.scalatest._

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util._

trait EventLogCleanupLeveldb extends Suite with BeforeAndAfterAll {
  def config: Config

  def storageLocations: List[File] =
    List("eventuate.log.leveldb.dir", "eventuate.snapshot.filesystem.dir").map(s => new File(config.getString(s)))

  override def beforeAll(): Unit = {
    storageLocations.foreach(FileUtils.deleteDirectory)
    storageLocations.foreach(_.mkdirs())
  }

  override def afterAll(): Unit = {
    storageLocations.foreach(FileUtils.deleteDirectory)
  }
}

trait EventLogLifecycleLeveldb extends EventLogCleanupLeveldb with BeforeAndAfterEach {
  import EventLogLifecycleLeveldb._

  private var _logCtr: Int = 0
  private var _log: ActorRef = _

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    _logCtr += 1
    _log = system.actorOf(logProps(logId))
  }

  def system: ActorSystem

  def config: Config =
    system.settings.config

  def batching: Boolean =
    true

  def log: ActorRef =
    _log

  def logId: String =
    _logCtr.toString

  def logProps(logId: String): Props =
    TestEventLog.props(logId, batching)
}

object EventLogLifecycleLeveldb {
  object TestEventLog {
    def props(logId: String, batching: Boolean): Props = {
      val logProps = Props(new EventLogLifecycleLeveldb.TestEventLog(logId)).withDispatcher("eventuate.log.leveldb.write-dispatcher")
      if (batching) Props(new BatchingEventLog(logProps)) else logProps
    }
  }

  class TestEventLog(id: String) extends LeveldbEventLog(id, "log-test") {
    override def currentSystemTime: Long =
      0L

    private[eventuate] override def replay(from: Long, classifier: Int)(f: (DurableEvent) => Unit): Unit =
      if (from == -1L) throw boom else super.replay(from, classifier)(f)

    private[eventuate] override def read(from: Long, max: Int, filter: ReplicationFilter, lower: VectorTime): ReadResult =
      if (from == -1L) throw boom else super.read(from, max, filter, lower)

    private[eventuate] override def write(events: Seq[DurableEvent], tracker: TimeTracker, batch: WriteBatch): TimeTracker = events match {
      case es if es.map(_.payload).contains("boom") => throw boom
      case _ => super.write(events, tracker, batch)
    }

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

trait EventLogCleanupCassandra extends Suite with BeforeAndAfterAll {
  def config: Config

  def storageLocations: List[File] =
    List("eventuate.snapshot.filesystem.dir").map(s => new File(config.getString(s)))

  override def beforeAll(): Unit = {
    storageLocations.foreach(FileUtils.deleteDirectory)
    storageLocations.foreach(_.mkdirs())
  }

  override def afterAll(): Unit = {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    storageLocations.foreach(FileUtils.deleteDirectory)
  }
}

trait EventLogLifecycleCassandra extends EventLogCleanupCassandra with BeforeAndAfterEach {
  import EventLogLifecycleCassandra._

  private var _logCtr: Int = 0
  private var _log: ActorRef = _

  var indexProbe: TestProbe = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    indexProbe = new TestProbe(system)

    _logCtr += 1
    _log = createLog(TestFailureSpec(), indexProbe.ref)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(60000)
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  def createLog(failureSpec: TestFailureSpec, indexProbe: ActorRef): ActorRef =
    system.actorOf(logProps(logId, failureSpec, indexProbe))

  def system: ActorSystem

  def config: Config =
    system.settings.config

  def batching: Boolean =
    true

  def log: ActorRef =
    _log

  def logId: String =
    _logCtr.toString

  def logProps(logId: String, failureSpec: TestFailureSpec, indexProbe: ActorRef): Props =
    TestEventLog.props(logId, failureSpec, indexProbe, batching)
}

object EventLogLifecycleCassandra {
  case class TestFailureSpec(
    failOnSequenceNrRead: Boolean = false,
    failBeforeIndexIncrementWrite: Boolean = false,
    failAfterIndexIncrementWrite: Boolean = false)

  object TestEventLog {
    def props(logId: String, batching: Boolean): Props =
      props(logId, TestFailureSpec(), None, batching)

    def props(logId: String, failureSpec: TestFailureSpec, indexProbe: ActorRef, batching: Boolean): Props =
      props(logId, failureSpec, Some(indexProbe), batching)

    def props(logId: String, failureSpec: TestFailureSpec, indexProbe: Option[ActorRef], batching: Boolean): Props = {
      val logProps = Props(new TestEventLog(logId, failureSpec, indexProbe)).withDispatcher("eventuate.log.cassandra.write-dispatcher")
      if (batching) Props(new BatchingEventLog(logProps)) else logProps
    }
  }

  class TestEventLog(id: String, failureSpec: TestFailureSpec, indexProbe: Option[ActorRef]) extends CassandraEventLog(id) {
    import context.dispatcher

    private var index: ActorRef = _

    override def currentSystemTime: Long =
      0L

    override def unhandled(message: Any): Unit = message match {
      case GetReplicationProgresses =>
        val sdr = sender()
        getReplicationProgressMap(List(EventLogSpec.remoteLogId, "x", "y")) onComplete {
          case Success(r) => sdr ! GetReplicationProgressesSuccess(r)
          case Failure(e) => sdr ! GetReplicationProgressesFailure(e)
        }
      case "boom" =>
        throw boom
      case _ =>
        super.unhandled(message)
    }

    private[eventuate] override def write(partition: Long, events: Seq[DurableEvent], tracker: TimeTracker): TimeTracker = events match {
      case es if es.map(_.payload).contains("boom") => throw boom
      case _ => super.write(partition, events, tracker)
    }

    private[eventuate] override def createEventReader(cassandra: Cassandra, logId: String) =
      new TestEventReader(cassandra, logId)

    private[eventuate] override def createIndex(cassandra: Cassandra, eventReader: CassandraEventReader, logId: String): ActorRef = {
      index = context.actorOf(Props(new TestIndex(cassandra, eventReader, logId, failureSpec, indexProbe)))
      index
    }

    private def getReplicationProgressMap(sourceLogIds: Seq[String]): Future[Map[String, Long]] = {
      implicit val timeout = Timeout(10.seconds)

      Future.sequence(sourceLogIds.map(sourceLogId => index.ask(GetReplicationProgress(sourceLogId)).mapTo[GetReplicationProgressSuccess])).map { results =>
        results.foldLeft[Map[String, Long]](Map.empty) {
          case (acc, GetReplicationProgressSuccess(logId, snr, _)) => if (snr == 0L) acc else acc + (logId -> snr)
        }
      }
    }
  }

  class TestEventReader(cassandra: Cassandra, logId: String) extends CassandraEventReader(cassandra, logId) {
    override def replay(from: Long)(f: (DurableEvent) => Unit): Unit =
      if (from == -1L) throw boom else super.replay(from)(f)

    override def read(from: Long, max: Int, filter: ReplicationFilter, lower: VectorTime, targetLogId: String): CassandraEventReader.ReadResult =
      if (from == -1L) throw boom else super.read(from, max, filter, lower, targetLogId)
  }

  class TestIndex(cassandra: Cassandra, eventReader: CassandraEventReader, logId: String, failureSpec: TestFailureSpec, indexProbe: Option[ActorRef]) extends CassandraIndex(cassandra, eventReader, logId) {
    val stream = context.system.eventStream

    private[eventuate] override def createIndexStore(cassandra: Cassandra, logId: String) =
      new TestIndexStore(cassandra, logId, failureSpec)

    override def onIndexEvent(event: Any): Unit =
      indexProbe.foreach(_ ! event)
  }

  class TestIndexStore(cassandra: Cassandra, logId: String, failureSpec: TestFailureSpec) extends CassandraIndexStore(cassandra, logId) {
    private var writeIndexIncrementFailed = false
    private var readSequenceNumberFailed = false

    private[eventuate] override def writeAsync(aggregateEvents: AggregateEvents, timeTracker: TimeTracker)(implicit executor: ExecutionContext): Future[TimeTracker] =
      if (failureSpec.failBeforeIndexIncrementWrite && !writeIndexIncrementFailed) {
        writeIndexIncrementFailed = true
        Future.failed(boom)
      } else if (failureSpec.failAfterIndexIncrementWrite && !writeIndexIncrementFailed) {
        writeIndexIncrementFailed = true
        for {
          _ <- super.writeAsync(aggregateEvents, timeTracker)
          r <- Future.failed(boom)
        } yield r
      } else super.writeAsync(aggregateEvents, timeTracker)

    private[eventuate] override def readTimeTrackerAsync: Future[TimeTracker] =
      if (failureSpec.failOnSequenceNrRead && !readSequenceNumberFailed) {
        readSequenceNumberFailed = true
        Future.failed(boom)
      } else super.readTimeTrackerAsync
  }
}
