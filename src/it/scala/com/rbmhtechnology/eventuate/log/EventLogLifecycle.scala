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

import java.io.{Closeable, File}

import akka.actor._
import akka.testkit.{TestProbe, TestKit}

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.log.EventLogLifecycle.ErrorSequenceNr
import com.rbmhtechnology.eventuate.log.cassandra._
import com.rbmhtechnology.eventuate.log.cassandra.CassandraIndex._
import com.rbmhtechnology.eventuate.log.leveldb._
import com.rbmhtechnology.eventuate.utilities.RestarterActor
import com.typesafe.config.Config

import org.apache.commons.io.FileUtils
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest._

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}

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
    RestarterActor.props(TestEventLog.props(logId, batching))
}

object EventLogLifecycleLeveldb {
  object TestEventLog {
    def props(logId: String, batching: Boolean): Props = {
      val logProps = Props(new TestEventLog(logId)).withDispatcher("eventuate.log.dispatchers.write-dispatcher")
      if (batching) Props(new BatchingLayer(logProps)) else logProps
    }
  }

  class TestEventLog(id: String) extends LeveldbEventLog(id, "log-test") with EventLogLifecycle.TestEventLog {
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

  def logProps(logId: String): Props =
    logProps(logId, TestFailureSpec(), system.deadLetters)

  def logProps(logId: String, failureSpec: TestFailureSpec, indexProbe: ActorRef): Props =
    TestEventLog.props(logId, failureSpec, indexProbe, batching)
}

object EventLogLifecycleCassandra {
  case class TestFailureSpec(
    failOnClockRead: Boolean = false,
    failBeforeIndexIncrementWrite: Boolean = false,
    failAfterIndexIncrementWrite: Boolean = false)

  object TestEventLog {
    def props(logId: String, batching: Boolean): Props =
      props(logId, TestFailureSpec(), None, batching)

    def props(logId: String, failureSpec: TestFailureSpec, indexProbe: ActorRef, batching: Boolean): Props =
      props(logId, failureSpec, Some(indexProbe), batching)

    def props(logId: String, failureSpec: TestFailureSpec, indexProbe: Option[ActorRef], batching: Boolean): Props = {
      val logProps = Props(new TestEventLog(logId, failureSpec, indexProbe)).withDispatcher("eventuate.log.dispatchers.write-dispatcher")
      if (batching) Props(new BatchingLayer(logProps)) else logProps
    }
  }

  class TestEventLog(id: String, failureSpec: TestFailureSpec, indexProbe: Option[ActorRef])
    extends CassandraEventLog(id) with EventLogLifecycle.TestEventLog {

    override def currentSystemTime: Long = 0L

    override def unhandled(message: Any): Unit = message match {
      case "boom" =>
        throw boom
      case _ =>
        super.unhandled(message)
    }

    private[eventuate] override def createIndexStore(cassandra: Cassandra, logId: String) =
      new TestIndexStore(cassandra, logId, failureSpec)

    private[eventuate] override def onIndexEvent(event: Any): Unit =
      indexProbe.foreach(_ ! event)
  }

  class TestIndexStore(cassandra: Cassandra, logId: String, failureSpec: TestFailureSpec) extends CassandraIndexStore(cassandra, logId) {
    private var writeIncrementFailed = false
    private var readClockFailed = false

    override def writeAsync(aggregateEvents: AggregateEvents, clock: EventLogClock)(implicit executor: ExecutionContext): Future[EventLogClock] =
      if (failureSpec.failBeforeIndexIncrementWrite && !writeIncrementFailed) {
        writeIncrementFailed = true
        Future.failed(boom)
      } else if (failureSpec.failAfterIndexIncrementWrite && !writeIncrementFailed) {
        writeIncrementFailed = true
        for {
          _ <- super.writeAsync(aggregateEvents, clock)
          r <- Future.failed(boom)
        } yield r
      } else super.writeAsync(aggregateEvents, clock)

    override def readEventLogClockAsync(implicit executor: ExecutionContext): Future[EventLogClock] =
      if (failureSpec.failOnClockRead && !readClockFailed) {
        readClockFailed = true
        Future.failed(boom)
      } else super.readEventLogClockAsync
  }
}

object EventLogLifecycle {
  val ErrorSequenceNr = -1L
  val IgnoreDeletedSequenceNr = -2L

  trait TestEventLog extends EventLog {
    override def currentSystemTime: Long = 0L

    abstract override def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int): Future[BatchReadResult] =
      if (fromSequenceNr == ErrorSequenceNr) Future.failed(boom) else super.read(fromSequenceNr, toSequenceNr, max)

    abstract override def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int, aggregateId: String): Future[BatchReadResult] =
      if (fromSequenceNr == ErrorSequenceNr) Future.failed(boom) else super.read(fromSequenceNr, toSequenceNr, max, aggregateId)

    abstract override def replicationRead(fromSequenceNr: Long, toSequenceNr: Long, max: Int, filter: (DurableEvent) => Boolean): Future[BatchReadResult] =
      if (fromSequenceNr == ErrorSequenceNr) Future.failed(boom) else super.replicationRead(fromSequenceNr, toSequenceNr, max, filter)

    abstract override def write(events: Seq[DurableEvent], partition: Long, clock: EventLogClock): Unit =
      if (events.map(_.payload).contains("boom")) throw boom else super.write(events, partition, clock)

    override private[eventuate] def adjustFromSequenceNr(seqNr: Long) = seqNr match {
      case ErrorSequenceNr => seqNr
      case IgnoreDeletedSequenceNr => 0
      case s => super.adjustFromSequenceNr(s)
    }
  }
}