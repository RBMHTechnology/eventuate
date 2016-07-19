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
import akka.testkit._

import com.rbmhtechnology.eventuate.log._
import com.rbmhtechnology.eventuate.log.cassandra._
import com.rbmhtechnology.eventuate.log.cassandra.CassandraIndex.AggregateEvents
import com.typesafe.config._

import scala.concurrent._

trait LocationCleanupCassandra extends LocationCleanup {
  override def storageLocations: List[File] =
    List("eventuate.snapshot.filesystem.dir").map(s => new File(config.getString(s)))

  override def afterAll(): Unit = {
    EmbeddedCassandra.clean()
    super.afterAll()
  }
}

object SingleLocationSpecCassandra {
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
      Props(new CircuitBreaker(logProps, batching, logId))
    }
  }

  class TestEventLog(id: String, failureSpec: TestFailureSpec, indexProbe: Option[ActorRef])
    extends CassandraEventLog(id) with SingleLocationSpec.TestEventLog[CassandraEventLogState] {

    override def currentSystemTime: Long = 0L

    override def unhandled(message: Any): Unit = message match {
      case "boom" => throw IntegrationTestException
      case _      => super.unhandled(message)
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
        Future.failed(IntegrationTestException)
      } else if (failureSpec.failAfterIndexIncrementWrite && !writeIncrementFailed) {
        writeIncrementFailed = true
        for {
          _ <- super.writeAsync(aggregateEvents, clock)
          r <- Future.failed(IntegrationTestException)
        } yield r
      } else super.writeAsync(aggregateEvents, clock)

    override def readEventLogClockSnapshotAsync(implicit executor: ExecutionContext): Future[EventLogClock] =
      if (failureSpec.failOnClockRead && !readClockFailed) {
        readClockFailed = true
        Future.failed(IntegrationTestException)
      } else super.readEventLogClockSnapshotAsync
  }
}

trait SingleLocationSpecCassandra extends SingleLocationSpec with LocationCleanupCassandra {
  import SingleLocationSpecCassandra._

  private var _indexProbe: TestProbe = _
  private var _log: ActorRef = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    _indexProbe = new TestProbe(system)
    _log = _createLog(TestFailureSpec(), indexProbe.ref)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandra.start(cassandraDir)
  }

  def cassandraDir: String =
    EmbeddedCassandra.DefaultCassandraDir

  def indexProbe: TestProbe =
    _indexProbe

  def log: ActorRef =
    _log

  def logProps(logId: String): Props =
    logProps(logId, TestFailureSpec(), system.deadLetters)

  def logProps(logId: String, failureSpec: TestFailureSpec, indexProbe: ActorRef): Props =
    TestEventLog.props(logId, failureSpec, indexProbe, batching)

  def createLog(failureSpec: TestFailureSpec, indexProbe: ActorRef): ActorRef = {
    generateLogId()
    _createLog(failureSpec, indexProbe)
  }

  private def _createLog(failureSpec: TestFailureSpec, indexProbe: ActorRef): ActorRef =
    system.actorOf(logProps(logId, failureSpec, indexProbe))
}

trait MultiLocationSpecCassandra extends MultiLocationSpec with LocationCleanupCassandra {
  override val logFactory: String => Props = id => CassandraEventLog.props(id)

  override val providerConfig = ConfigFactory.parseString(
    s"""
       |eventuate.log.cassandra.default-port = 9142
       |eventuate.log.cassandra.index-update-limit = 3
     """.stripMargin)

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandra.start(cassandraDir)
  }

  def cassandraDir: String =
    EmbeddedCassandra.DefaultCassandraDir

  override def location(name: String, customPort: Int = 0, customConfig: Config = ConfigFactory.empty()): Location = {
    val location = super.location(name, customPort, customConfig)
    Cassandra(location.system) // enforce keyspace/schema setup
    location
  }
}
