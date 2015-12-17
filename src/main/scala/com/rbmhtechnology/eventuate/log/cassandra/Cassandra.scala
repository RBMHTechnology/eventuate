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

package com.rbmhtechnology.eventuate.log.cassandra

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.event.{ LogSource, Logging }
import akka.serialization.SerializationExtension

import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.datastax.driver.core.utils.Bytes

import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.log.EventLogClock

import scala.concurrent.Future
import scala.util._

object Cassandra extends ExtensionId[Cassandra] with ExtensionIdProvider {
  implicit val logSource: LogSource[AnyRef] = new LogSource[AnyRef] {
    override def genString(o: AnyRef): String =
      o.getClass.getName

    override def getClazz(o: AnyRef): Class[_] =
      o.getClass
  }

  def createExtension(system: ExtendedActorSystem): Cassandra =
    new Cassandra(system)

  def lookup() =
    Cassandra
}

/**
 * An Akka extension for using [[http://cassandra.apache.org/ Apache Cassandra]] as event log storage backend.
 * The extension connects to the configured Cassandra cluster and creates the configured keyspace if it doesn't
 * exist yet. Keyspace auto-creation can be turned off by setting
 *
 * {{{
 *   eventuate.log.cassandra.keyspace-autocreate = false
 * }}}
 *
 * The name of the keyspace defaults to `eventuate` and can be configured with
 *
 * {{{
 *   eventuate.log.cassandra.keyspace = "eventuate"
 * }}}
 *
 * The Cassandra cluster contact points can be configured with
 *
 * {{{
 *   eventuate.log.cassandra.contact-points = [host1[:port1], host2[:port2], ...]
 * }}}
 *
 * Ports are optional and default to `9042` according to
 *
 * {{{
 *   eventuate.log.cassandra.default-port = 9042
 * }}}
 *
 * This extension also creates two index tables for storing replication progress data and event log indexing
 * progress data. The names of these tables have a prefix defined by
 *
 * {{{
 *   eventuate.log.cassandra.table-prefix = "log"
 * }}}
 *
 * Assuming a `log` prefix
 *
 *  - the replication progress table name is `log_rp` and
 *  - the log indexing progress table name is `log_snr`.
 *
 * If two instances of this extensions are created concurrently by two different actor systems, index table
 * creation can fail (see [[https://issues.apache.org/jira/browse/CASSANDRA-8387 CASSANDRA-8387]]). It is
 * therefore recommended to initialize a clean Cassandra cluster with a separate administrative application
 * that only creates an instance of this Akka extension before creating [[CassandraEventLog]] actors. This
 * must be done only once. Alternatively, different actor systems can be configured with different
 * `eventuate.log.cassandra.keyspace` names. In this case they won't share a keyspace and index tables and
 * concurrent initialization is not an issue.
 *
 * @see [[CassandraEventLog]]
 */
class Cassandra(val system: ExtendedActorSystem) extends Extension { extension =>

  /**
   * Settings used by the Cassandra storage backend. Closed when the `ActorSystem` of
   * this extension terminates.
   */
  private[eventuate] val settings = new CassandraEventLogSettings(system.settings.config)

  /**
   * Serializer used by the Cassandra storage backend. Closed when the `ActorSystem` of
   * this extension terminates.
   */
  private[eventuate] val serializer = SerializationExtension(system)

  private val logging = Logging(system, this)
  private val statements = new CassandraEventStatements with CassandraAggregateEventStatements with CassandraEventLogClockStatements with CassandraReplicationProgressStatements {

    override def settings: CassandraEventLogSettings =
      extension.settings
  }

  import settings._
  import statements._

  private var _session: Session = _

  Try {
    _session = connect()

    if (keyspaceAutoCreate)
      _session.execute(createKeySpaceStatement)

    _session.execute(createEventLogClockTableStatement)
    _session.execute(createReplicationProgressTableStatement)
  } match {
    case Success(_) => logging.info("Cassandra extension initialized")
    case Failure(e) =>
      logging.error(e, "Cassandra extension initialization failed")
      terminate()
      throw e
  }

  @annotation.tailrec
  private def connect(retries: Int = 0): Session = {
    val curAttempt = retries + 1
    val maxAttempts = settings.initialConnectRetryMax + 1

    Try(clusterBuilder.build().connect()) match {
      case Failure(e: NoHostAvailableException) if retries < settings.initialConnectRetryMax =>
        logging.error(e, s"Cannot connect to cluster (attempt ${curAttempt}/${maxAttempts} ...)")
        Thread.sleep(settings.initialConnectRetryDelay.toMillis)
        connect(retries + 1)
      case Failure(e) =>
        logging.error(e, s"Cannot connect to cluster (attempt ${curAttempt}/${maxAttempts} ...)")
        throw e
      case Success(session) =>
        session
    }
  }

  /**
   * Cassandra cluster reference.
   */
  def cluster: Cluster =
    session.getCluster

  /**
   * Cassandra session used by this extension.
   */
  def session: Session =
    _session

  /**
   * Dispatcher for event log and index reads.
   */
  private[eventuate] def createEventTable(logId: String): Unit =
    session.execute(createEventTableStatement(logId))

  private[eventuate] def createAggregateEventTable(logId: String): Unit =
    session.execute(createAggregateEventTableStatement(logId))

  private[eventuate] def prepareWriteEvent(logId: String): PreparedStatement =
    session.prepare(writeEventStatement(logId)).setConsistencyLevel(writeConsistency)

  private[eventuate] def prepareReadEvents(logId: String): PreparedStatement =
    session.prepare(readEventsStatement(logId)).setConsistencyLevel(readConsistency)

  private[eventuate] def prepareWriteAggregateEvent(logId: String): PreparedStatement =
    session.prepare(writeAggregateEventStatement(logId)).setConsistencyLevel(writeConsistency)

  private[eventuate] def prepareReadAggregateEvents(logId: String): PreparedStatement =
    session.prepare(readAggregateEventsStatement(logId)).setConsistencyLevel(readConsistency)

  private[eventuate] val preparedWriteEventLogClockStatement: PreparedStatement =
    session.prepare(writeEventLogClockStatement).setConsistencyLevel(writeConsistency)

  private[eventuate] val preparedReadEventLogClockStatement: PreparedStatement =
    session.prepare(readEventLogClockStatement).setConsistencyLevel(readConsistency)

  private[eventuate] val preparedWriteReplicationProgressStatement: PreparedStatement =
    session.prepare(writeReplicationProgressStatement).setConsistencyLevel(writeConsistency)

  private[eventuate] val preparedReadReplicationProgressesStatement: PreparedStatement =
    session.prepare(readReplicationProgressesStatement).setConsistencyLevel(readConsistency)

  private[eventuate] val preparedReadReplicationProgressStatement: PreparedStatement =
    session.prepare(readReplicationProgressStatement).setConsistencyLevel(readConsistency)

  private[eventuate] def eventToByteBuffer(event: DurableEvent): ByteBuffer =
    ByteBuffer.wrap(serializer.serialize(event).get)

  private[eventuate] def eventFromByteBuffer(buffer: ByteBuffer): DurableEvent =
    serializer.deserialize(Bytes.getArray(buffer), classOf[DurableEvent]).get

  private[eventuate] def clockToByteBuffer(clock: EventLogClock): ByteBuffer =
    ByteBuffer.wrap(serializer.serialize(clock).get)

  private[eventuate] def clockFromByteBuffer(buffer: ByteBuffer): EventLogClock =
    serializer.deserialize(Bytes.getArray(buffer), classOf[EventLogClock]).get

  private[eventuate] def executeBatch(body: BatchStatement => Unit): Unit =
    session.executeAsync(withBatch(body)).getUninterruptibly(2000, TimeUnit.MILLISECONDS)

  private[eventuate] def executeBatchAsync(body: BatchStatement => Unit): Future[Unit] = {
    implicit val dispatcher = system.dispatchers.defaultGlobalDispatcher
    session.executeAsync(withBatch(body)).map(_ => ())
  }

  private def withBatch(body: BatchStatement => Unit): BatchStatement = {
    val batch = new BatchStatement().setConsistencyLevel(writeConsistency).asInstanceOf[BatchStatement]
    body(batch)
    batch
  }

  private def terminate(): Future[Terminated] =
    system.terminate()

  system.registerOnTermination {
    session.close()
    cluster.close()
  }
}
