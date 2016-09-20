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

package com.rbmhtechnology.eventuate.log.cassandra

import java.io.Closeable

import akka.actor._

import com.datastax.driver.core.QueryOptions
import com.datastax.driver.core.exceptions._

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.log._
import com.rbmhtechnology.eventuate.log.CircuitBreaker._

import scala.collection.immutable.{ VectorBuilder, Seq }
import scala.concurrent._
import scala.language.postfixOps
import scala.util._

/**
 * Internal state of [[CassandraEventLog]].
 */
case class CassandraEventLogState(eventLogClock: EventLogClock, eventLogClockSnapshot: EventLogClock, deletionMetadata: DeletionMetadata) extends EventLogState

/**
 * An event log actor with [[http://cassandra.apache.org/ Apache Cassandra]] as storage backend. It uses
 * the [[Cassandra]] extension to connect to a Cassandra cluster. Applications should create an instance
 * of this actor using the `props` method of the `CassandraEventLog` [[CassandraEventLog$ companion object]].
 *
 * {{{
 *   val factory: ActorRefFactory = ... // ActorSystem or ActorContext
 *   val logId: String = "example"      // Unique log id
 *
 *   val log = factory.actorOf(CassandraEventLog.props(logId))
 * }}}
 *
 * Each event log actor creates two tables in the configured keyspace (see also [[Cassandra]]). Assuming
 * the following table prefix
 *
 * {{{
 *   eventuate.log.cassandra.table-prefix = "log"
 * }}}
 *
 * and a log `id` with value `example`, the names of these two tables are
 *
 *  - `log_example` which represents the local event log.
 *  - `log_example_agg` which is an index of the local event log for those events that have non-empty
 *    [[DurableEvent#destinationAggregateIds destinationAggregateIds]] set. It is used for fast recovery
 *    of event-sourced actors, views, stateful writers and processors that have an
 *    [[EventsourcedView#aggregateId aggregateId]] defined.
 *
 * @param id unique log id.
 *
 * @see [[Cassandra]]
 * @see [[DurableEvent]]
 */
class CassandraEventLog(id: String) extends EventLog[CassandraEventLogState](id) {
  import CassandraEventLog._
  import CassandraIndex._

  if (!isValidEventLogId(id))
    throw new IllegalArgumentException(s"invalid id '$id' specified - Cassandra allows alphanumeric and underscore characters only")

  private val cassandra: Cassandra = Cassandra(context.system)
  override val settings: CassandraEventLogSettings = cassandra.settings

  cassandra.createEventTable(id)
  cassandra.createAggregateEventTable(id)

  private val progressStore = createReplicationProgressStore(cassandra, id)
  private val deletedToStore = createDeletedToStore(cassandra, id)
  private val eventLogStore = createEventLogStore(cassandra, id)
  private val indexStore = createIndexStore(cassandra, id)

  private var index: ActorRef = null
  private var indexSequenceNr: Long = 0L
  private var updateCount: Long = 0L

  override def recoverState: Future[CassandraEventLogState] = {
    import services.readDispatcher
    for {
      dm <- deletedToStore.readDeletedToAsync.map(DeletionMetadata(_, Set.empty))
      sc <- indexStore.readEventLogClockSnapshotAsync
      rc <- recoverEventLogClockAsync(sc)
    } yield CassandraEventLogState(rc, sc, dm)
  }

  override def recoverStateSuccess(state: CassandraEventLogState): Unit = {
    index = createIndex(cassandra, state.eventLogClockSnapshot, indexStore, eventLogStore, id)
    indexSequenceNr = state.eventLogClockSnapshot.sequenceNr
    updateCount = state.eventLogClock.sequenceNr - indexSequenceNr
    context.parent ! ServiceInitialized(id)
  }

  override def readReplicationProgresses: Future[Map[String, Long]] =
    progressStore.readReplicationProgressesAsync(services.readDispatcher)

  override def readReplicationProgress(logId: String): Future[Long] =
    progressStore.readReplicationProgressAsync(logId)(services.readDispatcher)

  override def writeReplicationProgress(logId: String, progress: Long): Future[Unit] =
    progressStore.writeReplicationProgressAsync(logId, progress)(context.system.dispatchers.defaultGlobalDispatcher)

  override def replicationRead(fromSequenceNr: Long, toSequenceNr: Long, max: Int, scanLimit: Int, filter: DurableEvent => Boolean): Future[BatchReadResult] =
    eventLogStore.readAsync(fromSequenceNr, toSequenceNr, max, scanLimit, QueryOptions.DEFAULT_FETCH_SIZE, filter)(services.readDispatcher)

  override def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int): Future[BatchReadResult] =
    eventLogStore.readAsync(fromSequenceNr, toSequenceNr, max, Int.MaxValue, max + 1, _ => true)(services.readDispatcher)

  override def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int, aggregateId: String): Future[BatchReadResult] =
    compositeReadAsync(fromSequenceNr, indexSequenceNr, toSequenceNr, max, max + 1, aggregateId)(services.readDispatcher)

  override def writeDeletionMetadata(deleteMetadata: DeletionMetadata) =
    deletedToStore.writeDeletedTo(deleteMetadata.toSequenceNr)

  override def writeEventLogClockSnapshot(clock: EventLogClock): Future[Unit] = {
    import context.dispatcher
    indexStore.writeEventLogClockSnapshotAsync(clock).map(_ => ())
  }

  override def write(events: Seq[DurableEvent], partition: Long, clock: EventLogClock): Unit =
    writeRetry(events, partition, clock)

  /**
   * @see [[http://rbmhtechnology.github.io/eventuate/reference/event-sourcing.html#failure-handling Failure handling]]
   */
  @annotation.tailrec
  private def writeRetry(events: Seq[DurableEvent], partition: Long, clock: EventLogClock, num: Int = 0): Unit = {
    Try(writeBatch(events, partition, clock)) match {
      case Success(r) =>
        context.parent ! ServiceNormal(id)
      case Failure(e) if num >= cassandra.settings.writeRetryMax =>
        logger.error(e, s"write attempt ${num} failed: reached maximum number of retries - stop self")
        context.stop(self)
        throw e
      case Failure(e: TimeoutException) =>
        context.parent ! ServiceFailed(id, num, e)
        logger.error(e, s"write attempt ${num} failed: timeout after ${cassandra.settings.writeTimeout} ms - retry now")
        writeRetry(events, partition, clock, num + 1)
      case Failure(e: WriteTimeoutException) =>
        context.parent ! ServiceFailed(id, num, e)
        logger.error(e, s"write attempt ${num} failed - retry now")
        writeRetry(events, partition, clock, num + 1)
      case Failure(e: QueryExecutionException) =>
        context.parent ! ServiceFailed(id, num, e)
        logger.error(e, s"write attempt ${num} failed - retry in ${cassandra.settings.writeTimeout} ms")
        Thread.sleep(cassandra.settings.writeTimeout)
        writeRetry(events, partition, clock, num + 1)
      case Failure(e: NoHostAvailableException) =>
        context.parent ! ServiceFailed(id, num, e)
        logger.error(e, s"write attempt ${num} failed - retry in ${cassandra.settings.writeTimeout} ms")
        Thread.sleep(cassandra.settings.writeTimeout)
        writeRetry(events, partition, clock, num + 1)
      case Failure(e) =>
        logger.error(e, s"write attempt ${num} failed - stop self")
        context.stop(self)
        throw e
    }
  }

  private def writeBatch(events: Seq[DurableEvent], partition: Long, clock: EventLogClock): Unit = {
    eventLogStore.write(events, partition)
    updateCount += events.size
    if (updateCount >= cassandra.settings.indexUpdateLimit) {
      index ! UpdateIndex(null, clock.sequenceNr)
      updateCount = 0L
    }
  }

  override def unhandled(message: Any): Unit = message match {
    case u @ UpdateIndexSuccess(clock, _) =>
      indexSequenceNr = clock.sequenceNr
      onIndexEvent(u)
    case u @ UpdateIndexFailure(_) =>
      onIndexEvent(u)
    case other =>
      super.unhandled(other)
  }

  private[eventuate] def createIndex(cassandra: Cassandra, eventLogClock: EventLogClock, indexStore: CassandraIndexStore, eventLogStore: CassandraEventLogStore, logId: String) =
    context.actorOf(CassandraIndex.props(cassandra, eventLogClock, eventLogStore, indexStore, logId))

  private[eventuate] def createIndexStore(cassandra: Cassandra, logId: String) =
    new CassandraIndexStore(cassandra, logId)

  private[eventuate] def createEventLogStore(cassandra: Cassandra, logId: String) =
    new CassandraEventLogStore(cassandra, logId)

  private[eventuate] def createReplicationProgressStore(cassandra: Cassandra, logId: String) =
    new CassandraReplicationProgressStore(cassandra, logId)

  private[eventuate] def createDeletedToStore(cassandra: Cassandra, logId: String) =
    new CassandraDeletedToStore(cassandra, logId)

  private def recoverEventLogClockAsync(snapshot: EventLogClock)(implicit executor: ExecutionContext): Future[EventLogClock] =
    Future(eventLogStore.eventIterator(snapshot.sequenceNr + 1L, Long.MaxValue, cassandra.settings.indexUpdateLimit).foldLeft(snapshot)(_ update _))

  private def compositeReadAsync(fromSequenceNr: Long, indexSequenceNr: Long, toSequenceNr: Long, max: Int, fetchSize: Int, aggregateId: String)(implicit executor: ExecutionContext): Future[BatchReadResult] =
    Future(compositeRead(fromSequenceNr, toSequenceNr, max, fetchSize, aggregateId))

  private def compositeRead(fromSequenceNr: Long, toSequenceNr: Long, max: Int, fetchSize: Int, aggregateId: String): BatchReadResult = {
    val iter = compositeEventIterator(aggregateId, fromSequenceNr, indexSequenceNr, toSequenceNr, fetchSize)
    val builder = new VectorBuilder[DurableEvent]

    var lastSequenceNr = fromSequenceNr - 1L
    var scanned = 0

    while (iter.hasNext && scanned < max) {
      val event = iter.next()
      builder += event
      scanned += 1
      lastSequenceNr = event.localSequenceNr
    }
    BatchReadResult(builder.result(), lastSequenceNr)
  }

  private def compositeEventIterator(aggregateId: String, fromSequenceNr: Long, indexSequenceNr: Long, toSequenceNr: Long, fetchSize: Int): Iterator[DurableEvent] with Closeable =
    new CompositeEventIterator(aggregateId, fromSequenceNr, indexSequenceNr, toSequenceNr, fetchSize)

  private class CompositeEventIterator(aggregateId: String, fromSequenceNr: Long, indexSequenceNr: Long, toSequenceNr: Long, fetchSize: Int) extends Iterator[DurableEvent] with Closeable {
    private var iter: Iterator[DurableEvent] = indexStore.aggregateEventIterator(aggregateId, fromSequenceNr, indexSequenceNr, fetchSize)
    private var last = fromSequenceNr - 1L
    private var idxr = true

    @annotation.tailrec
    final def hasNext: Boolean =
      if (idxr && iter.hasNext) {
        true
      } else if (idxr) {
        idxr = false
        iter = eventLogStore.eventIterator((indexSequenceNr max last) + 1L, toSequenceNr, fetchSize).filter(_.destinationAggregateIds.contains(aggregateId))
        hasNext
      } else {
        iter.hasNext
      }

    def next(): DurableEvent = {
      val evt = iter.next()
      last = evt.localSequenceNr
      evt
    }

    def close(): Unit =
      ()
  }

  // ------------------------------------------------------------------
  //  Test support
  // ------------------------------------------------------------------

  private[eventuate] def onIndexEvent(event: Any): Unit = ()
}

object CassandraEventLog {
  val validCassandraIdentifier = "^[a-zA-Z0-9_]+$"r

  /**
   * Check whether the specified `logId` is valid for Cassandra
   * table, column and/or keyspace name usage.
   */
  def isValidEventLogId(logId: String): Boolean =
    validCassandraIdentifier.findFirstIn(logId).isDefined

  /**
   * Creates a [[CassandraEventLog]] configuration object.
   *
   * @param logId unique log id.
   * @param batching `true` if write-batching shall be enabled (recommended).
   */
  def props(logId: String, batching: Boolean = true): Props = {
    val logProps = Props(new CassandraEventLog(logId)).withDispatcher("eventuate.log.dispatchers.write-dispatcher")
    Props(new CircuitBreaker(logProps, batching))
  }
}
