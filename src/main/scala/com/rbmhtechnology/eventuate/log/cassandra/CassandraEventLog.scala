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

import java.io.Closeable
import java.util.concurrent.TimeUnit

import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.log._

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.language.postfixOps

case class CassandraEventIteratorParameters(fromSequenceNr: Long, indexSequenceNr: Long, toSequenceNr: Long, aggregateId: Option[String])

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
class CassandraEventLog(id: String) extends EventLog[CassandraEventIteratorParameters](id) {
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
  private val index = createIndex(cassandra, indexStore, eventLogStore, id)

  private var indexSequenceNr: Long = 0L
  private var updateCount: Long = 0L

  override def eventIteratorParameters(fromSequenceNr: Long, toSequenceNr: Long): CassandraEventIteratorParameters =
    CassandraEventIteratorParameters(fromSequenceNr, indexSequenceNr, toSequenceNr, None)

  override def eventIteratorParameters(fromSequenceNr: Long, toSequenceNr: Long, aggregateId: String): CassandraEventIteratorParameters =
    CassandraEventIteratorParameters(fromSequenceNr, indexSequenceNr, toSequenceNr, Some(aggregateId))

  override def eventIterator(parameters: CassandraEventIteratorParameters): Iterator[DurableEvent] with Closeable = parameters match {
    case CassandraEventIteratorParameters(from, idx, to, Some(aggregateId)) => compositeEventIterator(aggregateId, from, idx, to)
    case CassandraEventIteratorParameters(from, idx, to, None)              => eventLogStore.eventIterator(from, to)
  }

  override def readReplicationProgresses: Future[Map[String, Long]] =
    progressStore.readReplicationProgressesAsync(services.readDispatcher)

  override def readReplicationProgress(logId: String): Future[Long] =
    progressStore.readReplicationProgressAsync(logId)(services.readDispatcher)

  override def writeReplicationProgress(logId: String, progress: Long): Future[Unit] =
    progressStore.writeReplicationProgressAsync(logId, progress)(context.system.dispatchers.defaultGlobalDispatcher)

  override def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int, filter: (DurableEvent) => Boolean): Future[BatchReadResult] =
    eventLogStore.readAsync(fromSequenceNr, toSequenceNr, max, filter)(services.readDispatcher)

  override def recoverClockSuccess(clock: EventLogClock): Unit =
    indexSequenceNr = clock.sequenceNr

  override def recoverClock: Future[EventLogClock] = {
    implicit val timeout = Timeout(Int.MaxValue /* FIXME */ , TimeUnit.MILLISECONDS)
    import services.readDispatcher

    index ? InitIndex flatMap {
      case InitIndexSuccess(c) => Future.successful(c)
      case InitIndexFailure(e) => Future.failed(e)
    }
  }

  override def writeDeletionMetadata(deleteMetadata: DeletionMetadata) =
    deletedToStore.writeDeletedToSync(deleteMetadata.toSequenceNr)(context.system.dispatchers.defaultGlobalDispatcher)

  override def readDeletionMetadata = {
    import services.readDispatcher
    deletedToStore.readDeletedToAsync.map(DeletionMetadata(_, Set.empty))
  }

  override def write(events: Seq[DurableEvent], partition: Long, clock: EventLogClock): Unit = {
    eventLogStore.writeSync(events, partition)
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
    case r @ ReadClockFailure(_) =>
      onIndexEvent(r)
    case other =>
      super.unhandled(other)
  }

  private[eventuate] def createIndex(cassandra: Cassandra, indexStore: CassandraIndexStore, eventLogStore: CassandraEventLogStore, logId: String) =
    context.actorOf(CassandraIndex.props(cassandra, eventLogStore, indexStore, logId))

  private[eventuate] def createIndexStore(cassandra: Cassandra, logId: String) =
    new CassandraIndexStore(cassandra, logId)

  private[eventuate] def createEventLogStore(cassandra: Cassandra, logId: String) =
    new CassandraEventLogStore(cassandra, logId)

  private[eventuate] def createReplicationProgressStore(cassandra: Cassandra, logId: String) =
    new CassandraReplicationProgressStore(cassandra, logId)

  private[eventuate] def createDeletedToStore(cassandra: Cassandra, logId: String) =
    new CassandraDeletedToStore(cassandra, logId)

  private def compositeEventIterator(aggregateId: String, fromSequenceNr: Long, indexSequenceNr: Long, toSequenceNr: Long): Iterator[DurableEvent] with Closeable =
    new CompositeEventIterator(aggregateId, fromSequenceNr, indexSequenceNr, toSequenceNr)

  private class CompositeEventIterator(aggregateId: String, fromSequenceNr: Long, indexSequenceNr: Long, toSequenceNr: Long) extends Iterator[DurableEvent] with Closeable {
    private var iter: Iterator[DurableEvent] = indexStore.aggregateEventIterator(aggregateId, fromSequenceNr, indexSequenceNr)
    private var last = fromSequenceNr - 1L
    private var idxr = true

    @annotation.tailrec
    final def hasNext: Boolean =
      if (idxr && iter.hasNext) {
        true
      } else if (idxr) {
        idxr = false
        iter = eventLogStore.eventIterator((indexSequenceNr max last) + 1L, toSequenceNr).filter(_.destinationAggregateIds.contains(aggregateId))
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
    if (batching) Props(new BatchingLayer(logProps)) else logProps
  }
}