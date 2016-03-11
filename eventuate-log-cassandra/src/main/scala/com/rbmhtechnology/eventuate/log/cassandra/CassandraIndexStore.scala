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

import java.lang.{ Long => JLong }

import com.datastax.driver.core._

import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.log.EventLogClock

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent._

private[eventuate] class CassandraIndexStore(cassandra: Cassandra, logId: String) {
  import CassandraIndex._

  private val preparedReadAggregateEventStatement: PreparedStatement = cassandra.prepareReadAggregateEvents(logId)
  private val preparedWriteAggregateEventStatement: PreparedStatement = cassandra.prepareWriteAggregateEvent(logId)

  def readEventLogClockAsync(implicit executor: ExecutionContext): Future[EventLogClock] =
    cassandra.session.executeAsync(cassandra.preparedReadEventLogClockStatement.bind(logId)).map { resultSet =>
      if (resultSet.isExhausted) EventLogClock() else cassandra.clockFromByteBuffer(resultSet.one().getBytes("clock"))
    }

  def writeAsync(aggregateEvents: AggregateEvents, clock: EventLogClock)(implicit executor: ExecutionContext): Future[EventLogClock] = {
    for {
      _ <- writeAggregateEventsAsync(aggregateEvents)
      t <- writeEventLogClockAsync(clock) // must be after other writes
    } yield t
  }

  def writeAggregateEventsAsync(aggregateEvents: AggregateEvents)(implicit executor: ExecutionContext): Future[Unit] =
    Future.sequence(aggregateEvents.events.map {
      case (aggregateId, events) => writeAggregateEventsAsync(aggregateId, events)
    }).map(_ => ())

  def writeAggregateEventsAsync(aggregateId: String, events: Seq[DurableEvent])(implicit executor: ExecutionContext): Future[Unit] = cassandra.executeBatchAsync { batch =>
    events.foreach(event => batch.add(preparedWriteAggregateEventStatement.bind(aggregateId, event.localSequenceNr: JLong, cassandra.eventToByteBuffer(event))))
  }

  def writeEventLogClockAsync(clock: EventLogClock)(implicit executor: ExecutionContext): Future[EventLogClock] =
    cassandra.session.executeAsync(cassandra.preparedWriteEventLogClockStatement.bind(logId, cassandra.clockToByteBuffer(clock))).map(_ => clock)

  def aggregateEventIterator(aggregateId: String, fromSequenceNr: Long, toSequenceNr: Long, fetchSize: Int): Iterator[DurableEvent] =
    new AggregateEventIterator(aggregateId, fromSequenceNr, toSequenceNr, fetchSize)

  private class AggregateEventIterator(aggregateId: String, fromSequenceNr: Long, toSequenceNr: Long, fetchSize: Int) extends Iterator[DurableEvent] {
    var currentSequenceNr = fromSequenceNr
    var currentIter = newIter()
    var rowCount = 0

    def newIter(): Iterator[Row] =
      if (currentSequenceNr > toSequenceNr) Iterator.empty else read().iterator.asScala

    def read(): ResultSet =
      cassandra.session.execute(preparedReadAggregateEventStatement.bind(aggregateId, currentSequenceNr: JLong, toSequenceNr: JLong).setFetchSize(fetchSize))

    @annotation.tailrec
    final def hasNext: Boolean = {
      if (currentIter.hasNext) {
        true
      } else if (rowCount < cassandra.settings.partitionSize) {
        // all events consumed
        false
      } else {
        // max result set size reached, fetch again
        currentSequenceNr += 1L
        currentIter = newIter()
        rowCount = 0
        hasNext
      }
    }

    def next(): DurableEvent = {
      val row = currentIter.next()
      currentSequenceNr = row.getLong("sequence_nr")
      rowCount += 1
      cassandra.eventFromByteBuffer(row.getBytes("event"))
    }
  }
}
