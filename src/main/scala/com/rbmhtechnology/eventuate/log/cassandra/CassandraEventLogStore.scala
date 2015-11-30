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
import java.lang.{ Long => JLong }

import com.datastax.driver.core.{ PreparedStatement, Row }
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.log._

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent.{ ExecutionContext, Future }

private[eventuate] class CassandraEventLogStore(cassandra: Cassandra, logId: String) {
  val preparedWriteEventStatement: PreparedStatement =
    cassandra.prepareWriteEvent(logId)

  val preparedReadEventsStatement: PreparedStatement =
    cassandra.prepareReadEvents(logId)

  def writeSync(events: Seq[DurableEvent], partition: Long) =
    cassandra.executeBatch { batch =>
      events.foreach { event =>
        batch.add(preparedWriteEventStatement.bind(partition: JLong, event.localSequenceNr: JLong, cassandra.eventToByteBuffer(event)))
      }
    }

  def readAsync(fromSequenceNr: Long, toSequenceNr: Long, max: Int)(implicit executor: ExecutionContext): Future[BatchReadResult] =
    readAsync(fromSequenceNr, toSequenceNr, max, _ => true)

  def readAsync(fromSequenceNr: Long, toSequenceNr: Long, max: Int, filter: DurableEvent => Boolean)(implicit executor: ExecutionContext): Future[BatchReadResult] =
    Future(read(fromSequenceNr, toSequenceNr, max, filter))

  def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int, filter: DurableEvent => Boolean): BatchReadResult = {
    var lastSequenceNr = fromSequenceNr - 1L
    val events = eventIterator(fromSequenceNr, toSequenceNr).filter { evt =>
      lastSequenceNr = evt.localSequenceNr
      filter(evt)
    }.take(max).toVector
    BatchReadResult(events, lastSequenceNr)
  }

  def eventIterator(fromSequenceNr: Long, toSequenceNr: Long): Iterator[DurableEvent] with Closeable =
    new EventIterator(fromSequenceNr, toSequenceNr)

  private class EventIterator(fromSequenceNr: Long, toSequenceNr: Long) extends Iterator[DurableEvent] with Closeable {
    import cassandra.settings._
    import EventLog._

    var currentSequenceNr = math.max(fromSequenceNr, 1L)
    var currentPartition = partitionOf(currentSequenceNr, partitionSizeMax)

    var currentIter = newIter()
    var read = currentSequenceNr != firstSequenceNr(currentPartition, partitionSizeMax)

    def newIter(): Iterator[Row] =
      if (currentSequenceNr > toSequenceNr) Iterator.empty else {
        val upperSequenceNumber = lastSequenceNr(currentPartition, partitionSizeMax) min toSequenceNr
        cassandra.session.execute(preparedReadEventsStatement.bind(currentPartition: JLong, currentSequenceNr: JLong, upperSequenceNumber: JLong)).iterator.asScala
      }

    @annotation.tailrec
    final def hasNext: Boolean = {
      if (currentIter.hasNext) {
        true
      } else if (read) {
        // some events read from current partition, try next partition
        currentPartition += 1
        currentSequenceNr = firstSequenceNr(currentPartition, partitionSizeMax)
        currentIter = newIter()
        read = false
        hasNext
      } else /* rowCount == 0 */ {
        // no events read from current partition, we're done
        false
      }
    }

    def next(): DurableEvent = {
      val row = currentIter.next()
      currentSequenceNr = row.getLong("sequence_nr")
      read = true
      cassandra.eventFromByteBuffer(row.getBytes("event"))
    }

    override def close(): Unit =
      ()
  }
}
