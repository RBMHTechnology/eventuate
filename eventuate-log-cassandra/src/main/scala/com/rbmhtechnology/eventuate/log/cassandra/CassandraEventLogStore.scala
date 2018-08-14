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
import java.lang.{ Long => JLong }

import com.datastax.driver.core._
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.log._

import scala.collection.JavaConverters._
import scala.collection.immutable.{ VectorBuilder, Seq }
import scala.concurrent.{ ExecutionContext, Future }

private[eventuate] class CassandraEventLogStore(cassandra: Cassandra, logId: String) {
  val preparedWriteEventStatement: PreparedStatement =
    cassandra.prepareWriteEvent(logId)

  val preparedReadEventsStatement: PreparedStatement =
    cassandra.prepareReadEvents(logId)

  def write(events: Seq[DurableEvent], partition: Long) =
    cassandra.executeBatch { batch =>
      events.foreach { event =>
        batch.add(preparedWriteEventStatement.bind(partition: JLong, event.localSequenceNr: JLong, cassandra.eventToByteBuffer(event)))
      }
    }

  def readAsync(fromSequenceNr: Long, toSequenceNr: Long, max: Int, fetchSize: Int)(implicit executor: ExecutionContext): Future[BatchReadResult] =
    readAsync(fromSequenceNr, toSequenceNr, max, fetchSize, Int.MaxValue, _ => true)

  def readAsync(fromSequenceNr: Long, toSequenceNr: Long, max: Int, scanLimit: Int, fetchSize: Int, filter: DurableEvent => Boolean)(implicit executor: ExecutionContext): Future[BatchReadResult] =
    Future(read(fromSequenceNr, toSequenceNr, max, scanLimit, fetchSize, filter))

  def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int, scanLimit: Int, fetchSize: Int, filter: DurableEvent => Boolean): BatchReadResult = {
    val iter = eventIterator(fromSequenceNr, toSequenceNr, fetchSize)
    val builder = new VectorBuilder[DurableEvent]

    var lastSequenceNr = fromSequenceNr - 1L
    var scanned = 0
    var filtered = 0

    while (iter.hasNext && filtered < max && scanned < scanLimit) {
      val event = iter.next()
      if (filter(event)) {
        builder += event
        filtered += 1
      }
      scanned += 1
      lastSequenceNr = event.localSequenceNr
    }
    BatchReadResult(builder.result(), lastSequenceNr)
  }

  def eventIterator(fromSequenceNr: Long, toSequenceNr: Long, fetchSize: Int): Iterator[DurableEvent] with Closeable =
    new EventIterator(fromSequenceNr, toSequenceNr, fetchSize)

  private class EventIterator(fromSequenceNr: Long, toSequenceNr: Long, fetchSize: Int) extends Iterator[DurableEvent] with Closeable {
    import cassandra.settings._
    import EventLog._

    private val scanEmptyPartitions = toSequenceNr < Long.MaxValue
    private var currentSequenceNr = math.max(fromSequenceNr, 1L)
    private var currentPartition = partitionOf(currentSequenceNr, partitionSize)
    private var currentIter = newIter()
    private var continueWithNextPartition =
      currentSequenceNr != firstSequenceNr(currentPartition, partitionSize) || scanEmptyPartitions

    def newIter(): Iterator[Row] =
      if (currentSequenceNr > toSequenceNr) Iterator.empty else read(lastSequenceNr(currentPartition, partitionSize) min toSequenceNr).iterator.asScala

    def read(upperSequenceNr: Long): ResultSet =
      cassandra.session.execute(preparedReadEventsStatement.bind(currentPartition: JLong, currentSequenceNr: JLong, upperSequenceNr: JLong).setFetchSize(fetchSize))

    @annotation.tailrec
    final def hasNext: Boolean = {
      if (currentIter.hasNext) {
        true
      } else if (continueWithNextPartition) {
        // some events read from current partition, try next partition
        currentPartition += 1
        currentSequenceNr = firstSequenceNr(currentPartition, partitionSize)
        currentIter = newIter()
        continueWithNextPartition = scanEmptyPartitions
        currentSequenceNr <= toSequenceNr && hasNext
      } else /* rowCount == 0 */ {
        // no events read from current partition, we're done
        false
      }
    }

    def next(): DurableEvent = {
      val row = currentIter.next()
      currentSequenceNr = row.getLong("sequence_nr")
      continueWithNextPartition = true
      cassandra.eventFromByteBuffer(row.getBytes("event"))
    }

    override def close(): Unit =
      ()
  }
}
