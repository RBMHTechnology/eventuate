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
import java.lang.{Long => JLong}
import java.util.concurrent.atomic.AtomicReference
import java.util.function.BinaryOperator

import com.datastax.driver.core.{PreparedStatement, Row}
import com.rbmhtechnology.eventuate._

import scala.collection.JavaConverters._
import scala.collection.immutable.{VectorBuilder, Seq}
import scala.concurrent.Future

private[eventuate] class CassandraEventReader(cassandra: Cassandra, logId: String) {
  import CassandraEventReader._
  import CassandraEventLog._

  val preparedReadEventsStatement: PreparedStatement =
    cassandra.prepareReadEvents(logId)

  def readAsync(fromSequenceNr: Long, toSequenceNr: Long, max: Int): Future[ReadResult] =
    readAsync(fromSequenceNr, toSequenceNr, max, NoFilter, VectorTime(), logId)

  def readAsync(fromSequenceNr: Long, toSequenceNr: Long, max: Int, filter: ReplicationFilter, lower: VectorTime, targetLogId: String): Future[ReadResult] =
    Future(read(fromSequenceNr, toSequenceNr, max, filter, lower, targetLogId))(cassandra.readDispatcher)

  def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int, filter: ReplicationFilter, lower: VectorTime, targetLogId: String): ReadResult = {
    var lastSequenceNr = fromSequenceNr - 1L
    val events = eventIterator(fromSequenceNr, toSequenceNr).filter { evt =>
      lastSequenceNr = evt.localSequenceNr
      evt.replicable(lower, filter)
    }.take(max).toVector
    ReadResult(events, lastSequenceNr)
  }

  def eventIterator(fromSequenceNr: Long, toSequenceNr: Long): Iterator[DurableEvent] with Closeable =
    new EventIterator(fromSequenceNr, toSequenceNr)

  private class EventIterator(fromSequenceNr: Long, toSequenceNr: Long) extends Iterator[DurableEvent] with Closeable {
    import cassandra.settings._

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

private[eventuate] object CassandraEventReader {
  case class ReadResult(events: Seq[DurableEvent], to: Long)
}
