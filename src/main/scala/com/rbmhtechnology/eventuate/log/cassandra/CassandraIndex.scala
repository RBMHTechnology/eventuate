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

import java.lang.{Long => JLong}

import akka.actor._
import akka.pattern.pipe

import com.datastax.driver.core.{Row, PreparedStatement}
import com.rbmhtechnology.eventuate.{DurableEvent, DurableEventBatch}
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationProtocol._

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent._
import scala.util._

private[eventuate] class CassandraIndex(cassandra: Cassandra, eventReader: CassandraEventReader, logId: String) extends Actor with Stash with ActorLogging {
  import CassandraIndex._
  import CassandraEventLog._
  import context.dispatcher

  private val scheduler = context.system.scheduler
  private val eventLog = context.parent

  private val indexStore = createIndexStore(cassandra, logId)
  private val indexUpdater = context.actorOf(Props(new CassandraIndexUpdater(cassandra, eventReader, indexStore)))

  /**
   * Last sequence number in event log that has been successfully
   * processed and written to index.
   */
  private var lastIndexedSequenceNr: Long = 0L

  /**
   * This is an optimization which prevents redundant empty reads
   * from source logs in failure cases.
   */
  private var bufferedReplicationProgress = ReplicationProgress()

  def initializing: Receive = {
    case ReadSequenceNr =>
      indexStore.readSequenceNumberAsync onComplete {
        case Success(snr) => self ! ReadSequenceNrSuccess(snr)
        case Failure(e)   => self ! ReadSequenceNrFailure(e)
      }
    case ReadSequenceNrSuccess(snr) =>
      indexUpdater ! UpdateIndex(snr)
    case u @ UpdateIndexSuccess(snr, numWrites) =>
      this.lastIndexedSequenceNr = snr
      eventLog ! Initialize(snr)
      context.become(initialized)
      unstashAll()
      onIndexEvent(u)
    case u @ UpdateIndexFailure(cause) =>
      log.error(cause, "UpdateIndex failure")
      scheduleReadSequenceNr()
      onIndexEvent(u)
    case r @ ReadSequenceNrFailure(cause) =>
      log.error(cause, "ReadSequenceNr failed")
      scheduleReadSequenceNr()
      onIndexEvent(r)
    case other =>
      stash()
  }

  def initialized: Receive = {
    case UpdateIndex(_, _) =>
      indexUpdater ! UpdateIndex(lastIndexedSequenceNr, bufferedReplicationProgress)
      bufferedReplicationProgress = ReplicationProgress()
    case u @ UpdateIndexSuccess(snr, numWrites) =>
      this.lastIndexedSequenceNr = snr
      onIndexEvent(u)
    case u @ UpdateIndexFailure(cause) =>
      log.error(cause, "UpdateIndex failure")
      onIndexEvent(u)
    case w @ ReplicationWrite(Seq(), sourceLogId, lastSourceLogSequenceNrRead) =>
      bufferedReplicationProgress = bufferedReplicationProgress.update(sourceLogId, lastSourceLogSequenceNrRead)
      sender() ! ReplicationWriteSuccess(0, lastSourceLogSequenceNrRead)
    case r @ Replay(fromSequenceNr, requestor, Some(emitterAggregateId), iid) =>
      val isnr = lastIndexedSequenceNr
      val replay = for {
        rsnr <- indexStore.replayAsync(emitterAggregateId, fromSequenceNr)(event =>requestor ! Replaying(event, iid))
        nsnr = math.max(isnr, rsnr) + 1L
        s    <- eventReader.replayAsync(nsnr)(event => if (event.routingDestinations.contains(emitterAggregateId))requestor ! Replaying(event, iid))
      } yield s

      replay onComplete {
        case Success(_) => requestor ! ReplaySuccess(iid)
        case Failure(e) => requestor ! ReplayFailure(e, iid)
      }
    case GetLastSourceLogReadPosition(sourceLogId) =>
      val inc = IndexIncrement(lastIndexedSequenceNr, bufferedReplicationProgress)
      val sdr = sender()

      val ftr = for {
        prg <- indexStore.readReplicationProgressAsync
        res <- updateIncrementAsync(inc.update(prg))
      } yield res.replicationProgress(sourceLogId)

      ftr onComplete {
        case Success(snr) => sdr ! GetLastSourceLogReadPositionSuccess(sourceLogId, snr)
        case Failure(err) => sdr ! GetLastSourceLogReadPositionFailure(err); log.error(err, "GetLastSourceLogReadPosition failure")
      }
  }

  def receive =
    initializing

  private[eventuate] def createIndexStore(cassandra: Cassandra, logId: String) =
    new CassandraIndexStore(cassandra, logId)

  private def scheduleReadSequenceNr(): Unit =
    scheduler.scheduleOnce(cassandra.config.initRetryBackoff, self, ReadSequenceNr)

  private def updateIncrementAsync(increment: IndexIncrement): Future[IndexIncrement] =
    Future(updateIncrement(increment))(cassandra.readDispatcher)
  
  private def updateIncrement(increment: IndexIncrement): IndexIncrement = {
    eventReader.eventBatchIterator(increment.sequenceNr + 1L, Long.MaxValue).foldLeft(increment) {
      case (inc, batch) => inc.update(batch)
    }
  }

  override def preStart(): Unit =
    self ! ReadSequenceNr

  // ------------------------------------------------------------------
  //  Test support
  // ------------------------------------------------------------------

  def onIndexEvent(event: Any): Unit = ()
}

private[eventuate] object CassandraIndex {
  case object ReadSequenceNr
  case class ReadSequenceNrSuccess(lastIndexedSequenceNr: Long)
  case class ReadSequenceNrFailure(cause: Throwable)

  case class UpdateIndex(lastIndexedSequenceNr: Long = 0L, bufferedReplicationProgress: ReplicationProgress = ReplicationProgress())
  case class UpdateIndexSuccess(lastIndexedSequenceNr: Long, numWrites: Int)
  case class UpdateIndexFailure(cause: Throwable)

  case class ReplicationProgress(lastSourceLogSequenceNrs: Map[String, Long] = Map.empty) {
    def apply(sourceLogId: String): Long =
      lastSourceLogSequenceNrs.getOrElse(sourceLogId, 0L)

    def update(progress: ReplicationProgress): ReplicationProgress =
      progress.lastSourceLogSequenceNrs.foldLeft(this) {
        case (progress, (sourceLogId, lastSourceSequenceNrRead)) => progress.update(sourceLogId, lastSourceSequenceNrRead)
      }

    def update(sourceLogId: String, lastSourceLogSequenceNrRead: Long): ReplicationProgress =
      lastSourceLogSequenceNrs.get(sourceLogId) match {
        case Some(lastSourceLogSequenceNrStored) if lastSourceLogSequenceNrRead < lastSourceLogSequenceNrStored => this
        case _ => copy(lastSourceLogSequenceNrs + (sourceLogId -> lastSourceLogSequenceNrRead))
      }
  }

  case class IndexUpdateProgress(increment: IndexIncrement, pendingEvents: Int = 0, writes: Vector[Future[Long]] = Vector.empty) {
    def update(batch: DurableEventBatch): IndexUpdateProgress =
      copy(increment.update(batch), pendingEvents + batch.events.size)

    def writeIncrement(writer: IndexIncrement => Future[Long]): IndexUpdateProgress =
      copy(IndexIncrement(increment.sequenceNr), 0, writes = writes :+ writer(increment))
  }

  case class IndexIncrement(sequenceNr: Long, replicationProgress: ReplicationProgress = ReplicationProgress(), aggregateEvents: Map[String, Vector[DurableEvent]] = Map.empty) {
    def update(progress: ReplicationProgress): IndexIncrement =
      copy(replicationProgress = replicationProgress.update(progress))

    def update(batch: DurableEventBatch): IndexIncrement = {
      val events = addAggregateEvents(batch)
      val hsnr = batch.highestSequenceNr.get

      val updated = for {
        slid <- batch.sourceLogId
        rsnr <- batch.lastSourceLogSequenceNrRead
      } yield copy(hsnr, replicationProgress.update(slid, rsnr), events)

      updated.getOrElse(copy(hsnr, aggregateEvents = events))
    }

    private def addAggregateEvents(batch: DurableEventBatch): Map[String, Vector[DurableEvent]] =
      batch.events.foldLeft(aggregateEvents)(addAggregateEvent)

    private def addAggregateEvent(aggregateEvents: Map[String, Vector[DurableEvent]], event: DurableEvent): Map[String, Vector[DurableEvent]] =
      event.routingDestinations.foldLeft(aggregateEvents) {
        case (acc, dst) => acc.get(dst) match {
          case Some(events) => acc + (dst -> (events :+ event))
          case None         => acc + (dst -> Vector(event))
        }
      }
  }

  def props(cassandra: Cassandra, eventReader: CassandraEventReader, logId: String): Props =
    Props(new CassandraIndex(cassandra, eventReader, logId: String))
}

private[eventuate] class CassandraIndexUpdater(cassandra: Cassandra, eventReader: CassandraEventReader, indexStore: CassandraIndexStore) extends Actor {
  import CassandraIndex._
  import context.dispatcher

  val index = context.parent

  val idle: Receive = {
    case UpdateIndex(lastIndexedSequenceNr, bufferedReplicationProgress) =>
      runIndexUpdate(lastIndexedSequenceNr, bufferedReplicationProgress)
      context.become(updating)
  }

  val updating: Receive = {
    case r: UpdateIndexSuccess =>
      index ! r
      context.become(idle)
    case r: UpdateIndexFailure =>
      index ! r
      context.become(idle)
    case UpdateIndex =>
      // ignore
  }

  def receive = idle

  private def runIndexUpdate(lastIndexedSequenceNr: Long, bufferedReplicationProgress: ReplicationProgress): Unit = {
    val increment = IndexIncrement(lastIndexedSequenceNr, bufferedReplicationProgress)
    val update: Future[(Long, Int)] = Future {
      val initialProgress = eventReader.eventBatchIterator(increment.sequenceNr + 1L, Long.MaxValue).foldLeft(IndexUpdateProgress(increment)) {
        case (progress, batch) =>
          val updatedProgress = progress.update(batch)
          if (updatedProgress.pendingEvents >= cassandra.config.indexUpdateLimit) {
            // Enough data read from log. Write accumulated increment
            // so that we don't consume too much memory and continue
            // with an empty increment.
            updatedProgress.writeIncrement(indexStore.writeIndexIncrementAsync)
          } else updatedProgress
      }

      val finalProgress = if (initialProgress.pendingEvents > 0) {
        // Some log entries have been processed since last write
        // which must be finally written to the index.
        initialProgress.writeIncrement(indexStore.writeIndexIncrementAsync)
      } else initialProgress

      Future.sequence(finalProgress.writes).map {
        case Seq() => (finalProgress.increment.sequenceNr, 0)
        case snrs  => (snrs.max, snrs.size)
      }
    }.flatMap(identity)(cassandra.readDispatcher)

    update map {
      case (snr, numWrites) => UpdateIndexSuccess(snr, numWrites)
    } recover {
      case t => UpdateIndexFailure(t)
    } pipeTo self
  }
}

private[eventuate] class CassandraIndexStore(cassandra: Cassandra, logId: String) {
  import CassandraIndex._

  private val aggregateEventWriteStatement: PreparedStatement = cassandra.prepareWriteAggregateEventBatch(logId)
  private val aggregateEventReadStatement: PreparedStatement = cassandra.prepareReadAggregateEventBatches(logId)

  def replayAsync(aggregateId: String, fromSequenceNr: Long)(f: DurableEvent => Unit): Future[Long] = {
    import cassandra.readDispatcher
    Future(replay(aggregateId, fromSequenceNr)(f))
  }

  def replay(aggregateId: String, fromSequenceNr: Long)(f: DurableEvent => Unit): Long = {
    var highWatermark = fromSequenceNr - 1L
    aggregateEventIterator(aggregateId, fromSequenceNr, Long.MaxValue).foreach { event =>
      if (event.sequenceNr > highWatermark) {
        highWatermark = event.sequenceNr
        f(event)
      } else { /* this is a duplicate */ }
    }
    highWatermark
  }

  def readReplicationProgressAsync: Future[ReplicationProgress] = {
    import cassandra.readDispatcher
    cassandra.session.executeAsync(cassandra.preparedReadReplicationProgressStatement.bind(logId)).map { resultSet =>
      resultSet.iterator().asScala.foldLeft(ReplicationProgress()) {
        case (progress, row) => progress.update(row.getString("source_log_id"), row.getLong("source_log_read_pos"))
      }
    }
  }

  def readSequenceNumberAsync: Future[Long] = {
    import cassandra.readDispatcher
    cassandra.session.executeAsync(cassandra.preparedReadSequenceNumberStatement.bind(logId)).map { resultSet =>
      if (resultSet.isExhausted) 0L else resultSet.one().getLong("sequence_nr")
    }
  }

  def writeIndexIncrementAsync(increment: IndexIncrement)(implicit executor: ExecutionContext): Future[Long] = {
    val f1 = writeAggregateEventsAsync(increment.aggregateEvents)
    val f2 = writeReplicationProgressAsync(increment.replicationProgress)

    for {
      _   <- f1
      _   <- f2
      snr <- writeSequenceNrAsync(increment.sequenceNr)
    } yield snr
  }

  private def writeAggregateEventsAsync(aggregateEvents: Map[String, Vector[DurableEvent]])(implicit executor: ExecutionContext): Future[Unit] =
    Future.sequence(aggregateEvents.map {
      case (aggregateId, events) => writeAggregateEventsAsync(aggregateId, DurableEventBatch(events))
    }).map(_ => ())

  private def writeAggregateEventsAsync(aggregateId: String, batch: DurableEventBatch)(implicit executor: ExecutionContext): Future[Unit] =
    cassandra.session.executeAsync(aggregateEventWriteStatement.bind(aggregateId, batch.highestSequenceNr.get: JLong, cassandra.eventBatchToByteBuffer(batch))).map(_ => ())

  private def writeReplicationProgressAsync(progress: ReplicationProgress)(implicit executor: ExecutionContext): Future[Unit] =
    Future.sequence(progress.lastSourceLogSequenceNrs.map {
      case (sourceLogId, lastRead) => writeReplicationProgressAsync(sourceLogId, lastRead)
    }).map(_ => ())

  private def writeReplicationProgressAsync(sourceLogId: String, lastSourceLogSequenceNrRead: Long)(implicit executor: ExecutionContext): Future[Unit] =
    cassandra.session.executeAsync(cassandra.preparedWriteReplicationProgressStatement.bind(logId, sourceLogId, lastSourceLogSequenceNrRead: JLong)).map(_ => ())

  private def writeSequenceNrAsync(sequenceNr: Long)(implicit executor: ExecutionContext): Future[Long] =
    cassandra.session.executeAsync(cassandra.preparedWriteSequenceNumberStatement.bind(logId, sequenceNr: JLong)).map(_ => sequenceNr)

  private def aggregateEventIterator(aggregateId: String, fromSequenceNr: Long, toSequenceNr: Long): Iterator[DurableEvent] = for {
    batch <- aggregateEventBatchIterator(aggregateId, fromSequenceNr, toSequenceNr)
    event <- batch.events if event.sequenceNr >= fromSequenceNr && event.sequenceNr <= toSequenceNr
  } yield event

  private def aggregateEventBatchIterator(aggregateId: String, fromSequenceNr: Long, toSequenceNr: Long): Iterator[DurableEventBatch] =
    new AggregateEventBatchIterator(aggregateId, fromSequenceNr, toSequenceNr)

  private class AggregateEventBatchIterator(aggregateId: String, fromSequenceNr: Long, toSequenceNr: Long) extends Iterator[DurableEventBatch] {
    var currentSnr = fromSequenceNr
    var currentIter = newIter()
    var rowCount = 0

    def newIter(): Iterator[Row] =
      if (currentSnr > toSequenceNr) Iterator.empty else cassandra.session.execute(aggregateEventReadStatement.bind(aggregateId, currentSnr: JLong)).iterator.asScala

    @annotation.tailrec
    final def hasNext: Boolean = {
      if (currentIter.hasNext) {
        true
      } else if (rowCount < cassandra.config.maxResultSetSize) {
        // all batches consumed
        false
      } else {
        // max result set size reached, fetch again
        currentSnr += 1L
        currentIter = newIter()
        rowCount = 0
        hasNext
      }
    }

    def next(): DurableEventBatch = {
      val row = currentIter.next()
      currentSnr = row.getLong("sequence_nr")
      rowCount += 1
      cassandra.eventBatchFromByteBuffer(row.getBytes("eventBatch"))
    }
  }
}