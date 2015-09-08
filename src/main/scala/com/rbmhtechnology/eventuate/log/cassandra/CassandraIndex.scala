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

import com.datastax.driver.core.{Row, PreparedStatement}
import com.rbmhtechnology.eventuate.DurableEvent
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
    case u @ UpdateIndexSuccess(snr, _) =>
      this.lastIndexedSequenceNr = snr
      eventLog ! Initialize(snr)
      context.become(initialized)
      unstashAll()
      onIndexEvent(u)
    case u @ UpdateIndexFailure(cause) =>
      log.error(cause, "UpdateIndex failure. Retry ...")
      scheduleReadSequenceNr()
      onIndexEvent(u)
    case r @ ReadSequenceNrFailure(cause) =>
      log.error(cause, "ReadSequenceNr failed. Retry ...")
      scheduleReadSequenceNr()
      onIndexEvent(r)
    case other =>
      stash()
  }

  def initialized: Receive = {
    case UpdateIndex(_, _) =>
      indexUpdater ! UpdateIndex(lastIndexedSequenceNr, bufferedReplicationProgress)
      bufferedReplicationProgress = ReplicationProgress()
    case u @ UpdateIndexSuccess(snr, _) =>
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
        rsnr <- indexStore.replayAsync(emitterAggregateId, fromSequenceNr)(event => requestor ! Replaying(event, iid))
        nsnr = math.max(isnr, rsnr) + 1L
        s    <- eventReader.replayAsync(nsnr)(event => if (event.destinationAggregateIds.contains(emitterAggregateId)) requestor ! Replaying(event, iid))
      } yield s

      replay onComplete {
        case Success(_) => requestor ! ReplaySuccess(iid)
        case Failure(e) => requestor ! ReplayFailure(e, iid)
      }
    case GetLastSourceLogReadPosition(sourceLogId) =>
      val inc = IndexIncrement(bufferedReplicationProgress, AggregateEvents(), sequenceNr = lastIndexedSequenceNr)
      val sdr = sender()

      val ftr = for {
        prg <- indexStore.readReplicationProgressAsync
        res <- updateIncrementAsync(inc.update(prg))
      } yield res.replicationProgress.readPosition(sourceLogId)

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
    scheduler.scheduleOnce(cassandra.settings.initRetryBackoff, self, ReadSequenceNr)

  private def updateIncrementAsync(increment: IndexIncrement): Future[IndexIncrement] =
    Future(updateIncrement(increment))(cassandra.readDispatcher)
  
  private def updateIncrement(increment: IndexIncrement): IndexIncrement = {
    eventReader.eventIterator(increment.sequenceNr + 1L, Long.MaxValue).foldLeft(increment) {
      case (inc, event) => inc.update(event)
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
  case class UpdateIndexProgress(increment: IndexIncrement)
  case class UpdateIndexSuccess(lastIndexedSequenceNr: Long, steps: Int = 0)
  case class UpdateIndexFailure(cause: Throwable)

  case class ReplicationProgress(sourceLogReadPositions: Map[String, Long] = Map.empty) {
    def readPosition(sourceLogId: String): Long =
      sourceLogReadPositions.getOrElse(sourceLogId, 0L)

    def update(event: DurableEvent): ReplicationProgress =
      if (!event.replicated) this else update(event.sourceLogId, event.sourceLogReadPosition)

    def update(progress: ReplicationProgress): ReplicationProgress =
      progress.sourceLogReadPositions.foldLeft(this) {
        case (progress, (sourceLogId, position)) => progress.update(sourceLogId, position)
      }

    def update(sourceLogId: String, sourceLogReadPosition: Long): ReplicationProgress =
      sourceLogReadPositions.get(sourceLogId) match {
        case Some(position) if sourceLogReadPosition <= position => this
        case _ => copy(sourceLogReadPositions + (sourceLogId -> sourceLogReadPosition))
      }
  }

  case class AggregateEvents(events: Map[String, Vector[DurableEvent]] = Map.empty) {
    def update(event: DurableEvent): AggregateEvents =
      if (event.destinationAggregateIds.isEmpty) this else copy(event.destinationAggregateIds.foldLeft(events) {
        case (acc, dst) => acc.get(dst) match {
          case Some(events) => acc + (dst -> (events :+ event))
          case None         => acc + (dst -> Vector(event))
        }
      })
  }

  case class IndexIncrement(replicationProgress: ReplicationProgress, aggregateEvents: AggregateEvents, sequenceNr: Long) {
    def update(events: Seq[DurableEvent]): IndexIncrement =
      events.foldLeft(this) { case (acc, event) => acc.update(event) }

    def update(event: DurableEvent): IndexIncrement =
      copy(replicationProgress.update(event), aggregateEvents.update(event), event.sequenceNr)

    def update(progress: ReplicationProgress): IndexIncrement =
      copy(replicationProgress.update(progress), aggregateEvents, sequenceNr)

    def clearAggregateEvents: IndexIncrement = {
      copy(replicationProgress, AggregateEvents(), sequenceNr)
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
      update(lastIndexedSequenceNr + 1L, IndexIncrement(bufferedReplicationProgress, AggregateEvents(), lastIndexedSequenceNr))
      context.become(updating(0))
  }

  def updating(steps: Int): Receive = {
    case UpdateIndexFailure(err) =>
      index ! UpdateIndexFailure(err)
      context.become(idle)
    case UpdateIndexSuccess(snr, _) =>
      index ! UpdateIndexSuccess(snr, steps)
      context.become(idle)
    case UpdateIndexProgress(inc) =>
      update(inc.sequenceNr + 1L, inc.clearAggregateEvents)
      context.become(updating(steps + 1))
  }

  def receive = idle

  def update(fromSequenceNr: Long, increment: IndexIncrement): Unit =
    updateAsync(fromSequenceNr, increment) onComplete {
      case Success((inc, true))  => self ! UpdateIndexProgress(inc)
      case Success((inc, false)) => self ! UpdateIndexSuccess(inc.sequenceNr)
      case Failure(err)          => self ! UpdateIndexFailure(err)
    }
  
  def updateAsync(fromSequenceNr: Long, increment: IndexIncrement): Future[(IndexIncrement, Boolean)] =
    for {
      res <- eventReader.readAsync(fromSequenceNr, cassandra.settings.indexUpdateLimit)
      inc <- writeAsync(increment.update(res.events))
    } yield (inc, res.events.nonEmpty)

  def writeAsync(increment: IndexIncrement): Future[IndexIncrement] =
    indexStore.writeAsync(increment.replicationProgress, increment.aggregateEvents, increment.sequenceNr).map(_ => increment)
}

private[eventuate] class CassandraIndexStore(cassandra: Cassandra, logId: String) {
  import CassandraIndex._

  private val aggregateEventWriteStatement: PreparedStatement = cassandra.prepareWriteAggregateEvent(logId)
  private val aggregateEventReadStatement: PreparedStatement = cassandra.prepareReadAggregateEvents(logId)

  def replayAsync(aggregateId: String, fromSequenceNr: Long)(f: DurableEvent => Unit): Future[Long] = {
    import cassandra.readDispatcher
    Future(replay(aggregateId, fromSequenceNr)(f))
  }

  def replay(aggregateId: String, fromSequenceNr: Long)(f: DurableEvent => Unit): Long =
    aggregateEventIterator(aggregateId, fromSequenceNr, Long.MaxValue).foldLeft(fromSequenceNr - 1L) {
      case (_, event) => f(event); event.sequenceNr
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

  def writeAsync(replicationProgress: ReplicationProgress, aggregateEvents: AggregateEvents, sequenceNr: Long)(implicit executor: ExecutionContext): Future[Long] = {
    val f1 = writeAggregateEventsAsync(aggregateEvents)
    val f2 = writeReplicationProgressAsync(replicationProgress)

    for {
      _   <- f1
      _   <- f2
      snr <- writeSequenceNrAsync(sequenceNr)
    } yield snr
  }

  private def writeAggregateEventsAsync(aggregateEvents: AggregateEvents)(implicit executor: ExecutionContext): Future[Unit] =
    Future.sequence(aggregateEvents.events.map {
      case (aggregateId, events) => writeAggregateEventsAsync(aggregateId, events)
    }).map(_ => ())

  private def writeAggregateEventsAsync(aggregateId: String, events: Seq[DurableEvent])(implicit executor: ExecutionContext): Future[Unit] = cassandra.executeBatchAsync { batch =>
    events.foreach(event => batch.add(aggregateEventWriteStatement.bind(aggregateId, event.sequenceNr: JLong, cassandra.eventToByteBuffer(event))))
  }

  private def writeReplicationProgressAsync(progress: ReplicationProgress)(implicit executor: ExecutionContext): Future[Unit] =
    Future.sequence(progress.sourceLogReadPositions.map {
      case (sourceLogId, lastRead) => writeReplicationProgressAsync(sourceLogId, lastRead)
    }).map(_ => ())

  private def writeReplicationProgressAsync(sourceLogId: String, lastSourceLogSequenceNrRead: Long)(implicit executor: ExecutionContext): Future[Unit] =
    cassandra.session.executeAsync(cassandra.preparedWriteReplicationProgressStatement.bind(logId, sourceLogId, lastSourceLogSequenceNrRead: JLong)).map(_ => ())

  private def writeSequenceNrAsync(sequenceNr: Long)(implicit executor: ExecutionContext): Future[Long] =
    cassandra.session.executeAsync(cassandra.preparedWriteSequenceNumberStatement.bind(logId, sequenceNr: JLong)).map(_ => sequenceNr)

  private def aggregateEventIterator(aggregateId: String, fromSequenceNr: Long, toSequenceNr: Long): Iterator[DurableEvent] =
    new AggregateEventIterator(aggregateId, fromSequenceNr, toSequenceNr)

  private class AggregateEventIterator(aggregateId: String, fromSequenceNr: Long, toSequenceNr: Long) extends Iterator[DurableEvent] {
    var currentSequenceNr = fromSequenceNr
    var currentIter = newIter()
    var rowCount = 0

    def newIter(): Iterator[Row] =
      if (currentSequenceNr > toSequenceNr) Iterator.empty else cassandra.session.execute(aggregateEventReadStatement.bind(aggregateId, currentSequenceNr: JLong)).iterator.asScala

    @annotation.tailrec
    final def hasNext: Boolean = {
      if (currentIter.hasNext) {
        true
      } else if (rowCount < cassandra.settings.partitionSizeMax) {
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