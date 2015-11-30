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

package com.rbmhtechnology.eventuate.log

import java.io.Closeable

import akka.actor._
import akka.dispatch.MessageDispatcher

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.snapshot.filesystem._

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration.FiniteDuration
import scala.util._

/**
 * Event log settings to be implemented by storage providers.
 */
trait EventLogSettings {
  /**
   * Maximum number of events to store per partition. If a storage provider does not support
   * partitioned event storage it should return `Long.MaxValue`, otherwise the appropriate
   * partition size. Eventuate internally calculates the target partition for batch writes.
   * If an event batch doesn't fit into the current partition, it will be written to the next
   * partition, so that batch writes are always single-partition writes.
   */
  def partitionSizeMax: Long

  /**
   * Maximum number of clock recovery retries.
   */
  def initRetryMax: Int

  /**
   * Delay between clock recovery retries.
   */
  def initRetryDelay: FiniteDuration
}

/**
 * A clock that tracks the current sequence number and the version vector of an event log.
 * The version vector is the merge result of vector timestamps of all events that have been
 * written to that event log.
 */
case class EventLogClock(sequenceNr: Long = 0L, versionVector: VectorTime = VectorTime.Zero) {
  /**
   * Advances `sequenceNr` by given `delta`.
   */
  def advanceSequenceNr(delta: Long = 1L): EventLogClock =
    copy(sequenceNr = sequenceNr + delta)

  /**
   * Sets `sequenceNr` to the event's local sequence number and merges `versionVector` with
   * the event's vector timestamp.
   */
  def update(event: DurableEvent): EventLogClock =
    copy(sequenceNr = event.localSequenceNr, versionVector = versionVector.merge(event.vectorTimestamp))
}

/**
 * Result of an event batch-read operation.
 *
 * @param events Read event batch.
 * @param to Last read position in the event log.
 */
case class BatchReadResult(events: Seq[DurableEvent], to: Long) extends DurableEventBatch

/**
 * Event log storage provider interface (SPI).
 *
 * '''Please note:''' This interface is preliminary and likely to be changed in future versions.
 *
 * @tparam A Event iterator factory parameter type (see [[eventIterator]] factory method).
 */
trait EventLogSPI[A] { this: Actor =>
  /**
   * Event log settings.
   */
  def settings: EventLogSettings

  /**
   * Asynchronously recovers the event log clock during initialization.
   */
  def recoverClock: Future[EventLogClock]

  /**
   * Called after successful event log clock recovery.
   */
  def recoverClockSuccess(clock: EventLogClock): Unit = ()

  /**
   * Called after failed event log clock recovery.
   */
  def recoverClockFailure(cause: Throwable): Unit = ()

  /**
   * Asynchronously reads all stored replication progresses.
   */
  def readReplicationProgresses: Future[Map[String, Long]]

  /**
   * Asynchronously reads the replication progress for given source `logId`.
   */
  def readReplicationProgress(logId: String): Future[Long]

  /**
   * Asynchronously writes the replication `progress` for given source `logId`.
   */
  def writeReplicationProgress(logId: String, progress: Long): Future[Unit]

  /**
   * Creates a event iterator using the given `parameters` object. This method is called on a thread that is
   * '''not''' a dispatcher thread of an implementing [[EventLog]] actor. It is therefore important that the
   * returned iterator does not close over current actor state. If the initialization of the iterator depends
   * on current actor state, it must be captured during `eventIteratorParameters` calls and returned by these
   * methods as parameter object of type `A`. This object is then passed as argument to this method and used
   * for asynchronous event iterator construction.
   */
  def eventIterator(parameters: A): Iterator[DurableEvent] with Closeable

  /**
   * Creates an event iterator parameter object that encodes the given parameters. The returned parameter
   * object is passed as argument to `eventIterator`.
   *
   * @param fromSequenceNr sequence number to start iteration (inclusive).
   * @param toSequenceNr sequence number to stop iteration (inclusive).
   */
  def eventIteratorParameters(fromSequenceNr: Long, toSequenceNr: Long): A

  /**
   * Creates an event iterator parameter object that encodes the given parameters. The returned parameter
   * object is passed as argument to `eventIterator`.
   *
   * @param fromSequenceNr sequence number to start iteration (inclusive).
   * @param toSequenceNr sequence number to stop iteration (inclusive).
   * @param aggregateId must be contained in [[DurableEvent.destinationAggregateIds]].
   */
  def eventIteratorParameters(fromSequenceNr: Long, toSequenceNr: Long, aggregateId: String): A

  /**
   * Asynchronously batch-reads events from the raw event log. At most `max` events must be returned that are
   * within the sequence number bounds `fromSequenceNr` and `toSequenceNr` and that pass the given `filter`.
   *
   * @param fromSequenceNr sequence number to start reading (inclusive).
   * @param toSequenceNr sequence number to stop reading (inclusive)
   *                     or earlier if `max` events have already been read.
   */
  def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int, filter: DurableEvent => Boolean): Future[BatchReadResult]

  /**
   * Synchronously writes `events` to the given `partition`. The partition is calculated from the configured
   * `partitionSizeMax` and the current sequence number. Asynchronous writes will be supported in future versions.
   *
   * @see [[EventLogSettings]]
   */
  def write(events: Seq[DurableEvent], partition: Long, clock: EventLogClock): Unit
}

/**
 * An abstract event log that handles [[EventsourcingProtocol]] and [[ReplicationProtocol]] messages and
 * translates them to read and write operations declared on the [[EventLogSPI]] trait. Storage providers
 * implement an event log by implementing the [[EventLogSPI]] methods.
 */
abstract class EventLog[A](id: String) extends Actor with EventLogSPI[A] with Stash with ActorLogging {
  import EventLog._

  // ---------------------------------------------------------------------------
  //  TODO: only transfer version vector deltas to update replicaVersionVectors
  // ---------------------------------------------------------------------------

  /**
   * Service context for asynchronous read operations.
   */
  object services {
    /**
     * Dispatcher for asynchronous read operations.
     */
    implicit val readDispatcher: MessageDispatcher =
      context.system.dispatchers.lookup("eventuate.log.dispatchers.read-dispatcher")

    /**
     * Scheduler of current actor system.
     */
    implicit val scheduler: Scheduler =
      context.system.scheduler
  }

  /**
   * The clock that tracks the sequence number and version vector of this event log. The sequence
   * number is the log's logical time. The version vector is the merge result of vector timestamps
   * of all events that have been written to this event log. The version vector is used to exclude
   * events from being written if they are in the event log's causal past (which makes replication
   * writes idempotent).
   */
  private var clock: EventLogClock =
    EventLogClock()

  /**
   * Cached version vectors of event log replicas. They are used to exclude redundantly read events from
   * being transferred to a replication target. This is an optimization to save network bandwidth. Even
   * without this optimization, redundantly transferred events are reliably excluded at the target site,
   * using its local version version vector. The version vector cache is continuously updated during event
   * replication.
   */
  private var replicaVersionVectors: Map[String, VectorTime] =
    Map.empty[String, VectorTime].withDefaultValue(VectorTime.Zero)

  /**
   * Registry for event-sourced actors, views, writers and processors interacting with this event
   * log.
   */
  private var registry: SubscriberRegistry =
    SubscriberRegistry()

  /**
   * Channel to notify [[Replicator]]s, reading from this event log, about updates.
   */
  private val channel: ActorRef =
    context.actorOf(Props(new NotificationChannel(id)))

  /**
   * This event log's snapshot store.
   */
  private val snapshotStore: FilesystemSnapshotStore =
    new FilesystemSnapshotStore(new FilesystemSnapshotStoreSettings(context.system), id)

  private def initializing: Receive = {
    case RecoverySuccess(c) =>
      clock = c
      recoverClockSuccess(c)
      unstashAll()
      context.become(initialized)
    case RecoveryFailure(e) =>
      log.error(e, "Cannot recover clock")
      context.stop(self)
    case other =>
      stash()
  }

  private def initialized: Receive = {
    case GetEventLogClock =>
      sender() ! GetEventLogClockSuccess(clock)
    case GetReplicationProgresses =>
      import services.readDispatcher
      val sdr = sender()
      readReplicationProgresses onComplete {
        case Success(p) => sdr ! GetReplicationProgressesSuccess(p)
        case Failure(e) => sdr ! GetReplicationProgressesFailure(e)
      }
    case GetReplicationProgress(sourceLogId) =>
      import services.readDispatcher
      val sdr = sender()
      val tvv = clock.versionVector
      readReplicationProgress(sourceLogId) onComplete {
        case Success(p) => sdr ! GetReplicationProgressSuccess(sourceLogId, p, tvv)
        case Failure(e) => sdr ! GetReplicationProgressFailure(e)
      }
    case SetReplicationProgress(sourceLogId, progress) =>
      val sdr = sender()
      implicit val dispatcher = context.dispatcher
      writeReplicationProgress(sourceLogId, progress) onComplete {
        case Success(_) => sdr ! SetReplicationProgressSuccess(sourceLogId, progress)
        case Failure(e) => sdr ! SetReplicationProgressFailure(e)
      }
    case Replay(fromSequenceNr, max, requestor, Some(emitterAggregateId), iid) =>
      val iteratorSettings = eventIteratorParameters(fromSequenceNr, clock.sequenceNr, emitterAggregateId) // avoid async evaluation
      registry = registry.registerAggregateSubscriber(context.watch(requestor), emitterAggregateId)
      replayer(requestor, eventIterator(iteratorSettings), fromSequenceNr) ! ReplayNext(max, iid)
    case Replay(fromSequenceNr, max, requestor, None, iid) =>
      val iteratorSettings = eventIteratorParameters(fromSequenceNr, clock.sequenceNr) // avoid async evaluation
      registry = registry.registerDefaultSubscriber(context.watch(requestor))
      replayer(requestor, eventIterator(iteratorSettings), fromSequenceNr) ! ReplayNext(max, iid)
    case r @ ReplicationRead(from, max, filter, targetLogId, _, currentTargetVersionVector) =>
      import services.readDispatcher
      val sdr = sender()
      channel ! r
      read(from, clock.sequenceNr, max, evt => evt.replicable(currentTargetVersionVector, filter)) onComplete {
        case Success(BatchReadResult(events, progress)) =>
          val reply = ReplicationReadSuccess(events, progress, targetLogId, null)
          self.tell(reply, sdr)
        case Failure(cause) =>
          val reply = ReplicationReadFailure(cause.getMessage, targetLogId)
          sdr ! reply
          channel ! reply
      }
    case r @ ReplicationReadSuccess(events, _, targetLogId, _) =>
      // Post-exclude events using a possibly updated version vector received from the
      // target. This is an optimization to save network bandwidth. If omitted, events
      // are still excluded at target based on the current local version vector at the
      // target (for correctness).
      val currentTargetVersionVector = replicaVersionVectors(targetLogId)
      val updated = events.filterNot(_.before(currentTargetVersionVector))
      val reply = r.copy(updated, currentSourceVersionVector = clock.versionVector)
      sender() ! reply
      channel ! reply
      logFilterStatistics("source", events, updated)
    case w: Write =>
      processWrites(Seq(w))
    case WriteN(writes) =>
      processWrites(writes)
      sender() ! WriteNComplete
    case w: ReplicationWrite =>
      processReplicationWrites(Seq(w.copy(initiator = sender())))
    case ReplicationWriteN(writes) =>
      processReplicationWrites(writes)
      sender() ! ReplicationWriteNComplete
    case LoadSnapshot(emitterId, requestor, iid) =>
      import services.readDispatcher
      snapshotStore.loadAsync(emitterId) onComplete {
        case Success(s) => requestor ! LoadSnapshotSuccess(s, iid)
        case Failure(e) => requestor ! LoadSnapshotFailure(e, iid)
      }
    case SaveSnapshot(snapshot, initiator, requestor, iid) =>
      import context.dispatcher
      snapshotStore.saveAsync(snapshot) onComplete {
        case Success(_) => requestor.tell(SaveSnapshotSuccess(snapshot.metadata, iid), initiator)
        case Failure(e) => requestor.tell(SaveSnapshotFailure(snapshot.metadata, e, iid), initiator)
      }
    case DeleteSnapshots(lowerSequenceNr) =>
      import context.dispatcher
      val sdr = sender()
      snapshotStore.deleteAsync(lowerSequenceNr) onComplete {
        case Success(_) => sdr ! DeleteSnapshotsSuccess
        case Failure(e) => sdr ! DeleteSnapshotsFailure(e)
      }
    case Terminated(requestor) =>
      registry = registry.unregisterSubscriber(requestor)
  }

  override def receive =
    initializing

  private[eventuate] def currentSystemTime: Long =
    System.currentTimeMillis

  private def replayer(requestor: ActorRef, iterator: => Iterator[DurableEvent] with Closeable, fromSequenceNr: Long): ActorRef =
    context.actorOf(Props(new ChunkedEventReplay(requestor, iterator)).withDispatcher(services.readDispatcher.id))

  private def processWrites(writes: Seq[Write]): Unit = {
    val result = for {
      (partition, clock1) <- Try(adjustSequenceNr(writes.map(_.size).sum, settings.partitionSizeMax, clock))
      (updatedWrites, clock2) = prepareWrites(id, writes, currentSystemTime, clock1)
      updatedEvents = updatedWrites.flatMap(_.events)
      _ <- Try(write(updatedEvents, partition, clock2))
    } yield (updatedWrites, updatedEvents, clock2)

    result match {
      case Success((updatedWrites, updatedEvents, clock2)) =>
        clock = clock2
        updatedWrites.foreach(w => registry.pushWriteSuccess(w.events, w.initiator, w.requestor, w.instanceId))
        channel ! NotificationChannel.Updated(updatedEvents)
      case Failure(e) =>
        writes.foreach(w => registry.pushWriteFailure(w.events, w.initiator, w.requestor, w.instanceId, e))
    }
  }

  private def processReplicationWrites(writes: Seq[ReplicationWrite]): Unit = {
    writes.foreach { write =>
      replicaVersionVectors = replicaVersionVectors.updated(write.sourceLogId, write.currentSourceVersionVector)
    }
    val result = for {
      (partition, clock1) <- Try(adjustSequenceNr(writes.map(_.size).sum, settings.partitionSizeMax, clock))
      (updatedWrites, clock2) = prepareReplicationWrites(id, writes, clock1)
      updatedEvents = updatedWrites.flatMap(_.events)
      _ <- Try(write(updatedEvents, partition, clock2))
    } yield (updatedWrites, updatedEvents, clock2)

    result match {
      case Success((updatedWrites, updatedEvents, clock2)) =>
        clock = clock2
        updatedWrites.foreach { w =>
          val rws = ReplicationWriteSuccess(w.size, w.replicationProgress, clock2.versionVector)
          val sdr = w.initiator
          registry.pushReplicateSuccess(w.events)
          channel ! w
          implicit val dispatcher = context.system.dispatchers.defaultGlobalDispatcher
          writeReplicationProgress(w.sourceLogId, w.replicationProgress) onComplete {
            case Success(_) =>
              sdr ! rws
            case Failure(e) =>
              // Write failure of replication progress can be ignored. Using a stale
              // progress to resume replication will redundantly read events from a
              // source log but these events will be successfully identified as
              // duplicates, either at source or latest at target.
              log.warning(s"Writing of replication progress failed: ${e.getMessage}")
              sdr ! ReplicationWriteFailure(e)
          }
        }
        channel ! NotificationChannel.Updated(updatedEvents)
      case Failure(e) =>
        writes.foreach { write =>
          write.initiator ! ReplicationWriteFailure(e)
        }
    }
  }

  private def prepareWrites(logId: String, writes: Seq[Write], systemTimestamp: Long, clock: EventLogClock): (Seq[Write], EventLogClock) =
    writes.foldLeft((Vector.empty[Write], clock)) {
      case ((writes2, clock2), write) => prepareWrite(logId, write.events, systemTimestamp, clock2) match {
        case (updated, clock3) => (writes2 :+ write.copy(events = updated), clock3)
      }
    }

  private def prepareReplicationWrites(logId: String, writes: Seq[ReplicationWrite], clock: EventLogClock): (Seq[ReplicationWrite], EventLogClock) = {
    writes.foldLeft((Vector.empty[ReplicationWrite], clock)) {
      case ((writes2, clock2), write) => prepareReplicationWrite(logId, write.events, write.replicationProgress, clock2) match {
        case (updated, clock3) => (writes2 :+ write.copy(events = updated), clock3)
      }
    }
  }

  private def prepareWrite(logId: String, events: Seq[DurableEvent], systemTimestamp: Long, clock: EventLogClock): (Seq[DurableEvent], EventLogClock) = {
    var snr = clock.sequenceNr
    var lvv = clock.versionVector

    val updated = events.map { e =>
      snr += 1L

      val e2 = e.prepare(logId, snr, systemTimestamp)
      lvv = lvv.merge(e2.vectorTimestamp)
      e2
    }

    (updated, clock.copy(sequenceNr = snr, versionVector = lvv))
  }

  private def prepareReplicationWrite(logId: String, events: Seq[DurableEvent], replicationProgress: Long, clock: EventLogClock): (Seq[DurableEvent], EventLogClock) = {
    var snr = clock.sequenceNr
    var lvv = clock.versionVector

    val updated = events.foldLeft(Vector.empty[DurableEvent]) {
      case (acc, e) if e.before(clock.versionVector) =>
        // Exclude events from writing that are in the causal past of this event log. Excluding
        // them at the target is needed for correctness. Events are also excluded at sources
        // (to save network bandwidth) but this is only an optimization which cannot achieve
        // 100% filtering coverage for certain replication network topologies.
        acc
      case (acc, e) =>
        snr += 1L

        val e2 = e.prepare(logId, snr, e.systemTimestamp)
        lvv = lvv.merge(e2.vectorTimestamp)
        acc :+ e2
    }
    logFilterStatistics("target", events, updated)
    (updated, clock.copy(sequenceNr = snr, versionVector = lvv))
  }

  private def logFilterStatistics(location: String, before: Seq[DurableEvent], after: Seq[DurableEvent]): Unit = {
    val bl = before.length
    val al = after.length
    if (al < bl) {
      val diff = bl - al
      val perc = diff * 100.0 / bl
      log.info(f"[$id] excluded $diff events ($perc%3.1f%% at $location)")
    }
  }

  override def preStart(): Unit = {
    import services._
    Retry(recoverClock, settings.initRetryDelay, settings.initRetryMax) onComplete {
      case Success(c) => self ! RecoverySuccess(c)
      case Failure(e) => self ! RecoveryFailure(e)
    }
  }
}

object EventLog {
  /**
   * Internally sent to an [[EventLog]] after successful clock recovery.
   */
  private case class RecoverySuccess(clock: EventLogClock)

  /**
   * Internally sent to an [[EventLog]] after failed clock recovery.
   */
  private case class RecoveryFailure(cause: Throwable)

  /**
   * Partition number for given `sequenceNr`.
   */
  def partitionOf(sequenceNr: Long, partitionSizeMax: Long): Long =
    if (sequenceNr == 0L) -1L else (sequenceNr - 1L) / partitionSizeMax

  /**
   * Remaining partition size given the current `sequenceNr`.
   */
  def remainingPartitionSize(sequenceNr: Long, partitionSizeMax: Long): Long = {
    val m = sequenceNr % partitionSizeMax
    if (m == 0L) m else partitionSizeMax - m
  }

  /**
   * First sequence number of given `partition`.
   */
  def firstSequenceNr(partition: Long, partitionSizeMax: Long): Long =
    partition * partitionSizeMax + 1L

  /**
   * Last sequence number of given `partition`.
   */
  def lastSequenceNr(partition: Long, partitionSizeMax: Long): Long =
    (partition + 1L) * partitionSizeMax

  /**
   * Adjusts `clock.sequenceNumber` if a batch of `batchSize` doesn't fit in the current partition.
   */
  private def adjustSequenceNr(batchSize: Long, maxBatchSize: Long, clock: EventLogClock): (Long, EventLogClock) = {
    require(batchSize <= maxBatchSize, s"write batch size (${batchSize}) must not be greater than maximum partition size (${maxBatchSize})")

    val currentPartition = partitionOf(clock.sequenceNr, maxBatchSize)
    val remainingSize = remainingPartitionSize(clock.sequenceNr, maxBatchSize)
    if (remainingSize < batchSize) {
      (currentPartition + 1L, clock.advanceSequenceNr(remainingSize))
    } else {
      (currentPartition, clock)
    }
  }
}
