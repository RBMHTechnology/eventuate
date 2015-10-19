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

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.log._
import com.rbmhtechnology.eventuate.log.TimeTracker._
import com.rbmhtechnology.eventuate.snapshot.filesystem._

import scala.collection.immutable.Seq
import scala.language.implicitConversions
import scala.util._

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
 *    of event-sourced actors or views that have an [[EventsourcedView#aggregateId aggregateId]] defined.
 *
 * @param id unique log id.
 *
 * @see [[Cassandra]]
 * @see [[DurableEvent]]
 */
class CassandraEventLog(val id: String) extends Actor with Stash with ActorLogging {
  import CassandraEventLog._
  import NotificationChannel._

  val eventStream = context.system.eventStream
  val cassandra: Cassandra = Cassandra(context.system)

  cassandra.createEventTable(id)
  cassandra.createAggregateEventTable(id)

  private val preparedWriteEventStatement = cassandra.prepareWriteEvent(id)

  private val notificationChannel = context.actorOf(Props(new NotificationChannel(id)))
  private val snapshotStore = new FilesystemSnapshotStore(new FilesystemSnapshotStoreSettings(context.system), id)
  private val progressStore = createReplicationProgressStore(cassandra, id)
  private val eventReader = createEventReader(cassandra, id)
  private val index = createIndex(cassandra, eventReader, id)

  private var registry = SubscriberRegistry()
  private var timeTracker = TimeTracker()
  private var timeCache = Map.empty[String, VectorTime].withDefaultValue(VectorTime())

  // ------------------------------------------------------
  // TODO: consider exchanging only vector time deltas
  //
  // Messages:
  //
  // - ReplicationRead
  // - ReplicationReadSuccess
  // - ReplicationWrite
  // - ReplicationWriteSuccess
  //
  // This optimization might be necessary if many event-
  // sourced actors use their own entry in vector clocks.
  // ------------------------------------------------------

  def initializing: Receive = {
    case Initialize(t) =>
      timeTracker = t
      unstashAll()
      context.become(initialized)
    case other =>
      stash()
  }

  def initialized: Receive = {
    case GetTimeTracker =>
      sender() ! GetTimeTrackerSuccess(timeTracker)
    case GetReplicationProgresses =>
      import cassandra.readDispatcher
      val sdr = sender()
      progressStore.readReplicationProgressesAsync onComplete {
        case Success(p) => sdr ! GetReplicationProgressesSuccess(p)
        case Failure(e) => sdr ! GetReplicationProgressesFailure(e)
      }
    case GetReplicationProgress(sourceLogId) =>
      import cassandra.readDispatcher
      val sdr = sender()
      val tvt = timeTracker.vectorTime
      progressStore.readReplicationProgressAsync(sourceLogId) onComplete {
        case Success(p) => sdr ! GetReplicationProgressSuccess(sourceLogId, p, tvt)
        case Failure(e) => sdr ! GetReplicationProgressFailure(e)
      }
    case SetReplicationProgress(sourceLogId, progress) =>
      val sdr = sender()
      implicit val dispatcher = context.system.dispatchers.defaultGlobalDispatcher
      progressStore.writeReplicationProgressAsync(sourceLogId, progress) onComplete {
        case Success(_) => sdr ! SetReplicationProgressSuccess(sourceLogId, progress)
        case Failure(e) => sdr ! SetReplicationProgressFailure(e)
      }
    case cmd @ Replay(_, requestor, Some(emitterAggregateId), _) =>
      registry = registry.registerAggregateSubscriber(context.watch(requestor), emitterAggregateId)
      index.forward(cmd)
    case Replay(from, requestor, None, iid) =>
      import cassandra.readDispatcher
      registry = registry.registerDefaultSubscriber(context.watch(requestor))
      eventReader.replayAsync(from)(event => requestor ! Replaying(event, iid) ) onComplete {
        case Success(_) => requestor ! ReplaySuccess(iid)
        case Failure(e) => requestor ! ReplayFailure(e, iid)
      }
    case r @ ReplicationRead(from, max, filter, targetLogId, _, currentTargetVectorTime) =>
      import cassandra.readDispatcher
      val sdr = sender()
      notificationChannel ! r
      eventReader.readAsync(from, max, filter, currentTargetVectorTime, targetLogId) onComplete {
        case Success(result) =>
          val reply = ReplicationReadSuccess(result.events, result.to, targetLogId, null)
          self.tell(reply, sdr)
        case Failure(cause)  =>
          val reply = ReplicationReadFailure(cause.getMessage, targetLogId)
          sdr ! reply
          notificationChannel ! reply
      }
    case r @ ReplicationReadSuccess(events, _, targetLogId, _) =>
      // Post-filter events using a possibly updated vector time received from the target.
      // This is an optimization to reduce network bandwidth usage. If omitted, events are
      // still filtered at target based on the current local vector time at the target (for
      // correctness).
      val currentTargetVectorTime = timeCache(targetLogId)
      val updated = events.filter(_.replicate(currentTargetVectorTime))
      val reply = r.copy(updated, currentSourceVectorTime = timeTracker.vectorTime)
      sender() ! reply
      notificationChannel ! reply
      logFilterStatistics(log, id, "source", events, updated)
    case Write(events, initiator, requestor, iid) =>
      val result = for {
        (partition, tracker) <- Try(adjustSequenceNr(events.size, cassandra.settings.partitionSizeMax, timeTracker))
        (updated, tracker2)   = tracker.prepareWrite(id, events, currentSystemTime)
        tracker3             <- Try(write(partition, updated, tracker2))
      } yield (updated, tracker3)

      result match {
        case Success((updated, tracker)) =>
          timeTracker = tracker
          registry.pushWriteSuccess(updated, initiator, requestor, iid)
          notificationChannel ! Updated(updated)
        case Failure(e) =>
          registry.pushWriteFailure(events, initiator, requestor, iid, e)
      }
    case r @ WriteN(writes) =>
      val result = for {
        (partition, tracker)      <- Try(adjustSequenceNr(r.size, cassandra.settings.partitionSizeMax, timeTracker))
        (updatedWrites, tracker2)  = tracker.prepareWrites(id, writes, currentSystemTime)
        updatedEvents              = updatedWrites.flatMap(_.events)
        tracker3                  <- Try(write(partition, updatedEvents, tracker2))
      } yield (updatedWrites, updatedEvents, tracker3)

      result match {
        case Success((updatedWrites, updatedEvents, tracker)) =>
          timeTracker = tracker
          updatedWrites.foreach(w => registry.pushWriteSuccess(w.events, w.initiator, w.requestor, w.instanceId))
          notificationChannel ! Updated(updatedEvents)
        case Failure(e) =>
          writes.foreach(w => registry.pushWriteFailure(w.events, w.initiator, w.requestor, w.instanceId, e))
      }
      sender() ! WriteNComplete // notify batch layer that write completed
    case ReplicationWrite(events, sourceLogId, replicationProgress, currentSourceVectorTime) =>
      timeCache = timeCache.updated(sourceLogId, currentSourceVectorTime)
      val result = for {
        (partition, tracker) <- Try(adjustSequenceNr(events.size, cassandra.settings.partitionSizeMax, timeTracker))
        (updated, tracker2)   = tracker.prepareReplicate(id, events, replicationProgress)
        tracker3             <- Try(write(partition, updated, tracker2))
      } yield (updated, tracker3)

      result match {
        case Success((updated, tracker)) =>
          val rws = ReplicationWriteSuccess(events.size, replicationProgress, tracker.vectorTime)
          val sdr = sender()
          timeTracker = tracker
          registry.pushReplicateSuccess(updated)
          notificationChannel ! rws
          notificationChannel ! Updated(updated)
          logFilterStatistics(log, id, "target", events, updated)
          implicit val dispatcher = context.system.dispatchers.defaultGlobalDispatcher
          progressStore.writeReplicationProgressAsync(sourceLogId, replicationProgress) onComplete {
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
        case Failure(e) =>
          sender() ! ReplicationWriteFailure(e)
      }
    case LoadSnapshot(emitterId, requestor, iid) =>
      import cassandra.readDispatcher
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

  private[eventuate] def createEventReader(cassandra: Cassandra, logId: String) =
    new CassandraEventReader(cassandra, logId)

  private[eventuate] def createIndex(cassandra: Cassandra, eventReader: CassandraEventReader, logId: String) =
    context.actorOf(CassandraIndex.props(cassandra, eventReader, logId))

  private[eventuate] def createReplicationProgressStore(cassandra: Cassandra, logId: String) =
    new CassandraReplicationProgressStore(cassandra, logId)

  private[eventuate] def write(partition: Long, events: Seq[DurableEvent], tracker: TimeTracker): TimeTracker = {
    cassandra.executeBatch { batch =>
      events.foreach { event =>
        batch.add(preparedWriteEventStatement.bind(partition: JLong, event.localSequenceNr: JLong, cassandra.eventToByteBuffer(event)))
      }
    }
    if (tracker.updateCount >= cassandra.settings.indexUpdateLimit) {
      index ! CassandraIndex.UpdateIndex()
      tracker.copy(updateCount = 0L)
    } else {
      tracker
    }
  }

}

object CassandraEventLog {
  private[eventuate] case class Initialize(timeTracker: TimeTracker)

  /**
   * Adjusts `timeTracker.sequenceNumber` if a batch of `batchSize` doesn't fit in the current partition.
   */
  def adjustSequenceNr(batchSize: Long, maxBatchSize: Long, tracker: TimeTracker): (Long, TimeTracker) = {
    require(batchSize <= maxBatchSize, s"write batch size (${batchSize}) must not be greater than maximum partition size (${maxBatchSize})")

    val currentPartition = partitionOf(tracker.sequenceNr, maxBatchSize)
    val remainingPartitionSize = partitionSize(tracker.sequenceNr, maxBatchSize)
    if (remainingPartitionSize < batchSize) {
      (currentPartition + 1L, tracker.advanceSequenceNr(remainingPartitionSize))
    } else {
      (currentPartition, tracker)
    }
  }

  /**
   * Partition number for given `sequenceNr`.
   */
  def partitionOf(sequenceNr: Long, partitionSizeMax: Long): Long =
    if (sequenceNr == 0L) -1L else (sequenceNr - 1L) / partitionSizeMax

  /**
   * Remaining partition size given the current `sequenceNr`.
   */
  def partitionSize(sequenceNr: Long, partitionSizeMax: Long): Long = {
    val m = sequenceNr % partitionSizeMax
    if (m == 0L) m else partitionSizeMax - m
  }

  /**
   * First sequence number of given `partition`.
   */
  def firstSequenceNr(partition: Long, partitionSizeMax: Long): Long =
    partition * partitionSizeMax + 1L

  /**
   * Creates a [[CassandraEventLog]] configuration object.
   *
   * @param logId unique log id.
   * @param batching `true` if write-batching shall be enabled (recommended).
   */
  def props(logId: String, batching: Boolean = true): Props = {
    val logProps = Props(new CassandraEventLog(logId)).withDispatcher("eventuate.log.cassandra.write-dispatcher")
    if (batching) Props(new BatchingEventLog(logProps)) else logProps
  }
}
