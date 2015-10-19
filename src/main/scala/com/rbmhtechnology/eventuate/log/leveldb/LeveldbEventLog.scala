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

package com.rbmhtechnology.eventuate.log.leveldb

import java.io.File
import java.nio.ByteBuffer

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.util._

import akka.actor._
import akka.serialization.SerializationExtension

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory.factory

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.log._
import com.rbmhtechnology.eventuate.snapshot.filesystem._

/**
 * An event log actor with LevelDB as storage backend. The directory containing the LevelDB files
 * for this event log is named after the constructor parameters using the template "`prefix`-`id`"
 * and stored in a root directory defined by the `log.leveldb.dir` configuration.
 *
 * '''Please note:''' `prefix` and `id` are currently not escaped when creating the directory name.
 *
 * @param id unique log id.
 * @param prefix prefix of the directory that contains the LevelDB files
 */
class LeveldbEventLog(val id: String, prefix: String) extends Actor with ActorLogging {
  import LeveldbEventLog._
  import NotificationChannel._
  import TimeTracker._

  private val eventStream = context.system.eventStream
  private val serialization = SerializationExtension(context.system)

  private val leveldbSettings = new LeveldbSettings(context.system)
  private val leveldbOptions = new Options().createIfMissing(true)
  private val leveldbWriteOptions = new WriteOptions().sync(leveldbSettings.fsync).snapshot(false)
  private def leveldbReadOptions = new ReadOptions().verifyChecksums(false)

  private val leveldbDir = new File(leveldbSettings.rootDir, s"${prefix}-${id}"); leveldbDir.mkdirs()
  private val leveldb = factory.open(leveldbDir, leveldbOptions)

  private var registry = SubscriberRegistry()
  private var timeTracker = TimeTracker()
  private var timeCache = Map.empty[String, VectorTime].withDefaultValue(VectorTime())

  private val notificationChannel = context.actorOf(Props(new NotificationChannel(id)))
  private val snapshotStore = new FilesystemSnapshotStore(new FilesystemSnapshotStoreSettings(context.system), id)
  private val replicationProgressMap = new LeveldbReplicationProgressStore(leveldb, -3, eventLogIdMap.numericId, eventLogIdMap.findId)
  private val aggregateIdMap = new LeveldbNumericIdentifierStore(leveldb, -1)
  private val eventLogIdMap = new LeveldbNumericIdentifierStore(leveldb, -2)

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

  final def receive = {
    case GetTimeTracker =>
      sender() ! GetTimeTrackerSuccess(timeTracker)
    case GetReplicationProgresses =>
      Try(withIterator(iter => replicationProgressMap.readReplicationProgresses(iter))) match {
        case Success(r) => sender() ! GetReplicationProgressesSuccess(r)
        case Failure(e) => sender() ! GetReplicationProgressesFailure(e)
      }
    case GetReplicationProgress(sourceLogId) =>
      Try(replicationProgressMap.readReplicationProgress(sourceLogId)) match {
        case Success(r) => sender() ! GetReplicationProgressSuccess(sourceLogId, r, timeTracker.vectorTime)
        case Failure(e) => sender() ! GetReplicationProgressFailure(e)
      }
    case SetReplicationProgress(sourceLogId, progress) =>
      Try(withBatch(batch => replicationProgressMap.writeReplicationProgress(sourceLogId, progress, batch))) match {
        case Success(_) => sender() ! SetReplicationProgressSuccess(sourceLogId, progress)
        case Failure(e) => sender() ! SetReplicationProgressFailure(e)
      }
    case Replay(from, requestor, None, iid) =>
      import leveldbSettings.readDispatcher
      registry = registry.registerDefaultSubscriber(context.watch(requestor))
      Future(replay(from, EventKey.DefaultClassifier)(event => requestor ! Replaying(event, iid))) onComplete {
        case Success(_) => requestor ! ReplaySuccess(iid)
        case Failure(e) => requestor ! ReplayFailure(e, iid)
      }
    case Replay(from, requestor, Some(emitterAggregateId), iid) =>
      import leveldbSettings.readDispatcher
      val nid = aggregateIdMap.numericId(emitterAggregateId)
      registry = registry.registerAggregateSubscriber(context.watch(requestor), emitterAggregateId)
      Future(replay(from, nid)(event => requestor ! Replaying(event, iid))) onComplete {
        case Success(_) => requestor ! ReplaySuccess(iid)
        case Failure(e) => requestor ! ReplayFailure(e, iid)
      }
    case r @ ReplicationRead(from, max, filter, targetLogId, _, currentTargetVectorTime) =>
      import leveldbSettings.readDispatcher
      val sdr = sender()
      notificationChannel ! r
      Future(read(from, max, filter, currentTargetVectorTime)) onComplete {
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
      val (updated, tracker) = timeTracker.prepareWrite(id, events, currentSystemTime)
      Try(write(updated, tracker)) match {
        case Success(tracker2) =>
          timeTracker = tracker2
          registry.pushWriteSuccess(updated, initiator, requestor, iid)
          notificationChannel ! Updated(updated)
        case Failure(e) =>
          registry.pushWriteFailure(events, initiator, requestor, iid, e)
      }
    case WriteN(writes) =>
      val (updatedWrites, tracker) = timeTracker.prepareWrites(id, writes, currentSystemTime)
      val updatedEvents = updatedWrites.flatMap(_.events)
      Try(withBatch(batch => write(updatedEvents, tracker, batch))) match {
        case Success(tracker2) =>
          timeTracker = tracker2
          updatedWrites.foreach(w => registry.pushWriteSuccess(w.events, w.initiator, w.requestor, w.instanceId))
          notificationChannel ! Updated(updatedEvents)
        case Failure(e) =>
          writes.foreach(w => registry.pushWriteFailure(w.events, w.initiator, w.requestor, w.instanceId, e))
      }
      sender() ! WriteNComplete // notify batch layer that write completed
    case w @ ReplicationWrite(events, sourceLogId, replicationProgress, currentSourceVectorTime) =>
      timeCache = timeCache.updated(sourceLogId, currentSourceVectorTime)
      val (updated, tracker) = timeTracker.prepareReplicate(id, events, replicationProgress)
      Try {
        withBatch { batch =>
          replicationProgressMap.writeReplicationProgress(sourceLogId, replicationProgress, batch)
          write(updated, tracker, batch)
        }
      } match {
        case Success(tracker2) =>
          sender() ! ReplicationWriteSuccess(events.size, replicationProgress, tracker2.vectorTime)
          timeTracker = tracker2
          registry.pushReplicateSuccess(updated)
          notificationChannel ! w
          notificationChannel ! Updated(updated)
          logFilterStatistics(log, id, "target", events, updated)
        case Failure(e) =>
          sender() ! ReplicationWriteFailure(e)
      }
    case LoadSnapshot(emitterId, requestor, iid) =>
      import leveldbSettings.readDispatcher
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

  def logDir: File =
    leveldbDir

  def currentSystemTime: Long =
    System.currentTimeMillis

  private[eventuate] def write(events: Seq[DurableEvent], tracker: TimeTracker): TimeTracker =
    withBatch(write(events, tracker, _))

  private[eventuate] def write(events: Seq[DurableEvent], tracker: TimeTracker, batch: WriteBatch): TimeTracker = {
    events.foreach { event =>
      val sequenceNr = event.localSequenceNr
      val eventBytes = this.eventBytes(event)
      batch.put(eventKeyBytes(EventKey.DefaultClassifier, sequenceNr), eventBytes)
      event.destinationAggregateIds.foreach { id => // additionally index events by aggregate id
        batch.put(eventKeyBytes(aggregateIdMap.numericId(id), sequenceNr), eventBytes)
      }
    }
    if (tracker.updateCount >= leveldbSettings.stateSnapshotLimit) {
      batch.put(timeTrackerKeyBytes, timeTrackerBytes(tracker))
      tracker.copy(updateCount = 0L)
    } else {
      tracker
    }
  }

  private[eventuate] def read(from: Long, max: Int, filter: ReplicationFilter, lower: VectorTime): ReadResult = withIterator { iter =>
    val first = if (from < 1L) 1L else from
    var last = first - 1
    @annotation.tailrec
    def go(events: Vector[DurableEvent], num: Int): Vector[DurableEvent] = if (iter.hasNext && num > 0) {
      val nextEntry = iter.next()
      val nextKey = eventKey(nextEntry.getKey)
      if (nextKey.classifier == EventKey.DefaultClassifier) {
        val nextEvt = event(nextEntry.getValue)
        last = nextKey.sequenceNr
        if (!nextEvt.replicate(lower, filter)) go(events, num)
        else go(events :+ event(nextEntry.getValue), num - 1)
      } else events
    } else events
    iter.seek(eventKeyBytes(EventKey.DefaultClassifier, first))
    ReadResult(go(Vector.empty, max), last)
  }

  private[eventuate] def replay(from: Long, classifier: Int)(f: DurableEvent => Unit): Unit = withIterator { iter =>
    val first = if (from < 1L) 1L else from
    @annotation.tailrec
    def go(): Unit = if (iter.hasNext) {
      val nextEntry = iter.next()
      val nextKey = eventKey(nextEntry.getKey)
      if (nextKey.classifier == classifier) {
        f(event(nextEntry.getValue))
        go()
      }
    }
    iter.seek(eventKeyBytes(classifier, first))
    go()
  }

  private[eventuate] def withBatch[R](body: WriteBatch ⇒ R): R = {
    val batch = leveldb.createWriteBatch()
    try {
      val r = body(batch)
      leveldb.write(batch, leveldbWriteOptions)
      r
    } finally {
      batch.close()
    }
  }

  private[eventuate] def withIterator[R](body: DBIterator => R): R = {
    val so = snapshotOptions()
    val iter = leveldb.iterator(so)
    addActiveIterator(iter)
    try {
      body(iter)
    } finally {
      iter.close()
      removeActiveIterator(iter)
      so.snapshot().close()
    }
  }

  private def eventBytes(e: DurableEvent): Array[Byte] =
    serialization.serialize(e).get

  private def event(a: Array[Byte]): DurableEvent =
    serialization.deserialize(a, classOf[DurableEvent]).get

  private def timeTrackerBytes(t: TimeTracker): Array[Byte] =
    serialization.serialize(t).get

  private def timeTracker(a: Array[Byte]): TimeTracker =
    serialization.deserialize(a, classOf[TimeTracker]).get

  private def snapshotOptions(): ReadOptions =
    leveldbReadOptions.snapshot(leveldb.getSnapshot)

  override def preStart(): Unit = {
    withIterator(iter => aggregateIdMap.readIdMap(iter))
    withIterator(iter => eventLogIdMap.readIdMap(iter))
    leveldb.put(eventKeyEndBytes, Array.empty[Byte])
    leveldb.get(timeTrackerKeyBytes) match {
      case null => // use default tracker value
      case cval => timeTracker = timeTracker(cval)
    }

    replay(timeTracker.sequenceNr + 1L, EventKey.DefaultClassifier) { event =>
      timeTracker = timeTracker.update(event)
    }
  }

  override def postStop(): Unit = {
    while(activeIterators.get.nonEmpty) {
      // Wait a bit for all concurrent read iterators to be closed
      // See https://github.com/RBMHTechnology/eventuate/issues/87
      Thread.sleep(500)
    }
    leveldb.close()
    super.postStop()
  }

  // -------------------------------------------------------------------
  //  Support for tracking active iterators used by concurrent readers.
  //  It helps to avoid `pthread lock: invalid argument` errors raised
  //  by native code when closing the leveldb instance maintained by
  //  this event log actor, mainly during integration tests.
  // -------------------------------------------------------------------

  import java.util.concurrent.atomic._
  import java.util.function._

  private val activeIterators = new AtomicReference[Set[DBIterator]](Set())

  private val addAccumulator = new BinaryOperator[Set[DBIterator]] {
    override def apply(acc: Set[DBIterator], u: Set[DBIterator]): Set[DBIterator] =
      acc + u.head
  }

  private val removeAccumulator = new BinaryOperator[Set[DBIterator]] {
    override def apply(acc: Set[DBIterator], u: Set[DBIterator]): Set[DBIterator] =
      acc - u.head
  }

  def addActiveIterator(iter: DBIterator): Unit =
    activeIterators.accumulateAndGet(Set(iter), addAccumulator)

  def removeActiveIterator(iter: DBIterator): Unit =
    activeIterators.accumulateAndGet(Set(iter), removeAccumulator)
}

object LeveldbEventLog {
  private[eventuate] case class ReadResult(events: Seq[DurableEvent], to: Long)

  private[eventuate] case class EventKey(classifier: Int, sequenceNr: Long)

  private[eventuate] object EventKey {
    val DefaultClassifier: Int = 0
  }

  private[eventuate] val timeTrackerKeyBytes: Array[Byte] =
    eventKeyBytes(0, 0L)

  private[eventuate] val eventKeyEnd: EventKey =
    EventKey(Int.MaxValue, Long.MaxValue)

  private[eventuate] val eventKeyEndBytes: Array[Byte] =
    eventKeyBytes(eventKeyEnd.classifier, eventKeyEnd.sequenceNr)

  private[eventuate] def eventKeyBytes(classifier: Int, sequenceNr: Long): Array[Byte] = {
    val bb = ByteBuffer.allocate(12)
    bb.putInt(classifier)
    bb.putLong(sequenceNr)
    bb.array
  }

  private[eventuate] def eventKey(a: Array[Byte]): EventKey = {
    val bb = ByteBuffer.wrap(a)
    EventKey(bb.getInt, bb.getLong)
  }

  private[eventuate] def longBytes(l: Long): Array[Byte] =
    ByteBuffer.allocate(8).putLong(l).array

  private[eventuate] def longFromBytes(a: Array[Byte]): Long =
    ByteBuffer.wrap(a).getLong

  /**
   * Creates a [[LeveldbEventLog]] configuration object.
   *
   * @param logId unique log id.
   * @param prefix prefix of the directory that contains the LevelDB files
   * @param batching `true` if write-batching shall be enabled (recommended).
   */
  def props(logId: String, prefix: String = "log", batching: Boolean = true): Props = {
    val logProps = Props(new LeveldbEventLog(logId, prefix)).withDispatcher("eventuate.log.leveldb.write-dispatcher")
    if (batching) Props(new BatchingEventLog(logProps)) else logProps
  }
}
