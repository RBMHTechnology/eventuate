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

package com.rbmhtechnology.eventuate.log.leveldb

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import akka.actor._
import akka.serialization.SerializationExtension

import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.log._
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog.WithBatch
import com.typesafe.config.Config

import org.fusesource.leveldbjni.JniDBFactory._
import org.iq80.leveldb._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util._

class LeveldbEventLogSettings(config: Config) extends EventLogSettings {
  val rootDir: String =
    config.getString("eventuate.log.leveldb.dir")

  val fsync: Boolean =
    config.getBoolean("eventuate.log.leveldb.fsync")

  val stateSnapshotLimit: Int =
    config.getInt("eventuate.log.leveldb.state-snapshot-limit")

  val deletionBatchSize: Int =
    config.getInt("eventuate.log.leveldb.deletion-batch-size")

  val closeWaitMax: FiniteDuration =
    config.getDuration("eventuate.log.leveldb.iterator-close-wait-max", TimeUnit.MILLISECONDS).millis

  val initRetryDelay: FiniteDuration =
    Duration.Zero

  val initRetryMax: Int =
    0

  val deletionRetryDelay: FiniteDuration =
    config.getDuration("eventuate.log.leveldb.deletion-retry-delay", TimeUnit.MILLISECONDS).millis

  val partitionSize: Long =
    Long.MaxValue
}

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
class LeveldbEventLog(id: String, prefix: String) extends EventLog(id) with WithBatch {
  import LeveldbEventLog._

  override val settings = new LeveldbEventLogSettings(context.system.settings.config)
  private val serialization = SerializationExtension(context.system)

  private val leveldbDir = new File(settings.rootDir, s"${prefix}-${id}"); leveldbDir.mkdirs()
  private val leveldbOptions = new Options().createIfMissing(true)
  private def leveldbReadOptions = new ReadOptions().verifyChecksums(false)

  protected val leveldbWriteOptions = new WriteOptions().sync(settings.fsync).snapshot(false)
  protected val leveldb = factory.open(leveldbDir, leveldbOptions)

  private val aggregateIdMap = new LeveldbNumericIdentifierStore(leveldb, -1)
  private val eventLogIdMap = new LeveldbNumericIdentifierStore(leveldb, -2)
  private val replicationProgressMap = new LeveldbReplicationProgressStore(leveldb, -3, eventLogIdMap.numericId, eventLogIdMap.findId)
  private val deletionMetadataStore = new LeveldbDeletionMetadataStore(leveldb, leveldbWriteOptions, -4)

  private var updateCount: Long = 0L

  def logDir: File =
    leveldbDir

  override def write(events: Seq[DurableEvent], partition: Long, clock: EventLogClock): Unit =
    withBatch(batch => writeSync(events, clock, batch))

  override def writeReplicationProgress(logId: String, progress: Long): Future[Unit] =
    completed(withBatch(batch => replicationProgressMap.writeReplicationProgress(logId, progress, batch)))

  private def eventIterator(from: Long, classifier: Int): EventIterator =
    new EventIterator(from, classifier)

  override def readReplicationProgresses: Future[Map[String, Long]] =
    completed(withIterator(iter => replicationProgressMap.readReplicationProgresses(iter)))

  override def readReplicationProgress(logId: String): Future[Long] =
    completed(withIterator(iter => replicationProgressMap.readReplicationProgress(logId)))

  override def replicationRead(fromSequenceNr: Long, toSequenceNr: Long, max: Int, filter: DurableEvent => Boolean): Future[BatchReadResult] =
    Future(readSync(fromSequenceNr, toSequenceNr, EventKey.DefaultClassifier, max, filter))(services.readDispatcher)

  override def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int): Future[BatchReadResult] =
    Future(readSync(fromSequenceNr, toSequenceNr, EventKey.DefaultClassifier, max, _ => true))(services.readDispatcher)

  override def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int, aggregateId: String): Future[BatchReadResult] = {
    val numericId = aggregateIdMap.numericId(aggregateId)
    Future(readSync(fromSequenceNr, toSequenceNr, numericId, max, _ => true))(services.readDispatcher)
  }

  override def recoverClock: Future[EventLogClock] = completed {
    val snap = readEventLogClockSnapshot

    withEventIterator(snap.sequenceNr + 1L, EventKey.DefaultClassifier) { iter =>
      iter.foldLeft(snap) {
        case (clock, event) => clock.update(event)
      }
    }
  }

  override def readDeletionMetadata: Future[DeletionMetadata] =
    completed(deletionMetadataStore.readDeletionMetadata())

  override def writeDeletionMetadata(deleteMetadata: DeletionMetadata) =
    deletionMetadataStore.writeDeletionMetadata(deleteMetadata)

  override def delete(toSequenceNr: Long): Future[Long] = {
    val adjusted = readEventLogClockSnapshot.sequenceNr min toSequenceNr
    val promise = Promise[Unit]()
    spawnDeletionActor(adjusted, promise)
    promise.future.map(_ => adjusted)(context.dispatcher)
  }

  private def spawnDeletionActor(toSequenceNr: Long, promise: Promise[Unit]): ActorRef =
    context.actorOf(LeveldbDeletionActor.props(leveldb, leveldbReadOptions, leveldbWriteOptions, settings.deletionBatchSize, toSequenceNr, promise))

  private def readEventLogClockSnapshot: EventLogClock = {
    leveldb.get(clockKeyBytes) match {
      case null => EventLogClock()
      case cval => clockFromBytes(cval)
    }
  }

  private def readSync(fromSequenceNr: Long, toSequenceNr: Long, classifier: Int, max: Int, filter: DurableEvent => Boolean): BatchReadResult = {
    val first = 1L max fromSequenceNr
    withEventIterator(first, classifier) { iter =>
      var last = first - 1L
      val evts = iter.filter { evt =>
        last = evt.localSequenceNr
        filter(evt)
      }.take(max).toVector
      BatchReadResult(evts, last)
    }
  }

  private def writeSync(events: Seq[DurableEvent], clock: EventLogClock, batch: WriteBatch): Unit = {
    events.foreach { event =>
      val sequenceNr = event.localSequenceNr
      val eventBytes = this.eventBytes(event)
      batch.put(eventKeyBytes(EventKey.DefaultClassifier, sequenceNr), eventBytes)
      event.destinationAggregateIds.foreach { id => // additionally index events by aggregate id
        batch.put(eventKeyBytes(aggregateIdMap.numericId(id), sequenceNr), eventBytes)
      }
    }

    updateCount += events.size

    if (updateCount >= settings.stateSnapshotLimit) {
      batch.put(clockKeyBytes, clockBytes(clock))
      updateCount = 0
    }
  }

  private def withIterator[R](body: DBIterator => R): R = {
    val iter = allocateIterator()
    try body(iter.iterator)
    finally iter.close()
  }

  private def withEventIterator[R](from: Long, classifier: Int)(body: EventIterator => R): R = {
    val iter = eventIterator(from, classifier)
    try body(iter)
    finally iter.close()
  }

  private class EventIterator(from: Long, classifier: Int) extends Iterator[DurableEvent] with Closeable {
    val iter1 = allocateIterator()
    val iter2 = iter1.iterator.asScala.takeWhile(entry => eventKey(entry.getKey).classifier == classifier).map(entry => event(entry.getValue))
    iter1.iterator.seek(eventKeyBytes(classifier, from))

    override def hasNext: Boolean = iter2.hasNext
    override def next(): DurableEvent = iter2.next()
    override def close(): Unit = iter1.close()
  }

  private def eventBytes(e: DurableEvent): Array[Byte] =
    serialization.serialize(e).get

  private def event(a: Array[Byte]): DurableEvent =
    serialization.deserialize(a, classOf[DurableEvent]).get

  private def clockBytes(clock: EventLogClock): Array[Byte] =
    serialization.serialize(clock).get

  private def clockFromBytes(a: Array[Byte]): EventLogClock =
    serialization.deserialize(a, classOf[EventLogClock]).get

  private def snapshotOptions(): ReadOptions =
    leveldbReadOptions.snapshot(leveldb.getSnapshot)

  override def preStart(): Unit = {
    withIterator(iter => aggregateIdMap.readIdMap(iter))
    withIterator(iter => eventLogIdMap.readIdMap(iter))
    leveldb.put(eventKeyEndBytes, Array.empty[Byte])
    super.preStart()
  }

  override def postStop(): Unit = {
    waitForOutstandingIterators()
    leveldb.close()

    super.postStop()
  }

  // -------------------------------------------------------------------
  //  Support for tracking active iterators used by concurrent readers.
  //  It helps to avoid `pthread lock: invalid argument` errors raised
  //  by native code when closing the leveldb instance maintained by
  //  this event log actor, mainly during integration tests.
  // -------------------------------------------------------------------

  private trait ManagedIterator {
    def iterator: DBIterator
    def close(): Unit
  }

  // invariants:
  //  - after finished changed to true, outstandingIterators will never be increased above 0 again

  private case class State(finishing: Boolean, outstandingIterators: Int)
  private val state = new AtomicReference[State](State(false, 0))

  @tailrec private def waitForOutstandingIterators(): Unit = {
    val cur = state.get
    if (!state.compareAndSet(cur, cur.copy(finishing = true)))
      waitForOutstandingIterators()
    else {
      val start = System.nanoTime()
      // spin until all outstanding iterators have been given back
      while (state.get.outstandingIterators > 0 && (System.nanoTime() - start) < settings.closeWaitMax.toNanos) Thread.sleep(100)
      if (state.get.outstandingIterators > 0)
        throw new RuntimeException("Outstanding iterators were not closed after " +
          s"eventuate.log.leveldb.iterator-close-wait-max = ${settings.closeWaitMax} either because of " +
          "long running operations or because iterator handles were not closed.")
    }
  }

  private def allocateIterator(): ManagedIterator = {
    def doAllocate(): ManagedIterator = {
      val opts = snapshotOptions()
      val it = leveldb.iterator(opts)

      new ManagedIterator {
        def iterator: DBIterator = it

        var closed = false
        def close(): Unit =
          if (!closed) {
            it.close()
            opts.snapshot().close()
            decreaseCounter()
            closed = true
          }
      }
    }

    @tailrec def decreaseCounter(): Unit = {
      val cur = state.get
      if (!state.compareAndSet(cur, cur.copy(outstandingIterators = cur.outstandingIterators - 1)))
        decreaseCounter()
    }

    @tailrec def rec(): ManagedIterator =
      state.get match {
        case s @ State(false, n) =>
          if (!state.compareAndSet(s, s.copy(outstandingIterators = n + 1)))
            rec()
          else
            doAllocate()
        case State(true, _) => throw new RuntimeException("Cannot create iterators while shutting down!")
      }

    rec()
  }
}

object LeveldbEventLog {

  private[leveldb]type CloseableIterator[A] = Iterator[A] with Closeable

  private[leveldb] case class EventKey(classifier: Int, sequenceNr: Long)

  private[leveldb] object EventKey {
    val DefaultClassifier: Int = 0
  }

  private[leveldb] val eventKeyEnd: EventKey =
    EventKey(Int.MaxValue, Long.MaxValue)

  private[leveldb] def eventKeyBytes(classifier: Int, sequenceNr: Long): Array[Byte] = {
    val bb = ByteBuffer.allocate(12)
    bb.putInt(classifier)
    bb.putLong(sequenceNr)
    bb.array
  }

  private[leveldb] def eventKey(a: Array[Byte]): EventKey = {
    val bb = ByteBuffer.wrap(a)
    EventKey(bb.getInt, bb.getLong)
  }

  private val clockKeyBytes: Array[Byte] =
    eventKeyBytes(0, 0L)

  private val eventKeyEndBytes: Array[Byte] =
    eventKeyBytes(eventKeyEnd.classifier, eventKeyEnd.sequenceNr)

  private[leveldb] def longBytes(l: Long): Array[Byte] =
    ByteBuffer.allocate(8).putLong(l).array

  private[leveldb] def longFromBytes(a: Array[Byte]): Long =
    ByteBuffer.wrap(a).getLong

  private def completed[A](body: => A): Future[A] =
    Future.fromTry(Try(body))

  private[leveldb] trait WithBatch {

    protected def leveldb: DB
    protected def leveldbWriteOptions: WriteOptions

    protected def withBatch[R](body: WriteBatch => R): R = {
      val batch = leveldb.createWriteBatch()
      try {
        val r = body(batch)
        leveldb.write(batch, leveldbWriteOptions)
        r
      } finally {
        batch.close()
      }
    }
  }

  /**
   * Creates a [[LeveldbEventLog]] configuration object.
   *
   * @param logId unique log id.
   * @param prefix prefix of the directory that contains the LevelDB files.
   * @param batching `true` if write-batching shall be enabled (recommended).
   */
  def props(logId: String, prefix: String = "log", batching: Boolean = true): Props = {
    val logProps = Props(new LeveldbEventLog(logId, prefix)).withDispatcher("eventuate.log.dispatchers.write-dispatcher")
    if (batching) Props(new BatchingLayer(logProps)) else logProps
  }
}
