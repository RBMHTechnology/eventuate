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

import akka.actor._
import akka.pattern.ask
import akka.serialization.SerializationExtension

import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.log._
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog.WithBatch
import com.typesafe.config.Config

import org.fusesource.leveldbjni.JniDBFactory._
import org.iq80.leveldb._

import scala.collection.JavaConverters._
import scala.collection.immutable._
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util._

class LeveldbEventLogSettings(config: Config) extends EventLogSettings {
  val readTimeout: FiniteDuration =
    config.getDuration("eventuate.log.read-timeout", TimeUnit.MILLISECONDS).millis

  val rootDir: String =
    config.getString("eventuate.log.leveldb.dir")

  val fsync: Boolean =
    config.getBoolean("eventuate.log.leveldb.fsync")

  val stateSnapshotLimit: Int =
    config.getInt("eventuate.log.leveldb.state-snapshot-limit")

  val deletionBatchSize: Int =
    config.getInt("eventuate.log.leveldb.deletion-batch-size")

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
 * [[LeveldbEventLog]] actor state.
 */
case class LeveldbEventLogState(eventLogClock: EventLogClock, deletionMetadata: DeletionMetadata) extends EventLogState

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
class LeveldbEventLog(id: String, prefix: String) extends EventLog[LeveldbEventLogState](id) with WithBatch {
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

  override def writeReplicationProgresses(progresses: Map[String, Long]): Future[Unit] =
    completed(withBatch(batch => progresses.foreach(p => replicationProgressMap.writeReplicationProgress(p._1, p._2, batch))))

  def writeEventLogClockSnapshot(clock: EventLogClock): Future[Unit] =
    withBatch(batch => Future.fromTry(Try(writeEventLogClockSnapshotSync(clock, batch))))

  private def eventIterator(from: Long, classifier: Int): EventIterator =
    new EventIterator(from, classifier)

  private def eventReader(): ActorRef =
    context.actorOf(Props(new EventReader).withDispatcher("eventuate.log.dispatchers.read-dispatcher"))

  override def readReplicationProgresses: Future[Map[String, Long]] =
    completed(withIterator(iter => replicationProgressMap.readReplicationProgresses(iter)))

  override def readReplicationProgress(logId: String): Future[Long] =
    completed(withIterator(iter => replicationProgressMap.readReplicationProgress(logId)))

  override def replicationRead(fromSequenceNr: Long, toSequenceNr: Long, max: Int, scanLimit: Int, filter: DurableEvent => Boolean): Future[BatchReadResult] =
    eventReader().ask(EventReader.ReadSync(fromSequenceNr, toSequenceNr, EventKey.DefaultClassifier, max, scanLimit, filter))(settings.readTimeout, self).mapTo[BatchReadResult]

  override def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int): Future[BatchReadResult] =
    eventReader().ask(EventReader.ReadSync(fromSequenceNr, toSequenceNr, EventKey.DefaultClassifier, max, Int.MaxValue, _ => true))(settings.readTimeout, self).mapTo[BatchReadResult]

  override def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int, aggregateId: String): Future[BatchReadResult] =
    eventReader().ask(EventReader.ReadSync(fromSequenceNr, toSequenceNr, aggregateIdMap.numericId(aggregateId), max, Int.MaxValue, _ => true))(settings.readTimeout, self).mapTo[BatchReadResult]

  override def recoverState: Future[LeveldbEventLogState] = completed {
    val clockSnapshot = readEventLogClockSnapshot
    val clockRecovered = withEventIterator(clockSnapshot.sequenceNr + 1L, EventKey.DefaultClassifier) { iter =>
      iter.foldLeft(clockSnapshot)(_ update _)
    }
    LeveldbEventLogState(clockRecovered, deletionMetadataStore.readDeletionMetadata())
  }

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

  private def readSync(fromSequenceNr: Long, toSequenceNr: Long, classifier: Int, max: Int, scanLimit: Int, filter: DurableEvent => Boolean): BatchReadResult = {
    val builder = new VectorBuilder[DurableEvent]

    val first = 1L max fromSequenceNr
    var last = first - 1L

    var scanned = 0
    var filtered = 0

    withEventIterator(first, classifier) { iter =>
      while (iter.hasNext && filtered < max && scanned < scanLimit) {
        val event = iter.next()
        if (filter(event)) {
          builder += event
          filtered += 1
        }
        scanned += 1
        last = event.localSequenceNr
      }
      BatchReadResult(builder.result(), last)
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
      writeEventLogClockSnapshotSync(clock, batch)
      updateCount = 0
    }
  }

  private def writeEventLogClockSnapshotSync(clock: EventLogClock, batch: WriteBatch): Unit =
    batch.put(clockKeyBytes, clockBytes(clock))

  private def withIterator[R](body: DBIterator => R): R = {
    val so = snapshotOptions()
    val iter = leveldb.iterator(so)
    try {
      body(iter)
    } finally {
      iter.close()
      so.snapshot().close()
    }
  }

  private def withEventIterator[R](from: Long, classifier: Int)(body: EventIterator => R): R = {
    val iter = eventIterator(from, classifier)
    try {
      body(iter)
    } finally {
      iter.close()
    }
  }

  private class EventIterator(from: Long, classifier: Int) extends Iterator[DurableEvent] with Closeable {
    val opts = snapshotOptions()

    val iter1 = leveldb.iterator(opts)
    val iter2 = iter1.asScala.takeWhile(entry => eventKey(entry.getKey).classifier == classifier).map(entry => event(entry.getValue))

    iter1.seek(eventKeyBytes(classifier, from))

    override def hasNext: Boolean =
      iter2.hasNext

    override def next(): DurableEvent =
      iter2.next()

    override def close(): Unit = {
      iter1.close()
      opts.snapshot().close()
    }
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
    // Leveldb iterators that are used by threads other that this actor's dispatcher threads
    // are used in child actors of this actor. This ensures that these iterators are closed
    // before this actor closes the leveldb instance (fixing issue #234).
    leveldb.close()
    super.postStop()
  }

  private class EventReader() extends Actor {
    import EventReader._

    def receive = {
      case ReadSync(from, to, classifier, max, scanLimit, filter) =>
        Try(readSync(from, to, classifier, max, scanLimit, filter)) match {
          case Success(r) => sender() ! r
          case Failure(e) => sender() ! Status.Failure(e)
        }
        context.stop(self)
    }
  }

  private object EventReader {
    case class ReadSync(fromSequenceNr: Long, toSequenceNr: Long, classifier: Int, max: Int, scanLimit: Int, filter: DurableEvent => Boolean)
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
