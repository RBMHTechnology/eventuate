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
import akka.event.Logging
import scala.util._

import akka.actor._
import akka.serialization.SerializationExtension

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory.factory

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.log.{AggregateRegistry, BatchingEventLog}

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
class LeveldbEventLog(id: String, prefix: String) extends Actor {
  import LeveldbEventLog._

  val serialization = SerializationExtension(context.system)
  val eventStream = context.system.eventStream

  val leveldbConfig = context.system.settings.config.getConfig("eventuate.log.leveldb")
  val leveldbRootDir = leveldbConfig.getString("dir")
  val leveldbFsync = leveldbConfig.getBoolean("fsync")

  val leveldbOptions = new Options().createIfMissing(true)
  val leveldbWriteOptions = new WriteOptions().sync(leveldbFsync).snapshot(false)
  def leveldbReadOptions = new ReadOptions().verifyChecksums(false)

  val leveldbDir = new File(leveldbRootDir, s"${prefix}-${id}"); leveldbDir.mkdirs()
  val leveldb = factory.open(leveldbDir, leveldbOptions)

  implicit val dispatcher = context.system.dispatchers.lookup("eventuate.log.leveldb.read-dispatcher")

  var aggregateRegistry: AggregateRegistry = AggregateRegistry()
  var defaultRegistry: Set[ActorRef] = Set.empty
  var replicated: Map[String, Long] = Map.empty
  var sequenceNr = 0L

  val aggregateIdMap = new NumericIdentifierMap(leveldb, -1)
  val eventLogIdMap = new NumericIdentifierMap(leveldb, -2)
  val replicationProgressMap = new ReplicationProgressMap(leveldb, -3, eventLogIdMap.numericId)

  final def receive = {
    case GetLastSourceLogReadPosition(sourceLogId) =>
      Try(replicationProgressMap.readReplicationProgress(sourceLogId)) match {
        case Success(r) => sender() ! GetLastSourceLogReadPositionSuccess(sourceLogId, r)
        case Failure(e) => sender() ! GetLastSourceLogReadPositionFailure(e)
      }
    case Replay(from, requestor, None, iid) =>
      defaultRegistry = defaultRegistry + context.watch(requestor)
      Future(replay(from, EventKey.DefaultClassifier)(event => requestor ! Replaying(event, iid))) onComplete {
        case Success(_) => requestor ! ReplaySuccess(iid)
        case Failure(e) => requestor ! ReplayFailure(e, iid)
      }
    case Replay(from, requestor, Some(sourceAggregateId), iid) =>
      val nid = aggregateIdMap.numericId(sourceAggregateId)
      aggregateRegistry = aggregateRegistry.add(context.watch(requestor), sourceAggregateId)
      Future(replay(from, nid)(event => requestor ! Replaying(event, iid))) onComplete {
        case Success(_) => requestor ! ReplaySuccess(iid)
        case Failure(e) => requestor ! ReplayFailure(e, iid)
      }
    case r @ ReplicationRead(from, max, filter, targetLogId) =>
      val sdr = sender()
      eventStream.publish(r)
      Future(read(from, max, filter)) onComplete {
        case Success(result) =>
          val r = ReplicationReadSuccess(result.events, result.to, targetLogId)
          sdr ! r
          eventStream.publish(r)
        case Failure(cause)  =>
          val r = ReplicationReadFailure(cause.getMessage, targetLogId)
          sdr ! r
          eventStream.publish(r)
      }
    case WriteN(writes) =>
      val updatedWrites = writes.map(w => w.copy(events = prepareWrite(w.events)))
      val updatedEvents = updatedWrites.map(_.events).flatten
      Try(withBatch(batch => write(updatedEvents, batch))) match {
        case Success(_) =>
          updatedWrites.foreach(w => pushWriteSuccess(w.events, w.eventsSender, w.requestor, w.instanceId))
          publishUpdateNotification(updatedEvents)
        case Failure(e) =>
          updatedWrites.foreach(w => pushWriteFailure(w.events, w.eventsSender, w.requestor, w.instanceId, e))
      }
      sender() ! WriteNComplete // notify batch layer that write completed
    case Write(events, eventsSender, requestor, iid) =>
      val updated = prepareWrite(events)
      val result = Try(write(updated))
      result match {
        case Failure(e) =>
          pushWriteFailure(updated, eventsSender, requestor, iid, e)
        case Success(_) =>
          pushWriteSuccess(updated, eventsSender, requestor, iid)
          publishUpdateNotification(updated)
      }
    case ReplicationWrite(events, sourceLogId, lastSourceLogSequenceNrRead) =>
      val updated = prepareReplicate(events)
      Try {
        withBatch { batch =>
          // atomic write of events and replication progress
          replicationProgressMap.writeReplicationProgress(sourceLogId, lastSourceLogSequenceNrRead, batch)
          write(updated, batch)
        }
      } match {
        case Failure(e) =>
          sender() ! ReplicationWriteFailure(e)
        case Success(_) =>
          sender() ! ReplicationWriteSuccess(events.size, lastSourceLogSequenceNrRead)
          pushReplicateSuccess(updated)
          publishUpdateNotification(updated)
      }
    case Terminated(requestor) =>
      aggregateRegistry.aggregateId(requestor) match {
        case Some(aggregateId) => aggregateRegistry = aggregateRegistry.remove(requestor, aggregateId)
        case None              => defaultRegistry = defaultRegistry - requestor
      }
  }

  def publishUpdateNotification(update: Seq[DurableEvent] = Seq()): Unit = {
    if (update.nonEmpty) eventStream.publish(Updated(id, update))
  }

  def pushReplicateSuccess(events: Seq[DurableEvent]): Unit = {
    events.foreach { event =>
      // in any case, notify all default subscribers
      defaultRegistry.foreach(_ ! Written(event))
      // notify subscribers with matching aggregate id
      for {
        aggregateId <- event.routingDestinations
        aggregate <- aggregateRegistry(aggregateId)
      } aggregate ! Written(event)
    }
  }

  def pushWriteSuccess(events: Seq[DurableEvent], eventsSender: ActorRef, requestor: ActorRef, instanceId: Int): Unit = {
    events.foreach { event =>
      requestor.tell(WriteSuccess(event, instanceId), eventsSender)
      // in any case, notify all default subscribers (except requestor)
      defaultRegistry.foreach(r => if (r != requestor) r ! Written(event))
      // notify subscribers with matching aggregate id (except requestor)
      for {
        aggregateId <- event.routingDestinations
        aggregate <- aggregateRegistry(aggregateId) if aggregate != requestor
      } aggregate ! Written(event)
    }
  }

  def pushWriteFailure(events: Seq[DurableEvent], eventsSender: ActorRef, requestor: ActorRef, instanceId: Int, cause: Throwable): Unit = {
    events.foreach { event =>
      requestor.tell(WriteFailure(event, cause, instanceId), eventsSender)
    }
  }

  def prepareWrite(events: Seq[DurableEvent]): Seq[DurableEvent] = {
    events.map { event =>
      val snr = nextSequenceNr()
      event.copy(
        sourceLogId = id,
        targetLogId = id,
        sourceLogSequenceNr = snr,
        targetLogSequenceNr = snr)
    }
  }

  def prepareReplicate(events: Seq[DurableEvent]): Seq[DurableEvent] = {
    events.map { event =>
      val snr = nextSequenceNr()
      event.copy(
        sourceLogId = event.targetLogId,
        targetLogId = id,
        sourceLogSequenceNr = event.targetLogSequenceNr,
        targetLogSequenceNr = snr)
    }
  }

  def write(events: Seq[DurableEvent]): Unit =
    withBatch(write(events, _))

  def write(events: Seq[DurableEvent], batch: WriteBatch): Unit = events.foreach { event =>
    val sequenceNr = event.sequenceNr
    val eventBytes = this.eventBytes(event)

    batch.put(counterKeyBytes, longBytes(sequenceNr))
    batch.put(eventKeyBytes(EventKey.DefaultClassifier, sequenceNr), eventBytes)
    event.routingDestinations.foreach { id => // additionally index events by aggregate id
      batch.put(eventKeyBytes(aggregateIdMap.numericId(id), sequenceNr), eventBytes)
    }
  }

  def read(from: Long, max: Int, filter: ReplicationFilter): ReadResult = withIterator { iter =>
    val first = if (from < 1L) 1L else from
    var last = first - 1
    @annotation.tailrec
    def go(events: Vector[DurableEvent], num: Int): Vector[DurableEvent] = if (iter.hasNext && num > 0) {
      val nextEntry = iter.next()
      val nextKey = eventKey(nextEntry.getKey)
      if (nextKey.classifier == EventKey.DefaultClassifier) {
        val nextEvt = event(nextEntry.getValue)
        last = nextKey.sequenceNr
        if (!filter(nextEvt)) go(events, num)
        else go(events :+ event(nextEntry.getValue), num - 1)
      } else events
    } else events
    iter.seek(eventKeyBytes(EventKey.DefaultClassifier, first))
    ReadResult(go(Vector.empty, max), last)
  }

  def replay(from: Long, classifier: Int)(f: DurableEvent => Unit): Unit = withIterator { iter =>
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

  def eventBytes(e: DurableEvent): Array[Byte] =
    serialization.serialize(e).get

  def event(a: Array[Byte]): DurableEvent =
    serialization.deserialize(a, classOf[DurableEvent]).get

  def withBatch[R](body: WriteBatch ⇒ R): R = {
    val batch = leveldb.createWriteBatch()
    try {
      val r = body(batch)
      leveldb.write(batch, leveldbWriteOptions)
      r
    } finally {
      batch.close()
    }
  }

  def withIterator[R](body: DBIterator => R): R = {
    val so = snapshotOptions()
    val iter = leveldb.iterator(so)
    try {
      body(iter)
    } finally {
      iter.close()
      so.snapshot().close()
    }
  }

  private def snapshotOptions(): ReadOptions =
    leveldbReadOptions.snapshot(leveldb.getSnapshot)

  private def nextSequenceNr(): Long = {
    sequenceNr += 1L
    sequenceNr
  }

  override def preStart(): Unit = {
    withIterator(iter => aggregateIdMap.readIdMap(iter))
    withIterator(iter => eventLogIdMap.readIdMap(iter))
    withIterator(iter => replicationProgressMap.readRpMap(iter))
    leveldb.put(eventKeyEndBytes, Array.empty[Byte])
    leveldb.get(counterKeyBytes) match {
      case null => sequenceNr = 0L
      case cval => sequenceNr = longFromBytes(cval)
    }
  }

  override def postStop(): Unit = {
    leveldb.close()
    super.postStop()
  }
}

object LeveldbEventLog {
  private[eventuate] case class ReadResult(events: Seq[DurableEvent], to: Long)

  private[eventuate] case class EventKey(classifier: Int, sequenceNr: Long)

  private[eventuate] object EventKey {
    val DefaultClassifier: Int = 0
  }

  private[eventuate] val counterKeyBytes: Array[Byte] =
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
