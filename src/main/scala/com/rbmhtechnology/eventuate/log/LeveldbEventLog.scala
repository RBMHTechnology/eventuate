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

import java.io.File
import java.nio.ByteBuffer

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.util._

import akka.actor._
import akka.serialization.SerializationExtension

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory.factory

import com.rbmhtechnology.eventuate.{ReplicationFilter, DurableEvent}
import com.rbmhtechnology.eventuate.EventLogProtocol._
import com.rbmhtechnology.eventuate.ReplicationProtocol._

class LeveldbEventLog(id: String, prefix: String) extends Actor with LeveldbNumericIdentifierMap with LeveldbReplicationProgressMap {
  import LeveldbEventLog._

  val serialization = SerializationExtension(context.system)

  val leveldbOptions = new Options().createIfMissing(true)
  val leveldbWriteOptions = new WriteOptions().sync(true).snapshot(false)
  def leveldbReadOptions = new ReadOptions().verifyChecksums(false)

  val leveldbRootDir = context.system.settings.config.getString("log.leveldb.dir")
  val leveldbDir = new File(leveldbRootDir, s"${prefix}-${id}")
  var leveldb = factory.open(leveldbDir, leveldbOptions)

  implicit val dispatcher = context.system.dispatchers.lookup("log.leveldb.read-dispatcher")

  var registered: Set[ActorRef] = Set.empty
  var replicated: Map[String, Long] = Map.empty
  var sequenceNr = 0L

  final def receive = {
    case GetLastSourceLogSequenceNrReplicated(sourceLogId) =>
      Try(readReplicationProgress(sourceLogId)) match {
        case Success(r) => sender() ! GetLastSourceLogSequenceNrReplicatedSuccess(sourceLogId, r)
        case Failure(e) => sender() ! GetLastSourceLogSequenceNrReplicatedFailure(e)
      }
    case Replay(from, requestor, iid) =>
      registered = registered + context.watch(requestor)
      Future(replay(from)(event => requestor ! Replaying(event, iid))) onComplete {
        case Success(_) => requestor ! ReplaySuccess(iid)
        case Failure(e) => requestor ! ReplayFailure(e, iid)
      }
    case Read(from, max, filter) =>
      val sdr = sender()
      Future(read(from, max, filter)) onComplete {
        case Success(result) => sdr ! ReadSuccess(result.events, result.to)
        case Failure(cause)  => sdr ! ReadFailure(cause)
      }
    case Delay(commands, commandsSender, requestor, iid) =>
      commands.foreach(cmd => requestor.tell(DelaySuccess(cmd, iid), commandsSender))
    case WriteN(writes) =>
      val updated = writes.map(w => w.copy(events = prepareWrite(w.events)))
      val result = Try(withBatch(batch => updated.foreach(w => write(w.events, batch))))
      sender() ! WriteNComplete // notify batch layer that write completed
      result match {
        case Failure(e) => updated.foreach {
          case Write(events, eventsSender, requestor, iid) =>
            replyFailure(events, eventsSender, requestor, iid, e)
        }
        case Success(_) => updated.foreach {
          case Write(events, eventsSender, requestor, iid) =>
            replySuccess(events, eventsSender, requestor, iid)
            context.system.eventStream.publish(Updated(events))
        }
      }
    case Write(events, eventsSender, requestor, iid) =>
      val updated = prepareWrite(events)
      val result = Try(write(updated))
      result match {
        case Failure(e) =>
          replyFailure(updated, eventsSender, requestor, iid, e)
        case Success(_) =>
          replySuccess(updated, eventsSender, requestor, iid)
          context.system.eventStream.publish(Updated(updated))
      }
    case Replicate(events, sourceLogId, lastSourceLogSequenceNrRead) =>
      Try(readReplicationProgress(sourceLogId)) match {
        case Failure(e) => sender() ! ReplicateFailure(e)
        case Success(lastSourceLogSequenceNrReplicated) =>
          if (lastSourceLogSequenceNrRead > lastSourceLogSequenceNrReplicated) {
            val updated = prepareReplicate(events)
            val result = Try {
              withBatch { batch =>
                // atomic write of events and replication progress
                writeReplicationProgress(sourceLogId, lastSourceLogSequenceNrRead, batch)
                write(updated, batch)
              }
            }
            result match {
              case Failure(e) =>
                sender() ! ReplicateFailure(e)
              case Success(_) =>
                updated.foreach { event => registered.foreach(_ ! Written(event))}
                context.system.eventStream.publish(Updated(updated))
                sender() ! ReplicateSuccess(events.size, lastSourceLogSequenceNrRead)
            }
          } else {
            // duplicate detected
            context.system.eventStream.publish(Updated(Seq()))
            sender() ! ReplicateSuccess(0, lastSourceLogSequenceNrReplicated)
          }
      }
    case Terminated(requestor) =>
      registered = registered - requestor
  }

  def replySuccess(events: Seq[DurableEvent], eventsSender: ActorRef, requestor: ActorRef, instanceId: Int): Unit = {
    events.foreach { event =>
      requestor.tell(WriteSuccess(event, instanceId), eventsSender)
      registered.foreach(r => if (r != requestor) r ! Written(event))
    }
  }

  def replyFailure(events: Seq[DurableEvent], eventsSender: ActorRef, requestor: ActorRef, instanceId: Int, cause: Throwable): Unit = {
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
    val snr = event.sequenceNr
    batch.put(counterKeyBytes, longBytes(snr))
    batch.put(eventKeyBytes(snr), eventBytes(event))
  }

  def read(from: Long, max: Int, filter: ReplicationFilter): ReadResult = withIterator { iter =>
    val first = if (from < 1L) 1L else from
    var last = first - 1
    @annotation.tailrec
    def go(events: Vector[DurableEvent], num: Int): Vector[DurableEvent] = if (iter.hasNext && num > 0) {
      val nextEntry = iter.next()
      val nextKey = eventKey(nextEntry.getKey)
      if (nextKey != eventKeyEnd) {
        val nextEvt = event(nextEntry.getValue)
        last = nextKey
        if (!filter(nextEvt)) go(events, num)
        else go(events :+ event(nextEntry.getValue), num - 1)
      } else events
    } else events
    iter.seek(eventKeyBytes(first))
    ReadResult(go(Vector.empty, max), last)
  }

  def replay(from: Long)(f: DurableEvent => Unit): Unit = withIterator { iter =>
    val first = if (from < 1L) 1L else from
    @annotation.tailrec
    def go(): Unit = if (iter.hasNext) {
      val nextEntry = iter.next()
      val nextKey = eventKey(nextEntry.getKey)
      if (nextKey != eventKeyEnd) {
        f(event(nextEntry.getValue))
        go()
      }
    }
    iter.seek(eventKeyBytes(first))
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

  def withIterator[R](body: DBIterator ⇒ R): R = {
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
    super.preStart()
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
  case class ReadResult(events: Seq[DurableEvent], to: Long)

  val counterKey: Long = 0L
  val counterKeyBytes: Array[Byte] =
    longBytes(counterKey)

  val eventKeyEnd: Long = Long.MaxValue
  val eventKeyEndBytes: Array[Byte] =
    longBytes(eventKeyEnd)

  def eventKeyBytes(sequenceNr: Long): Array[Byte] =
    longBytes(sequenceNr)

  def eventKey(a: Array[Byte]): Long =
    longFromBytes(a)

  def longBytes(l: Long): Array[Byte] =
    ByteBuffer.allocate(8).putLong(l).array

  def longFromBytes(a: Array[Byte]): Long =
    ByteBuffer.wrap(a).getLong

  def props(id: String, prefix: String = "log", batching: Boolean = true): Props = {
    val logProps = Props(new LeveldbEventLog(id, prefix)).withDispatcher("log.leveldb.write-dispatcher")
    if (batching) Props(new BatchingLayer(logProps)) else logProps
  }
}