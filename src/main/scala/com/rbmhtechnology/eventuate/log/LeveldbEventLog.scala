/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
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
    case Delay(commands, requestor, iid) =>
      commands.foreach(cmd => requestor ! DelaySuccess(cmd, iid))
    case Write(events, requestor, iid) =>
      val updated = events.map { event =>
        val snr = nextSequenceNr()
        event.copy(
          sourceLogId = id,
          targetLogId = id,
          sourceLogSequenceNr = snr,
          targetLogSequenceNr = snr)
      }
      Try(write(updated)) match {
        case Failure(e) =>
          updated.foreach { event => requestor forward WriteFailure(event, e, iid) }
        case Success(_) =>
          updated.foreach { event =>
            requestor forward WriteSuccess(event, iid)
            registered.foreach(r => if (r != requestor) r ! Written(event))
          }
          context.system.eventStream.publish(Updated(updated))
      }
    case Replicate(events, sourceLogId, lastSourceLogSequenceNrRead) =>
      val updated = events.map { event =>
        val snr = nextSequenceNr()
        event.copy(
          sourceLogId = event.targetLogId,
          targetLogId = id,
          sourceLogSequenceNr = event.targetLogSequenceNr,
          targetLogSequenceNr = snr)
      }
      Try(readReplicationProgress(sourceLogId)) match {
        case Failure(e) => sender() ! ReplicateFailure(e)
        case Success(lastSourceLogSequenceNrReplicated) =>
          if (lastSourceLogSequenceNrRead > lastSourceLogSequenceNrReplicated) {
            Try {
              withBatch { batch =>
                // atomic write of events and replication progress
                writeReplicationProgress(sourceLogId, lastSourceLogSequenceNrRead, batch)
                write(updated, batch)
              }
            } match {
              case Failure(e) => sender() ! ReplicateFailure(e)
              case Success(_) =>
                updated.foreach { event => registered.foreach(_ ! Written(event))}
                context.system.eventStream.publish(Updated(updated))
                sender() ! ReplicateSuccess(events.size)
            }
          } else {
            // duplicate detected
            context.system.eventStream.publish(Updated(Seq()))
            sender() ! ReplicateSuccess(0)
          }
      }
    case Terminated(requestor) =>
      registered = registered - requestor
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

  def props(id: String, prefix: String = "log"): Props =
    Props(classOf[LeveldbEventLog], id, prefix).withDispatcher("log.leveldb.write-dispatcher")
}