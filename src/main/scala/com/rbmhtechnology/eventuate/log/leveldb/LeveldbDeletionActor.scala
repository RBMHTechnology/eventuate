/*
 * Copyright (C) 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
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

import java.io.Closeable

import akka.actor.Actor
import akka.actor.PoisonPill
import akka.actor.Props
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog._

import org.iq80.leveldb.DB
import org.iq80.leveldb.ReadOptions
import org.iq80.leveldb.WriteOptions

import scala.annotation.tailrec
import scala.concurrent.Promise

private object LeveldbDeletionActor {
  case object DeleteBatch

  def props(leveldb: DB, leveldbReadOptions: ReadOptions, leveldbWriteOptions: WriteOptions, batchSize: Int, toSequenceNr: Long, promise: Promise[Unit]): Props =
    Props(new LeveldbDeletionActor(leveldb, leveldbReadOptions, leveldbWriteOptions, batchSize, toSequenceNr, promise))
}

class LeveldbDeletionActor(
  val leveldb: DB,
  val leveldbReadOptions: ReadOptions,
  val leveldbWriteOptions: WriteOptions,
  batchSize: Int,
  toSequenceNr: Long,
  promise: Promise[Unit])
  extends Actor with WithBatch {

  import LeveldbDeletionActor._

  val eventKeyIterator: CloseableIterator[EventKey] = newEventKeyIterator

  override def preStart() = self ! DeleteBatch

  override def postStop() = eventKeyIterator.close()

  override def receive = {
    case DeleteBatch =>
      withBatch { batch =>
        eventKeyIterator.take(batchSize).foreach { eventKey =>
          batch.delete(eventKeyBytes(eventKey.classifier, eventKey.sequenceNr))
        }
      }
      if (eventKeyIterator.hasNext) {
        self ! DeleteBatch
      } else {
        promise.success(())
        self ! PoisonPill
      }
  }

  private def newEventKeyIterator: CloseableIterator[EventKey] = {
    new Iterator[EventKey] with Closeable {
      val iterator = leveldb.iterator(leveldbReadOptions.snapshot(leveldb.getSnapshot))
      iterator.seek(eventKeyBytes(EventKey.DefaultClassifier, 1L))

      @tailrec
      override def hasNext: Boolean = {
        val key = eventKey(iterator.peekNext().getKey)
        key != eventKeyEnd &&
          (key.sequenceNr <= toSequenceNr || {
            iterator.seek(eventKeyBytes(key.classifier + 1, 1L))
            hasNext
          })
      }

      override def next() = eventKey(iterator.next().getKey)
      override def close() = {
        iterator.close()
        leveldbReadOptions.snapshot().close()
      }
    }
  }
}
