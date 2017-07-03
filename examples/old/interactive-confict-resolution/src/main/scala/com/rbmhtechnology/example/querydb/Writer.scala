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

package com.rbmhtechnology.example.querydb

//#writer
import java.lang.{ Long => JLong }

import akka.actor.ActorRef

import com.datastax.driver.core._
import com.rbmhtechnology.eventuate.EventsourcedWriter

import scala.concurrent.Future

/**
 * Processes `CustomerCreated` and `AddressUpdated` events and updates
 * a `CUSTOMER` table in Cassandra with incremental batches.
 */
class Writer(val id: String, val eventLog: ActorRef, session: Session)
  extends EventsourcedWriter[Long, Unit] {

  import Writer._
  import context.dispatcher

  val insertCustomerStmt = session.prepare(
    "INSERT INTO CUSTOMER (id, first, last, address) VALUES (?, ?, ?, ?)")

  val updateCustomerStmt = session.prepare(
    "UPDATE CUSTOMER SET address = ? WHERE id = ?")

  val updateProgressStmt = session.prepare(
    "UPDATE PROGRESS SET sequence_nr = ? WHERE id = 0")

  /**
   * Batch of Cassandra update statements collected during event processing.
   */
  var batch: Vector[BoundStatement] = Vector.empty

  /**
   * Suspends replay after 16 events, triggers a `write` and then continues
   * with the next 16 events. This is implements event replay backpressure,
   * needed if writing to the database is slower than replaying from the
   * `eventLog` (which is usually the case).
   */
  override def replayBatchSize: Int =
    16

  override def onCommand = {
    case _ =>
  }

  /**
   * Prepares an update `batch` from handled events that is written to the
   * database when `write` is called. An event handler never writes to the
   * database directly.
   */
  override def onEvent = {
    case c @ CustomerCreated(cid, first, last, address) =>
      batch = batch :+ insertCustomerStmt.bind(cid: JLong, first, last, address)
    case u @ AddressUpdated(cid, address) =>
      batch = batch :+ updateCustomerStmt.bind(address, cid: JLong)
  }

  /**
   * Asynchronously writes the prepared update `batch` to the database
   * together with the sequence number of the last processed event. After
   * having submitted the batch, it is cleared so that further events can
   * be processed while the write is in progress.
   */
  override def write(): Future[Unit] = {
    val snr = lastSequenceNr
    val res = for {
      _ <- Future.sequence(batch.map(stmt => session.executeAsync(stmt).toFuture))
      _ <- session.executeAsync(updateProgressStmt.bind(snr: JLong)).toFuture
    } yield ()
    batch = Vector.empty // clear batch
    res
  }

  /**
   * Reads the sequence number of the last update. This method is called only
   * once during writer initialization (after start or restart).
   */
  override def read(): Future[Long] = {
    session.executeAsync("SELECT sequence_nr FROM PROGRESS WHERE id = 0").toFuture
      .map(rs => if (rs.isExhausted) 0L else rs.one().getLong(0))
  }

  /**
   * Handles the `read` result by returning the read value + 1, indicating the
   * start position for further reads from the event log.
   */
  override def readSuccess(result: Long): Option[Long] =
    Some(result + 1L)
}

object Writer {
  import java.util.concurrent.Executor

  import com.google.common.util.concurrent.ListenableFuture

  import scala.concurrent.{ ExecutionContext, Promise }
  import scala.language.implicitConversions
  import scala.util.Try

  implicit class ListenableFutureConverter[A](lf: ListenableFuture[A])(implicit executionContext: ExecutionContext) {

    def toFuture: Future[A] = {
      val promise = Promise[A]
      lf.addListener(new Runnable {
        def run() = promise.complete(Try(lf.get()))
      }, executionContext.asInstanceOf[Executor])
      promise.future
    }
  }
}
//#
