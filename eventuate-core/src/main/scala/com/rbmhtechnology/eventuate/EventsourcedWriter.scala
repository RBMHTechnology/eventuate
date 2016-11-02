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

package com.rbmhtechnology.eventuate

import akka.actor._
import com.typesafe.config.Config

import scala.concurrent.Future
import scala.util._

private class EventsourcedWriterSettings(config: Config) {
  val replayRetryMax =
    config.getInt("eventuate.log.replay-retry-max")
}

object EventsourcedWriter {
  /**
   * Thrown by [[EventsourcedWriter]] to indicate that a read operation from an external database failed.
   *
   * @see [[EventsourcedWriter#readFailure]]
   */
  class ReadException(message: String, cause: Throwable) extends RuntimeException(message, cause)

  /**
   * Thrown by [[EventsourcedWriter]] to indicate that a write operation to the external database failed.
   *
   * @see [[EventsourcedWriter#writeFailure]]
   */
  class WriteException(message: String, cause: Throwable) extends RuntimeException(message, cause)
}

/**
 * An [[EventsourcedView]] designed to update external databases from events stored in its event log. It
 * supports event processing patterns optimized for batch-updating external databases to create persistent
 * views or read models:
 *
 *  - During initialization, a concrete `EventsourcedWriter` asynchronously `read`s data from the external
 *    database to obtain information about the actual event processing progress. For example, if the last
 *    processed event sequence number is written with every batch update to the database, it can be read
 *    during initialization and used by the writer to detect duplicates during further event processing so
 *    that event processing can be made idempotent.
 *
 *  - During event processing in its `onEvent` handler, a concrete writer usually builds a database-specific
 *    write-batch (representing an incremental update). After a configurable number of events, `EventsourcedWriter`
 *    calls `write` to asynchronously write the prepared batch to the database.
 *
 * An `EventsourcedWriter` may also implement an `onCommand` handler to process commands and save snapshots of
 * internal state. Internal state is recovered by replaying events from the event log, optionally starting from
 * a saved snapshot (see [[EventsourcedView]] for details). If a writer doesn't require full internal state
 * recovery, it may define a custom starting position in the event log by returning a sequence number from
 * `readSuccess`. If full internal state recovery is required instead, `readSuccess` should return `None`
 * (which is the default).
 *
 * Implementation notes:
 *
 *  - After having started an asynchronous write but before returning from `write`, a writer should clear the
 *    prepared write batch so that further events can be processed while the asynchronous write operation is
 *    in progress.
 *  - Event processing during replay (either starting from a default or application-defined position) is subject
 *    to backpressure. After a configurable number of events (see `eventuate.log.replay-batch-size` configuration
 *    parameter), replay is suspended until these events have been written to the target database and then resumed
 *    again. There's no backpressure mechanism for live event processing yet (but will come in future releases).
 *
 * @see [[EventsourcedProcessor]]
 * @see [[StatefulProcessor]]
 *
 * @tparam R Result type of the asynchronous read operation.
 * @tparam W Result type of the asynchronous write operations.
 */
trait EventsourcedWriter[R, W] extends EventsourcedView {
  import EventsourcedWriter._
  import EventsourcingProtocol._
  import context.dispatcher

  private case class ReadSuccess(result: R, instanceId: Int)
  private case class ReadFailure(cause: Throwable, instanceId: Int)

  private case class WriteSuccess(result: W, instanceId: Int)
  private case class WriteFailure(cause: Throwable, instanceId: Int)

  private val settings = new EventsourcedWriterSettings(context.system.settings.config)

  private var numPending: Int = 0

  /**
   * Disallow for [[EventsourcedWriter]] and subclasses as event processing progress is determined by `read`
   * and `readSuccess`.
   */
  override final def replayFromSequenceNr: Option[Long] = None

  /**
   * Asynchronously reads an initial value from the target database, usually to obtain information about
   * event processing progress. This method is called during initialization.
   */
  def read(): Future[R]

  /**
   * Asynchronously writes an incremental update to the target database. Incremental updates are prepared
   * during event processing by a concrete `onEvent` handler.
   *
   * During event replay, this method is called latest after having replayed `eventuate.log.replay-batch-size`
   * events and immediately after replay completes. During live processing, `write` is called immediately if
   * no write operation is in progress and an event has been handled by `onEvent`. If a write operation is in
   * progress, further event handling may run concurrently to that operation. If events are handled while a
   * write operation is in progress, another write will follow immediately after the previous write operation
   * completes.
   */
  def write(): Future[W]

  /**
   * Called with a read result after a `read` operation successfully completes. This method may update
   * internal actor state. If `None` is returned, the writer continues with state recovery by replaying
   * events, optionally starting from a snapshot. If the return value is defined, replay starts from the
   * returned sequence number without ever loading a snapshot. Does nothing and returns `None` by default
   * and can be overridden.
   */
  def readSuccess(result: R): Option[Long] =
    replayFromSequenceNr

  /**
   * Called with a write result after a `write` operation successfully completes. This method may update
   * internal actor state. Does nothing by default and can be overridden.
   */
  def writeSuccess(result: W): Unit =
    ()

  /**
   * Called with failure details after a `read` operation failed. Throws [[EventsourcedWriter#ReadException]]
   * by default (causing the writer to restart) and can be overridden.
   */
  def readFailure(cause: Throwable): Unit =
    throw new ReadException("read failed", cause)

  /**
   * Called with failure details after a `write` operation failed. Throws [[EventsourcedWriter#WriteException]]
   * by default (causing the writer to restart) and can be overridden.
   */
  def writeFailure(cause: Throwable): Unit =
    throw new WriteException("write failed", cause)

  /**
   * Internal API.
   */
  override private[eventuate] def receiveEventInternal(event: DurableEvent): Unit = {
    super.receiveEventInternal(event)
    numPending += 1
  }

  /**
   * Internal API.
   */
  override private[eventuate] def init(): Unit = {
    read onComplete {
      case Success(r) => self ! ReadSuccess(r, instanceId)
      case Failure(e) => self ! ReadFailure(e, instanceId)
    }
  }

  /**
   * Internal API.
   */
  override private[eventuate] def initiating(replayAttempts: Int): Receive = {
    case ReadSuccess(r, iid) => if (iid == instanceId) {
      readSuccess(r) match {
        case Some(snr) => replay(snr, subscribe = true)
        case None      => load()
      }
    }
    case ReadFailure(e, iid) => if (iid == instanceId) {
      readFailure(e)
    }
    case ReplaySuccess(Seq(), progress, iid) => if (iid == instanceId) {
      context.become(initiated)
      versionChanged(currentVersion)
      recovered()
      unstashAll()
    }
    case ReplaySuccess(events, progress, iid) => if (iid == instanceId) {
      events.foreach(receiveEvent)
      if (numPending > 0) {
        context.become(initiatingWrite(progress) orElse initiating(settings.replayRetryMax))
        write(instanceId)
      } else {
        replay(progress + 1L)
      }
    }
    case other =>
      super.initiating(replayAttempts)(other)
  }

  /**
   * Internal API.
   */
  override private[eventuate] def initiated: Receive = {
    case Written(event) => if (event.localSequenceNr > lastSequenceNr) {
      receiveEvent(event)
      if (numPending > 0) {
        context.become(initiatedWrite orElse initiated)
        write(instanceId)
      }
    }
    case other =>
      super.initiated(other)
  }

  private def initiatingWrite(progress: Long): Receive = {
    case WriteSuccess(r, iid) => if (iid == instanceId) {
      writeSuccess(r)
      context.become(initiating(settings.replayRetryMax))
      replay(progress + 1L)
    }
    case WriteFailure(cause, iid) => if (iid == instanceId) {
      writeFailure(cause)
    }
  }

  private def initiatedWrite: Receive = {
    case Written(event) => if (event.localSequenceNr > lastSequenceNr) {
      receiveEvent(event)
    }
    case WriteSuccess(r, iid) => if (iid == instanceId) {
      writeSuccess(r)
      if (numPending > 0) write(instanceId) else context.become(initiated)
    }
    case WriteFailure(cause, iid) => if (iid == instanceId) {
      writeFailure(cause)
    }
  }

  private def write(instanceId: Int): Unit = {
    write().onComplete {
      case Success(r) => self ! WriteSuccess(r, instanceId)
      case Failure(e) => self ! WriteFailure(e, instanceId)
    }
    numPending = 0
  }
}
