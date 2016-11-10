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

package com.rbmhtechnology.eventuate.adapter.stream

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor._
import akka.pattern.ask
import akka.stream._
import akka.stream.scaladsl.Flow
import akka.stream.stage._
import akka.util.Timeout

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.DurableEvent.UndefinedLogId
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.typesafe.config._

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration._
import scala.util._

private class DurableEventWriterSettings(config: Config) {
  val writeBatchSize =
    config.getInt("eventuate.log.write-batch-size")

  val writeTimeout =
    config.getDuration("eventuate.log.write-timeout", TimeUnit.MILLISECONDS).millis
}

/**
 * Stream-based alternative to [[EventsourcedActor]].
 */
object DurableEventWriter {
  import BatchWriteStage._
  import Batch._

  /**
   * Creates an [[http://doc.akka.io/docs/akka/2.4/scala/stream/index.html Akka Streams]] stage that writes input
   * [[DurableEvent]]s to `eventLog` and emits the written [[DurableEvent]]s. The input [[DurableEvent]]'s `emitterId`
   * is set to this writer's `id`. The `processId`, `localLogId`, `localSequenceNr` and `systemTimestamp` are set by
   * `eventLog`. The event log also updates the local time of `vectorTimestamp`. All other input [[DurableEvent]]
   * fields are written to the event log without modification. Behavior of the writer can be configured with:
   *
   *  - `eventuate.log.write-batch-size`. Maximum size of [[DurableEvent]] batches written to the event log. Events
   *  are batched (with [[Flow.batch]]) if they are produced faster than this writer can write events.
   *  - `eventuate.log.write-timeout`. Timeout for writing events to the event log. A write timeout or another write
   *  failure causes this stage to fail.
   *
   * @param id global unique writer id.
   * @param eventLog target event log.
   */
  def apply(id: String, eventLog: ActorRef)(implicit system: ActorSystem): Graph[FlowShape[DurableEvent, DurableEvent], NotUsed] =
    emissionWriter(id, eventLog)

  private[stream] def emissionWriter(id: String, eventLog: ActorRef)(implicit system: ActorSystem): Graph[FlowShape[DurableEvent, DurableEvent], NotUsed] = {
    val settings = new DurableEventWriterSettings(system.settings.config)
    batch1(settings.writeBatchSize, emissionWriteStage(id, eventLog, settings.writeTimeout))
  }

  private[stream] def replicationWriter(id: String, eventLog: ActorRef)(implicit system: ActorSystem): Graph[FlowShape[Seq[DurableEvent], DurableEvent], NotUsed] = {
    val settings = new DurableEventWriterSettings(system.settings.config)
    batchN(settings.writeBatchSize, replicationWriteStage(id, eventLog, settings.writeTimeout))
  }
}

private object Batch {
  def batch1(batchSize: Int, batchWriteStage: GraphStage[FlowShape[Seq[DurableEvent], Seq[DurableEvent]]])(implicit system: ActorSystem): Graph[FlowShape[DurableEvent, DurableEvent], NotUsed] =
    Flow[DurableEvent].batch(batchSize, Seq(_)) { case (s, e) => s :+ e }.via(batchWriteStage).mapConcat(identity)

  def batchN(batchSize: Int, batchWriteStage: GraphStage[FlowShape[Seq[DurableEvent], Seq[DurableEvent]]])(implicit system: ActorSystem): Graph[FlowShape[Seq[DurableEvent], DurableEvent], NotUsed] =
    Flow[Seq[DurableEvent]].batchWeighted(batchSize, _.size, identity)(_ ++ _).via(batchWriteStage).mapConcat(identity)
}

private object BatchWriteStage {
  type BatchWriter = Seq[DurableEvent] => Future[Seq[DurableEvent]]

  def emissionWriteStage(id: String, eventLog: ActorRef, writeTimeout: FiniteDuration): GraphStage[FlowShape[Seq[DurableEvent], Seq[DurableEvent]]] =
    new BatchWriteStage(ec => emissionBatchWriter(id, eventLog)(ec, Timeout(writeTimeout)))

  def replicationWriteStage(id: String, eventLog: ActorRef, writeTimeout: FiniteDuration): GraphStage[FlowShape[Seq[DurableEvent], Seq[DurableEvent]]] =
    new BatchWriteStage(ec => replicationBatchWriter(id, eventLog)(ec, Timeout(writeTimeout)))

  def emissionBatchWriter(id: String, eventLog: ActorRef)(implicit executionContext: ExecutionContext, timeout: Timeout): BatchWriter = (events: Seq[DurableEvent]) =>
    eventLog.ask(Write(events.map(_.copy(emitterId = id, processId = UndefinedLogId)), null, null, 0, 0)).flatMap {
      case s: WriteSuccess => Future.successful(s.events)
      case f: WriteFailure => Future.failed(f.cause)
    }

  def replicationBatchWriter(id: String, eventLog: ActorRef)(implicit executionContext: ExecutionContext, timeout: Timeout): BatchWriter = (events: Seq[DurableEvent]) =>
    eventLog.ask(ReplicationWrite(events.map(_.copy(emitterId = id, processId = UndefinedLogId)), replicationProgresses(events).mapValues(ReplicationMetadata(_, VectorTime.Zero)))).flatMap {
      case s: ReplicationWriteSuccess => Future.successful(s.events)
      case f: ReplicationWriteFailure => Future.failed(f.cause)
    }

  def replicationProgresses(events: Seq[DurableEvent]): Map[String, Long] =
    events.foldLeft[Map[String, Long]](Map.empty) {
      case (acc, event) => acc + (event.localLogId -> event.localSequenceNr)
    }
}

private class BatchWriteStage(batchWriterFactory: ExecutionContext => BatchWriteStage.BatchWriter) extends GraphStage[FlowShape[Seq[DurableEvent], Seq[DurableEvent]]] {
  val in = Inlet[Seq[DurableEvent]]("BatchWriteStage.in")
  val out = Outlet[Seq[DurableEvent]]("BatchWriteStage.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    lazy val batchWriter = batchWriterFactory(materializer.executionContext)

    var writing = false
    var finished = false

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val events = grab(in)
        val callback = getAsyncCallback[Try[Seq[DurableEvent]]] {
          case Success(r) =>
            writing = false
            push(out, r)
            if (finished) {
              // deferred stage completion
              completeStage()
            }
          case Failure(t) =>
            failStage(t)
        }
        batchWriter(events).onComplete(callback.invoke)(materializer.executionContext)
        writing = true
      }

      override def onUpstreamFinish(): Unit =
        if (writing) {
          // defer stage completion
          finished = true
        } else super.onUpstreamFinish()
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = pull(in)
    })
  }
}
