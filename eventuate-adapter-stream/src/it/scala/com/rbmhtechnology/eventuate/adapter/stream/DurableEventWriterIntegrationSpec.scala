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

import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl._
import akka.testkit.TestKit

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.utilities._

import org.scalatest._

import scala.collection.immutable.Seq

class DurableEventWriterIntegrationSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with SingleLocationSpecLeveldb {
  implicit val materializer: Materializer = ActorMaterializer()

  val SrcEmitterId = "src-emitter"
  val SrcLogId = "src-log"
  val WriterId = "writer"

  "An emission writer" must {
    "write a DurableEvent stream to a log (with an emission-write strategy) and continue the stream with the logged events" in {
      val writerId = "w1"

      val (src, snk) = TestSource.probe[DurableEvent]
        .via(DurableEventWriter.emissionWriter(WriterId, log))
        .toMat(TestSink.probe[DurableEvent])(Keep.both)
        .run()

      val expected = Seq(
        ("a", WriterId, logId, logId, 1L, VectorTime(logId -> 1L)),
        ("b", WriterId, logId, logId, 2L, VectorTime(logId -> 2L)),
        ("c", WriterId, logId, logId, 3L, VectorTime(logId -> 3L)))

      def extract(e: DurableEvent) =
        (e.payload, e.emitterId, e.processId, e.localLogId, e.localSequenceNr, e.vectorTimestamp)

      snk.request(3)

      src.sendNext(DurableEvent("a"))
      src.sendNext(DurableEvent("b"))
      src.sendNext(DurableEvent("c"))

      snk.expectNextN(3).map(extract) should be(expected)
      snk.cancel()

      Source.fromGraph(DurableEventSource(log)).map(extract).take(3).toMat(Sink.seq)(Keep.right).run().await should be(expected)
    }
  }

  "A replication writer" must {
    "write a Seq[DurableEvent] stream to a log (with a replication-write strategy) and continue the stream with the logged events" in {
      val writerId = "w2"

      val (src, snk) = TestSource.probe[Seq[DurableEvent]]
        .via(DurableEventWriter.replicationWriter(WriterId, log))
        .toMat(TestSink.probe[DurableEvent])(Keep.both)
        .run()

      val expected = Seq(
        ("a", WriterId, logId, logId, 1L, VectorTime(SrcLogId -> 11L, logId -> 1L)),
        ("b", WriterId, logId, logId, 2L, VectorTime(SrcLogId -> 12L, logId -> 2L)),
        ("c", WriterId, logId, logId, 3L, VectorTime(SrcLogId -> 13L, logId -> 3L)))

      def extract(e: DurableEvent) =
        (e.payload, e.emitterId, e.processId, e.localLogId, e.localSequenceNr, e.vectorTimestamp)

      def durableEvent(payload: String, sequenceNr: Long): DurableEvent =
        DurableEvent(payload, SrcEmitterId, processId = SrcLogId, localLogId = SrcLogId, localSequenceNr = sequenceNr, vectorTimestamp = VectorTime(SrcLogId -> sequenceNr))

      snk.request(3)

      src.sendNext(Seq(durableEvent("a", 11)))
      src.sendNext(Seq(durableEvent("b", 12), durableEvent("c", 13)))

      snk.expectNextN(3).map(extract) should be(expected)
      snk.cancel()

      Source.fromGraph(DurableEventSource(log)).map(extract).take(3).toMat(Sink.seq)(Keep.right).run().await should be(expected)
    }
  }
}
