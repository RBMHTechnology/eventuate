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

import akka.NotUsed
import akka.actor._
import akka.stream._
import akka.stream.scaladsl._

import com.rbmhtechnology.eventuate._

import scala.collection.immutable.Seq

/**
 * Stream-based alternative to [[EventsourcedProcessor]] and [[StatefulProcessor]].
 */
object DurableEventProcessor {
  import DurableEventWriter._

  /**
   * Creates an [[http://doc.akka.io/docs/akka/2.4/scala/stream/index.html Akka Streams]] stage that processes input
   * [[DurableEvent]]s with stateless `logic`, writes the processed events to `eventLog` and emits the written
   * [[DurableEvent]]s. The processor supports idempotent event processing by ignoring processed [[DurableEvent]]s
   * that have already been written in the past to `eventLog` which is determined by their `vectorTimestamp`s.
   * Behavior of the processor can be configured with:
   *
   *  - `eventuate.log.write-batch-size`. Maximum size of [[DurableEvent]] batches written to the event log. Events
   *  are batched (with [[Flow.batch]]) if they are produced faster than this processor can process and write events.
   *  - `eventuate.log.write-timeout`. Timeout for writing events to the event log. A write timeout or another write
   *  failure causes this stage to fail.
   *
   * This processor should be used in combination with one or more [[DurableEventSource]]s.
   *
   * @param id global unique writer id.
   * @param eventLog target event log.
   * @param logic stateless processing logic. Input is the [[DurableEvent]] input of this processor. The result
   *              must be a sequence of zero or more [[DurableEvent]] '''payloads'''. Restricting the processing
   *              logic to only read [[DurableEvent]] metadata allows the processor to correctly update these metadata
   *              before processed events are written to the target event log. The processing logic can be used to
   *              drop, transform and split [[DurableEvent]]s:
   *
   *               - to drop an event, an empty sequence should be returned
   *               - to transform an event, a sequence of length 1 should be returned
   *               - to split an event, a sequence of length > 1 should be returned
   */
  def statelessProcessor[O](id: String, eventLog: ActorRef)(logic: DurableEvent => Seq[O])(implicit system: ActorSystem): Graph[FlowShape[DurableEvent, DurableEvent], NotUsed] =
    statefulProcessor(id, eventLog)(())((s, e) => (s, logic(e)))

  /**
   * Creates an [[http://doc.akka.io/docs/akka/2.4/scala/stream/index.html Akka Streams]] stage that processes input
   * [[DurableEvent]]s with stateful `logic`, writes the processed events to `eventLog` and emits the written
   * [[DurableEvent]]s. The processor supports idempotent event processing by ignoring processed [[DurableEvent]]s
   * that have already been written in the past to `eventLog` which is determined by their `vectorTimestamp`s.
   * Behavior of the processor can be configured with:
   *
   *  - `eventuate.log.write-batch-size`. Maximum size of [[DurableEvent]] batches written to the event log. Events
   *  are batched (with [[Flow.batch]]) if they are produced faster than this processor can process and write events.
   *  - `eventuate.log.write-timeout`. Timeout for writing events to the event log. A write timeout or another write
   *  failure causes this stage to fail.
   *
   * This processor should be used in combination with one or more [[DurableEventSource]]s.
   *
   * @param id global unique writer id.
   * @param eventLog target event log.
   * @param zero initial processing state.
   * @param logic stateful processing logic. The state part of the input is either `zero` for the first stream element
   *              or the updated state from the previous processing step for all other stream elements. The event part
   *              of the input is the the [[DurableEvent]] input of this processor. The event part of the result must
   *              be a sequence of zero or more [[DurableEvent]] '''payloads'''. Restricting the processing logic to
   *              only read [[DurableEvent]] metadata allows the processor to correctly update these metadata before
   *              processed events are written to the target event log. The processing logic can be used to drop,
   *              transform and split [[DurableEvent]]s:
   *
   *               - to drop an event, an empty sequence should be returned in the event part of the result
   *               - to transform an event, a sequence of length 1 should be returned in the event part of the result
   *               - to split an event, a sequence of length > 1 should be returned in the event part of the result
   */
  def statefulProcessor[S, O](id: String, eventLog: ActorRef)(zero: S)(logic: (S, DurableEvent) => (S, Seq[O]))(implicit system: ActorSystem): Graph[FlowShape[DurableEvent, DurableEvent], NotUsed] =
    Flow.fromGraph(transformer(zero)(logic)).via(replicationWriter(id, eventLog))

  private def transformer[S, O](zero: S)(logic: (S, DurableEvent) => (S, Seq[O])): Graph[FlowShape[DurableEvent, Seq[DurableEvent]], NotUsed] =
    Flow.fromGraph[DurableEvent, Seq[DurableEvent], NotUsed](GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val unzip = builder.add(UnzipWith[DurableEvent, DurableEvent, DurableEvent](
        event => (event, event)))

      val zip = builder.add(ZipWith[Seq[O], DurableEvent, Seq[DurableEvent]] {
        case (payloads, event) => payloads.map(p => event.copy(payload = p))
      })

      val transform = Flow[DurableEvent].scan((zero, Seq.empty[O])) {
        case ((s, _), p) => logic(s, p)
      }.map(_._2).drop(1)

      unzip.out0 ~> transform ~> zip.in0
      unzip.out1 ~> zip.in1

      FlowShape(unzip.in, zip.out)
    })
}
