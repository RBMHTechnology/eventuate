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

package com.rbmhtechnology.eventuate.adapter.vertx.api

import com.rbmhtechnology.eventuate.{ DurableEvent, EventsourcedActor, EventsourcedProcessor }
import io.vertx.core.MultiMap
import io.vertx.rxjava.core.{ MultiMap => RxMultiMap }

object EventMetadata {
  import com.rbmhtechnology.eventuate.adapter.vertx.VertxExtensions._

  private[vertx] val MessageProducer = "producer"
  private[vertx] val VertxProducer = "vertx-producer"

  object Headers {
    val LocalSequenceNr = "localSequenceNr"
    val LocalLogId = "localLogId"
    val EmitterId = "emitterId"
  }

  /**
   * Creates an instance of [[EventMetadata]] containing event metadata extracted from the headers of an event bus message
   * if this message originated from a Vert.x producer.
   *
   * @param headers Headers of the event bus message.
   * @return An [[Option]] containing the metadata if this message was sent by a Vert.x producer otherwise [[None]].
   */
  def fromHeaders(headers: MultiMap): Option[EventMetadata] =
    headers.getAsOption(MessageProducer).filter(_ == VertxProducer)
      .map { s =>
        new EventMetadata(
          headers.getOrElseThrow(Headers.LocalLogId),
          headers.getOrElseThrow(Headers.LocalSequenceNr).toLong,
          headers.getOrElseThrow(Headers.EmitterId))
      }

  /**
   * Creates an instance of [[EventMetadata]] containing event metadata extracted from the headers of an event bus message
   * if this message originated from a Vert.x producer.
   *
   * @param headers Headers of the event bus message.
   * @return An [[Option]] containing the metadata if this message was sent by a Vert.x producer otherwise [[None]].
   */
  def fromHeaders(headers: RxMultiMap): Option[EventMetadata] =
    fromHeaders(headers.getDelegate.asInstanceOf[MultiMap])

  private[vertx] def apply(event: DurableEvent): EventMetadata =
    new EventMetadata(event.localLogId, event.localSequenceNr, event.emitterId)
}

/**
 * Contains metadata of an event originating from an event log.
 *
 * @param localLogId      Id of the local event log.
 * @param localSequenceNr Sequence number in the local event log.
 * @param emitterId       Id of the emitter ([[EventsourcedActor]] or [[EventsourcedProcessor]]).
 */
class EventMetadata(val localLogId: String, val localSequenceNr: Long, val emitterId: String) {
  import EventMetadata._

  private[vertx] def toHeaders: MultiMap = {
    val map = MultiMap.caseInsensitiveMultiMap()
    map.set(MessageProducer, VertxProducer)
    map.set(Headers.LocalLogId, localLogId)
    map.set(Headers.LocalSequenceNr, localSequenceNr.toString)
    map.set(Headers.EmitterId, emitterId)
    map
  }
}
