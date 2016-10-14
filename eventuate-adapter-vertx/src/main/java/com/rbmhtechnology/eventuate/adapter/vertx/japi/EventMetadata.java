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

package com.rbmhtechnology.eventuate.adapter.vertx.japi;

import io.vertx.core.MultiMap;
import scala.Option;

import java.util.Optional;

/**
 * Contains metadata of an event originating from an event log.
 */
public class EventMetadata {

  public static class Headers {
    public static final String LOCAL_SEQUENCE_NR =
      com.rbmhtechnology.eventuate.adapter.vertx.api.EventMetadata.Headers$.MODULE$.LocalSequenceNr();
    public static final String LOCAL_LOG_ID =
      com.rbmhtechnology.eventuate.adapter.vertx.api.EventMetadata.Headers$.MODULE$.LocalLogId();
    public static final String EMITTER_ID =
      com.rbmhtechnology.eventuate.adapter.vertx.api.EventMetadata.Headers$.MODULE$.EmitterId();
  }

  private final com.rbmhtechnology.eventuate.adapter.vertx.api.EventMetadata underlying;

  private EventMetadata(com.rbmhtechnology.eventuate.adapter.vertx.api.EventMetadata underlying) {
    this.underlying = underlying;
  }

  /**
   * Creates an instance of [[EventMetadata]] containing event metadata extracted from the headers of an event bus message
   * if this message originated from a Vert.x producer.
   *
   * @param headers Headers of the event bus message.
   * @return An [[Optional]] containing the metadata if this message was sent by a Vert.x producer otherwise an empty [[Optional]].
   */
  public static Optional<EventMetadata> fromHeaders(final MultiMap headers) {
    return toOptional(com.rbmhtechnology.eventuate.adapter.vertx.api.EventMetadata.fromHeaders(headers)).map(EventMetadata::new);
  }

  /**
   * Creates an instance of [[EventMetadata]] containing event metadata extracted from the headers of an event bus message
   * if this message originated from a Vert.x producer.
   *
   * @param headers Headers of the event bus message.
   * @return An [[Optional]] containing the metadata if this message was sent by a Vert.x producer otherwise an empty [[Optional]].
   */
  public static Optional<EventMetadata> fromHeaders(final io.vertx.rxjava.core.MultiMap headers) {
    return toOptional(com.rbmhtechnology.eventuate.adapter.vertx.api.EventMetadata.fromHeaders(headers)).map(EventMetadata::new);
  }

  private static <A> Optional<A> toOptional(final Option<A> opt) {
    return opt.isDefined() ? Optional.of(opt.get()) : Optional.empty();
  }

  /**
   * Id of the local event log.
   */
  public String localLogId() {
    return underlying.localLogId();
  }

  /**
   * Sequence number in the local event log.
   */
  public Long localSequenceNr() {
    return underlying.localSequenceNr();
  }

  /**
   * Id of the emitter ([[EventsourcedActor]] or [[EventsourcedProcessor]]).
   */
  public String emitterId() {
    return underlying.emitterId();
  }
}
