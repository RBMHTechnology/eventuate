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

package com.rbmhtechnology.docs.vertx.japi;

//#adapter-example
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.rbmhtechnology.eventuate.ApplicationVersion;
import com.rbmhtechnology.eventuate.EndpointFilters$;
import com.rbmhtechnology.eventuate.ReplicationEndpoint;
import com.rbmhtechnology.eventuate.adapter.vertx.Confirmation;
import com.rbmhtechnology.eventuate.adapter.vertx.ProcessingResult;
import com.rbmhtechnology.eventuate.adapter.vertx.VertxAdapter;
import com.rbmhtechnology.eventuate.adapter.vertx.japi.ConfirmationType;
import com.rbmhtechnology.eventuate.adapter.vertx.japi.EventProducer;
import com.rbmhtechnology.eventuate.adapter.vertx.japi.StorageProvider;
import com.rbmhtechnology.eventuate.adapter.vertx.japi.VertxAdapterConfig;
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog;
import io.vertx.core.Vertx;
//#

import com.rbmhtechnology.eventuate.adapter.vertx.japi.EventMetadata;
import io.vertx.core.MultiMap;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import static java.util.stream.Collectors.toSet;

public class Documentation {

  public static void main(String[] args) {

    StorageProvider storageProvider = new FakeStorageProvider();

    //#adapter-example

    ActorSystem actorSystem = ActorSystem.create("system");
    Vertx vertx = Vertx.vertx();

    ReplicationEndpoint endpoint = ReplicationEndpoint.create("endpoint",
      set("sourceLog", "destinationLog"),
      id -> LeveldbEventLog.props(id, "log", true), set(), EndpointFilters$.MODULE$.NoFilters(), "default", ApplicationVersion.apply("0.1"), actorSystem);

    ActorRef sourceLog = endpoint.logs().apply("sourceLog");
    ActorRef destinationLog = endpoint.logs().apply("destinationLog");

    VertxAdapterConfig config = VertxAdapterConfig.create()
      .addProducer(
        EventProducer.fromLog(sourceLog)
          .publishTo("address-1")
          .as("vertx-producer")
      )
      .addProducer(
        EventProducer.fromEndpoints("address-2")
          .writeTo(destinationLog)
          .as("log-producer")
      );

    VertxAdapter adapter = VertxAdapter.create(config, vertx, storageProvider, actorSystem);

    // receive events from sourceLog...
    vertx.eventBus().<Event>consumer("address-1").handler(message -> {
      final Event event = message.body();

      // ...and persist the event in destinationLog
      vertx.eventBus().send("address-2", event);
    });

    adapter.start();
    //#

    //#vertx-event-producer
    EventProducer.fromLog(sourceLog)
      .publishTo("address-1")
      .as("vertx-producer");
    //#

    //#log-event-producer
    EventProducer.fromEndpoints("address-2")
      .writeTo(destinationLog)
      .as("log-producer");
    //#

    //#event-processing-vertx-producer
    vertx.eventBus().<Event>consumer("address-1").handler(message -> {
      Event event = message.body();
    });
    //#

    //#event-processing-log-producer
    vertx.eventBus().send("address-2", new Event("event-1"));
    //#

    //#vertx-publish-producer
    EventProducer.fromLog(sourceLog)
      .publishTo(ev -> {
        if (ev instanceof Event) {
          return Optional.of("address1");
        } else if (ev instanceof Event2) {
          return Optional.of("address2");
        }
        return Optional.of("default-address");
      })
      .as("vertx-publish-producer");
    //#

    //#vertx-ptp-producer-at-most-once
    EventProducer.fromLog(sourceLog)
      .sendTo(ev -> {
        if (ev instanceof Event) {
          return Optional.of("address1");
        }
        if (ev instanceof Event2) {
          return Optional.of("address2");
        }
        return Optional.of("default-address");
      })
    .as("vertx-ptp-producer");
    //#

    //#vertx-ptp-producer-at-least-once
    EventProducer.fromLog(sourceLog)
      .sendTo("address-1")
      .atLeastOnce(ConfirmationType.Batch.withSize(5), Duration.ofSeconds(5))
      .as("vertx-ptp-producer");
    //#

    //#vertx-ptp-producer-handler
    vertx.eventBus().<Event>consumer("address-1").handler(message -> {
      Event event = message.body();
      // confirm event receipt
      message.reply(Confirmation.create());
    });
    //#

    //#log-event-multiple-producer
    EventProducer.fromEndpoints("address-1", "address-2")
      .writeTo(destinationLog,
        ev -> {
          if (ev instanceof Event) {
            return true;
          }
          if (ev instanceof Event2) {
            return true;
          }
          return false;
        })
    .as("log-producer");
    //#

    //#log-producer-handler
    vertx.eventBus().<ProcessingResult>send("address-1", new Event("event-1"), ar -> {
      if (ar.succeeded()) {
        ProcessingResult result = ar.result().body();
      } else {
        // write failed
        Throwable failure = ar.cause();
      }
    });
    //#

    //#message-codec
    VertxAdapterConfig.create()
      .registerDefaultCodecFor(Event.class, Event2.class);
    //#

    //#event-metadata-from-headers
    vertx.eventBus().<Event>consumer("address-1").handler(message -> {
      MultiMap headers = message.headers();

      String localLogId = headers.get(EventMetadata.Headers.LOCAL_LOG_ID);
      Long localSeqNr = Long.valueOf(headers.get(EventMetadata.Headers.LOCAL_SEQUENCE_NR));
      String emitterId = headers.get(EventMetadata.Headers.EMITTER_ID);
    });
    //#

    //#event-metadata-from-helper
    vertx.eventBus().<Event>consumer("address-1").handler(message -> {
      EventMetadata metadata = EventMetadata.fromHeaders(message.headers()).get();

      String localLogId = metadata.localLogId();
      Long localSeqNr = metadata.localSequenceNr();
      String emitterId = metadata.emitterId();
    });
    //#
  }

  @SafeVarargs
  private static <T> Set<T> set(final T... vals) {
    return Arrays.stream(vals).collect(toSet());
  }

  public static class Event {
    private final String id;

    public Event(String id) {
      this.id = id;
    }
  }

  public static class Event2 {
    private final String id;

    public Event2(String id) {
      this.id = id;
    }
  }

  public static class FakeStorageProvider implements StorageProvider {
    @Override
    public CompletionStage<Long> readProgress(String logName) {
      return null;
    }

    @Override
    public CompletionStage<Long> writeProgress(String logName, Long sequenceNr) {
      return null;
    }
  }
}
