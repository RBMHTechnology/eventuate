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

package com.rbmhtechnology.example.japi.dbreplica.util;

import com.rbmhtechnology.example.japi.dbreplica.event.AssetEvent;
import javaslang.Tuple2;
import javaslang.collection.Stream;
import scala.Option;
import scala.collection.immutable.Set$;

import static com.rbmhtechnology.example.japi.dbreplica.util.CollectionUtil.asScala;

public final class ScalaObjects {
    private ScalaObjects() {
    }

    public static class ReplicationProtocol {
        public static final com.rbmhtechnology.eventuate.ReplicationProtocol.GetEventLogClock$ GetEventLogClock = com.rbmhtechnology.eventuate.ReplicationProtocol.GetEventLogClock$.MODULE$;
        public static final com.rbmhtechnology.eventuate.ReplicationProtocol.ReplicationEndpointInfo$ ReplicationEndpointInfo = com.rbmhtechnology.eventuate.ReplicationProtocol.ReplicationEndpointInfo$.MODULE$;
    }

    public static class VectorTime {
        private static final scala.math.PartialOrdering<com.rbmhtechnology.eventuate.VectorTime> ordering = com.rbmhtechnology.eventuate.VectorTime.VectorTimePartialOrdering$.MODULE$;

        private final com.rbmhtechnology.eventuate.VectorTime vectorTime;

        private VectorTime(final com.rbmhtechnology.eventuate.VectorTime vectorTime) {
            this.vectorTime = vectorTime;
        }

        @SafeVarargs
        public static com.rbmhtechnology.eventuate.VectorTime create(final Tuple2<String, Long>... entries) {
            return com.rbmhtechnology.eventuate.VectorTime.apply(asScala(Stream.of(entries).map(e -> scala.Tuple2.apply(e._1, e._2)).toList()).toBuffer());
        }

        public static VectorTime from(final com.rbmhtechnology.eventuate.VectorTime vectorTime) {
            return new VectorTime(vectorTime);
        }

        public boolean lt(final com.rbmhtechnology.eventuate.VectorTime other) {
            return vectorTime.$less(other, ordering);
        }

        public com.rbmhtechnology.eventuate.VectorTime get() {
            return vectorTime;
        }
    }

    public static class DurableEvent {
        private final com.rbmhtechnology.eventuate.DurableEvent durableEvent;

        private DurableEvent(final com.rbmhtechnology.eventuate.DurableEvent durableEvent) {
            this.durableEvent = durableEvent;
        }

        public static com.rbmhtechnology.eventuate.DurableEvent create(final AssetEvent assetEvent, final long sequenceNr,
                                                                       final com.rbmhtechnology.eventuate.VectorTime vectorTimestamp,
                                                                       final long systemTimestamp, final String logId) {
            return new com.rbmhtechnology.eventuate.DurableEvent(
                    assetEvent,
                    assetEvent.assetId,
                    Option.empty(),
                    Set$.MODULE$.<String>empty(),
                    systemTimestamp,
                    vectorTimestamp,
                    logId,
                    logId,
                    sequenceNr,
                    Option.empty());
        }

        public static DurableEvent from(final com.rbmhtechnology.eventuate.DurableEvent durableEvent) {
            return new DurableEvent(durableEvent);
        }

        public com.rbmhtechnology.eventuate.DurableEvent get() {
            return durableEvent;
        }

        public com.rbmhtechnology.eventuate.DurableEvent setVectorTimestamp(final com.rbmhtechnology.eventuate.VectorTime vectorTimestamp) {
            return durableEvent.copy(
                    durableEvent.payload(),
                    durableEvent.emitterId(),
                    durableEvent.emitterAggregateId(),
                    durableEvent.customDestinationAggregateIds(),
                    durableEvent.systemTimestamp(),
                    vectorTimestamp,
                    durableEvent.processId(),
                    durableEvent.localLogId(),
                    durableEvent.localSequenceNr(),
                    durableEvent.persistOnEventSequenceNr());
        }

        public com.rbmhtechnology.eventuate.DurableEvent setLocalLogIdAndSequenceNr(final String localLogId, final Long localSequenceNr) {
            return durableEvent.copy(
                    durableEvent.payload(),
                    durableEvent.emitterId(),
                    durableEvent.emitterAggregateId(),
                    durableEvent.customDestinationAggregateIds(),
                    durableEvent.systemTimestamp(),
                    durableEvent.vectorTimestamp(),
                    durableEvent.processId(),
                    localLogId,
                    localSequenceNr,
                    durableEvent.persistOnEventSequenceNr());
        }
    }
}
