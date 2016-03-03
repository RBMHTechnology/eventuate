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

package com.rbmhtechnology.example.japi.dbreplica.cdc;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.rbmhtechnology.eventuate.DurableEvent;
import com.rbmhtechnology.eventuate.ReplicationEndpoint;
import com.rbmhtechnology.eventuate.ReplicationProtocol.*;
import com.rbmhtechnology.eventuate.VectorTime;
import com.rbmhtechnology.eventuate.log.EventLogClock;
import com.rbmhtechnology.eventuate.log.NotificationChannel;
import com.rbmhtechnology.eventuate.log.NotificationChannel.Updated;
import javaslang.Tuple;
import javaslang.Tuple2;
import javaslang.collection.List;
import javaslang.collection.Stream;
import javaslang.collection.Vector;
import javaslang.control.Try;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static com.rbmhtechnology.example.japi.dbreplica.util.CollectionUtil.asJava;
import static com.rbmhtechnology.example.japi.dbreplica.util.CollectionUtil.asScala;
import static com.rbmhtechnology.example.japi.dbreplica.util.ScalaObjects.ReplicationProtocol.GetEventLogClock;

public class AssetCdcActor extends AbstractActor {

    @Component
    public static class Runner {
        private final AssetCdcOutbound assetCdcOutbound;
        private final ReplicationEndpoint endpoint;

        @Autowired
        public Runner(final AssetCdcInbound assetCdcInbound, final AssetCdcOutbound assetCdcOutbound, final ActorSystem system) {
            this.assetCdcOutbound = assetCdcOutbound;

            this.endpoint = ReplicationEndpoint.create(
                    id -> Props.create(AssetCdcActor.class, () -> new AssetCdcActor(id, assetCdcInbound, assetCdcOutbound)),
                    system
            );
        }

        private ActorRef getLog() {
            return endpoint.logs().get(ReplicationEndpoint.DefaultLogName()).get();
        }

        @PostConstruct
        private void activate() {
            assetCdcOutbound.setUpdateNotificationTarget(getLog());
            endpoint.activate();
        }
    }

    private static final EventLogClock EVENT_LOG_CLOCK = new EventLogClock(0L, VectorTime.Zero());

    private final String id;
    private final AssetCdcInbound assetCdcInbound;
    private final AssetCdcOutbound assetCdcOutbound;
    private final ActorRef channel;
    private EventLogClock clock;

    public AssetCdcActor(final String id, final AssetCdcInbound assetCdcInbound, final AssetCdcOutbound assetCdcOutbound) {
        this.id = id;
        this.assetCdcInbound = assetCdcInbound;
        this.assetCdcOutbound = assetCdcOutbound;
        this.channel = getContext().actorOf(Props.create(NotificationChannel.class, () -> new NotificationChannel(id)));
        this.clock = EVENT_LOG_CLOCK;

        receive(ReceiveBuilder
                .match(GetReplicationProgress.class, this::handleGetReplicationProgress)
                .match(ReplicationRead.class, this::handleReplicationRead)
                .match(ReplicationWrite.class, this::handleReplicationWrite)
                .match(Updated.class, updated -> channel.tell(updated, self()))
                .match(GetEventLogClock.getClass(), getClock -> sender().tell(new GetEventLogClockSuccess(EVENT_LOG_CLOCK), self()))
                .build());
    }

    private void handleGetReplicationProgress(final GetReplicationProgress replicationProgess) {
        final ActorRef sender = sender();
        final EventLogClock clk = this.clock;
        final String sourceLogId = replicationProgess.sourceLogId();

        CompletableFuture.supplyAsync(() -> assetCdcInbound.readReplicationProgressAndVersion(sourceLogId, clk))
                .whenComplete((result, err) -> {
                    if (err == null) {
                        sender.tell(new GetReplicationProgressSuccess(sourceLogId, result._1, result._2), self());
                    } else {
                        sender.tell(new GetReplicationProgressFailure(err), self());
                    }
                });
    }

    private void handleReplicationRead(final ReplicationRead replicationRead) {
        final List<ActorRef> targets = List.of(sender(), channel);
        final EventLogClock clk = this.clock;

        channel.tell(replicationRead, self());

        CompletableFuture.supplyAsync(() -> assetCdcOutbound.readEventsAndVersion(replicationRead.fromSequenceNr(), clk))
                .whenComplete((result, err) -> {
                    if (err == null) {
                        final Collection<DurableEvent> events = result._1;
                        final VectorTime version = result._2;

                        final Collection<DurableEvent> filteredEvents = Stream.ofAll(events)
                                .filter(e -> e.replicable(replicationRead.currentTargetVersionVector(), replicationRead.filter()))
                                .toJavaList();
                        final Long readProgress = Stream.ofAll(events).lastOption().map(DurableEvent::localSequenceNr).getOrElse(() -> version.localTime(id));

                        targets.forEach(t -> t.tell(new ReplicationReadSuccess(asScala(filteredEvents), readProgress, replicationRead.targetLogId(), version), self()));
                    } else {
                        targets.forEach(t -> t.tell(new ReplicationReadFailure(err.getMessage(), replicationRead.targetLogId()), self()));
                    }
                });
    }

    private void handleReplicationWrite(final ReplicationWrite replicationWrite) {
        clock = assetCdcInbound.readClock(this.clock);

        final Tuple2<EventLogClock, Vector<DurableEvent>> updatedClockWithEvents = Stream.ofAll(asJava(replicationWrite.events()))
                .foldLeft(
                        Tuple.of(clock, Vector.<DurableEvent>empty()),
                        (acc, evt) -> {
                            final EventLogClock updatedClock = acc._1;
                            final Vector<DurableEvent> updatedEvents = acc._2;

                            if (evt.before(updatedClock.versionVector())) {
                                return acc;
                            } else {
                                return Tuple.of(updatedClock.update(evt), updatedEvents.append(evt));
                            }
                        });

        final EventLogClock updatedClock = updatedClockWithEvents._1;
        final Vector<DurableEvent> updatedEvents = updatedClockWithEvents._2;

        Try.run(() -> updatedEvents.forEach(assetCdcInbound::handle))
                .onSuccess(res -> {
                    clock = updatedClock;
                    Try.run(() -> assetCdcInbound.writeReplicationProgress(replicationWrite.sourceLogId(), replicationWrite.replicationProgress()))
                            .onSuccess(res2 -> sender().tell(new ReplicationWriteSuccess(updatedEvents.size(),
                                    replicationWrite.sourceLogId(), replicationWrite.replicationProgress(), VectorTime.Zero()), self()))
                            .onFailure(err -> sender().tell(new ReplicationWriteFailure(err), self()));
                    channel.tell(replicationWrite, self());
                    channel.tell(new Updated(asScala(updatedEvents)), self());
                })
                .onFailure(err -> sender().tell(new ReplicationWriteFailure(err), self()));
    }

    @Override
    public void preStart() throws Exception {
        clock = assetCdcInbound.readClock(clock);
    }
}
