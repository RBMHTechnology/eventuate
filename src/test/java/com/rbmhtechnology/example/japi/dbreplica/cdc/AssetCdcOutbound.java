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

import akka.actor.ActorRef;
import com.rbmhtechnology.eventuate.DurableEvent;
import com.rbmhtechnology.eventuate.VectorTime;
import com.rbmhtechnology.eventuate.log.EventLogClock;
import com.rbmhtechnology.eventuate.log.NotificationChannel.Updated;
import com.rbmhtechnology.example.japi.dbreplica.domain.Asset;
import com.rbmhtechnology.example.japi.dbreplica.domain.AssetDoesNotExistException;
import com.rbmhtechnology.example.japi.dbreplica.event.AssetEvent;
import com.rbmhtechnology.example.japi.dbreplica.event.AssetEvent.AssetCreated;
import com.rbmhtechnology.example.japi.dbreplica.repository.AssetClockRepository;
import com.rbmhtechnology.example.japi.dbreplica.repository.AssetEventRepository;
import com.rbmhtechnology.example.japi.dbreplica.service.AssetService;
import com.rbmhtechnology.example.japi.dbreplica.util.ScalaObjects;
import javaslang.Tuple;
import javaslang.Tuple2;
import javaslang.control.Match;
import javaslang.control.Option;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.Collections;

import static com.rbmhtechnology.example.japi.dbreplica.cdc.AssetCdc.eventProjection;
import static com.rbmhtechnology.example.japi.dbreplica.util.CollectionUtil.asScala;


@Service
public class AssetCdcOutbound {
    @Autowired
    private AssetService assetService;
    @Autowired
    private AssetEventRepository assetEventRepository;
    @Autowired
    private AssetClockRepository assetClockRepository;
    @Autowired
    private AssetCdcSettings assetCdcSettings;

    private Option<ActorRef> updateNotificationTarget = Option.none();

    public void setUpdateNotificationTarget(final ActorRef updateNotificationTarget) {
        this.updateNotificationTarget = Option.some(updateNotificationTarget);
    }

    public Tuple2<Collection<DurableEvent>, VectorTime> readEventsAndVersion(final long sequenceNr, final EventLogClock clock) {
        return Tuple.of(assetEventRepository.findFrom(sequenceNr), assetEventRepository.readClock(clock).versionVector());
    }

    @Transactional
    public void handle(final AssetEvent assetEvent) {
        final long sequenceNr = assetEventRepository.updateSequenceNr();
        final DurableEvent durableEvent =
                ScalaObjects.DurableEvent.create(assetEvent, sequenceNr, ScalaObjects.VectorTime.create(Tuple.of(getLogId(), sequenceNr)), System.currentTimeMillis(), getLogId());

        final DurableEvent updatedEvent = Match.of(assetEvent)
                .whenType(AssetCreated.class).then(ev -> handleCreated(durableEvent, ev))
                .whenType(AssetEvent.class).then(ev -> handleUpdated(durableEvent, ev))
                .get();

        updateNotificationTarget.forEach(t -> t.tell(new Updated(asScala(Collections.singletonList(updatedEvent))), null));
    }

    private DurableEvent handleCreated(final DurableEvent durableEvent, final AssetCreated assetEvent) {
        assetEventRepository.insert(assetEvent.assetId, durableEvent);
        assetClockRepository.insert(assetEvent.assetId, durableEvent.vectorTimestamp());
        assetService.create(eventProjection.apply(null, assetEvent));

        return durableEvent;
    }

    private DurableEvent handleUpdated(final DurableEvent durableEvent, final AssetEvent assetEvent) {
        final Tuple2<Asset, VectorTime> assetWithClock =
                assetService.find(assetEvent.assetId)
                        .flatMap(a -> assetClockRepository.find(assetEvent.assetId).map(c -> Tuple.of(a, c)))
                        .getOrElseThrow(() -> new AssetDoesNotExistException(assetEvent.assetId));

        final Asset asset = assetWithClock._1;
        final VectorTime clock = assetWithClock._2;

        final Asset updatedAsset = eventProjection.apply(asset, assetEvent);
        final VectorTime updatedClock = durableEvent.vectorTimestamp().merge(clock);
        final DurableEvent updatedEvent = ScalaObjects.DurableEvent.from(durableEvent).setVectorTimestamp(updatedClock);

        assetEventRepository.insert(assetEvent.assetId, updatedEvent);
        assetClockRepository.update(assetEvent.assetId, updatedClock);
        assetService.update(updatedAsset);

        return updatedEvent;
    }

    private String getLogId() {
        return this.assetCdcSettings.getLogId();
    }
}
