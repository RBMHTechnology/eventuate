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

import com.rbmhtechnology.eventuate.*;
import com.rbmhtechnology.eventuate.log.EventLogClock;
import com.rbmhtechnology.example.japi.dbreplica.domain.Asset;
import com.rbmhtechnology.example.japi.dbreplica.domain.AssetDoesNotExistException;
import com.rbmhtechnology.example.japi.dbreplica.event.AssetEvent;
import com.rbmhtechnology.example.japi.dbreplica.event.AssetEvent.AssetCreated;
import com.rbmhtechnology.example.japi.dbreplica.repository.AssetClockRepository;
import com.rbmhtechnology.example.japi.dbreplica.repository.AssetEventRepository;
import com.rbmhtechnology.example.japi.dbreplica.repository.ProgressRepository;
import com.rbmhtechnology.example.japi.dbreplica.service.AssetService;
import com.rbmhtechnology.example.japi.dbreplica.util.ScalaObjects;
import javaslang.Tuple;
import javaslang.Tuple2;
import javaslang.collection.Stream;
import javaslang.control.Match;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static com.rbmhtechnology.example.japi.dbreplica.cdc.AssetCdc.eventProjection;

@Service
public class AssetCdcInbound {
    @Autowired
    private AssetService assetService;
    @Autowired
    private AssetEventRepository assetEventRepository;
    @Autowired
    private AssetClockRepository assetClockRepository;
    @Autowired
    private ProgressRepository progressRepository;
    @Autowired
    private AssetCdcSettings assetCdcSettings;

    @Transactional(readOnly = true)
    public EventLogClock readClock(final EventLogClock clock) {
        return assetEventRepository.readClock(clock);
    }

    @Transactional(readOnly = true)
    public Tuple2<Long, VectorTime> readReplicationProgressAndVersion(final String logId, final EventLogClock clock) {
        return Tuple.of(progressRepository.readReplicationProgress(logId), readClock(clock).versionVector());
    }

    @Transactional
    public void writeReplicationProgress(final String logId, final Long progress) {
        progressRepository.writeReplicationProgress(logId, progress);
    }

    @Transactional
    public void handle(final DurableEvent durableEvent) {
        final Long sequenceNr = assetEventRepository.updateSequenceNr();
        final DurableEvent updatedEvent = ScalaObjects.DurableEvent.from(durableEvent).setLocalLogIdAndSequenceNr(getLogId(), sequenceNr);

        Match.of(updatedEvent.payload())
                .whenType(AssetCreated.class).thenRun(ev -> handleCreated(updatedEvent, ev))
                .whenType(AssetEvent.class).thenRun(ev -> handleUpdated(updatedEvent, ev));
    }

    private void handleCreated(final DurableEvent durableEvent, final AssetCreated assetEvent) {
        assetEventRepository.insert(assetEvent.assetId, durableEvent);
        assetClockRepository.insert(assetEvent.assetId, durableEvent.vectorTimestamp());
        assetService.create(eventProjection.apply(null, assetEvent));
    }

    private void handleUpdated(final DurableEvent durableEvent, final AssetEvent assetEvent) {
        final Tuple2<Asset, VectorTime> assetWithClock =
                assetService.find(assetEvent.assetId)
                        .flatMap(a -> assetClockRepository.find(assetEvent.assetId).map(c -> Tuple.of(a, c)))
                        .getOrElseThrow(() -> new AssetDoesNotExistException(assetEvent.assetId));

        final Asset asset = assetWithClock._1;
        final ScalaObjects.VectorTime clock = ScalaObjects.VectorTime.from(assetWithClock._2);

        if (clock.lt(durableEvent.vectorTimestamp())) {
            final Asset updatedAsset = eventProjection.apply(asset, assetEvent);
            persistUpdate(assetEvent, durableEvent, clock.get(), updatedAsset, durableEvent.vectorTimestamp());
        } else {
            final ConcurrentVersions<Asset, AssetEvent> recoveredVersions = recoverVersions(assetEvent.assetId);
            final ConcurrentVersions<Asset, AssetEvent> concurrentVersions = recoveredVersions.update(assetEvent, durableEvent.vectorTimestamp(), durableEvent.systemTimestamp(), durableEvent.processId());
            final List<Versioned<Asset>> conflictingVersions = concurrentVersions.getAll();
            final Versioned<Asset> resolvedVersion = Stream.ofAll(resolveVersions(concurrentVersions).getAll()).last();

            assetService.notifySelected(resolvedVersion.value(), Stream.ofAll(conflictingVersions).map(Versioned::value).toJavaList());
            persistUpdate(assetEvent, durableEvent, clock.get(), resolvedVersion.value(), resolvedVersion.vectorTimestamp());
        }
    }

    private void persistUpdate(final AssetEvent assetEvent, final DurableEvent durableEvent, final VectorTime clock, final Asset updatedAsset,
                               final VectorTime updateTimestamp) {
        assetEventRepository.insert(assetEvent.assetId, durableEvent);
        assetClockRepository.update(assetEvent.assetId, clock.merge(updateTimestamp));
        assetService.update(updatedAsset);
    }

    private ConcurrentVersions<Asset, AssetEvent> recoverVersions(final String assetId) {
        return Stream.ofAll(assetEventRepository.findFor(assetId))
                .foldLeft(
                        zeroConcurrentVersions(),
                        (acc, upd) -> {
                            final ConcurrentVersions<Asset, AssetEvent> updated = acc.update((AssetEvent) upd.payload(), upd.vectorTimestamp(), upd.systemTimestamp(), upd.processId());
                            if (updated.conflict()) {
                                return resolveVersions(updated);
                            } else {
                                return updated;
                            }
                        }
                );
    }

    private ConcurrentVersions<Asset, AssetEvent> resolveVersions(final ConcurrentVersions<Asset, AssetEvent> concurrentVersions) {
        final Versioned<Asset> winningVersion = Stream.ofAll(concurrentVersions.getAll())
                .sort((v1, v2) -> {
                    // Let version with higher timestamp win, in case of equal timestamps that with higher processId (= creator) wins
                    if (v1.systemTimestamp() == v2.systemTimestamp()) {
                        return v1.creator().compareTo(v2.creator());
                    } else {
                        return new Long(v1.systemTimestamp()).compareTo(v2.systemTimestamp());
                    }
                }).last();

        return concurrentVersions.resolve(winningVersion.vectorTimestamp());
    }

    private String getLogId() {
        return assetCdcSettings.getLogId();
    }

    private ConcurrentVersions<Asset, AssetEvent> zeroConcurrentVersions() {
        return ConcurrentVersionsTree.create(eventProjection);
    }
}
