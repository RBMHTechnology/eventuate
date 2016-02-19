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

package com.rbmhtechnology.example.japi.dbreplica.repository;

import akka.actor.ActorSystem;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import com.rbmhtechnology.eventuate.DurableEvent;
import com.rbmhtechnology.eventuate.log.EventLogClock;
import javaslang.collection.List;
import javaslang.control.Try;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.Collection;

import static com.rbmhtechnology.example.japi.dbreplica.util.RepositoryUtil.createJdbcBlob;
import static org.apache.commons.io.IOUtils.toByteArray;

@Repository
public class AssetEventRepository {
    private final JdbcTemplate template;
    private final Serialization serialization;

    private RowMapper<Long> sequenceNrMapper = (rs, rowNum) -> rs.getLong(1);

    private RowMapper<DurableEvent> eventMapper = (rs, rowNum) ->
            Try.of(() -> deserializeDurableEvent(toByteArray(rs.getBlob(1).getBinaryStream()))).get();

    private DurableEvent deserializeDurableEvent(final byte[] eventBytes) {
        return serialization.deserialize(eventBytes, DurableEvent.class).get();
    }

    @Autowired
    public AssetEventRepository(final JdbcTemplate template, final ActorSystem system) {
        this.template = template;
        this.serialization = SerializationExtension.get(system);
    }

    public EventLogClock readClock(final EventLogClock clock) {
        return List.ofAll(findFrom(clock.sequenceNr() + 1)).foldLeft(clock, EventLogClock::update);
    }

    public Collection<DurableEvent> findFrom(final Long sequenceNr) {
        return template.query("SELECT event FROM AssetEventLog WHERE id >= ? ORDER BY id ASC", eventMapper, sequenceNr);
    }

    public Collection<DurableEvent> findFor(final String assetId) {
        return template.query("SELECT event FROM AssetEventLog WHERE assetId = ? ORDER BY id ASC", eventMapper, assetId);
    }

    public int insert(final String assetId, final DurableEvent event) {
        return template.update("INSERT INTO AssetEventLog (id, assetId, event) VALUES (?, ?, ?)", event.localSequenceNr(), assetId, createJdbcBlob(serializeDurableEvent(event)));
    }

    private byte[] serializeDurableEvent(final DurableEvent event) {
        return serialization.serialize(event).get();
    }

    public long updateSequenceNr() {
        return template.queryForObject("CALL NEXT VALUE FOR AssetEventLogSequence", sequenceNrMapper);
    }
}
