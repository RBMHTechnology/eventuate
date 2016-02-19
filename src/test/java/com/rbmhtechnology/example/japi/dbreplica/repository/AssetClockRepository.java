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
import com.rbmhtechnology.eventuate.VectorTime;
import javaslang.control.Option;
import javaslang.control.Try;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import static com.rbmhtechnology.example.japi.dbreplica.util.CollectionUtil.headOption;
import static com.rbmhtechnology.example.japi.dbreplica.util.RepositoryUtil.createJdbcBlob;

@Repository
public class AssetClockRepository {
    private final JdbcTemplate template;
    private final Serialization serialization;

    private final RowMapper<VectorTime> assetClockMapper = (rs, rowNum) ->
            Try.of(() -> deserializeVectorTime(IOUtils.toByteArray(rs.getBlob("vectorClock").getBinaryStream()))).get();

    private VectorTime deserializeVectorTime(final byte[] vectorTimeBytes) {
        return serialization.deserialize(vectorTimeBytes, VectorTime.class).get();
    }

    @Autowired
    public AssetClockRepository(final JdbcTemplate template, final ActorSystem system) {
        this.template = template;
        this.serialization = SerializationExtension.get(system);
    }

    public Option<VectorTime> find(final String assetId) {
        return headOption(template.query("SELECT * FROM AssetClock WHERE assetId = ? FOR UPDATE", assetClockMapper, assetId));
    }

    public int insert(final String assetId, final VectorTime clock) {
        return template.update("INSERT INTO AssetClock (assetId, vectorClock) VALUES (?, ?)", assetId, createJdbcBlob(serializeVectorTime(clock)));
    }

    private byte[] serializeVectorTime(final VectorTime vectorTime) {
        return serialization.serialize(vectorTime).get();
    }

    public int update(final String assetId, final VectorTime clock) {
        return template.update("UPDATE AssetClock SET vectorClock = ? WHERE assetId = ?", serializeVectorTime(clock), assetId);
    }

    public int delete(final String assetId) {
        return template.update("DELETE FROM AssetClock WHERE assetId = ?", assetId);
    }
}
