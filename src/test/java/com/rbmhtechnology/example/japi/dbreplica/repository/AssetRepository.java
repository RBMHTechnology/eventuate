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

import com.rbmhtechnology.example.japi.dbreplica.domain.Asset;
import javaslang.control.Option;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.Collection;

import static com.rbmhtechnology.example.japi.dbreplica.util.CollectionUtil.headOption;

@Repository
public class AssetRepository {

    @Autowired
    private JdbcTemplate template;

    private final RowMapper<Asset> assetMapper = (rs, rowNum) ->
            new Asset(
                    rs.getString("id"),
                    rs.getString("subject"),
                    rs.getString("content")
            );

    public Collection<Asset> findAll() {
        return template.query("SELECT * FROM Asset ORDER BY id ASC", assetMapper);
    }

    public Option<Asset> find(final String assetId) {
        return headOption(template.query("SELECT * FROM Asset WHERE id = ?", assetMapper, assetId));
    }

    public int insert(final Asset asset) {
        return template.update("INSERT INTO Asset(id, subject, content) VALUES (?,?,?)", asset.getId(), asset.getSubject(), asset.getContent());
    }

    public int update(final Asset asset) {
        return template.update("UPDATE Asset SET subject = ?, content = ? WHERE id = ?", asset.getSubject(), asset.getContent(), asset.getId());
    }
}
