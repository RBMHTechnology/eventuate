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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.rbmhtechnology.example.japi.dbreplica.util.CollectionUtil.headOption;

@Repository
public class ProgressRepository {

    @Autowired
    private JdbcTemplate template;

    private final RowMapper<Long> progressMapper = (rs, rowNum) -> rs.getLong("progress");

    public long readReplicationProgress(final String logId) {
        return headOption(template.query("SELECT progress FROM ReplicationProgress WHERE logId = ?", progressMapper, logId)).getOrElse(0L);
    }

    public void writeReplicationProgress(final String logId, final long progress) {
        template.execute(new WriteCallback(logId, progress));
    }

    private static class WriteCallback implements ConnectionCallback<Void> {
        private final String logId;
        private final long progress;

        public WriteCallback(final String logId, final long progress) {
            this.logId = logId;
            this.progress = progress;
        }

        @Override
        public Void doInConnection(final Connection con) throws SQLException, DataAccessException {
            insertOrUpdate(con, String.format("SELECT * FROM ReplicationProgress WHERE logId = '%s'", logId), rs -> {
                rs.updateString("logId", logId);
                rs.updateLong("progress", progress);
            });
            return null;
        }

        private void insertOrUpdate(final Connection connection, final String query, final CheckedSqlConsumer<ResultSet> operations) throws SQLException {
            final Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            final ResultSet rs = statement.executeQuery(query);
            if (rs.next()) {
                operations.accept(rs);
                rs.updateRow();
            } else {
                rs.moveToInsertRow();
                operations.accept(rs);
                rs.insertRow();
            }
        }
    }

    private interface CheckedSqlConsumer<T> {
        void accept(T t) throws SQLException;
    }
}
