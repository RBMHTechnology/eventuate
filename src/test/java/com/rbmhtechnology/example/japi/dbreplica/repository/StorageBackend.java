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

import com.typesafe.config.Config;
import org.hsqldb.jdbc.JDBCDriver;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.jdbc.datasource.embedded.ConnectionProperties;
import org.springframework.jdbc.datasource.embedded.DataSourceFactory;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;

import javax.sql.DataSource;
import java.sql.Driver;

public class StorageBackend {
    private static final String DB_URL = "jdbc:hsqldb:file:target/DB/EP-%s";
    private static final String USERNAME = "sa";
    private static final String PASSWORD = "";
    private static final String INIT_SCRIPT_PATH = "dbreplica/create.sql";
    private static final String CONFIG_EVENTUATE_ENDPOINT_ID = "eventuate.endpoint.id";

    private final DataSource dataSource;

    private StorageBackend(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public static StorageBackend create(final Config config) {
        final String endpoint = config.getString(CONFIG_EVENTUATE_ENDPOINT_ID);
        final String url = String.format(DB_URL, endpoint);

        return new StorageBackend(
                new EmbeddedDatabaseBuilder().setDataSourceFactory(StorageDataSourceFactory.create(url)).addScript(INIT_SCRIPT_PATH).build()
        );
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    private static class StorageDataSourceFactory implements DataSourceFactory {
        private final DataSource dataSource;

        private StorageDataSourceFactory(final DataSource dataSource) {
            this.dataSource = dataSource;
        }

        public static StorageDataSourceFactory create(final String url) {
            return new StorageDataSourceFactory(new SimpleDriverDataSource(JDBCDriver.driverInstance, url, USERNAME, PASSWORD));
        }

        @Override
        public ConnectionProperties getConnectionProperties() {
            return new ConnectionProperties() {
                @Override
                public void setDriverClass(final Class<? extends Driver> driverClass) {
                }

                @Override
                public void setUrl(final String url) {
                }

                @Override
                public void setUsername(final String username) {
                }

                @Override
                public void setPassword(final String password) {
                }
            };
        }

        @Override
        public DataSource getDataSource() {
            return dataSource;
        }
    }
}
