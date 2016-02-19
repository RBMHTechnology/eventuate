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

package com.rbmhtechnology.example.japi.dbreplica;

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.rbmhtechnology.eventuate.ReplicationConnection;
import com.rbmhtechnology.example.japi.dbreplica.cdc.AssetCdcOutbound;
import com.rbmhtechnology.example.japi.dbreplica.cdc.AssetCdcSettings;
import com.rbmhtechnology.example.japi.dbreplica.cli.DBReplicaCLI;
import com.rbmhtechnology.example.japi.dbreplica.domain.Asset;
import com.rbmhtechnology.example.japi.dbreplica.repository.StorageBackend;
import com.rbmhtechnology.example.japi.dbreplica.service.AssetFinder;
import com.rbmhtechnology.example.japi.dbreplica.service.AssetListener;
import com.rbmhtechnology.example.japi.dbreplica.service.AssetListeners;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.Collection;

public class DBReplica {
    private static final String LOCATION_PATH = "com.rbmhtechnology.example.japi.dbreplica.DBReplica$Location%s";
    private static final String EVENTUATE_CLI_DISPATCHER = "eventuate.cli-dispatcher";

    @Configuration
    public static class LocationA extends DBReplicaLocation {
        @Override
        public Config getConfig() {
            return ConfigFactory.load("dbreplica/location-A.conf");
        }
    }

    @Configuration
    public static class LocationB extends DBReplicaLocation {
        @Override
        public Config getConfig() {
            return ConfigFactory.load("dbreplica/location-B.conf");
        }
    }

    @Configuration
    public static class LocationC extends DBReplicaLocation {
        @Override
        public Config getConfig() {
            return ConfigFactory.load("dbreplica/location-C.conf");
        }
    }

    @EnableAspectJAutoProxy
    @EnableTransactionManagement
    @ComponentScan("com.rbmhtechnology.example.japi.dbreplica")
    public static abstract class DBReplicaLocation {

        @Bean
        public abstract Config getConfig();

        @Bean
        public AssetCdcSettings getSettings() {
            return AssetCdcSettings.create(getConfig());
        }

        @Bean
        public StorageBackend getStorageBackend() {
            return StorageBackend.create(getConfig());
        }

        @Bean
        public PlatformTransactionManager getTxManager() {
            return new DataSourceTransactionManager(getStorageBackend().getDataSource());
        }

        @Bean
        public JdbcTemplate getJdbcTemplate() {
            return new JdbcTemplate(getStorageBackend().getDataSource());
        }

        @Bean(destroyMethod = "terminate")
        public ActorSystem getSystem() {
            return ActorSystem.create(ReplicationConnection.DefaultRemoteSystemName(), getConfig());
        }
    }

    public static class DBReplicaListener implements AssetListener {

        @Override
        public void assetCreated(final Asset asset) {
            printAsset(asset, "created");
        }

        @Override
        public void assetUpdated(final Asset asset) {
            printAsset(asset, "updated");
        }

        @Override
        public void assetSelected(final Asset asset, final Collection<Asset> conflicts) {
            conflicts.forEach(conflict -> printAsset(conflict, "conflict"));
            printAsset(asset, "selected");
        }

        private void printAsset(final Asset asset, final String action) {
            System.out.println(String.format("%s: %s", action, asset));
        }
    }

    public static void main(final String[] args) throws ClassNotFoundException {
        final String location = args[0];
        final String locationClass = String.format(LOCATION_PATH, location);
        final ApplicationContext context = new AnnotationConfigApplicationContext(Class.forName(locationClass));

        final AssetListeners listeners = context.getBean(AssetListeners.class);
        final AssetCdcOutbound assetService = context.getBean(AssetCdcOutbound.class);
        final AssetFinder assetFinder = context.getBean(AssetFinder.class);
        final ActorSystem system = context.getBean(ActorSystem.class);

        listeners.addListener(new DBReplicaListener());
        system.actorOf(Props.create(DBReplicaCLI.class, () -> new DBReplicaCLI(assetService, assetFinder)).withDispatcher(EVENTUATE_CLI_DISPATCHER));
    }
}
