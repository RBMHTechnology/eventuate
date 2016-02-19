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

import com.rbmhtechnology.eventuate.ReplicationEndpoint;
import com.typesafe.config.Config;

import static com.rbmhtechnology.example.japi.dbreplica.util.ScalaObjects.ReplicationProtocol.ReplicationEndpointInfo;

public class AssetCdcSettings {

    private final String logId;

    private AssetCdcSettings(final String logId) {
        this.logId = logId;
    }

    public static AssetCdcSettings create(final Config config) {
        return new AssetCdcSettings(ReplicationEndpointInfo.logId(config.getString("eventuate.endpoint.id"),
                ReplicationEndpoint.DefaultLogName()));
    }

    public String getLogId() {
        return logId;
    }
}
