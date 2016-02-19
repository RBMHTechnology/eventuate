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

import com.rbmhtechnology.example.japi.dbreplica.domain.Asset;
import com.rbmhtechnology.example.japi.dbreplica.event.AssetEvent;
import com.rbmhtechnology.example.japi.dbreplica.event.AssetEvent.AssetContentUpdated;
import com.rbmhtechnology.example.japi.dbreplica.event.AssetEvent.AssetCreated;
import com.rbmhtechnology.example.japi.dbreplica.event.AssetEvent.AssetSubjectUpdated;
import javaslang.control.Match;

import java.util.function.BiFunction;

public class AssetCdc {

    public static BiFunction<Asset, AssetEvent, Asset> eventProjection = (asset, event) ->
            Match.of(event)
                    .whenType(AssetCreated.class).then(ev -> new Asset(ev.assetId, ev.subject, ev.content))
                    .whenType(AssetSubjectUpdated.class).then(ev -> asset.setSubject(ev.subject))
                    .whenType(AssetContentUpdated.class).then(ev -> asset.setContent(ev.content))
                    .get();
}
