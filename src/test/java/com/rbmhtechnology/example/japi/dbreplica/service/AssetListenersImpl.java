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

package com.rbmhtechnology.example.japi.dbreplica.service;

import com.rbmhtechnology.example.japi.dbreplica.domain.Asset;
import javaslang.collection.List;

import java.util.Collection;

public abstract class AssetListenersImpl implements AssetListeners {
    volatile private List<AssetListener> listeners = List.empty();

    public void addListener(final AssetListener listener) {
        this.listeners = listeners.append(listener);
    }

    public void notifyCreated(final Asset asset) {
        listeners.forEach(l -> l.assetCreated(asset));
    }

    public void notifyUpdated(final Asset asset) {
        listeners.forEach(l -> l.assetUpdated(asset));
    }

    public void notifySelected(final Asset asset, final Collection<Asset> conflicts) {
        listeners.forEach(l -> l.assetSelected(asset, conflicts));
    }
}
