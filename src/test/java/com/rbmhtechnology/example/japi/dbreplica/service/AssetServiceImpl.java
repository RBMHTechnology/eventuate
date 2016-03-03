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
import com.rbmhtechnology.example.japi.dbreplica.repository.AssetRepository;
import javaslang.control.Option;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;

import static org.springframework.transaction.annotation.Propagation.REQUIRED;

@Service
public class AssetServiceImpl extends AssetListenersImpl implements AssetService {

    @Autowired
    private AssetRepository assetRepository;

    @Transactional(readOnly = true)
    public Collection<Asset> findAll() {
        return assetRepository.findAll();
    }

    @Transactional(readOnly = true)
    public Option<Asset> find(final String assetId) {
        return assetRepository.find(assetId);
    }

    @Transactional(propagation = REQUIRED)
    public void create(final Asset asset) {
        assetRepository.insert(asset);
        notifyCreated(asset);
    }

    @Transactional(propagation = REQUIRED)
    public void update(final Asset asset) {
        assetRepository.update(asset);
        notifyUpdated(asset);
    }
}
