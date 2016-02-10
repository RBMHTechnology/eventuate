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

package com.rbmhtechnology.example.dbreplica.service

import com.rbmhtechnology.example.dbreplica.domain._
import com.rbmhtechnology.example.dbreplica.repository._

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.transaction.annotation.Propagation._

import scala.collection.immutable.Seq

trait AssetFinder {
  def findAll: Seq[Asset]
  def find(assetId: String): Option[Asset]
}

trait AssetService extends AssetFinder with AssetListeners {
  def create(asset: Asset): Unit
  def update(asset: Asset): Unit
}

@Service
class AssetServiceImpl @Autowired() (val assetRepository: AssetRepository) extends AssetService {
  @Transactional(readOnly = true)
  def findAll: Seq[Asset] =
    assetRepository.findAll()

  @Transactional(readOnly = true)
  def find(assetId: String): Option[Asset] =
    assetRepository.find(assetId)

  @Transactional(propagation = REQUIRED)
  def create(asset: Asset): Unit = {
    assetRepository.insert(asset)
    notifyCreated(asset)
  }

  @Transactional(propagation = REQUIRED)
  def update(asset: Asset): Unit = {
    assetRepository.update(asset)
    notifyUpdated(asset)
  }
}