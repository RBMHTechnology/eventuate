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

package com.rbmhtechnology.example.dbreplica.cdc

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.ConcurrentVersions
import com.rbmhtechnology.eventuate.log.EventLogClock
import com.rbmhtechnology.example.dbreplica.domain._
import com.rbmhtechnology.example.dbreplica.event._
import com.rbmhtechnology.example.dbreplica.repository._
import com.rbmhtechnology.example.dbreplica.service._

import org.springframework.beans.factory.annotation._
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class AssetCdcInbound @Autowired() (
  assetService: AssetService,
  assetEventRepository: AssetEventRepository,
  assetClockRepository: AssetClockRepository,
  progressRepository: ProgressRepository,
  assetCdcSettings: AssetCdcSettings) {

  import assetCdcSettings._

  @Transactional(readOnly = true)
  def readClock(clock: EventLogClock): EventLogClock =
    assetEventRepository.readClock(clock)

  @Transactional(readOnly = true)
  def readReplicationProgressAndVersion(logId: String, clock: EventLogClock): (Long, VectorTime) =
    (progressRepository.readReplicationProgress(logId), assetEventRepository.readClock(clock).versionVector)

  @Transactional
  def writeReplicationProgress(logId: String, progress: Long): Unit =
    progressRepository.writeReplicationProgress(logId, progress)

  @Transactional
  def handle(durableEvent: DurableEvent): Unit = {
    // ------------------------------------------
    //  TODO: implement batch replication writes
    // ------------------------------------------
    val sequenceNr = assetEventRepository.updateSequenceNr()
    val updatedEvent = durableEvent.copy(localLogId = logId, localSequenceNr = sequenceNr)

    updatedEvent.payload match {
      case e: AssetCreated => handleCreated(updatedEvent, e)
      case e: AssetEvent   => handleUpdated(updatedEvent, e)
    }
  }

  def handleCreated(durableEvent: DurableEvent, assetEvent: AssetCreated): Unit = {
    assetEventRepository.insert(assetEvent.assetId, durableEvent)
    assetClockRepository.insert(assetEvent.assetId, durableEvent.vectorTimestamp)
    assetService.create(eventProjection(null, assetEvent))
  }

  def handleUpdated(durableEvent: DurableEvent, assetEvent: AssetEvent): Unit = {
    val current = for {
      a <- assetService.find(assetEvent.assetId)
      c <- assetClockRepository.find(assetEvent.assetId)
    } yield (a, c)

    val (asset, clock) = current.get
    val (updatedAsset, updateTimestamp) =
      if (clock < durableEvent.vectorTimestamp) {
        (eventProjection(asset, assetEvent), durableEvent.vectorTimestamp)
      } else { // concurrent update
        val recoveredVersions = recoverVersions(assetEvent.assetId)
        val concurrentVersions = recoveredVersions.update(assetEvent, durableEvent.vectorTimestamp, durableEvent.systemTimestamp, durableEvent.processId)
        val conflictingVersions = concurrentVersions.all
        val resolvedVersion = resolveVersions(concurrentVersions).all.head
        assetService.notifySelected(resolvedVersion.value, conflictingVersions.map(_.value))
        (resolvedVersion.value, resolvedVersion.vectorTimestamp)
      }

    assetEventRepository.insert(assetEvent.assetId, durableEvent)
    assetClockRepository.update(assetEvent.assetId, clock.merge(updateTimestamp))
    assetService.update(updatedAsset)
  }

  private def recoverVersions(assetId: String): ConcurrentVersions[Asset, AssetEvent] = {
    assetEventRepository.findFor(assetId).foldLeft(zeroConcurrentVersions) {
      case (acc, upd) =>
        val updated = acc.update(upd.payload.asInstanceOf[AssetEvent], upd.vectorTimestamp, upd.systemTimestamp, upd.processId)
        if (updated.conflict) resolveVersions(updated) else updated
    }
  }

  private def resolveVersions(concurrentVersions: ConcurrentVersions[Asset, AssetEvent]): ConcurrentVersions[Asset, AssetEvent] = {
    val conflictingVersions = concurrentVersions.all.sortWith { (v1, v2) =>
      // Let version with higher timestamp win, in case of equal timestamps that with higher processId (= creator) wins
      if (v1.systemTimestamp == v2.systemTimestamp) v1.creator < v2.creator else v1.systemTimestamp < v2.systemTimestamp
    }
    concurrentVersions.resolve(conflictingVersions.last.vectorTimestamp)
  }

  private def zeroConcurrentVersions: ConcurrentVersions[Asset, AssetEvent] =
    ConcurrentVersions[Asset, AssetEvent](null, eventProjection)
}
