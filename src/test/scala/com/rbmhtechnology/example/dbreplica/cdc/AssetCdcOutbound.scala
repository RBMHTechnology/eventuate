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

import akka.actor.ActorRef

import com.rbmhtechnology.example.dbreplica.domain._
import com.rbmhtechnology.example.dbreplica.event._
import com.rbmhtechnology.example.dbreplica.repository._
import com.rbmhtechnology.example.dbreplica.service.AssetService
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.log.NotificationChannel.Updated

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

import scala.collection.immutable.Seq

@Service
class AssetCdcOutbound @Autowired() (
  assetService: AssetService,
  assetEventRepository: AssetEventRepository,
  assetClockRepository: AssetClockRepository,
  assetCdcSettings: AssetCdcSettings) {

  import assetCdcSettings._

  private var updateNotificationTarget: Option[ActorRef] =
    None

  def setUpdateNotificationTarget(target: ActorRef): Unit =
    updateNotificationTarget = Some(target)

  @Transactional(readOnly = true)
  def readEventsAndVersion(fromSequenceNr: Long): (Seq[DurableEvent], VectorTime) =
    (assetEventRepository.findFrom(fromSequenceNr), assetEventRepository.readVersion)

  @Transactional
  def handle(assetEvent: AssetEvent): Int = assetEventRepository.updateVersion {
    val sequenceNr = assetEventRepository.updateSequenceNr()
    val durableEvent = createDurableEvent(assetEvent, sequenceNr, VectorTime(logId -> sequenceNr), System.currentTimeMillis)

    val updatedEvent = assetEvent match {
      case e: AssetCreated => handleCreated(durableEvent, e)
      case e: AssetEvent   => handleUpdated(durableEvent, e)
    }

    updateNotificationTarget.foreach(_ ! Updated(Seq(updatedEvent)))
    updatedEvent.vectorTimestamp
  }

  private def handleCreated(durableEvent: DurableEvent, assetEvent: AssetCreated): DurableEvent = {
    assetEventRepository.insert(assetEvent.assetId, durableEvent)
    assetClockRepository.insert(assetEvent.assetId, durableEvent.vectorTimestamp)
    assetService.create(eventProjection(null, assetEvent))

    durableEvent
  }

  private def handleUpdated(durableEvent: DurableEvent, assetEvent: AssetEvent): DurableEvent = {
    val current = for {
      a <- assetService.find(assetEvent.assetId)
      c <- assetClockRepository.find(assetEvent.assetId)
    } yield (a, c)

    val (asset, clock) = current.getOrElse(throw new AssetDoesNotExistException(assetEvent.assetId))
    val updatedAsset = eventProjection(asset, assetEvent)
    val updatedClock = durableEvent.vectorTimestamp.merge(clock)
    val updatedEvent = durableEvent.copy(vectorTimestamp = updatedClock)

    assetEventRepository.insert(assetEvent.assetId, updatedEvent)
    assetClockRepository.update(assetEvent.assetId, updatedClock)
    assetService.update(updatedAsset)

    updatedEvent
  }

  private def createDurableEvent(event: AssetEvent, sequenceNr: Long, vectorTimestamp: VectorTime, systemTimestamp: Long): DurableEvent =
    DurableEvent(
      payload = event,
      emitterId = event.assetId,
      vectorTimestamp = vectorTimestamp,
      systemTimestamp = systemTimestamp,
      processId = logId,
      localLogId = logId,
      localSequenceNr = sequenceNr)
}
