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

import javax.annotation.PostConstruct

import akka.actor._

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.log._
import com.rbmhtechnology.eventuate.log.NotificationChannel.Updated

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype._

import scala.concurrent.Future
import scala.util._

@Component
class AssetCdc @Autowired() (
  assetCdcInbound: AssetCdcInbound,
  assetCdcOutbound: AssetCdcOutbound,
  system: ActorSystem) {

  val endpoint =
    ReplicationEndpoint(id => Props(new AssetCdcActor(id, assetCdcInbound, assetCdcOutbound)))(system)

  val log: ActorRef =
    endpoint.logs(ReplicationEndpoint.DefaultLogName)

  @PostConstruct
  def activate(): Unit = {
    assetCdcOutbound.setUpdateNotificationTarget(log)
    endpoint.activate()
  }

}

private class AssetCdcActor(
  id: String,
  assetCdcInbound: AssetCdcInbound,
  assetCdcOutbound: AssetCdcOutbound) extends Actor {

  import context.dispatcher

  val channel: ActorRef =
    context.actorOf(Props(new NotificationChannel(id)))

  def receive = {
    case GetReplicationProgress(sourceLogId) =>
      val sdr = sender()
      Future(assetCdcInbound.readReplicationProgressAndVersion(sourceLogId)) onComplete {
        case Success((progress, version)) => sdr ! GetReplicationProgressSuccess(sourceLogId, progress, version)
        case Failure(e)                   => sdr ! GetReplicationProgressFailure(e)
      }
    case r @ ReplicationRead(from, _, filter, targetLogId, _, currentTargetVersionVector) =>
      val sdr = sender()
      channel ! r
      Future(assetCdcOutbound.readEventsAndVersion(from)) onComplete {
        case Success((events, version)) =>
          val filteredEvents = events.filter(_.replicable(currentTargetVersionVector, filter))
          val readProgress = events.lastOption.map(_.localSequenceNr).getOrElse(version.localTime(id))
          List(sdr, channel).foreach(_ ! ReplicationReadSuccess(filteredEvents, readProgress, targetLogId, version))
        case Failure(e) =>
          List(sdr, channel).foreach(_ ! ReplicationReadFailure(e.getMessage, targetLogId))
      }
    case w @ ReplicationWrite(events, sourceLogId, progress, _, _) =>
      // ------------------------------------------
      //  TODO: implement batch replication writes
      // ------------------------------------------
      Try {
        events.foldLeft(Vector.empty[DurableEvent]) {
          case (acc, evt) => if (assetCdcInbound.handle(evt) == 0) acc else acc :+ evt
        }
      } match {
        case Success(written) =>
          Try(assetCdcInbound.writeReplicationProgress(sourceLogId, progress)) match {
            case Success(_) => sender() ! ReplicationWriteSuccess(written.length, sourceLogId, progress, VectorTime.Zero)
            case Failure(e) => sender() ! ReplicationWriteFailure(e)
          }
          channel ! w
          channel ! Updated(written)
        case Failure(e) =>
          sender() ! ReplicationWriteFailure(e)
      }
    case Updated(written) =>
      channel ! Updated(written)
    case GetEventLogClock =>
      sender() ! GetEventLogClockSuccess(EventLogClock())
  }
}