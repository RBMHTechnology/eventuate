/*
 * Copyright (C) 2015 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
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

package com.rbmhtechnology.eventuate

import scala.collection.immutable.SortedMap

import akka.actor._

object ConfirmedDelivery {
  case class DeliveryAttempt(deliveryId: String, message: Any, destination: ActorPath)
}

/**
 * Supports the reliable delivery of messages to destinations by enabling applications
 * to redeliver messages until they are confirmed by their destinations. Both, message
 * delivery and confirmation must be executed within an [[EventsourcedActor]]'s event handler.
 * New messages are delivered by calling `deliver`. When the destination replies with a confirmation
 * message, the [[EventsourcedActor]] must generate an event for which the handler calls `confirm`.
 * Until confirmation, delivered messages are tracked as ''unconfirmed'' messages. Unconfirmed
 * messages can be redelivered by calling `redeliverUnconfirmed`. This is usually done within a
 * command handler by processing scheduler messages. Redelivery occurs automatically when the
 * [[EventsourcedActor]] successfully recovered after initial start or a re-start.
 */
trait ConfirmedDelivery extends EventsourcedActor {
  import ConfirmedDelivery._

  private var _unconfirmed: SortedMap[String, DeliveryAttempt] = SortedMap.empty

  /**
   * Delivers the given `message` to a `destination`. The delivery of `message` is identified by
   * the given `deliveryId` which must be unique in context of the sending actor. The message is
   * tracked as unconfirmed message until delivery is confirmed with `confirm`, using the same
   * `deliveryId`.
   */
  def deliver(deliveryId: String, message: Any, destination: ActorPath): Unit = {
    _unconfirmed = _unconfirmed + (deliveryId -> DeliveryAttempt(deliveryId, message, destination))
    if (!recovering) send(message, destination)
  }

  /**
   * Redelivers all unconfirmed messages.
   */
  def redeliverUnconfirmed(): Unit = _unconfirmed.foreach {
    case (_, DeliveryAttempt(_, m, d)) => send(m, d)
  }

  /**
   * Confirms the message delivery identified by `deliveryId`.
   */
  def confirm(deliveryId: String): Unit =
    _unconfirmed = _unconfirmed - deliveryId

  /**
   * Delivery ids of unconfirmed messages.
   */
  def unconfirmed: Set[String] =
    _unconfirmed.keySet

  /**
   * Internal API.
   */
  override private[eventuate] def snapshotCaptured(snapshot: Snapshot): Snapshot = {
    _unconfirmed.values.foldLeft(super.snapshotCaptured(snapshot)) {
      case (s, da) => s.add(da)
    }
  }

  /**
   * Internal API.
   */
  override private[eventuate] def snapshotLoaded(snapshot: Snapshot): Unit = {
    super.snapshotLoaded(snapshot)
    snapshot.deliveryAttempts.foreach { da =>
      _unconfirmed = _unconfirmed + (da.deliveryId -> da)
    }
  }

  /**
   * Internal API.
   */
  private[eventuate] override def recovered(): Unit = {
    super.recovered()
    redeliverUnconfirmed()
  }

  private def send(message: Any, destination: ActorPath): Unit =
    context.actorSelection(destination) ! message
}
