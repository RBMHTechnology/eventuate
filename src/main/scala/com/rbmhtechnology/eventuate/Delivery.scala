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

object Delivery {
  case class DeliveryAttempt(message: Any, destination: ActorPath)
}

trait Delivery extends Eventsourced { this: EventsourcedActor =>
  import Delivery._

  private var _unconfirmed: SortedMap[String, DeliveryAttempt] = SortedMap.empty

  def deliver(deliveryId: String, message: Any, destination: ActorPath): Unit = {
    _unconfirmed = _unconfirmed + (deliveryId -> DeliveryAttempt(message, destination))
    if (!recovering) send(message, destination)
  }

  def redeliverUnconfirmed(): Unit = _unconfirmed.foreach {
    case (_, DeliveryAttempt(m, d)) => send(m, d)
  }

  def confirm(deliveryId: String): Unit =
    _unconfirmed = _unconfirmed - deliveryId

  def unconfirmed: Set[String] =
    _unconfirmed.keySet

  override def onRecoverySuccess(): Unit = {
    super.onRecoverySuccess()
    redeliverUnconfirmed()
  }

  private def send(message: Any, destination: ActorPath): Unit =
    context.actorSelection(destination) ! message
}
