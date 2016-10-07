/*
 * Copyright 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.eventuate.adapter.vertx

import akka.actor.{ ActorLogging, ActorRef, Props }
import akka.pattern.pipe
import com.rbmhtechnology.eventuate.adapter.vertx.api.EndpointRouter
import com.rbmhtechnology.eventuate.{ ConfirmedDelivery, EventsourcedActor }
import io.vertx.core.Vertx

import scala.concurrent.duration.FiniteDuration
import scala.util.{ Failure, Success }

private[vertx] object VertxSingleConfirmationSender {

  case class DeliverEvent(evt: EventEnvelope, deliveryId: String)
  case class Confirm(deliveryId: String)
  case class DeliverFailed(evt: EventEnvelope, deliveryId: String, err: Throwable)
  case object Redeliver

  case class DeliveryConfirmed()

  def props(id: String, eventLog: ActorRef, endpointRouter: EndpointRouter, vertx: Vertx, confirmationTimeout: FiniteDuration): Props =
    Props(new VertxSingleConfirmationSender(id, eventLog, endpointRouter, vertx, confirmationTimeout))
}

private[vertx] class VertxSingleConfirmationSender(val id: String, val eventLog: ActorRef, val endpointRouter: EndpointRouter, val vertx: Vertx, confirmationTimeout: FiniteDuration)
  extends EventsourcedActor with ConfirmedDelivery with VertxSender with ActorLogging {

  import VertxSingleConfirmationSender._
  import context.dispatcher

  context.system.scheduler.schedule(confirmationTimeout, confirmationTimeout, self, Redeliver)

  override def onCommand: Receive = {
    case DeliverEvent(envelope, deliveryId) =>
      send[Any](envelope.address, envelope.evt, confirmationTimeout)
        .map(_ => Confirm(deliveryId))
        .recover {
          case err => DeliverFailed(envelope, deliveryId, err)
        }
        .pipeTo(self)

    case Confirm(deliveryId) if unconfirmed.contains(deliveryId) =>
      persistConfirmation(DeliveryConfirmed(), deliveryId) {
        case Success(evt) =>
        case Failure(err) => log.error(s"Confirmation for delivery with id '$deliveryId' could not be persisted.", err)
      }

    case Redeliver =>
      redeliverUnconfirmed()

    case DeliverFailed(evt, deliveryId, err) =>
      log.warning(s"Delivery with id '$deliveryId' for event [$evt] failed with $err. The delivery will be retried.")
  }

  override def onEvent: Receive = {
    case DeliveryConfirmed() =>
    // confirmations should not be published
    case ev =>
      endpointRouter.endpoint(ev) match {
        case Some(endpoint) =>
          val deliveryId = lastSequenceNr.toString
          deliver(deliveryId, DeliverEvent(EventEnvelope(endpoint, ev), deliveryId), self.path)
        case None =>
      }
  }
}
