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

import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.adapter.vertx.api.EventMetadata
import io.vertx.core.Vertx
import io.vertx.core.eventbus.{ DeliveryOptions, Message }

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future, Promise }

trait VertxProducer {
  def vertx: Vertx

  protected def deliveryOptions(event: DurableEvent): DeliveryOptions =
    new DeliveryOptions().setHeaders(EventMetadata(event).toHeaders)
}

trait VertxPublisher extends VertxProducer {
  def publish(address: String, evt: DurableEvent): Unit =
    vertx.eventBus().publish(address, evt.payload, deliveryOptions(evt))
}

trait VertxSender extends VertxProducer {

  import VertxHandlerConverters._

  def send[A](address: String, evt: DurableEvent, timeout: FiniteDuration)(implicit ec: ExecutionContext): Future[A] = {
    val promise = Promise[Message[A]]
    vertx.eventBus().send(address, evt.payload, deliveryOptions(evt).setSendTimeout(timeout.toMillis), promise.asVertxHandler)
    promise.future.map(_.body)
  }

  def send(address: String, evt: DurableEvent): Unit =
    vertx.eventBus().send(address, evt.payload, deliveryOptions(evt))
}
