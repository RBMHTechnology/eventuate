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

import io.vertx.core.Vertx
import io.vertx.core.eventbus.{ DeliveryOptions, Message }

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future, Promise }

trait VertxProducer {
  def vertx: Vertx
}

trait VertxPublisher extends VertxProducer {
  def publish(address: String, evt: Any): Unit =
    vertx.eventBus().publish(address, evt)
}

trait VertxSender extends VertxProducer {
  import VertxHandlerConverters._

  def send[A](address: String, evt: Any, timeout: FiniteDuration)(implicit ec: ExecutionContext): Future[A] = {
    val promise = Promise[Message[A]]
    vertx.eventBus().send(address, evt, new DeliveryOptions().setSendTimeout(timeout.toMillis), promise.asVertxHandler)
    promise.future.map(_.body)
  }

  def send(address: String, evt: Any): Unit =
    vertx.eventBus().send(address, evt)
}
