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

package com.rbmhtechnology.eventuate.adapter.vertx.api

import akka.actor.ActorRef

import scala.concurrent.duration.FiniteDuration

sealed trait EventProducerConfig {
  def id: String
  def log: ActorRef
}

sealed trait VertxProducerConfig extends EventProducerConfig {
  def endpointRouter: EndpointRouter
}

case class VertxPublisherConfig(id: String, log: ActorRef, endpointRouter: EndpointRouter) extends VertxProducerConfig
case class VertxSenderConfig(id: String, log: ActorRef, endpointRouter: EndpointRouter, deliveryMode: DeliveryMode) extends VertxProducerConfig
case class LogProducerConfig(id: String, log: ActorRef, endpoints: Set[String], filter: PartialFunction[Any, Boolean]) extends EventProducerConfig

sealed trait ConfirmationType
case object Single extends ConfirmationType
case class Batch(size: Int) extends ConfirmationType

sealed trait DeliveryMode
case object AtMostOnce extends DeliveryMode
case class AtLeastOnce(confirmationType: ConfirmationType, confirmationTimeout: FiniteDuration) extends DeliveryMode

object EndpointRouter {

  def route(f: PartialFunction[Any, String]): EndpointRouter =
    new EndpointRouter(f)

  def routeAllTo(s: String): EndpointRouter =
    new EndpointRouter({ case _ => s })
}

class EndpointRouter(f: PartialFunction[Any, String]) {
  val endpoint: Any => Option[String] = f.lift
}

/**
 * Factory to create instances of `event producers`.
 */
object EventProducer {

  /**
   * Creates a `Vert.x event producer` which consumes events from an event log and publishes or sends the events to a configurable event bus endpoint.
   *
   * @param log Source event log.
   */
  def fromLog(log: ActorRef): VertxProducerConfigFactory =
    new VertxProducerConfigFactory(log)

  /**
   *
   * Creates an `Log event producer` which consumes events from a given event bus endpoint and persists the events in an event log.
   *
   * @define vertxEventBus http://vertx.io/docs/apidocs/io/vertx/core/eventbus/EventBus.html
   * @param endpoints Source [[$vertxEventBus event bus]] endpoints.
   */
  def fromEndpoints(endpoints: String*): LogProducerConfigFactory =
    new LogProducerConfigFactory(endpoints.toSet)
}

trait CompletableEventProducerConfigFactory {

  /**
   * Specifies the id of the `event producer`.
   *
   * @param id Id of the producer.
   */
  def as(id: String): EventProducerConfig
}

class VertxProducerConfigFactory(log: ActorRef) {

  /**
   * Defines a `publish event producer` which publishes events from an event log to multiple subscribers on the event bus.
   * Events are delivered to the event bus endpoints specified by the partial function.
   * If no endpoint is specified for an event, the event will not be processed.
   *
   * Uses `At-Most-Once` delivery semantics.
   *
   * @param routes A partial function that maps source events to destination event bus endpoint.
   */
  def publishTo(routes: PartialFunction[Any, String]): VertxPublisherConfigFactory =
    new VertxPublisherConfigFactory(log, EndpointRouter.route(routes))

  /**
   * Defines a `point-to-point event producer` which sends an event to a single subscriber on the event bus.
   * Events are delivered to the event bus endpoints specified by the partial function.
   * If no endpoint is specified for an event, the event will not be processed.
   *
   * Uses `At-Most-Once` delivery semantics.
   *
   * @param routes A partial function that maps source events to destination event bus endpoint.
   */
  def sendTo(routes: PartialFunction[Any, String]): VertxSenderConfigFactory =
    new VertxSenderConfigFactory(log, EndpointRouter.route(routes))
}

class VertxPublisherConfigFactory(log: ActorRef, endpoints: EndpointRouter)
  extends CompletableEventProducerConfigFactory {

  override def as(id: String): EventProducerConfig =
    VertxPublisherConfig(id, log, endpoints)
}

class VertxSenderConfigFactory(log: ActorRef, endpointRouter: EndpointRouter, deliveryMode: DeliveryMode = AtMostOnce)
  extends CompletableEventProducerConfigFactory {

  /**
   * Specifies that this `point-to-point event producer` should use `At-Least-Once` delivery semantics.
   *
   * @param confirmationType Confirmation type - can either be `Batch` or `Single`.
   * @param confirmationTimeout Timeout after which events should be redelivered if no confirmation was received.
   */
  def atLeastOnce(confirmationType: ConfirmationType, confirmationTimeout: FiniteDuration): VertxSenderConfigFactory =
    new VertxSenderConfigFactory(log, endpointRouter, AtLeastOnce(confirmationType, confirmationTimeout))

  override def as(id: String): EventProducerConfig =
    VertxSenderConfig(id, log, endpointRouter, deliveryMode)
}

class LogProducerConfigFactory(endpoints: Set[String]) {

  /**
   * Sets the destination event log all received events are persisted to.
   * An optional event filter can be supplied. Only events passing the filter are persisted.
   *
   * @param log The destination event log.
   * @param filter A function specifying if an event should be persisted.
   */
  def writeTo(log: ActorRef, filter: PartialFunction[Any, Boolean] = { case _ => true }) = new CompletableEventProducerConfigFactory {
    override def as(id: String): LogProducerConfig =
      LogProducerConfig(id, log, endpoints, filter)
  }
}
