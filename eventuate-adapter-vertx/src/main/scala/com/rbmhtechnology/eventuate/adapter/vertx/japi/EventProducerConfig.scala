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

package com.rbmhtechnology.eventuate.adapter.vertx.japi

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.function.{ Predicate, Function => JFunction }
import java.util.{ Optional => JOption }

import akka.actor.ActorRef
import com.rbmhtechnology.eventuate.adapter.vertx.api.{ EventProducerConfig => SEventProducerConfig, _ }

import scala.annotation.varargs
import scala.concurrent.duration.FiniteDuration

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
  @varargs
  def fromEndpoints(endpoints: String*): LogProducerConfigFactory =
    new LogProducerConfigFactory(endpoints.toSet)
}

class EventProducerConfig(private[vertx] val underlying: SEventProducerConfig)

abstract class CompletableEventProducerConfigFactory {

  /**
   * Specifies the id of the `event producer`.
   *
   * @param id Id of the producer.
   */
  def as(id: String): EventProducerConfig
}

class VertxProducerConfigFactory(log: ActorRef) {
  import JavaConfigConverters._

  /**
   * Defines a `publish event producer` which publishes events from an event log to multiple subscribers on the event bus.
   * Events are delivered to the given event bus endpoint.
   *
   * Uses `At-Most-Once` delivery semantics.
   *
   * @param endpoint Event bus endpoint all events are delivered to.
   */
  def publishTo(endpoint: String): VertxPublisherConfigFactory =
    new VertxPublisherConfigFactory(log, new EndpointRouter(endpoint.asPF))

  /**
   * Defines a `publish event producer` which publishes events from an event log to multiple subscribers on the event bus.
   * Events are delivered to the event bus endpoints specified by the given function.
   * If no endpoint is specified for an event, the event will not be processed.
   *
   * Uses `At-Most-Once` delivery semantics.
   *
   * @param routes A function that maps source events to destination event bus endpoint.
   */
  def publishTo(routes: JFunction[Any, JOption[String]]): VertxPublisherConfigFactory =
    new VertxPublisherConfigFactory(log, new EndpointRouter(routes.asScala))

  /**
   * Defines a `point-to-point event producer` which sends an event to a single subscriber on the event bus.
   * Events are delivered to the given event bus endpoint.
   *
   * Uses `At-Most-Once` delivery semantics.
   *
   * @param endpoint Event bus endpoint all events are delivered to.
   */
  def sendTo(endpoint: String): VertxSenderConfigFactory =
    new VertxSenderConfigFactory(log, new EndpointRouter(endpoint.asPF))

  /**
   * Defines a `point-to-point event producer` which sends an event to a single subscriber on the event bus.
   * Events are delivered to the event bus endpoints specified by the given function.
   * If no endpoint is specified for an event, the event will not be processed.
   *
   * Uses `At-Most-Once` delivery semantics.
   *
   * @param routes A function that maps source events to destination event bus endpoint.
   */
  def sendTo(routes: JFunction[Any, JOption[String]]): VertxSenderConfigFactory =
    new VertxSenderConfigFactory(log, new EndpointRouter(routes.asScala))
}

class VertxPublisherConfigFactory(log: ActorRef, endpoints: EndpointRouter)
  extends CompletableEventProducerConfigFactory {
  import JavaConfigConverters._

  override def as(id: String): EventProducerConfig =
    VertxPublisherConfig(id, log, endpoints).asJava
}

class VertxSenderConfigFactory(log: ActorRef, endpointRouter: EndpointRouter, deliveryMode: DeliveryMode = AtMostOnce)
  extends CompletableEventProducerConfigFactory {
  import JavaConfigConverters._

  /**
   * Specifies that this `point-to-point event producer` should use `At-Least-Once` delivery semantics.
   *
   * @param confirmationType Confirmation type - can either be `Batch` or `Single`.
   * @param confirmationTimeout Timeout after which events should be redelivered if no confirmation was received.
   */
  def atLeastOnce(confirmationType: ConfirmationType, confirmationTimeout: Duration): VertxSenderConfigFactory =
    new VertxSenderConfigFactory(log, endpointRouter, AtLeastOnce(confirmationType.asScala, confirmationTimeout.asScala))

  override def as(id: String): EventProducerConfig =
    VertxSenderConfig(id, log, endpointRouter, deliveryMode).asJava
}

class LogProducerConfigFactory(endpoints: Set[String]) {
  import JavaConfigConverters._

  /**
   * Sets the destination event log all received events are persisted to.
   *
   * @param log The destination event log.
   */
  def writeTo(log: ActorRef) = new CompletableEventProducerConfigFactory {
    override def as(id: String): EventProducerConfig =
      LogProducerConfig(id, log, endpoints, { case _ => true }).asJava
  }

  /**
   * Sets the destination event log all received events are persisted to.
   * Only events passing the filter are persisted.
   *
   * @param log The destination event log.
   * @param filter A predicate specifying if an event should be persisted.
   */
  def writeTo(log: ActorRef, filter: Predicate[Any]) = new CompletableEventProducerConfigFactory {
    override def as(id: String): EventProducerConfig =
      LogProducerConfig(id, log, endpoints, toPF(filter)).asJava
  }

  private def toPF(p: Predicate[Any]): PartialFunction[Any, Boolean] = {
    case v => p.test(v)
  }
}

object JavaConfigConverters {
  import com.rbmhtechnology.eventuate.adapter.vertx.api.{ ConfirmationType => SConfirmationType }

  implicit class AsScalaConfirmationType(ct: ConfirmationType) {
    def asScala: SConfirmationType = ct match {
      case c: SingleConfirmation => Single
      case c: BatchConfirmation  => Batch(c.size)
    }
  }

  implicit class AsScalaDuration(d: Duration) {
    def asScala: FiniteDuration =
      FiniteDuration(d.toNanos, TimeUnit.NANOSECONDS)
  }

  implicit class AsScalaPF[-A, +B](f: JFunction[A, JOption[B]]) {
    def asScala: PartialFunction[A, B] = new PartialFunction[A, B] {
      override def isDefinedAt(v: A): Boolean = f.apply(v).isPresent
      override def apply(v: A): B = f.apply(v).get()
    }
  }

  implicit class StringAsScalaPF[-A](s: String) {
    def asPF: PartialFunction[A, String] = new PartialFunction[A, String] {
      override def isDefinedAt(v: A): Boolean = true
      override def apply(v: A): String = s
    }
  }

  implicit class AsJavaProducerConfig(c: SEventProducerConfig) {
    def asJava: EventProducerConfig =
      new EventProducerConfig(c)
  }
}
