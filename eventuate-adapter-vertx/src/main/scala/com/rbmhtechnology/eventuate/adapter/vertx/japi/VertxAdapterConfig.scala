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

import java.util.{ Collection => JCollection }

import com.rbmhtechnology.eventuate.adapter.vertx.api.{ VertxAdapterConfig => SVertxAdapterSystemConfig }
import com.rbmhtechnology.eventuate.adapter.vertx.VertxAdapter

import scala.annotation.varargs

object VertxAdapterConfig {

  /**
   * Creates an empty [[VertxAdapterConfig]].
   */
  def create(): VertxAdapterConfig =
    new VertxAdapterConfig(SVertxAdapterSystemConfig())
}

/**
 * An adapter configuration specifies the behaviour of a [[VertxAdapter]].
 *
 * The configuration contains the definition of all `event producers` the adapter will instantiate as well as the classes for which a
 * generic [[$messageCodec MessageCodec]] will be registered on the [[$vertxEventBus Vert.x event bus]].
 *
 * @define messageCodec http://vertx.io/docs/apidocs/io/vertx/core/eventbus/MessageCodec.html
 * @define vertxEventBus http://vertx.io/docs/apidocs/io/vertx/core/eventbus/EventBus.html
 */
class VertxAdapterConfig(val underlying: SVertxAdapterSystemConfig) {
  import scala.collection.JavaConverters._

  /**
   * Adds a single `event producer` to this configuration.
   * `Event producers` are created using the [[EventProducer]] factory methods.
   *
   * @param producer Definition of the `event producer`.
   */
  def addProducer(producer: EventProducerConfig): VertxAdapterConfig =
    new VertxAdapterConfig(underlying.addProducer(producer.underlying))

  /**
   * Adds multiple `event producers` to this configuration.
   * `Event producers` are created using the [[EventProducer]] factory methods.
   *
   * @param producers Definition of multiple `event producers`.
   */
  def addProducers(producers: JCollection[EventProducerConfig]): VertxAdapterConfig =
    new VertxAdapterConfig(underlying.addProducers(producers.asScala.toVector.map(_.underlying)))

  /**
   * Registers a generic [[$messageCodec MessageCodec]] per given class on the [[$vertxEventBus Vert.x event bus]].
   *
   * @param first Class the generic codec will be registered for.
   * @param rest Other classes the generic codec will be registered for.
   * @define messageCodec http://vertx.io/docs/apidocs/io/vertx/core/eventbus/MessageCodec.html
   * @define vertxEventBus http://vertx.io/docs/apidocs/io/vertx/core/eventbus/EventBus.html
   */
  @varargs
  def registerDefaultCodecFor(first: Class[_], rest: Class[_]*): VertxAdapterConfig =
    new VertxAdapterConfig(underlying.registerDefaultCodecFor(first, rest: _*))

  /**
   * Registers a generic [[$messageCodec MessageCodec]] per given class on the [[$vertxEventBus Vert.x event bus]].
   *
   * @param classes Classes the generic codec will be registered for.
   * @define messageCodec http://vertx.io/docs/apidocs/io/vertx/core/eventbus/MessageCodec.html
   * @define vertxEventBus http://vertx.io/docs/apidocs/io/vertx/core/eventbus/EventBus.html
   */
  def registerDefaultCodecForAll(classes: JCollection[Class[_]]): VertxAdapterConfig =
    new VertxAdapterConfig(underlying.registerDefaultCodecForAll(classes.asScala.toVector))
}
