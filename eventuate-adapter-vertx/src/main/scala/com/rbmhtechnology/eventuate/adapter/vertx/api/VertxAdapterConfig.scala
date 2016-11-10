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

import com.rbmhtechnology.eventuate.adapter.vertx.{ Confirmation, ProcessingResult, VertxAdapter }

import scala.collection.immutable.Seq
import scala.language.existentials

object VertxAdapterConfig {

  private val AdapterCodecClasses = Seq(classOf[ProcessingResult], Confirmation.getClass)

  /**
   * Creates an empty [[VertxAdapterConfig]].
   */
  def apply(): VertxAdapterConfig =
    VertxAdapterConfig(Seq.empty, AdapterCodecClasses)

  private def apply(producerConfigurations: Seq[EventProducerConfig], codecClasses: Seq[Class[_]]): VertxAdapterConfig = {
    validateConfigurations(producerConfigurations) match {
      case Right(cs) =>
        new VertxAdapterConfig(cs, codecClasses)
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid configuration given. Cause: $err")
    }
  }

  private def validateConfigurations(configs: Seq[EventProducerConfig]): Either[String, Seq[EventProducerConfig]] = {
    for {
      _ <- validateConfigurationIds(configs).right
      _ <- validateLogProducerConfigurationEndpoints(configs).right
    } yield configs
  }

  private def validateConfigurationIds(configs: Seq[EventProducerConfig]): Either[String, Seq[EventProducerConfig]] = {
    val invalid = configs.groupBy(_.id).filter(c => c._2.size > 1)

    if (invalid.isEmpty)
      Right(configs)
    else
      Left(s"Ambiguous definition for event producer(s) [${invalid.keys.map(id => s"'$id'").mkString(", ")}] given. " +
        s"An id must be used uniquely for a single producer.")
  }

  private def validateLogProducerConfigurationEndpoints(configs: Seq[EventProducerConfig]): Either[String, Seq[EventProducerConfig]] = {
    val invalid = configs.collect({ case c: LogProducerConfig => c }).flatMap(_.endpoints).groupBy(identity).filter(c => c._2.size > 1)

    if (invalid.isEmpty)
      Right(configs)
    else
      Left(s"Source-endpoint(s) [${invalid.keys.map(e => s"'$e'").mkString(",")}] were configured multiple times. " +
        s"A source-endpoint may only be configured once.")
  }
}

/**
 * An adapter configuration specifies the behaviour of a [[VertxAdapter]].
 *
 * The configuration contains the definition of all `event producers` the adapter will instantiate as well as the classes for which a
 * generic [[$messageCodec MessageCodec]] will be registered on the [[$vertxEventBus Vert.x event bus]].
 *
 * @param configurations Definitions of `event producers`.
 * @param codecClasses Classes for which a generic [[$messageCodec MessageCodec]] will be registered.
 * @define messageCodec http://vertx.io/docs/apidocs/io/vertx/core/eventbus/MessageCodec.html
 * @define vertxEventBus http://vertx.io/docs/apidocs/io/vertx/core/eventbus/EventBus.html
 */
class VertxAdapterConfig(private[vertx] val configurations: Seq[EventProducerConfig], private[vertx] val codecClasses: Seq[Class[_]]) {
  val vertxProducerConfigurations: Seq[VertxProducerConfig] =
    configurations.collect({ case c: VertxProducerConfig => c })

  val logProducerConfigurations: Seq[LogProducerConfig] =
    configurations.collect({ case c: LogProducerConfig => c })

  /**
   * Adds a single `event producer` to this configuration.
   * `Event producers` are created using the [[EventProducer]] factory methods.
   *
   * @param producer Definition of the `event producer`.
   */
  def addProducer(producer: EventProducerConfig): VertxAdapterConfig =
    addProducers(Seq(producer))

  /**
   * Adds multiple `event producers` to this configuration.
   * `Event producers` are created using the [[EventProducer]] factory methods.
   *
   * @param producers Definition of multiple `event producers`.
   */
  def addProducers(producers: Seq[EventProducerConfig]): VertxAdapterConfig =
    VertxAdapterConfig(configurations ++ producers, codecClasses)

  /**
   * Registers a generic [[$messageCodec MessageCodec]] per given class on the [[$vertxEventBus Vert.x event bus]].
   *
   * @param first Class the generic codec will be registered for.
   * @param rest Other classes the generic codec will be registered for.
   * @define messageCodec http://vertx.io/docs/apidocs/io/vertx/core/eventbus/MessageCodec.html
   * @define vertxEventBus http://vertx.io/docs/apidocs/io/vertx/core/eventbus/EventBus.html
   */
  def registerDefaultCodecFor(first: Class[_], rest: Class[_]*): VertxAdapterConfig =
    registerDefaultCodecForAll(first +: rest.toVector)

  /**
   * Registers a generic [[$messageCodec MessageCodec]] per given class on the [[$vertxEventBus Vert.x event bus]].
   *
   * @param classes Classes the generic codec will be registered for.
   * @define messageCodec http://vertx.io/docs/apidocs/io/vertx/core/eventbus/MessageCodec.html
   * @define vertxEventBus http://vertx.io/docs/apidocs/io/vertx/core/eventbus/EventBus.html
   */
  def registerDefaultCodecForAll(classes: Seq[Class[_]]): VertxAdapterConfig =
    VertxAdapterConfig(configurations, codecClasses ++ classes)
}
