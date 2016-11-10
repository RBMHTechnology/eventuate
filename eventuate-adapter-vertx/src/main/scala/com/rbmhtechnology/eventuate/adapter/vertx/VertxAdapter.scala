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

import akka.actor.{ ActorSystem, Props }
import com.rbmhtechnology.eventuate.adapter.vertx.LogEventDispatcher.{ EndpointRoute, EventProducerRef }
import com.rbmhtechnology.eventuate.adapter.vertx.api._
import com.rbmhtechnology.eventuate.adapter.vertx.japi.rx.{ StorageProvider => RxStorageProvider }
import com.rbmhtechnology.eventuate.adapter.vertx.japi.{ StorageProvider => JStorageProvider, VertxAdapterConfig => JVertxAdapterConfig }
import io.vertx.core.Vertx
import io.vertx.rxjava.core.{ Vertx => RxVertx }

import scala.collection.immutable.Seq

object VertxAdapter {

  import VertxConverters._

  /**
   * Creates a [[VertxAdapter]] with the given [[com.rbmhtechnology.eventuate.adapter.vertx.api.VertxAdapterConfig VertxAdapterConfig]].
   *
   * @param config Configuration of the adapter.
   * @param vertx Vert.x instance the event logs are connected to.
   * @param storageProvider Storage provider used to persist replication progress of individual `event producers`.
   * @param system ActorSystem used for executing `event producers`.
   */
  def apply(config: VertxAdapterConfig, vertx: Vertx, storageProvider: StorageProvider)(implicit system: ActorSystem): VertxAdapter =
    new VertxAdapter(config, vertx, storageProvider)

  /**
   * Java API that creates a [[VertxAdapter]] with the given [[com.rbmhtechnology.eventuate.adapter.vertx.japi.VertxAdapterConfig VertxAdapterConfig]].
   *
   * @param config Configuration of the adapter.
   * @param vertx Vert.x instance the event logs are connected to.
   * @param storageProvider Storage provider used to persist replication progress of individual `event producers`.
   * @param system ActorSystem used for executing `event producers`.
   * @define vertx http://vertx.io/docs/apidocs/io/vertx/core/Vertx.html
   */
  def create(
    config: JVertxAdapterConfig,
    vertx: Vertx,
    storageProvider: JStorageProvider,
    system: ActorSystem): VertxAdapter =
    new VertxAdapter(config.underlying, vertx, storageProvider.asScala)(system)

  /**
   * Java API that creates a [[VertxAdapter]] with the given [[com.rbmhtechnology.eventuate.adapter.vertx.japi.VertxAdapterConfig VertxAdapterConfig]].
   * Uses the <a href="http://vertx.io/docs/vertx-rx/java/">rx-ified</a> version of Vert.x.
   *
   * @param config Configuration of the adapter.
   * @param vertx Rx-ified Vert.x instance the event logs are connected to.
   * @param storageProvider Storage provider used to persist replication progress of individual `event producers`.
   * @param system ActorSystem used for executing `event producers`.
   */
  def create(
    config: JVertxAdapterConfig,
    vertx: RxVertx,
    storageProvider: RxStorageProvider,
    system: ActorSystem): VertxAdapter =
    new VertxAdapter(config.underlying, vertx, storageProvider.asScala)(system)
}

/**
 * A [[$vertx Vert.x]] adapter connects event logs to a [[$vertx Vert.x]] instance.
 *
 * Event exchange is performed over the [[$vertxEventBus Vert.x event bus]] by `event producers`.
 * An `event producer` is a unidirectional connection between an event log and one or multiple event bus endpoints.
 * Events can be exchanged in both directions by different `event producers`.
 *
 * `Event producers` are created using the [[com.rbmhtechnology.eventuate.adapter.vertx.api.EventProducer EventProducer]]
 * factory methods and supplied to the adapter via the [[com.rbmhtechnology.eventuate.adapter.vertx.api.VertxAdapterConfig VertxAdapterConfig]].
 *
 * An adapter manages multiple `event producers`, initializes the producers and establishes the connection to the supplied [[$vertx Vert.x]] instance.
 *
 * A [[com.rbmhtechnology.eventuate.adapter.vertx.api.StorageProvider StorageProvider]] is used by the adapter to persist the replication progress of the individual `event producers`.
 *
 * Example:
 *         {{{
 *           val config = VertxAdapterConfig()
 *              .addProducer(
 *                EventProducer.fromLog(sourceLog)
 *                  .publishTo { case _ => "address-1" }
 *                  .as("vertx-producer")
 *              )
 *              .addProducer(
 *                EventProducer.fromEndpoints("address-2")
 *                  .writeTo(destinationLog)
 *                  .as("log-producer")
 *              )
 *
 *           val adapter = VertxAdapter(config, vertx, storageProvider)(actorSystem)
 *           adapter.start()
 *         }}}
 *
 * @param config Configuration of the adapter.
 * @param vertx Vert.x instance the event logs are connected to.
 * @param storageProvider Storage provider used to persist replication progress of individual `event producers`.
 * @param system ActorSystem used for executing `event producers`.
 * @define vertx http://vertx.io/docs/apidocs/io/vertx/core/Vertx.html
 * @define vertxEventBus http://vertx.io/docs/apidocs/io/vertx/core/eventbus/EventBus.html
 */
class VertxAdapter private[vertx] (config: VertxAdapterConfig, vertx: Vertx, storageProvider: StorageProvider)(implicit system: ActorSystem) {

  private def registerEventBusCodecs(): Unit = {
    config.codecClasses.foreach(c =>
      try {
        vertx.eventBus().registerDefaultCodec(c.asInstanceOf[Class[AnyRef]], AkkaSerializationMessageCodec(c))
      } catch {
        case e: IllegalStateException =>
          throw new IllegalStateException(s"An adapter codec for class ${c.getName} was configured, even though a default codec was already registered for this class.")
      })
  }

  /**
   * Starts event replication of all configured `event producers`.
   */
  def start(): Unit = {
    registerEventBusCodecs()
    val supervisor = system.actorOf(VertxSupervisor.props(adapters))
  }

  private def adapters: Seq[Props] =
    vertxProducers ++ logProducers

  private def vertxProducers: Seq[Props] = config.vertxProducerConfigurations.map {
    case VertxPublisherConfig(id, log, endpointRouter) =>
      VertxNoConfirmationPublisher.props(id, log, endpointRouter, vertx, storageProvider)

    case VertxSenderConfig(id, log, endpointRouter, AtMostOnce) =>
      VertxNoConfirmationSender.props(id, log, endpointRouter, vertx, storageProvider)

    case VertxSenderConfig(id, log, endpointRouter, AtLeastOnce(Single, timeout)) =>
      VertxSingleConfirmationSender.props(id, log, endpointRouter, vertx, timeout)

    case VertxSenderConfig(id, log, endpointRouter, AtLeastOnce(Batch(size), timeout)) =>
      VertxBatchConfirmationSender.props(id, log, endpointRouter, vertx, storageProvider, size, timeout)
  }

  private def logProducers: Seq[Props] = {
    if (config.logProducerConfigurations.nonEmpty)
      Seq(LogEventDispatcher.props(toEndpointRoutes(config.logProducerConfigurations), vertx))
    else
      Seq.empty
  }

  private def toEndpointRoutes(configs: Seq[LogProducerConfig]): Seq[EndpointRoute] =
    configs.flatMap(c => c.endpoints.map(e => EndpointRoute(e, EventProducerRef(c.id, c.log), c.filter)))
}