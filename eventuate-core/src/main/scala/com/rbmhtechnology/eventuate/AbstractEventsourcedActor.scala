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

package com.rbmhtechnology.eventuate

import java.lang.{ Iterable => JIterable }
import java.util.{ Set => JSet }

import akka.actor.ActorRef

import scala.collection.JavaConverters._

/**
 * Java API for actors that implement [[EventsourcedActor]].
 *
 * @see [[AbstractEventsourcedView]] for a detailed usage of the Java API
 * @see [[EventsourcedActor]]
 */
abstract class AbstractEventsourcedActor(id: String, eventLog: ActorRef) extends AbstractEventsourcedView(id, eventLog)
  with EventsourcedActor with ConfirmedDelivery with PersistOnEvent {

  /**
   * Java API of [[EventsourcedActor.persist persist]].
   *
   * Calls the given `handler` with the persist result for the event.
   *
   * @see [[EventsourcedActor]]
   */
  final def persist[A](event: A, handler: ResultHandler[A]): Unit =
    persist[A](event)(handler.asScala)

  /**
   * Java API of [[EventsourcedActor.persist persist]].
   *
   * Calls the given `handler` with the persist result for the event.
   *
   * Multiple routing destinations can be defined with the `customDestinationAggregateIds`.
   *
   * @see [[EventsourcedActor]]
   */
  final def persist[A](event: A, customDestinationAggregateIds: JSet[String], handler: ResultHandler[A]): Unit =
    persist[A](event, customDestinationAggregateIds.asScala.toSet)(handler.asScala)

  /**
   * Java API of [[EventsourcedActor.persistN persistN]].
   *
   * Calls the given `handler` with the persist result for each event in the sequence.
   *
   * @see [[EventsourcedActor]]
   */
  final def persistN[A](events: JIterable[A], handler: ResultHandler[A]): Unit =
    persistN(events.asScala.toSeq)(handler.asScala)

  /**
   * Java API of [[EventsourcedActor.persistN persistN]].
   *
   * Calls the given `handler` with the persist result for each event in the sequence.
   *
   * Accepts an `onLast` handler that will be called after persisting the last event of the sequence.
   *
   * @see [[EventsourcedActor]]
   */
  final def persistN[A](events: JIterable[A], onLast: ResultHandler[A], handler: ResultHandler[A]): Unit =
    persistN(events.asScala.toSeq, onLast = onLast.asScala)(handler.asScala)

  /**
   * Java API of [[EventsourcedActor.persistN persistN]].
   *
   * Calls the given `handler` with the persist result for each event in the sequence.
   *
   * Multiple routing destinations can be defined with the `customDestinationAggregateIds`.
   *
   * @see [[EventsourcedActor]]
   */
  final def persistN[A](events: JIterable[A], customDestinationAggregateIds: JSet[String], handler: ResultHandler[A]): Unit =
    persistN(events.asScala.toSeq, customDestinationAggregateIds = customDestinationAggregateIds.asScala.toSet)(handler.asScala)

  /**
   * Java API of [[EventsourcedActor.persistN persistN]].
   *
   * Calls the given `handler` with the persist result for each event in the sequence.
   *
   * Accepts an `onLast` handler that will be called after persisting the last event of the sequence.
   *
   * Multiple routing destinations can be defined with the `customDestinationAggregateIds`.
   *
   * @see [[EventsourcedActor]]
   */
  final def persistN[A](events: JIterable[A], onLast: ResultHandler[A], customDestinationAggregateIds: JSet[String],
    handler: ResultHandler[A]): Unit =
    persistN(events.asScala.toSeq, onLast.asScala, customDestinationAggregateIds.asScala.toSet)(handler.asScala)

  /**
   * Java API of [[ConfirmedDelivery.persistConfirmation persistConfirmation]].
   *
   * @see [[ConfirmedDelivery]]
   */
  final def persistConfirmation[A](event: A, deliveryId: String, handler: ResultHandler[A]): Unit =
    persistConfirmation(event, deliveryId, Set[String]())(handler.asScala)

  /**
   * Java API of [[ConfirmedDelivery.persistConfirmation persistConfirmation]].
   *
   * @see [[ConfirmedDelivery]]
   */
  final def persistConfirmation[A](event: A, deliveryId: String, customDestinationAggregateIds: JSet[String], handler: ResultHandler[A]): Unit =
    persistConfirmation(event, deliveryId, customDestinationAggregateIds.asScala.toSet)(handler.asScala)

  /**
   * Java API of [[PersistOnEvent.persistOnEvent persistOnEvent]].
   *
   * @see [[PersistOnEvent]]
   */
  final def persistOnEvent[A](event: A): Unit =
    persistOnEvent(event, Set[String]())

  /**
   * Java API of [[PersistOnEvent.persistOnEvent persistOnEvent]].
   *
   * Multiple routing destinations can be defined with the `customDestinationAggregateIds`.
   *
   * @see [[PersistOnEvent]]
   */
  final def persistOnEvent[A](event: A, customDestinationAggregateIds: JSet[String]): Unit =
    persistOnEvent(event, customDestinationAggregateIds.asScala.toSet)

  /**
   * Java API of [[ConfirmedDelivery.unconfirmed unconfirmed]].
   *
   * @see [[ConfirmedDelivery]]
   */
  final def getUnconfirmed: JSet[String] =
    unconfirmed.asJava
}
