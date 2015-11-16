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

package com.rbmhtechnology.eventuate.log

import akka.actor.ActorRef
import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.EventsourcingProtocol._

import scala.collection.immutable.Seq

private case class SubscriberRegistry(
  aggregateRegistry: AggregateRegistry = AggregateRegistry(),
  defaultRegistry: Set[ActorRef] = Set.empty) {

  def registerDefaultSubscriber(subscriber: ActorRef): SubscriberRegistry =
    copy(defaultRegistry = defaultRegistry + subscriber)

  def registerAggregateSubscriber(subscriber: ActorRef, aggregateId: String): SubscriberRegistry =
    copy(aggregateRegistry = aggregateRegistry.add(subscriber, aggregateId))

  def unregisterSubscriber(subscriber: ActorRef): SubscriberRegistry =
    aggregateRegistry.aggregateId(subscriber) match {
      case Some(aggregateId) => copy(aggregateRegistry = aggregateRegistry.remove(subscriber, aggregateId))
      case None              => copy(defaultRegistry = defaultRegistry - subscriber)
    }

  def pushReplicateSuccess(events: Seq[DurableEvent]): Unit = {
    events.foreach { event =>
      val written = Written(event)
      // in any case, notify all default subscribers
      defaultRegistry.foreach(_ ! written)
      // notify subscribers with matching aggregate id
      for {
        aggregateId <- event.destinationAggregateIds
        aggregate <- aggregateRegistry(aggregateId)
      } aggregate ! written
    }
  }

  def pushWriteSuccess(events: Seq[DurableEvent], initiator: ActorRef, requestor: ActorRef, instanceId: Int): Unit = {
    events.foreach { event =>
      requestor.tell(WriteSuccess(event, instanceId), initiator)
      val written = Written(event)
      // in any case, notify all default subscribers (except requestor)
      defaultRegistry.foreach(r => if (r != requestor) r ! written)
      // notify subscribers with matching aggregate id (except requestor)
      for {
        aggregateId <- event.destinationAggregateIds
        aggregate <- aggregateRegistry(aggregateId) if aggregate != requestor
      } aggregate ! written
    }
  }

  def pushWriteFailure(events: Seq[DurableEvent], initiator: ActorRef, requestor: ActorRef, instanceId: Int, cause: Throwable): Unit =
    events.foreach { event =>
      requestor.tell(WriteFailure(event, cause, instanceId), initiator)
    }
}

private case class AggregateRegistry(
  aggregateRegistry: Map[String, Set[ActorRef]] = Map.empty,
  aggregateRegistryIndex: Map[ActorRef, String] = Map.empty) {

  def apply(aggregateId: String): Set[ActorRef] =
    aggregateRegistry.getOrElse(aggregateId, Set.empty)

  def aggregateId(aggregate: ActorRef): Option[String] =
    aggregateRegistryIndex.get(aggregate)

  def add(aggregate: ActorRef, aggregateId: String): AggregateRegistry = {
    val aggregates = aggregateRegistry.get(aggregateId) match {
      case Some(as) => as + aggregate
      case None     => Set(aggregate)
    }
    copy(
      aggregateRegistry + (aggregateId -> aggregates),
      aggregateRegistryIndex + (aggregate -> aggregateId))
  }

  def remove(aggregate: ActorRef, aggregateId: String): AggregateRegistry = {
    val aggregates = aggregateRegistry.get(aggregateId) match {
      case Some(as) => as - aggregate
      case None     => Set(aggregate)
    }
    copy(
      aggregateRegistry + (aggregateId -> aggregates),
      aggregateRegistryIndex - aggregate)
  }
}
