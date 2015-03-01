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

case class AggregateRegistry(
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
