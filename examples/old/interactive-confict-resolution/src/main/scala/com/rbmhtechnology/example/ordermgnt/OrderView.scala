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

package com.rbmhtechnology.example.ordermgnt

import akka.actor.ActorRef

import com.rbmhtechnology.eventuate.EventsourcedView

object OrderView {
  case class GetUpdateCount(orderId: String)
  case class GetUpdateCountSuccess(orderId: String, count: Int)
}

/**
 * Consumes events written by all event-sourced [[OrderActor]]s.
 */
class OrderView(replicaId: String, val eventLog: ActorRef) extends EventsourcedView {
  import OrderActor._
  import OrderView._

  var updateCounts: Map[String, Int] = Map.empty

  override val id = s"s-ov-$replicaId"

  override def onCommand = {
    case GetUpdateCount(orderId) => sender() ! GetUpdateCountSuccess(orderId, updateCounts.getOrElse(orderId, 0))
  }

  override def onEvent = {
    case oe: OrderEvent => updateCounts.get(oe.orderId) match {
      case Some(count) => updateCounts += (oe.orderId -> (count + 1))
      case None        => updateCounts += (oe.orderId -> 1)
    }
  }
}
