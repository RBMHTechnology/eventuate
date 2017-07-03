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

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import com.rbmhtechnology.eventuate.EventsourcedView
import com.rbmhtechnology.eventuate.VersionedAggregate.Resolve

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util._

/**
 * Manages event-sourced [[OrderActor]]s of a location where the location identifier is
 * `replicaId`. [[OrderActor]]s can be
 *
 *  - instantiated on-demand in the command handler
 *  - instantiated on-demand in the event handler
 *  - stopped and removed from the internal map to free memory
 *
 *  This order manager implements [[EventsourcedView]] only for the purpose to eagerly
 *  instantiate [[OrderActor]]s as soon as an [[OrderActor.OrderCreated]] event has
 *  been logged. This is usually not necessary but we do it here to see the console
 *  output of all [[OrderActor]]s at each location immediately.
 */
class OrderManager(replicaId: String, val eventLog: ActorRef) extends EventsourcedView {
  import OrderActor._
  import context.dispatcher

  private implicit val timeout = Timeout(10.seconds)
  private var orderActors: Map[String, ActorRef] = Map.empty

  override val id = s"s-om-$replicaId"

  override def onCommand = {
    case c: OrderCommand => orderActor(c.orderId) forward c
    case c: SaveSnapshot => orderActor(c.orderId) forward c
    case r: Resolve      => orderActor(r.id) forward r
    case GetState if orderActors.isEmpty =>
      sender() ! GetStateSuccess(Map.empty)
    case GetState =>
      val sdr = sender()
      val statesF = orderActors.values.map(_.ask(GetState).mapTo[GetStateSuccess].map(_.state))
      Future.sequence(statesF).map(_.reduce(_ ++ _)) onComplete {
        case Success(states) => sdr ! GetStateSuccess(states)
        case Failure(cause)  => sdr ! GetStateFailure(cause)
      }
  }

  override def onEvent = {
    // eagerly create order actor so that their console output is immediately visible
    case OrderCreated(orderId, _) if !orderActors.contains(orderId) => orderActor(orderId)
  }

  private def orderActor(orderId: String): ActorRef = orderActors.get(orderId) match {
    case Some(orderActor) => orderActor
    case None =>
      orderActors = orderActors + (orderId -> context.actorOf(Props(new OrderActor(orderId, replicaId, eventLog))))
      orderActors(orderId)
  }
}
