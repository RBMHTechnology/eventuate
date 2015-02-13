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

package com.rbmhtechnology.example

import scala.util._

import akka.actor.ActorRef

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.VersionedObjects._

object OrderManager {
  trait OrderCommand {
    def orderId: String
    def event: OrderEvent
  }

  trait OrderEvent {
    def orderId: String
  }

  case class CreateOrder(orderId: String) extends OrderCommand { val event = OrderCreated(orderId) }
  case class CancelOrder(orderId: String) extends OrderCommand { val event = OrderCancelled(orderId) }
  case class AddOrderItem(orderId: String, item: String) extends OrderCommand  { val event = OrderItemAdded(orderId, item) }
  case class RemoveOrderItem(orderId: String, item: String) extends OrderCommand { val event = OrderItemRemoved(orderId, item) }

  case class OrderCreated(orderId: String, creator: String = "") extends OrderEvent
  case class OrderItemAdded(orderId: String, item: String) extends OrderEvent
  case class OrderItemRemoved(orderId: String, item: String) extends OrderEvent
  case class OrderCancelled(orderId: String) extends OrderEvent

  case class CommandSuccess(orderId: String)
  case class CommandFailure(orderId: String, cause: Throwable)

  case object GetState
  case class GetStateSuccess(state: Map[String, Seq[Versioned[Order]]])

  implicit object OrderDomainCmd extends VersionedObjects.DomainCmd[OrderCommand] {
    override def id(cmd: OrderCommand): String = cmd.orderId
    override def origin(cmd: OrderCommand): String = ""
  }

  implicit object OrderDomainEvt extends VersionedObjects.DomainEvt[OrderEvent] {
    override def id(evt: OrderEvent): String = evt.orderId
    override def origin(evt: OrderEvent): String = evt match {
      case OrderCreated(_, creator) => creator
      case _ => ""
    }
  }
}

class OrderManager(id: String, val eventLog: ActorRef) extends EventsourcedActor {
  import OrderManager._

  val processId = id

  private val orders: VersionedObjects[Order, OrderCommand, OrderEvent] =
    new VersionedObjects(commandValidation, eventProjection)

  override val onCommand: Receive = {
    case c: CreateOrder =>
      processValidationResult(c.orderId, orders.validateCreate(c))
    case c: OrderCommand =>
      processValidationResult(c.orderId, orders.validateUpdate(c))
    case c: Resolve =>
      processValidationResult(c.id, orders.validateResolve(c.withOrigin(processId)))
    case GetState =>
      sender() ! GetStateSuccess(orders.current.mapValues(_.all))
  }

  override val onEvent: Receive = {
    case e: OrderCreated =>
      orders.handleCreated(e, lastVectorTimestamp, lastSequenceNr)
      if (!recovering) printOrder(orders.versions(e.orderId))
    case e: OrderEvent =>
      orders.handleUpdated(e, lastVectorTimestamp, lastSequenceNr)
      if (!recovering) printOrder(orders.versions(e.orderId))
    case e: Resolved =>
      orders.handleResolved(e, lastVectorTimestamp, lastSequenceNr)
      if (!recovering) printOrder(orders.versions(e.id))
  }

  private def commandValidation: (Order, OrderCommand) => Try[OrderEvent] = {
    case (_, c: CreateOrder) => Success(c.event.copy(creator = processId))
    case (_, c: OrderCommand) => Success(c.event)
  }

  private def eventProjection: (Order, OrderEvent) => Order = {
    case (_    , OrderCreated(id, _)) => Order(id)
    case (order, OrderCancelled(_)) => order.cancel
    case (order, OrderItemAdded(_, item)) => order.addItem(item)
    case (order, OrderItemRemoved(_, item)) => order.removeItem(item)
  }

  private def processValidationResult(orderId: String, result: Try[Any]): Unit = result match {
    case Failure(err) =>
      sender() ! CommandFailure(orderId, err)
    case Success(evt) => persist(evt) {
      case Success(e) =>
        onEvent(e)
        sender() ! CommandSuccess(orderId)
      case Failure(e) =>
        sender() ! CommandFailure(orderId, e)
    }
  }
}

