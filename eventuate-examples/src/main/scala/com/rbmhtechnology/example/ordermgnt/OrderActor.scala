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

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.VersionedAggregate._

import scala.util._

object OrderActor {
  trait OrderCommand {
    def orderId: String
    def event: OrderEvent
  }

  trait OrderEvent {
    def orderId: String
  }

  // Order commands
  case class CreateOrder(orderId: String) extends OrderCommand { val event = OrderCreated(orderId) }
  case class CancelOrder(orderId: String) extends OrderCommand { val event = OrderCancelled(orderId) }
  case class AddOrderItem(orderId: String, item: String) extends OrderCommand { val event = OrderItemAdded(orderId, item) }
  case class RemoveOrderItem(orderId: String, item: String) extends OrderCommand { val event = OrderItemRemoved(orderId, item) }

  // Order events
  case class OrderCreated(orderId: String, creator: String = "") extends OrderEvent
  case class OrderItemAdded(orderId: String, item: String) extends OrderEvent
  case class OrderItemRemoved(orderId: String, item: String) extends OrderEvent
  case class OrderCancelled(orderId: String) extends OrderEvent

  // Order queries + replies
  case object GetState
  case class GetStateSuccess(state: Map[String, Seq[Versioned[Order]]])
  case class GetStateFailure(cause: Throwable)

  // General replies
  case class CommandSuccess(orderId: String)
  case class CommandFailure(orderId: String, cause: Throwable)

  // Snapshot command and replies
  case class SaveSnapshot(orderId: String)
  case class SaveSnapshotSuccess(orderId: String, metadata: SnapshotMetadata)
  case class SaveSnapshotFailure(orderId: String, cause: Throwable)

  implicit object OrderDomainCmd extends DomainCmd[OrderCommand] {
    override def origin(cmd: OrderCommand): String = ""
  }

  implicit object OrderDomainEvt extends DomainEvt[OrderEvent] {
    override def origin(evt: OrderEvent): String = evt match {
      case OrderCreated(_, creator) => creator
      case _                        => ""
    }
  }
}

/**
 * An event-sourced actor that manages a single order aggregate, identified by `orderId`.
 */
class OrderActor(orderId: String, replicaId: String, val eventLog: ActorRef) extends EventsourcedActor {
  import OrderActor._

  override val id = s"s-${orderId}-${replicaId}"
  override val aggregateId = Some(orderId)

  private var order = VersionedAggregate(orderId, commandValidation, eventProjection)

  override def onCommand = {
    case c: CreateOrder =>
      processValidationResult(c.orderId, order.validateCreate(c))
    case c: OrderCommand =>
      processValidationResult(c.orderId, order.validateUpdate(c))
    case c: Resolve =>
      processValidationResult(c.id, order.validateResolve(c.selected, replicaId))
    case GetState =>
      val reply = order.aggregate match {
        case Some(aggregate) => GetStateSuccess(Map(orderId -> aggregate.all))
        case None            => GetStateSuccess(Map.empty)
      }
      sender() ! reply
    case c: SaveSnapshot => order.aggregate match {
      case None =>
        sender() ! SaveSnapshotFailure(orderId, new AggregateDoesNotExistException(orderId))
      case Some(aggregate) =>
        save(aggregate) {
          case Success(m) => sender() ! SaveSnapshotSuccess(orderId, m)
          case Failure(e) => sender() ! SaveSnapshotFailure(orderId, e)
        }
    }
  }

  override def onEvent = {
    case e: OrderCreated =>
      order = order.handleCreated(e, lastVectorTimestamp, lastSequenceNr)
      if (!recovering) printOrder(order.versions)
    case e: OrderEvent =>
      order = order.handleUpdated(e, lastVectorTimestamp, lastSequenceNr)
      if (!recovering) printOrder(order.versions)
    case e: Resolved =>
      order = order.handleResolved(e, lastVectorTimestamp, lastSequenceNr)
      if (!recovering) printOrder(order.versions)
  }

  override def onSnapshot = {
    case aggregate: ConcurrentVersionsTree[Order, OrderEvent] =>
      order = order.withAggregate(aggregate.withProjection(eventProjection))
      println(s"[$orderId] Snapshot loaded:")
      printOrder(order.versions)
  }

  override def onRecovery = {
    case Failure(e) =>
    case Success(_) =>
      println(s"[$orderId] Recovery succeeded:")
      printOrder(order.versions)
  }

  private def processValidationResult(orderId: String, result: Try[Any]): Unit = result match {
    case Failure(err) =>
      sender() ! CommandFailure(orderId, err)
    case Success(evt) => persist(evt) {
      case Success(e) => sender() ! CommandSuccess(orderId)
      case Failure(e) => sender() ! CommandFailure(orderId, e)
    }
  }

  private def commandValidation: (Order, OrderCommand) => Try[OrderEvent] = {
    case (_, c: CreateOrder)  => Success(c.event.copy(creator = replicaId))
    case (_, c: OrderCommand) => Success(c.event)
  }

  private def eventProjection: (Order, OrderEvent) => Order = {
    case (_, OrderCreated(`orderId`, _))            => Order(orderId)
    case (order, OrderCancelled(`orderId`))         => order.cancel
    case (order, OrderItemAdded(`orderId`, item))   => order.addItem(item)
    case (order, OrderItemRemoved(`orderId`, item)) => order.removeItem(item)
  }
}
