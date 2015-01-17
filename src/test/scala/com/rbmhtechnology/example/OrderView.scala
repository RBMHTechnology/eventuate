/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
 */

package com.rbmhtechnology.example

import akka.actor.ActorRef

import com.rbmhtechnology.eventuate.EventsourcedView

object OrderView {
  case class GetUpdateCount(orderId: String)
  case class GetUpdateCountSuccess(orderId: String, count: Int)
}

class OrderView(id: String, val eventLog: ActorRef) extends EventsourcedView {
  import OrderManager._
  import OrderView._

  val processId: String = id
  var updateCounts: Map[String, Int] = Map.empty

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
