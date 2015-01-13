/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
 */

package com.rbmhtechnology.eventuate

import scala.collection.immutable.SortedMap

import akka.actor._

object Delivery {
  case class DeliveryAttempt(message: Any, destination: ActorPath)
}

trait Delivery extends Eventsourced { this: EventsourcedActor =>
  import Delivery._

  private var _unconfirmed: SortedMap[String, DeliveryAttempt] = SortedMap.empty

  def deliver(deliveryId: String, message: Any, destination: ActorPath): Unit = {
    _unconfirmed = _unconfirmed + (deliveryId -> DeliveryAttempt(message, destination))
    if (!recovering) send(message, destination)
  }

  def redeliverUnconfirmed(): Unit = _unconfirmed.foreach {
    case (_, DeliveryAttempt(m, d)) => send(m, d)
  }

  def confirm(deliveryId: String): Unit =
    _unconfirmed = _unconfirmed - deliveryId

  def unconfirmed: Set[String] =
    _unconfirmed.keySet

  override def onRecoverySuccess(): Unit = {
    super.onRecoverySuccess()
    redeliverUnconfirmed()
  }

  private def send(message: Any, destination: ActorPath): Unit =
    context.actorSelection(destination) ! message
}
