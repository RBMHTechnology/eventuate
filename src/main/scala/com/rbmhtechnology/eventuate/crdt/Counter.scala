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

package com.rbmhtechnology.eventuate.crdt

import akka.actor._

import com.rbmhtechnology.eventuate.DurableEvent

import scala.concurrent.Future

/**
 * Replicated counter.
 *
 * @param value Current counter value.
 * @tparam A Counter value type.
 *
 * @see [[http://hal.upmc.fr/docs/00/55/55/88/PDF/techreport.pdf A comprehensive study of Convergent and Commutative Replicated Data Types]]
 */
case class Counter[A : Integral](value: A) {
  /**
   * Adds `delta` (which can also be negative) to the counter `value` and
   * returns an updated counter.
   */
  def update(delta: A): Counter[A] =
    copy(value = implicitly[Integral[A]].plus(value, delta))
}

object Counter {
  def apply[A : Integral]: Counter[A] =
    Counter[A](implicitly[Integral[A]].zero)

  implicit def CounterServiceOps[A : Integral] = new CRDTServiceOps[Counter[A], A] {
    override def zero: Counter[A] =
      Counter.apply[A]

    override def value(crdt: Counter[A]): A =
      crdt.value

    override def precondition: Boolean =
      false

    override def update(crdt: Counter[A], operation: Any, event: DurableEvent): Counter[A] = operation match {
      case UpdateOp(delta) => crdt.update(delta.asInstanceOf[A])
    }
  }
}

/**
 * Replicated [[Counter]] CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log Event log.
 * @tparam A Counter value type.
 */
class CounterService[A](val serviceId: String, val log: ActorRef)(implicit system: ActorSystem, integral: Integral[A], val ops: CRDTServiceOps[Counter[A], A])
  extends CRDTService[Counter[A], A] {

  /**
   * Adds `delta` (which can also be negative) to the counter identified by `id` and returns the updated counter value.
   */
  def update(id: String, delta: A): Future[A] =
    op(id, UpdateOp(delta))

  start()
}

private case class UpdateOp(delta: Any)
