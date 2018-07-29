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

package com.rbmhtechnology.eventuate.crdt.pure

import akka.actor._
import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.Operation

import scala.concurrent.Future

object CounterService {

  def zero[A: Integral]: A = implicitly[Integral[A]].zero

  implicit def CounterServiceOps[A: Integral] = new CRDTServiceOps[A, A] {

    override def zero: A = CounterService.zero[A]

    override def precondition: Boolean =
      false

    override def eval(crdt: A): A = crdt

    override def effect(crdt: A, op: Operation, vt: VectorTime, systemTimestamp: Long, creator: String): A = implicitly[Integral[A]].plus(crdt, op.asInstanceOf[UpdateOp].delta.asInstanceOf[A])

  }
}

/**
 * Replicated Counter CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log       Event log.
 * @tparam A Counter value type.
 */
class CounterService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, integral: Integral[A])
  extends CRDTService[A, A] {

  val ops = CounterService.CounterServiceOps[A]

  /**
   * Adds `delta` (which can also be negative) to the counter identified by `id` and returns the updated counter value.
   */
  def update(id: String, delta: A): Future[A] =
    op(id, UpdateOp(delta))

  start()
}

/**
 * Persistent update operation used for Counter.
 */
case class UpdateOp(delta: Any) extends CRDTFormat
