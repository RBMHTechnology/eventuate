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

package com.rbmhtechnology.eventuate.crdt

import akka.actor.ActorRef
import akka.actor.ActorSystem

import com.rbmhtechnology.eventuate._

import scala.concurrent.Future

/**
 * Operation-based LWW-Register CRDT with an [[MVRegister]]-based implementation. Instead of returning multiple
 * values in case of concurrent assignments, the last written value is returned. The last written value is
 * determined by comparing the following [[Versioned]] fields in given order:
 *
 *  - `vectorTimestamp`: if causally related, return the value with the higher timestamp, otherwise compare
 *  - `systemTimestamp`: if not equal, return the value with the higher timestamp, otherwise compare
 *  - `emitterId`
 *
 * Note that this relies on synchronized system clocks. [[LWWRegister]] should only be used when the choice of
 * value is not important for concurrent updates occurring within the clock skew.
 *
 * @param mvRegister Initially empty [[MVRegister]].
 *
 * @see [[http://hal.upmc.fr/docs/00/55/55/88/PDF/techreport.pdf A comprehensive study of Convergent and Commutative Replicated Data Types]], specification 9
 */
case class LWWRegister[A](mvRegister: MVRegister[A] = MVRegister.apply[A]) extends CRDTFormat {
  def value: Option[A] = {
    mvRegister.versioned.toVector.sorted(LWWRegister.LWWOrdering[A]).lastOption.map(_.value)

  }

  /**
   * Assigns a [[Versioned]] value from `v` and `vectorTimestamp` and returns an updated MV-Register.
   *
   * @param v assigned value.
   * @param vectorTimestamp vector timestamp of the assigned value.
   * @param systemTimestamp system timestamp of the assigned value.
   * @param emitterId id of the value emitter.
   */
  def set(v: A, vectorTimestamp: VectorTime, systemTimestamp: Long, emitterId: String): LWWRegister[A] = {
    copy(mvRegister.set(v, vectorTimestamp, systemTimestamp, emitterId))
  }
}

object LWWRegister {
  def apply[A]: LWWRegister[A] =
    new LWWRegister[A]()

  implicit def LWWRegisterServiceOps[A] = new CRDTServiceOps[LWWRegister[A], Option[A]] {
    override def zero: LWWRegister[A] =
      LWWRegister.apply[A]()

    override def value(crdt: LWWRegister[A]): Option[A] =
      crdt.value

    override def precondition: Boolean =
      false

    override def update(crdt: LWWRegister[A], operation: Any, event: DurableEvent): LWWRegister[A] = operation match {
      case SetOp(value) => crdt.set(value.asInstanceOf[A], event.vectorTimestamp, event.systemTimestamp, event.emitterId)
    }
  }

  implicit def LWWOrdering[A] = new Ordering[Versioned[A]] {
    override def compare(x: Versioned[A], y: Versioned[A]): Int =
      if (x.systemTimestamp == y.systemTimestamp)
        x.creator.compareTo(y.creator)
      else
        x.systemTimestamp.compareTo(y.systemTimestamp)
  }
}

/**
 * Replicated [[LWWRegister]] CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log Event log.
 * @tparam A [[LWWRegister]] value type.
 */
class LWWRegisterService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTServiceOps[LWWRegister[A], Option[A]])
  extends CRDTService[LWWRegister[A], Option[A]] {

  /**
   * Assigns a `value` to the LWW-Register identified by `id` and returns the updated LWW-Register value.
   */
  def set(id: String, value: A): Future[Option[A]] =
    op(id, SetOp(value))

  start()
}
