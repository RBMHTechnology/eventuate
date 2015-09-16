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

import com.rbmhtechnology.eventuate._

import scala.concurrent.Future

/**
 * Replicated MV-Register. Has several [[Versioned]] values assigned in case of concurrent assignments,
 * otherwise, a single [[Versioned]] value. Concurrent assignments can be reduced to a single assignment
 * by assigning a [[Versioned]] value with a timestamp that is greater than those of the currently assigned
 * [[Versioned]] values.
 *
 * @param versionedValues Assigned, versioned values. Initially empty.
 * @tparam A MV-Register value type.
 *
 * @see [[http://hal.upmc.fr/docs/00/55/55/88/PDF/techreport.pdf A comprehensive study of Convergent and Commutative Replicated Data Types]]
 */
case class MVRegister[A](versionedValues: Set[Versioned[A]] = Set.empty[Versioned[A]]) {
  def value: Set[A] =
    versionedValues.map(_.value)

  /**
   * Assigns a [[Versioned]] value from `v` and `timestamp` and returns an updated MV-Register.
   *
   * @param v assigned value.
   * @param timestamp assignment timestamp.
   */
  def set(v: A, timestamp: VectorTime): MVRegister[A] = {
    val (vvs, updated) = versionedValues.foldLeft((Set.empty[Versioned[A]], false)) {
      case ((acc, updated), vv) if timestamp > vv.updateTimestamp =>
        ((acc + vv.copy(v, timestamp)), true)
      case ((acc, updated), vv) =>
        ((acc + vv), updated)
    }
    if (!updated) copy(versionedValues + Versioned(v, timestamp)) else copy(vvs)
  }
}

object MVRegister {
  def apply[A]: MVRegister[A] =
    new MVRegister[A]()

  implicit def MVRegisterServiceOps[A] = new CRDTServiceOps[MVRegister[A], Set[A]] {
    override def zero: MVRegister[A] =
      MVRegister.apply[A]

    override def value(crdt: MVRegister[A]): Set[A] =
      crdt.value

    override def precondition: Boolean =
      false

    override def update(crdt: MVRegister[A], operation: Any, event: DurableEvent): MVRegister[A] = operation match {
      case SetOp(value) => crdt.set(value.asInstanceOf[A], event.vectorTimestamp)
    }
  }
}

/**
 * Replicated [[MVRegister]] CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log Event log.
 * @tparam A [[MVRegister]] value type.
 */
class MVRegisterService[A](val serviceId: String, val log: ActorRef)(implicit system: ActorSystem, val ops: CRDTServiceOps[MVRegister[A], Set[A]])
  extends CRDTService[MVRegister[A], Set[A]] {

  /**
   * Assigns a `value` to the MV-Register identified by `id` and returns the updated MV-Register value.
   */
  def set(id: String, value: A): Future[Set[A]] =
    op(id, SetOp(value))

  start()
}

private case class SetOp(value: Any)
