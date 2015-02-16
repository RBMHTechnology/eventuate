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

case class MVRegister[A](versionedValues: Set[Versioned[A]] = Set.empty[Versioned[A]]) {
  def value: Set[A] =
    versionedValues.map(_.value)

  def set(v: A, timestamp: VectorTime): MVRegister[A] = {
    val (vvs, updated) = versionedValues.foldLeft((Set.empty[Versioned[A]], false)) {
      case ((acc, updated), vv) if timestamp > vv.version =>
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

    override def update(crdt: MVRegister[A], operation: Any, vectorTimestamp: VectorTime, systemTimestamp: Long): MVRegister[A] = operation match {
      case SetOp(value) => crdt.set(value.asInstanceOf[A], vectorTimestamp)
    }
  }
}

/**
 * Replicated [[MVRegister]] CRDT service.
 *
 * @param processId unique process id of this service replica.
 * @param log event log
 * @tparam A [[MVRegister]] value type
 */
class MVRegisterService[A](val processId: String, val log: ActorRef)(implicit system: ActorSystem, val ops: CRDTServiceOps[MVRegister[A], Set[A]])
  extends CRDTService[MVRegister[A], Set[A]] {

  /**
   * Updates the MV-Register identified by `id` with specified `value` and returns the updated MV-Register value.
   */
  def set(id: String, value: A): Future[Set[A]] =
    op(id, SetOp(value))

  start()
}

private case class SetOp(value: Any)
