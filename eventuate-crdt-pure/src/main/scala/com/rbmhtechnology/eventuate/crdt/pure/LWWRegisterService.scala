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

import akka.actor.ActorRef
import akka.actor.ActorSystem
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.CausalRedundancy
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.Operation
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.Redundancy
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.Redundancy_
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.SimpleCRDT

import scala.concurrent.Future

object LWWRegisterService {

  def zero(): SimpleCRDT = LWWRegisterServiceOps.zero

  implicit def LWWOrdering[A] = new Ordering[Versioned[_]] {
    override def compare(x: Versioned[_], y: Versioned[_]): Int =
      if (x.systemTimestamp == y.systemTimestamp)
        x.creator.compareTo(y.creator)
      else
        x.systemTimestamp.compareTo(y.systemTimestamp)
  }

  implicit def LWWRegisterServiceOps[A] = new CvRDTPureOpSimple[Option[A]] {

    override def customEval(ops: Seq[Versioned[Operation]]): Option[A] =
      ops.sorted(LWWRegisterService.LWWOrdering[A]).lastOption.map(_.value.asInstanceOf[AssignOp].value.asInstanceOf[A])

    val r: Redundancy = (op, _) => op.value equals ClearOp

    val r0: Redundancy_ = newOp => op => op.vectorTimestamp < newOp.vectorTimestamp

    override implicit val causalRedundancy: CausalRedundancy = new CausalRedundancy(r, r0)

  }

}

/**
 * Replicated LWWRegister CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log Event log.
 * @tparam A LWWRegister value type.
 */
class LWWRegisterService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem)
  extends CRDTService[SimpleCRDT, Option[A]] {

  val ops = LWWRegisterService.LWWRegisterServiceOps[A]

  /**
   * Assigns a `value` to the LWW-Register identified by `id` and returns the updated LWW-Register value.
   */
  def assign(id: String, value: A): Future[Option[A]] =
    op(id, AssignOp(value))

  def clear(id: String): Future[Option[A]] =
    op(id, ClearOp)

  start()
}
