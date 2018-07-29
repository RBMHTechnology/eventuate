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
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.CausalRedundancy
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.Operation
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.Redundancy
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.Redundancy_
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.SimpleCRDT

import scala.concurrent.Future

object MVRegisterService {

  def zero(): SimpleCRDT = MVRegisterServiceOps.zero

  implicit def MVRegisterServiceOps[A] = new CvRDTPureOpSimple[Set[A]] {

    override protected def customEval(ops: Seq[Versioned[Operation]]): Set[A] = ops.map(_.value.asInstanceOf[AssignOp].value.asInstanceOf[A]).toSet

    val r: Redundancy = (op, _) => op.value.isInstanceOf[ClearOp.type]

    val r0: Redundancy_ = op1 => op2 => op2.vectorTimestamp < op1.vectorTimestamp

    override implicit val causalRedundancy: CausalRedundancy = new CausalRedundancy(r, r0)

    override val optimizedUpdateState: PartialFunction[(Operation, Seq[Operation]), Seq[Operation]] = {
      case (ClearOp, _) => Seq.empty
      case (_, state)   => state
    }

  }

}

/**
 * Replicated [[MVRegisterService]] CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log Event log.
 * @tparam A [[MVRegisterService]] value type.
 */
class MVRegisterService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem)
  extends CRDTService[SimpleCRDT, Set[A]] {

  val ops = MVRegisterService.MVRegisterServiceOps[A]

  /**
   * Assigns a `value` to the MV-Register identified by `id` and returns the updated MV-Register value.
   */
  def assign(id: String, value: A): Future[Set[A]] =
    op(id, AssignOp(value))

  def clear(id: String): Future[Set[A]] =
    op(id, ClearOp)

  start()
}

/**
 * Persistent assign operation used for MVRegister and LWWRegister.
 */
case class AssignOp(value: Any) extends CRDTFormat
