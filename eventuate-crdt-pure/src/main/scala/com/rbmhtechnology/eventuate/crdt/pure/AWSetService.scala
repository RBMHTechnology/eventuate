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
import com.rbmhtechnology.eventuate.crdt.pure.AWSetService.AWSet
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes._

import scala.collection.immutable.Set
import scala.concurrent.Future

object AWSetService {

  type AWSet[A] = CRDT[Set[A]]

  def zero[A]: AWSet[A] = CRDT(Set.empty)

  implicit def AWSetServiceOps[A] = new CvRDTPureOp[Set[A], Set[A]] {

    val r: Redundancy = (v, _) => v.value match {
      case _: RemoveOp => true
      case ClearOp     => true
      case _           => false
    }

    val r0: Redundancy_ = newOp => op => {
      ((op.vectorTimestamp, op.value), (newOp.vectorTimestamp, newOp.value)) match {
        case ((t1, AddOp(v1)), (t2, AddOp(v2)))    => (t1 < t2) && (v1 equals v2)
        case ((t1, AddOp(v1)), (t2, RemoveOp(v2))) => (t1 < t2) && (v1 equals v2)
        case ((t1, AddOp(_)), (t2, ClearOp))       => t1 < t2
      }
    }

    override implicit val causalRedundancy: CausalRedundancy = new CausalRedundancy(r, r0)

    override def eval(crdt: AWSet[A]): Set[A] =
      crdt.polog.log.map(_.value.asInstanceOf[AddOp].entry.asInstanceOf[A]) ++ crdt.state

    override protected def stabilizeState(state: Set[A], stableOps: Seq[Operation]): Set[A] =
      state ++ stableOps.map(_.asInstanceOf[AddOp].entry.asInstanceOf[A]).toSet

    override def zero: AWSet[A] = AWSetService.zero[A]

    override def updateState(op: Operation, redundant: Boolean, state: Set[A]): Set[A] = op match {
      case RemoveOp(entry) => state - entry.asInstanceOf[A]
      case ClearOp         => Set.empty
      case _               => state
    }
  }
}

//#or-set-service
/**
 * Replicated [[AWSet]] CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log       Event log.
 * @tparam A [[AWSet]] entry type.
 */
class AWSetService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem)
  extends CRDTService[AWSet[A], Set[A]] {

  val ops = AWSetService.AWSetServiceOps[A]

  /**
   * Adds `entry` to the OR-Set identified by `id` and returns the updated entry set.
   */
  def add(id: String, entry: A): Future[Set[A]] =
    op(id, AddOp(entry))

  /**
   * Removes `entry` from the OR-Set identified by `id` and returns the updated entry set.
   */
  def remove(id: String, entry: A): Future[Set[A]] =
    op(id, RemoveOp(entry))

  def clear(id: String): Future[Set[A]] =
    op(id, ClearOp)

  start()

}

/**
 * Persistent add operation used for [[AWSet]] and AWCart.
 */
case class AddOp(entry: Any) extends CRDTFormat

/**
 * Persistent remove operation used for [[AWSet]] and AWCart.
 */
case class RemoveOp(entry: Any) extends CRDTFormat

case object ClearOp extends CRDTFormat