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
import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.Operation
import com.rbmhtechnology.eventuate.crdt.pure.TPSetService.TPSet

import scala.collection.immutable.Set
import scala.concurrent.Future

object TPSetService {

  type TPSet[A] = (Set[A], Set[A])

  def zero[A]: TPSet[A] = (Set.empty[A], Set.empty[A])

  implicit def TPSetServiceOps[A] = new CRDTServiceOps[TPSet[A], Set[A]] {

    override def zero: TPSet[A] = TPSetService.zero[A]

    override def eval(crdt: TPSet[A]): Set[A] = crdt._1

    override def effect(crdt: TPSet[A], op: Operation, vt: VectorTime, systemTimestamp: Long = 0L, creator: String = ""): (Set[A], Set[A]) =
      (op, crdt) match {
        case (AddOp(e: A @unchecked), (added, removed)) if !removed.contains(e) => (added + e, removed)
        case (RemoveOp(e: A @unchecked), (added, removed)) => (added - e, removed + e)
        case (_, c) => c
      }

    override def precondition: Boolean = false
  }

}

class TPSetService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem) extends CRDTService[TPSet[A], Set[A]] {

  val ops = TPSetService.TPSetServiceOps[A]

  def add(id: String, entry: A): Future[Set[A]] =
    op(id, AddOp(entry))

  def remove(id: String, entry: A): Future[Set[A]] =
    op(id, RemoveOp(entry))

  start()
}
