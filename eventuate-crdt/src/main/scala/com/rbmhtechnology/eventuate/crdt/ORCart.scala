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

import akka.actor._

import com.rbmhtechnology.eventuate._

import scala.collection.immutable.Set
import scala.concurrent.Future

/**
 * [[ORCart]] entry.
 *
 * @param key Entry key. Used to identify a product in the shopping cart.
 * @param quantity Entry quantity.
 * @tparam A Key type.
 */
case class ORCartEntry[A](key: A, quantity: Int) extends CRDTFormat

/**
 * Operation-based OR-Cart CRDT with an [[ORSet]]-based implementation. [[Versioned]] entry values are of
 * type [[ORCartEntry]] and are uniquely identified with vector timestamps.
 *
 * @param orSet Backing [[ORSet]].
 * @tparam A Key type.
 *
 * @see [[http://hal.upmc.fr/docs/00/55/55/88/PDF/techreport.pdf A comprehensive study of Convergent and Commutative Replicated Data Types]], specification 21.
 * @see [[ORCartEntry]]
 */
case class ORCart[A](orSet: ORSet[ORCartEntry[A]] = ORSet[ORCartEntry[A]]()) extends CRDTFormat {
  /**
   * Returns the `quantity`s of the contained `key`s, reducing multiple [[ORCartEntry]]s with the same `key`
   * by adding their `quantity`s.
   */
  def value: Map[A, Int] = {
    orSet.versionedEntries.foldLeft(Map.empty[A, Int]) {
      case (acc, Versioned(ORCartEntry(key, quantity), _, _, _)) => acc.get(key) match {
        case Some(c) => acc + (key -> (c + quantity))
        case None    => acc + (key -> quantity)
      }
    }
  }

  /**
   * Adds the given `quantity` of `key`, uniquely identified by `timestamp`, and returns an updated `ORCart`.
   */
  def add(key: A, quantity: Int, timestamp: VectorTime): ORCart[A] =
    copy(orSet = orSet.add(ORCartEntry(key, quantity), timestamp))

  /**
   * Collects all timestamps of given `key`.
   */
  def prepareRemove(key: A): Set[VectorTime] =
    orSet.versionedEntries.collect { case Versioned(ORCartEntry(`key`, _), timestamp, _, _) => timestamp }

  /**
   * Removes all [[ORCartEntry]]s identified by given `timestamps` and returns an updated `ORCart`.
   */
  def remove(timestamps: Set[VectorTime]): ORCart[A] =
    copy(orSet = orSet.remove(timestamps))
}

object ORCart {
  def apply[A]: ORCart[A] =
    new ORCart[A]()

  implicit def ORCartServiceOps[A] = new CRDTServiceOps[ORCart[A], Map[A, Int]] {
    override def zero: ORCart[A] =
      ORCart.apply[A]

    override def value(crdt: ORCart[A]): Map[A, Int] =
      crdt.value

    override def prepare(crdt: ORCart[A], operation: Any): Option[Any] = operation match {
      case op @ RemoveOp(key, _) => crdt.prepareRemove(key.asInstanceOf[A]) match {
        case timestamps if timestamps.nonEmpty =>
          Some(op.copy(timestamps = timestamps))
        case _ =>
          None
      }
      case op =>
        super.prepare(crdt, op)
    }

    override def update(crdt: ORCart[A], operation: Any, event: DurableEvent): ORCart[A] = operation match {
      case RemoveOp(_, timestamps) =>
        crdt.remove(timestamps)
      case AddOp(ORCartEntry(key, quantity)) =>
        crdt.add(key.asInstanceOf[A], quantity, event.vectorTimestamp)
    }
  }
}

//#or-cart-service
/**
 * Replicated [[ORCart]] CRDT service.
 *
 *  - For adding a new `key` of given `quantity` a client should call `add`.
 *  - For incrementing the `quantity` of an existing `key` a client should call `add`.
 *  - For decrementing the `quantity` of an existing `key` a client should call `remove`, followed by `add`
 *    (after `remove` successfully completed).
 *  - For removing a `key` a client should call `remove`.
 *
 * @param serviceId Unique id of this service.
 * @param log Event log.
 * @tparam A [[ORCart]] key type.
 */
class ORCartService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTServiceOps[ORCart[A], Map[A, Int]])
  extends CRDTService[ORCart[A], Map[A, Int]] {

  /**
   * Adds the given `quantity` of `key` to the OR-Cart identified by `id` and returns the updated OR-Cart content.
   */
  def add(id: String, key: A, quantity: Int): Future[Map[A, Int]] =
    if (quantity > 0) op(id, AddOp(ORCartEntry(key, quantity))) else Future.failed(new IllegalArgumentException("quantity must be positive"))

  /**
   * Removes the given `key` from the OR-Cart identified by `id` and returns the updated OR-Cart content.
   */
  def remove(id: String, key: A): Future[Map[A, Int]] =
    op(id, RemoveOp(key))

  start()
}
//#