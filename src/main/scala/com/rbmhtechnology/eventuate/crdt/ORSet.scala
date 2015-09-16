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
 * Replicated OR-Set. In case of a concurrent `add` and `remove`, `add` has precedence.
 *
 * @param versionedEntries [[Versioned]] entries.
 * @tparam A Entry value type.
 *
 * @see [[http://hal.upmc.fr/docs/00/55/55/88/PDF/techreport.pdf A comprehensive study of Convergent and Commutative Replicated Data Types]]
 */
case class ORSet[A](versionedEntries: Set[Versioned[A]] = Set.empty[Versioned[A]]) {
  /**
   * Returns all entries, masking duplicates of different version.
   */
  def value: Set[A] =
    versionedEntries.map(_.value)

  /**
   * Adds a [[Versioned]] entry from `entry` and `timestamp` and returns an updated OR-Set.
   */
  def add(entry: A, timestamp: VectorTime): ORSet[A] =
    copy(versionedEntries = versionedEntries + Versioned(entry, timestamp))

  /**
   * Collects all timestamps of given `entry`.
   */
  def prepareRemove(entry: A): Set[VectorTime] =
    versionedEntries.collect { case Versioned(`entry`, timestamp, _) => timestamp }

  /**
   * Removes [[Versioned]] entries matching `entry` and `timestamps` and returns an updated OR-Set.
   */
  def remove(entry: A, timestamps: Set[VectorTime]): ORSet[A] =
    copy(versionedEntries = versionedEntries -- timestamps.map(t => Versioned(entry, t)))
}

object ORSet {
  def apply[A]: ORSet[A] =
    new ORSet[A]()

  implicit def ORSetServiceOps[A] = new CRDTServiceOps[ORSet[A], Set[A]] {
    override def zero: ORSet[A] =
      ORSet.apply[A]

    override def value(crdt: ORSet[A]): Set[A] =
      crdt.value

    override def prepare(crdt: ORSet[A], operation: Any): Option[Any] = operation match {
      case op @ RemoveOp(entry, _) => crdt.prepareRemove(entry.asInstanceOf[A]) match {
        case timestamps if timestamps.nonEmpty =>
          Some(op.copy(timestamps = timestamps))
        case _ =>
          None
      }
      case op =>
        super.prepare(crdt, op)
    }

    override def update(crdt: ORSet[A], operation: Any, event: DurableEvent): ORSet[A] = operation match {
      case RemoveOp(entry, timestamps) =>
        crdt.remove(entry.asInstanceOf[A], timestamps)
      case AddOp(entry) =>
        crdt.add(entry.asInstanceOf[A], event.vectorTimestamp)
    }
  }
}

//#or-set-service
/**
 * Replicated [[ORSet]] CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log Event log
 * @tparam A [[ORSet]] entry type
 */
class ORSetService[A](val serviceId: String, val log: ActorRef)(implicit system: ActorSystem, val ops: CRDTServiceOps[ORSet[A], Set[A]])
  extends CRDTService[ORSet[A], Set[A]] {

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

  start()
}

/** Persistent add operation */
private[eventuate] case class AddOp(entry: Any)

/** Persistent remove operation */
private[eventuate] case class RemoveOp(entry: Any, timestamps: Set[VectorTime] = Set.empty)
//#