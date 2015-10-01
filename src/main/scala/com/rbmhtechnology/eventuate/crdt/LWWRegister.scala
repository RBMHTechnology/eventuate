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

import akka.actor.ActorRef
import akka.actor.ActorSystem
import com.rbmhtechnology.eventuate.{Versioned, DurableEvent, VectorTime}

import scala.concurrent.Future

/**
 * A LWWVersioned value.
 *
 * @param value The value.
 * @param vectorTimestamp Timestamp of the event that caused this version.
 * @param systemTimestamp Wall Clock
 * @param emitterId Creator of the event that caused this version.
 */
case class LWWVersioned[A](value: A, vectorTimestamp: VectorTime, systemTimestamp: Long = 0L, emitterId: String = "")

/**
 * Replicated Last Writer Wins Register. Stores a single value and timestamp at which it was set. The value is updated
 * only if the vector clock is later, if the vector clocks are concurrent the register compares the system time,
 * if the values are still concurrent the source with the greatest emitterId wins
 * Note that this relies on synchronized system clocks. [[LWWRegister]] should only be used when the choice of
 * value is not important for concurrent updates occurring within the clock skew.
 *
 * @param versionedValue Initially empty.
 *
 * @see [[http://hal.upmc.fr/docs/00/55/55/88/PDF/techreport.pdf A comprehensive study of Convergent and Commutative Replicated Data Types]]
 */
case class LWWRegister[A](versionedValue: Option[LWWVersioned[A]] = None) {

  def value: Option[A] =
    versionedValue.map(_.value)

  def set(v: A, vt : VectorTime, st : Long, emitterId : String): LWWRegister[A] = {
    val nvv = LWWVersioned(v, vt, st, emitterId)
    val result = versionedValue match {
      case None =>
        Some(nvv)
      case Some(vv) if nvv.vectorTimestamp > vv.vectorTimestamp =>
        Some(nvv)
      case Some(vv) if nvv.systemTimestamp > vv.systemTimestamp =>
        Some(nvv)
      case Some(vv) if nvv.systemTimestamp == vv.systemTimestamp && nvv.emitterId > vv.emitterId =>
        Some(nvv)
      case _ =>
        versionedValue
    }
    copy(result)
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
      case UpdateOp(value) => crdt.set(value.asInstanceOf[A], event.vectorTimestamp, event.systemTimestamp, event.emitterId)
    }
  }
}

/**
 * Replicated [[LWWRegister]] CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log Event log.
 * @tparam A LWWRegister value type.
 */
class LWWRegisterService[A](val serviceId: String, val log: ActorRef)(implicit system: ActorSystem, val ops: CRDTServiceOps[LWWRegister[A], A])
  extends CRDTService[LWWRegister[A], A] {

  /**
   * Updates the value
   */
  def update(id: String, value: A): Future[A] =
    op(id, SetOp(value))

  start()
}
