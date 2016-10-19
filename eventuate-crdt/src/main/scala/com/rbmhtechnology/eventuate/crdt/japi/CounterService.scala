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

package com.rbmhtechnology.eventuate.crdt.japi

import java.util.concurrent.CompletionStage

import akka.actor.{ ActorRef, ActorSystem }
import com.rbmhtechnology.eventuate.crdt.Counter
import java.lang.{ Integer => JInt, Long => JLong }

/**
 * Java API of a replicated [[Counter]] CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log       Event log.
 * @param system    Actor system.
 * @tparam A Counter value type.
 */
class CounterService[A](val serviceId: String, val log: ActorRef, implicit val system: ActorSystem)(implicit val integral: Integral[A])
  extends CRDTService[Counter[A], A, A] {

  import CRDTConverter._
  import system._

  override protected val delegate =
    new com.rbmhtechnology.eventuate.crdt.CounterService[A](serviceId, log)

  implicit protected def c: CRDTConverter[A, A] =
    CRDTConverter(identity[A])

  /**
   * Adds `delta` (which can also be negative) to the counter identified by `id` and returns the updated counter value.
   */
  def update(id: String, delta: A): CompletionStage[A] =
    delegate.update(id, delta).asJava
}

object CounterService {

  def ofInt(serviceId: String, log: ActorRef, system: ActorSystem): CounterService[JInt] =
    new CounterService[Int](serviceId, log, system).asInstanceOf[CounterService[JInt]]

  def ofLong(serviceId: String, log: ActorRef, system: ActorSystem): CounterService[JLong] =
    new CounterService[Long](serviceId, log, system).asInstanceOf[CounterService[JLong]]
}