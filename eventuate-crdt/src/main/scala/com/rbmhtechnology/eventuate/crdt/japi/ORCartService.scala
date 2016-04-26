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

import java.lang.{ Integer => JInt }
import java.util.concurrent.CompletionStage
import java.util.{ Map => JMap }

import akka.actor.{ ActorRef, ActorSystem }
import com.rbmhtechnology.eventuate.crdt._

import scala.collection.JavaConverters._

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
class ORCartService[A](val serviceId: String, val log: ActorRef, implicit val system: ActorSystem)
  extends CRDTService[ORCart[A], Map[A, Int], JMap[A, JInt]] {

  import CRDTConverter._
  import system._

  override protected val delegate = new com.rbmhtechnology.eventuate.crdt.ORCartService[A](serviceId, log)

  implicit protected def c: CRDTConverter[Map[A, Int], JMap[A, JInt]] = CRDTConverter(_.mapValues(v => v: JInt).asJava)

  /**
   * Adds the given `quantity` of `key` to the OR-Cart identified by `id` and returns the updated OR-Cart content.
   */
  def add(id: String, key: A, quantity: Int): CompletionStage[JMap[A, JInt]] =
    delegate.add(id, key, quantity).asJava

  /**
   * Removes the given `key` from the OR-Cart identified by `id` and returns the updated OR-Cart content.
   */
  def remove(id: String, key: A): CompletionStage[JMap[A, JInt]] =
    delegate.remove(id, key).asJava
}
