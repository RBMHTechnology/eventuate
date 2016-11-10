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
import java.util.{ Set => JSet }

import akka.actor.{ ActorRef, ActorSystem }
import com.rbmhtechnology.eventuate.crdt.MVRegister

import scala.collection.JavaConverters._
import scala.collection.immutable.Set

/**
 * Java API of a replicated [[MVRegister]] CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log Event log.
 * @param system Actor system.
 * @tparam A [[MVRegister]] value type.
 */
class MVRegisterService[A](val serviceId: String, val log: ActorRef, implicit val system: ActorSystem)
  extends CRDTService[MVRegister[A], Set[A], JSet[A]] {

  import CRDTConverter._
  import system._

  override protected val delegate =
    new com.rbmhtechnology.eventuate.crdt.MVRegisterService[A](serviceId, log)

  implicit protected def c: CRDTConverter[Set[A], JSet[A]] =
    CRDTConverter(_.asJava)

  /**
   * Assigns a `value` to the MV-Register identified by `id` and returns the updated MV-Register value.
   */
  def assign(id: String, value: A): CompletionStage[JSet[A]] =
    delegate.assign(id, value).asJava
}
