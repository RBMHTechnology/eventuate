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

package com.rbmhtechnology.eventuate.crdt.pure.japi

import java.util.concurrent.CompletionStage
import java.util.{ Optional => JOption }

import akka.actor.ActorRef
import akka.actor.ActorSystem
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.SimpleCRDT

import scala.compat.java8.OptionConverters._

/**
 * Java API of a replicated LWWRegister CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log       Event log.
 * @param system    Actor system.
 * @tparam A LWWRegister value type.
 */
class LWWRegisterService[A](val serviceId: String, val log: ActorRef, implicit val system: ActorSystem)
  extends CRDTService[SimpleCRDT, Option[A], JOption[A]] {

  import CRDTConverter._
  import system._

  override protected val delegate =
    new com.rbmhtechnology.eventuate.crdt.pure.LWWRegisterService[A](serviceId, log)

  implicit protected def c: CRDTConverter[Option[A], JOption[A]] =
    CRDTConverter(_.asJava)

  /**
   * Assigns a `value` to the LWW-Register identified by `id` and returns the updated LWW-Register value.
   */
  def assign(id: String, value: A): CompletionStage[JOption[A]] =
    delegate.assign(id, value).asJava
}
