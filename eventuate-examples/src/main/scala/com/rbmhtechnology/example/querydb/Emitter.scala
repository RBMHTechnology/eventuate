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

package com.rbmhtechnology.example.querydb

import akka.actor._

import com.rbmhtechnology.eventuate._

import scala.util._

// -----------------
//  Domain commands
// -----------------

case class CreateCustomer(first: String, last: String, address: String)
case class UpdateAddress(cid: Long, address: String)

// ---------------
//  Domain events
// ---------------

//#events
case class CustomerCreated(cid: Long, first: String, last: String, address: String)
case class AddressUpdated(cid: Long, address: String)
//#

// ---------------
//  Event emitter
// ---------------

class Emitter(val id: String, val eventLog: ActorRef) extends EventsourcedActor {
  private var highestCustomerId = 0L

  override def onCommand = {
    case CreateCustomer(first, last, address) =>
      persist(CustomerCreated(highestCustomerId + 1L, first, last, address)) {
        case Success(c) => sender() ! c
        case Failure(e) => throw e
      }
    case UpdateAddress(cid, address) if cid <= highestCustomerId =>
      persist(AddressUpdated(cid, address)) {
        case Success(c) => sender() ! c
        case Failure(e) => throw e
      }
    case UpdateAddress(cid, _) =>
      sender() ! new Exception(s"Customer with $cid does not exist")
  }

  override def onEvent = {
    case CustomerCreated(cid, first, last, address) =>
      highestCustomerId = cid
    case AddressUpdated(_, _) =>
    // ...
  }
}
