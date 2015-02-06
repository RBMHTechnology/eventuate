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

package com.rbmhtechnology.eventuate

import akka.actor._

import scala.collection.immutable.Seq

object EventLogProtocol {
  case class Read(from: Long, max: Int, filter: ReplicationFilter)
  case class ReadSuccess(events: Seq[DurableEvent], last: Long)
  case class ReadFailure(cause: Throwable)

  case class Delay(commands: Seq[Any], commandsSender: ActorRef, requestor: ActorRef, instanceId: Int)
  case class DelaySuccess(command: Any, instanceId: Int)

  case class WriteN(writes: Seq[Write])
  case object WriteNComplete

  case class Write(events: Seq[DurableEvent], eventsSender: ActorRef, requestor: ActorRef, instanceId: Int)
  case class WriteSuccess(event: DurableEvent, instanceId: Int)
  case class WriteFailure(event: DurableEvent, cause: Throwable, instanceId: Int)
  case class Written(event: DurableEvent)

  case class Replay(from: Long, requestor: ActorRef, instanceId: Int)
  case class Replaying(event: DurableEvent, instanceId: Int)
  case class ReplaySuccess(instanceId: Int)
  case class ReplayFailure(cause: Throwable, instanceId: Int)
}
