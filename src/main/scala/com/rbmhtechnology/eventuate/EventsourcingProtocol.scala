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

object EventsourcingProtocol {

  /**
   * Instructs an event log to read up to `max` events starting from sequence number `from`
   * and applying the given replication `filter`.
   */
  case class Read(from: Long, max: Int, filter: ReplicationFilter)

  /**
   * Success reply after a [[Read]].
   *
   * @param events read events
   * @param last last read sequence number. This is greater than or equal the sequence number
   *             of the last read event (if any).
   */
  case class ReadSuccess(events: Seq[DurableEvent], last: Long)

  /**
   * Failure reply after a [[Read]].
   */
  case class ReadFailure(cause: Throwable)

  /**
   * Instructs an event log to loop the given `commands` to the given `requestor`, preserving
   * the relative order to events being written by that event log. Commands are looped to the
   * `requestor` one-by-one within a [[DelayComplete]] message with `commandSender` as the
   * message sender.
   */
  case class Delay(commands: Seq[Any], commandsSender: ActorRef, requestor: ActorRef, instanceId: Int)

  /**
   * Completion reply after a [[Delay]].
   */
  case class DelayComplete(command: Any, instanceId: Int)

  /**
   * Instructs an event log to batch-execute the given `writes`.
   */
  case class WriteN(writes: Seq[Write])

  /**
   * Completion reply after a [[WriteN]].
   */
  case object WriteNComplete

  /**
   * Instructs an event log to write the given `events` and send the written events one-by-one
   * to the given `requestor`. In case of a successful write, events are sent within [[WriteSuccess]]
   * messages, otherwise within [[WriteFailure]] messages with `eventSender` as message sender.
   */
  case class Write(events: Seq[DurableEvent], eventsSender: ActorRef, requestor: ActorRef, instanceId: Int)

  /**
   * Success reply after a [[Write]].
   */
  case class WriteSuccess(event: DurableEvent, instanceId: Int)

  /**
   * Failure reply after a [[Write]].
   */
  case class WriteFailure(event: DurableEvent, cause: Throwable, instanceId: Int)

  /**
   * Sent by an event log to all registered participants, if `event` has been successfully written.
   * This message is not sent to a participant if that participant has sent a corresponding [[Write]].
   */
  case class Written(event: DurableEvent)

  /**
   * Instructs an event log to replay events from sequence number `from` to the given `requestor`.
   * Replayed events are sent within [[Replaying]] messages. If replay successfully completes the
   * event log must additionally send a [[ReplaySuccess]] message, otherwise, a [[ReplayFailure]]
   * message.
   */
  case class Replay(from: Long, requestor: ActorRef, instanceId: Int)

  /**
   * Single `event` replay after a [[Replay]].
   */
  case class Replaying(event: DurableEvent, instanceId: Int)

  /**
   * Success reply after a [[Replay]], sent when all [[Replaying]] messages have been sent.
   */
  case class ReplaySuccess(instanceId: Int)

  /**
   * Failure reply after a [[Replay]].
   */
  case class ReplayFailure(cause: Throwable, instanceId: Int)
}
