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

package akka.actor

import akka.dispatch._

import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.EventsourcingProtocol.Unstashed

abstract class MessageStash {
  def context: ActorContext
  def self: ActorRef

  def unstash(): Unit
  def unstashAll(): Unit

  protected val mailbox: DequeBasedMessageQueueSemantics =
    cell.mailbox.messageQueue match {
      case queue: DequeBasedMessageQueueSemantics => queue
      case other => throw ActorInitializationException(self, s"DequeBasedMailbox required, got: ${other.getClass.getName}")
    }

  protected def enqueueFirst(envelope: Envelope): Unit = {
    mailbox.enqueueFirst(self, envelope)
    envelope.message match {
      case Terminated(ref) => cell.terminatedQueuedFor(ref)
      case _               =>
    }
  }

  protected def cell =
    context.asInstanceOf[ActorCell]
}

class EventStash(instanceId: Int)(implicit val context: ActorContext, val self: ActorRef) extends MessageStash {
  private var _events: Vector[DurableEvent] =
    Vector.empty

  def events: Vector[DurableEvent] =
    _events

  def stash(event: DurableEvent): Unit =
    _events :+= event

  override def unstash(): Unit =
    if (_events.nonEmpty) try enqueueFirst(envelope(_events.head)) finally _events = _events.tail

  override def unstashAll(): Unit = {
    val iter = _events.reverseIterator
    try while (iter.hasNext) enqueueFirst(envelope(iter.next())) finally _events = Vector.empty
  }

  private def envelope(event: DurableEvent): Envelope =
    Envelope(Unstashed(event, instanceId), Actor.noSender, context.system)
}

class CommandStash()(implicit val context: ActorContext, val self: ActorRef) extends MessageStash {
  private var envelopes: Vector[Envelope] =
    Vector.empty

  def stash(): Unit =
    envelopes :+= cell.currentMessage

  override def unstash(): Unit =
    if (envelopes.nonEmpty) try enqueueFirst(envelopes.head) finally envelopes = envelopes.tail

  override def unstashAll(): Unit = {
    val iter = envelopes.reverseIterator
    try while (iter.hasNext) enqueueFirst(iter.next()) finally envelopes = Vector.empty
  }
}
