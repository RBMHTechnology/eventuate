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

class MessageStash(implicit context: ActorContext, self: ActorRef) {
  private var envelopes: Vector[Envelope] =
    Vector.empty

  def stash(): Unit =
    envelopes :+= cell.currentMessage

  def unstash(): Unit =
    if (envelopes.nonEmpty) try enqueueFirst(envelopes.head) finally envelopes = envelopes.tail

  def unstashAll(): Unit = {
    val iter = envelopes.reverseIterator
    try while (iter.hasNext) enqueueFirst(iter.next()) finally envelopes = Vector.empty
  }

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
