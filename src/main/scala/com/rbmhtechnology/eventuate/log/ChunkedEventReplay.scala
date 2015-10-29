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

package com.rbmhtechnology.eventuate.log

import java.io.Closeable

import akka.actor._

import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.EventsourcingProtocol._

import scala.util._

private class ChunkedEventReplay(requestor: ActorRef, iterator: => Iterator[DurableEvent] with Closeable) extends Actor {
  val iter = iterator

  def receive = {
    case ReplayNext(max, iid) =>
      Try(iter.take(max).foreach(event => requestor ! Replaying(event, iid))) match {
        case Success(_) if iter.hasNext =>
          requestor ! ReplaySuspended(iid)
        case Success(_) =>
          requestor ! ReplaySuccess(iid)
          context.stop(self)
        case Failure(e) =>
          requestor ! ReplayFailure(e, iid)
          context.stop(self)
      }
    case Terminated(r) if r == requestor =>
      context.stop(self)
  }

  override def preStart(): Unit = {
    context.watch(requestor)
  }

  override def postStop(): Unit = {
    iter.close()
  }
}
