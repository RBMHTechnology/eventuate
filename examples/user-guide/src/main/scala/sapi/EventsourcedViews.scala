/*
 * Copyright 2015 - 2017 Red Bull Media House GmbH <http://www.redbullmediahouse.com> and Mike Slinn - all rights reserved.
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

package sapi

/** Used by [[ConditionalExample]] */
object EventsourcedViews {
  //#event-sourced-view
  import akka.actor.ActorRef
  import com.rbmhtechnology.eventuate.{EventsourcedView, VectorTime}

  sealed trait EventSourcedMsg

  case class Resolved(selectedTimestamp: VectorTime) extends EventSourcedMsg

  case object GetAppendCount extends EventSourcedMsg
  case class GetAppendCountReply(count: Long) extends EventSourcedMsg

  case object GetResolveCount extends EventSourcedMsg
  case class GetResolveCountReply(count: Long) extends EventSourcedMsg

  class ExampleView(override val id: String, override val eventLog: ActorRef) extends EventsourcedView {
    private var appendCount: Long = 0L
    private var resolveCount: Long = 0L

    override def onCommand: PartialFunction[Any, Unit] = {
      case GetAppendCount  => sender() ! GetAppendCountReply(appendCount)
      case GetResolveCount => sender() ! GetResolveCountReply(resolveCount)
    }

    override def onEvent: PartialFunction[Any, Unit] = {
      case AppendedEvent(_) => appendCount += 1L
      case Resolved(_) => resolveCount += 1L
    }
  }
  //#
}
