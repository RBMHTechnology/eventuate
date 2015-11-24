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

/**
 * A conditional request is a request to an actor in the [[EventsourcedView]]
 * hierarchy whose delivery to the actor's command handler is delayed until
 * the request's `condition` is in the causal past of that actor (i.e. if the
 * `condition` is `<=` the view's current time).
 */
case class ConditionalRequest(condition: VectorTime, req: Any)

/**
 * Thrown by an actor in the [[EventsourcedView]] hierarchy if it receives
 * a [[ConditionalRequest]] but does not extends the [[ConditionalRequests]]
 * trait.
 */
class ConditionalRequestException(msg: String) extends RuntimeException(msg)

/**
 * Must be extended by actors in the [[EventsourcedView]] hierarchy if they
 * want to support [[ConditionalRequest]] processing.
 */
trait ConditionalRequests extends EventsourcedView with EventsourcedClock {
  import ConditionalRequests._

  private val requestManager = context.actorOf(Props(new RequestManager(self)))

  /**
   * Internal API.
   */
  override private[eventuate] def conditionalSend(condition: VectorTime, req: Any): Unit =
    requestManager ! Request(condition, req, sender())

  /**
   * Internal API.
   */
  override private[eventuate] def conditionChanged(condition: VectorTime): Unit =
    requestManager ! condition
}

private object ConditionalRequests {
  case class Request(condition: VectorTime, req: Any, sdr: ActorRef)

  case class Send(olderThan: VectorTime)
  case class Sent(olderThan: VectorTime, num: Int)

  class RequestManager(owner: ActorRef) extends Actor {
    val requestBuffer = context.actorOf(Props(new RequestBuffer(owner)))
    var currentTime: VectorTime = VectorTime.Zero

    val idle: Receive = {
      case cr: Request =>
        process(cr)
      case t: VectorTime =>
        currentTime = t
        requestBuffer ! Send(t)
        context.become(sending)
    }

    val sending: Receive = {
      case cr: Request =>
        process(cr)
      case t: VectorTime =>
        currentTime = t
      case Sent(olderThan, num) if olderThan == currentTime =>
        context.become(idle)
      case Sent(olderThan, num) =>
        requestBuffer ! Send(currentTime)
    }

    def receive = idle

    def process(cr: Request): Unit = {
      if (cr.condition <= currentTime) owner.tell(cr.req, cr.sdr)
      else requestBuffer ! cr
    }
  }

  class RequestBuffer(owner: ActorRef) extends Actor {
    // TODO: cleanup requests older than threshold
    var requests: Vector[Request] = Vector.empty

    def receive = {
      case Send(olderThan) =>
        sender() ! Sent(olderThan, send(olderThan))
      case cc: Request =>
        requests = requests :+ cc
    }

    def send(olderThan: VectorTime): Int = {
      val (older, other) = requests.partition(_.condition <= olderThan)
      requests = other
      older.foreach(cc => owner.tell(cc.req, cc.sdr))
      older.length
    }
  }
}
