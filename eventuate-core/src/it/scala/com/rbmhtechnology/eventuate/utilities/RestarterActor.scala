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

package com.rbmhtechnology.eventuate.utilities

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

class RestarterActor(props: Props, name: Option[String]) extends Actor {

  import RestarterActor._

  var child: ActorRef = newActor
  var requester: ActorRef = _

  override def receive = {
    case Restart =>
      requester = sender()
      context.watch(child)
      context.stop(child)
    case Terminated(_) =>
      child = newActor
      requester ! child
    case msg =>
      child forward msg
  }

  private def newActor: ActorRef =
    name.map(context.actorOf(props, _)).getOrElse(context.actorOf(props))
}

object RestarterActor {
  case object Restart

  implicit val timeout = Timeout(timeoutDuration)

  def restartActor(restarterRef: ActorRef): ActorRef =
    (restarterRef ? Restart).mapTo[ActorRef].await

  def props(props: Props, name: Option[String] = None) = Props(new RestarterActor(props, name))
}
