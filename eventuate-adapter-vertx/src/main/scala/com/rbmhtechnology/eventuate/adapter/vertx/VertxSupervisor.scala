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

package com.rbmhtechnology.eventuate.adapter.vertx

import akka.actor.{ Actor, Props }

import scala.collection.immutable.Seq

private[vertx] object VertxSupervisor {
  def props(adapters: Seq[Props]): Props =
    Props(new VertxSupervisor(adapters))
}

private[vertx] class VertxSupervisor(adapters: Seq[Props]) extends Actor {

  val adapterActors = adapters.map(context.actorOf)

  override def receive: Receive = Actor.emptyBehavior
}
