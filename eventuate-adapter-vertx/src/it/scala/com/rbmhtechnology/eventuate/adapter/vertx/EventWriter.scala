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

import akka.actor.{ActorRef, ActorSystem}
import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.log.EventLogWriter
import com.rbmhtechnology.eventuate.utilities._

import scala.collection.immutable.Seq

trait EventWriter {

  def log: ActorRef

  def writeEvents(prefix: String, eventCount: Int = 100, start: Int = 1)(implicit system: ActorSystem): Seq[DurableEvent] =
    new EventLogWriter("w1", log).write((start until eventCount + start).map(i => s"$prefix-$i")).await

  def isEvenEvent(ev: String, prefix: String): Boolean =
    Integer.valueOf(ev.replace(prefix, "")) % 2 == 0

  def isOddEvent(ev: String, prefix: String): Boolean =
    !isEvenEvent(ev, prefix)
}
