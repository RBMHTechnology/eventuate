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

package com.rbmhtechnology.eventuate.log

import akka.actor._

import com.rbmhtechnology.eventuate._

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util._

private object EventLogWriter {
  class EventLogWriterActor(val id: String, val eventLog: ActorRef, override val aggregateId: Option[String]) extends EventsourcedActor {
    override def onCommand: Receive = {
      case event => persist(event) {
        case Success(r) => sender() ! lastHandledEvent
        case Failure(e) => sender() ! Status.Failure(e)
      }
    }

    override def onEvent: Receive = {
      case event =>
    }
  }
}

/**
 * Utility for writing events to an event log.
 *
 * @param id Unique emitter id.
 * @param eventLog Event log to write to.
 * @param aggregateId Optional aggregate id.
 */
class EventLogWriter(id: String, eventLog: ActorRef, aggregateId: Option[String])(implicit val system: ActorSystem) {
  import EventLogWriter._
  import system.dispatcher

  import akka.pattern.ask
  import akka.util.Timeout

  // --------------------------
  //  FIXME: make configurable
  // --------------------------

  private implicit val timeout =
    Timeout(10.seconds)

  private val actor =
    system.actorOf(Props(new EventLogWriterActor(id, eventLog, aggregateId)))

  def this(id: String, eventLog: ActorRef)(implicit system: ActorSystem) =
    this(id, eventLog, None)

  /**
   * Asynchronously writes the given `events` to `eventLog`.
   */
  def write(events: Seq[Any]): Future[Seq[DurableEvent]] =
    Future.sequence(events.map(event => actor.ask(event).mapTo[DurableEvent]))

  /**
   * Stops this writer.
   */
  def stop(): Unit =
    system.stop(actor)
}
