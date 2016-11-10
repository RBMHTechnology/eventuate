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

import com.rbmhtechnology.eventuate.{ DurableEvent, EventsourcedWriter }
import com.rbmhtechnology.eventuate.adapter.vertx.api.EndpointRouter

import scala.collection.immutable.Seq
import scala.concurrent.{ ExecutionContext, Future }

case class EventEnvelope(address: String, evt: DurableEvent)

trait VertxEventDispatcher[R, W] extends EventsourcedWriter[R, W] with ProgressStore[R, W] {
  import context.dispatcher

  def endpointRouter: EndpointRouter

  def dispatch(events: Seq[EventEnvelope])(implicit ec: ExecutionContext): Future[Unit]

  var events: Vector[EventEnvelope] = Vector.empty

  override def onCommand: Receive = {
    case _ =>
  }

  override def onEvent: Receive = {
    case ev =>
      events = endpointRouter.endpoint(ev) match {
        case Some(endpoint) => events :+ EventEnvelope(endpoint, lastHandledEvent)
        case None           => events
      }
  }

  override def write(): Future[W] = {
    val snr = lastSequenceNr
    val ft = dispatch(events).flatMap(x => writeProgress(id, snr))

    events = Vector.empty
    ft
  }

  override def read(): Future[R] =
    readProgress(id)

  override def readSuccess(result: R): Option[Long] =
    Some(progress(result) + 1L)
}
