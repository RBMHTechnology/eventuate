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

import akka.actor.{ Actor, ActorRef, Props }
import com.rbmhtechnology.eventuate.adapter.vertx.LogEventDispatcher.EndpointRoute
import com.rbmhtechnology.eventuate.adapter.vertx.LogProducer.PersistMessage
import io.vertx.core.Vertx
import io.vertx.core.eventbus.{ Message, MessageConsumer }

object LogEventDispatcher {

  case class EventProducerRef(id: String, log: ActorRef)
  case class EndpointRoute(sourceEndpoint: String, producer: EventProducerRef, filter: PartialFunction[Any, Boolean] = { case _ => true })

  def props(routes: Seq[EndpointRoute], vertx: Vertx): Props =
    Props(new LogEventDispatcher(routes, vertx))
}

class LogEventDispatcher(routes: Seq[EndpointRoute], vertx: Vertx) extends Actor {

  import VertxHandlerConverters._

  val producers = routes
    .groupBy(_.producer)
    .map { case (producer, _) => producer.id -> context.actorOf(LogProducer.props(producer.id, producer.log)) }

  val consumers = routes
    .map { r => installMessageConsumer(r.sourceEndpoint, producers(r.producer.id), r.filter) }

  private def installMessageConsumer(endpoint: String, producer: ActorRef, filter: PartialFunction[Any, Boolean]): MessageConsumer[Any] = {
    val handler = (msg: Message[Any]) => {
      if (filter.applyOrElse(msg.body(), (_: Any) => false)) {
        producer ! PersistMessage(msg)
      } else {
        msg.reply(ProcessingResult.FILTERED)
      }
    }
    vertx.eventBus().consumer[Any](endpoint, handler.asVertxHandler)
  }

  override def receive: Receive = Actor.emptyBehavior

  override def postStop(): Unit = {
    consumers.foreach(_.unregister())
  }
}
