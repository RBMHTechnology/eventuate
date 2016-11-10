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

package com.rbmhtechnology.docs.vertx

import com.rbmhtechnology.eventuate.adapter.vertx.{ Confirmation, ProcessingResult }
import com.rbmhtechnology.eventuate.adapter.vertx.api.{ Batch, EventMetadata, StorageProvider }
import io.vertx.core.AsyncResult

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

object Documentation {

  case class Event(id: String)
  case class Event2(id: String)

  val storageProvider = new StorageProvider {
    override def readProgress(logName: String)(implicit executionContext: ExecutionContext): Future[Long] = ???

    override def writeProgress(logName: String, sequenceNr: Long)(implicit executionContext: ExecutionContext): Future[Long] = ???
  }

  //#adapter-example
  import akka.actor.ActorSystem
  import com.rbmhtechnology.eventuate.ReplicationEndpoint
  import com.rbmhtechnology.eventuate.adapter.vertx.VertxAdapter
  import com.rbmhtechnology.eventuate.adapter.vertx.api.{ EventProducer, VertxAdapterConfig }
  import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog
  import io.vertx.core.eventbus.Message
  import io.vertx.core.{ Handler, Vertx }

  implicit val actorSystem = ActorSystem("system")
  val vertx = Vertx.vertx()

  val endpoint = new ReplicationEndpoint(
    id = "endpoint",
    logNames = Set("sourceLog", "destinationLog"),
    logFactory = logId => LeveldbEventLog.props(logId), connections = Set.empty)

  val sourceLog = endpoint.logs("sourceLog")
  val destinationLog = endpoint.logs("destinationLog")

  val config = VertxAdapterConfig()
    .addProducer(
      EventProducer.fromLog(sourceLog)
        .publishTo { case _ => "address-1" }
        .as("vertx-producer"))
    .addProducer(
      EventProducer.fromEndpoints("address-2")
        .writeTo(destinationLog)
        .as("log-producer"))

  val adapter = VertxAdapter(config, vertx, storageProvider)(actorSystem)

  // receive events from sourceLog...
  vertx.eventBus().consumer[Event]("address-1").handler(new Handler[Message[Event]] {
    override def handle(message: Message[Event]): Unit = {
      val event = message.body
      // ...and persist the event in destinationLog
      vertx.eventBus().send("address-2", event)
    }
  })

  adapter.start()
  //#

  //#vertx-event-producer
  EventProducer.fromLog(sourceLog)
    .publishTo { case _ => "address-1" }
    .as("vertx-producer")
  //#

  //#log-event-producer
  EventProducer.fromEndpoints("address-2")
    .writeTo(destinationLog)
    .as("log-producer")
  //#

  //#event-processing-vertx-producer
  vertx.eventBus().consumer[Event]("address-1").handler(new Handler[Message[Event]] {
    override def handle(message: Message[Event]): Unit = {
      val event = message.body
    }
  })
  //#

  //#event-processing-log-producer
  vertx.eventBus().send("address-2", Event("event-1"))
  //#

  //#vertx-publish-producer
  EventProducer.fromLog(sourceLog)
    .publishTo {
      case e: Event  => "address-1"
      case e: Event2 => "address-2"
      case _         => "default-address"
    }
    .as("vertx-publish-producer")
  //#

  //#vertx-ptp-producer-at-most-once
  EventProducer.fromLog(sourceLog)
    .sendTo {
      case e: Event  => "address-1"
      case e: Event2 => "address-2"
      case _         => "default-address"
    }
    .as("vertx-ptp-producer")
  //#

  //#vertx-ptp-producer-at-least-once
  EventProducer.fromLog(sourceLog)
    .sendTo { case _ => "address-1" }
    .atLeastOnce(confirmationType = Batch(size = 5), confirmationTimeout = 5.seconds)
    .as("vertx-ptp-producer")
  //#

  //#vertx-ptp-producer-handler
  vertx.eventBus().consumer[Event]("address-1").handler(new Handler[Message[Event]] {
    override def handle(message: Message[Event]): Unit = {
      val event = message.body
      // confirm event receipt
      message.reply(Confirmation)
    }
  })
  //#

  //#log-event-multiple-producer
  EventProducer.fromEndpoints("address-1", "address-2")
    .writeTo(destinationLog, {
      case _: Event | _: Event2 => true
      case _                    => false
    })
    .as("log-producer")
  //#

  //#log-producer-handler
  vertx.eventBus().send[ProcessingResult]("address-1", Event("event-1"), new Handler[AsyncResult[Message[ProcessingResult]]] {
    override def handle(ar: AsyncResult[Message[ProcessingResult]]): Unit = {
      if (ar.succeeded()) {
        var result = ar.result.body
      } else {
        // write failed
        var failure = ar.cause
      }
    }
  })
  //#

  //#message-codec
  VertxAdapterConfig()
    .registerDefaultCodecFor(classOf[Event], classOf[Event2])
  //#

  //#event-metadata-from-headers
  vertx.eventBus().consumer[Event]("address-1").handler(new Handler[Message[Event]] {
    override def handle(message: Message[Event]): Unit = {
      val headers = message.headers

      val localLogId = headers.get(EventMetadata.Headers.LocalLogId)
      val localSeqNr = headers.get(EventMetadata.Headers.LocalSequenceNr).toLong
      val emitterId = headers.get(EventMetadata.Headers.EmitterId)
    }
  })
  //#

  //#event-metadata-from-helper
  vertx.eventBus().consumer[Event]("address-1").handler(new Handler[Message[Event]] {
    override def handle(message: Message[Event]): Unit = {
      val metadata = EventMetadata.fromHeaders(message.headers).get

      val localLogId = metadata.localLogId
      val localSeqNr = metadata.localSequenceNr
      val emitterId = metadata.emitterId
    }
  })
  //#
}
