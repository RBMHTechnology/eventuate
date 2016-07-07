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
import akka.testkit._
import com.rbmhtechnology.eventuate.SingleLocationSpecLeveldb
import com.rbmhtechnology.eventuate.adapter.vertx.api.EndpointRouter
import org.scalatest._

import scala.concurrent.duration._

class VertxNoConfirmationPublisherSpec extends TestKit(ActorSystem("test", TestConfig.withReplayBatchSize(50)))
  with WordSpecLike with MustMatchers with SingleLocationSpecLeveldb with StopSystemAfterAll with ActorStorage with EventWriter
  with VertxEnvironment with VertxEventBusProbes {

  import utilities._

  val inboundLogId = "log_inbound"

  def vertxPublisher(endpointRouter: EndpointRouter): ActorRef =
    system.actorOf(VertxNoConfirmationPublisher.props(inboundLogId, log, endpointRouter, vertx, actorStorageProvider()))

  def read: String = read(inboundLogId)

  def write: (Long) => String = write(inboundLogId)

  "A VertxNoConfirmationPublisher" must {
    "publish events from the beginning of the event log" in {
      val writtenEvents = writeEvents("ev", 50)
      vertxPublisher(EndpointRouter.routeAllTo(endpoint1))

      storageProbe.expectMsg(read)
      storageProbe.reply(0L)

      storageProbe.expectMsg(write(50))
      storageProbe.reply(50L)

      storageProbe.expectNoMsg(1.second)

      endpoint1Probe.receiveNVertxMsg[String](50).map(_.body) must be(writtenEvents.map(_.payload))
    }
    "publish events from a stored sequence number" in {
      val writtenEvents = writeEvents("ev", 50)
      vertxPublisher(EndpointRouter.routeAllTo(endpoint1))

      storageProbe.expectMsg(read)
      storageProbe.reply(10L)

      storageProbe.expectMsg(write(50))
      storageProbe.reply(50L)

      storageProbe.expectNoMsg(1.second)

      endpoint1Probe.receiveNVertxMsg[String](40).map(_.body) must be(writtenEvents.drop(10).map(_.payload))
    }
    "publish events in batches" in {
      val writtenEvents = writeEvents("ev", 100)
      vertxPublisher(EndpointRouter.routeAllTo(endpoint1))

      storageProbe.expectMsg(read)
      storageProbe.reply(0L)

      storageProbe.expectMsg(write(50))
      storageProbe.reply(50L)

      storageProbe.expectMsg(write(100))
      storageProbe.reply(100L)

      storageProbe.expectNoMsg(1.second)

      endpoint1Probe.receiveNVertxMsg[String](100).map(_.body) must be(writtenEvents.map(_.payload))
    }
    "publish events to multiple consumers" in {
      val otherConsumer = eventBusProbe(endpoint1)

      writeEvents("e", 3)
      vertxPublisher(EndpointRouter.routeAllTo(endpoint1))

      storageProbe.expectMsg(read)
      storageProbe.reply(0L)

      endpoint1Probe.expectVertxMsg(body = "e-1")
      endpoint1Probe.expectVertxMsg(body = "e-2")
      endpoint1Probe.expectVertxMsg(body = "e-3")

      otherConsumer.expectVertxMsg(body = "e-1")
      otherConsumer.expectVertxMsg(body = "e-2")
      otherConsumer.expectVertxMsg(body = "e-3")

      storageProbe.expectMsg(write(3))
      storageProbe.reply(3L)
    }
    "publish selected events only" in {
      writeEvents("e", 10)
      vertxPublisher(EndpointRouter.route {
        case ev: String if isOddEvent(ev, "e") => endpoint1
      })

      storageProbe.expectMsg(read)
      storageProbe.reply(0L)

      endpoint1Probe.expectVertxMsg(body = "e-1")
      endpoint1Probe.expectVertxMsg(body = "e-3")
      endpoint1Probe.expectVertxMsg(body = "e-5")
      endpoint1Probe.expectVertxMsg(body = "e-7")
      endpoint1Probe.expectVertxMsg(body = "e-9")

      storageProbe.expectMsg(write(10))
      storageProbe.reply(10L)
    }
    "route events to different endpoints" in {
      writeEvents("e", 10)
      vertxPublisher(EndpointRouter.route {
        case ev: String if isEvenEvent(ev, "e") => endpoint1
        case ev: String if isOddEvent(ev, "e") => endpoint2
      })

      storageProbe.expectMsg(read)
      storageProbe.reply(0L)

      endpoint1Probe.expectVertxMsg(body = "e-2")
      endpoint1Probe.expectVertxMsg(body = "e-4")
      endpoint1Probe.expectVertxMsg(body = "e-6")
      endpoint1Probe.expectVertxMsg(body = "e-8")
      endpoint1Probe.expectVertxMsg(body = "e-10")

      endpoint2Probe.expectVertxMsg(body = "e-1")
      endpoint2Probe.expectVertxMsg(body = "e-3")
      endpoint2Probe.expectVertxMsg(body = "e-5")
      endpoint2Probe.expectVertxMsg(body = "e-7")
      endpoint2Probe.expectVertxMsg(body = "e-9")

      storageProbe.expectMsg(write(10))
      storageProbe.reply(10L)
    }
    "deliver no events if the routing does not match" in {
      writeEvents("e", 10)
      vertxPublisher(EndpointRouter.route {
        case "i-will-never-match" => endpoint1
      })

      storageProbe.expectMsg(read)
      storageProbe.reply(0L)

      endpoint1Probe.expectNoMsg(1.second)

      storageProbe.expectMsg(write(10))
      storageProbe.reply(10L)
    }
  }
}
