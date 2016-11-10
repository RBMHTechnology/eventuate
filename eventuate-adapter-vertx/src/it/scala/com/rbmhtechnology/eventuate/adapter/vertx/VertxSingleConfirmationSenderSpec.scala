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

import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.TestKit
import com.rbmhtechnology.eventuate.SingleLocationSpecLeveldb
import com.rbmhtechnology.eventuate.adapter.vertx.api.{ EndpointRouter, EventMetadata }
import org.scalatest.{ MustMatchers, WordSpecLike }

import scala.concurrent.duration._

class VertxSingleConfirmationSenderSpec extends TestKit(ActorSystem("test", TestConfig.default()))
  with WordSpecLike with MustMatchers with SingleLocationSpecLeveldb with StopSystemAfterAll with EventWriter
  with VertxEnvironment with VertxEventBusProbes {

  import utilities._

  val redeliverDelay = 2.seconds
  val inboundLogId = "log_inbound_confirm"

  override def beforeEach(): Unit = {
    super.beforeEach()
    registerEventBusCodec(Confirmation.getClass)
  }

  def vertxSingleConfirmationSender(endpointRouter: EndpointRouter): ActorRef =
    system.actorOf(VertxSingleConfirmationSender.props(inboundLogId, log, endpointRouter, vertx, redeliverDelay))

  "A VertxSingleConfirmationSender" when {
    "reading events from an event log" must {
      "deliver the events to a single consumer" in {
        vertxSingleConfirmationSender(EndpointRouter.routeAllTo(endpoint1.address))
        writeEvents("e", 5)

        endpoint1.probe.expectVertxMsg(body = "e-1")
        endpoint1.probe.expectVertxMsg(body = "e-2")
        endpoint1.probe.expectVertxMsg(body = "e-3")
        endpoint1.probe.expectVertxMsg(body = "e-4")
        endpoint1.probe.expectVertxMsg(body = "e-5")
      }
      "redeliver all unconfirmed events" in {
        vertxSingleConfirmationSender(EndpointRouter.routeAllTo(endpoint1.address))
        writeEvents("e", 2)

        endpoint1.probe.expectVertxMsg(body = "e-1")
        endpoint1.probe.expectVertxMsg(body = "e-2")

        endpoint1.probe.expectVertxMsg(body = "e-1")
        endpoint1.probe.expectVertxMsg(body = "e-2")

        endpoint1.probe.expectVertxMsg(body = "e-1")
        endpoint1.probe.expectVertxMsg(body = "e-2")
      }
      "redeliver only unconfirmed events" in {
        vertxSingleConfirmationSender(EndpointRouter.routeAllTo(endpoint1.address))
        writeEvents("e", 5)

        endpoint1.probe.expectVertxMsg(body = "e-1")
        endpoint1.probe.expectVertxMsg(body = "e-2").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-3").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-4").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-5")

        endpoint1.probe.expectVertxMsg(body = "e-1")
        endpoint1.probe.expectVertxMsg(body = "e-5")
      }
      "redeliver only unconfirmed events while processing new events" in {
        vertxSingleConfirmationSender(EndpointRouter.routeAllTo(endpoint1.address))
        writeEvents("e", 3)

        endpoint1.probe.expectVertxMsg(body = "e-1")
        endpoint1.probe.expectVertxMsg(body = "e-2").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-3")

        writeEvents("e", 2, start = 4)

        endpoint1.probe.expectVertxMsg(body = "e-4").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-5")

        endpoint1.probe.expectVertxMsg(body = "e-1")
        endpoint1.probe.expectVertxMsg(body = "e-3")
        endpoint1.probe.expectVertxMsg(body = "e-5")
      }
      "deliver selected events only" in {
        vertxSingleConfirmationSender(EndpointRouter.route {
          case ev: String if isOddEvent(ev, "e") => endpoint1.address
        })
        writeEvents("e", 10)

        endpoint1.probe.expectVertxMsg(body = "e-1").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-3").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-5").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-7").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-9").confirm()
      }
      "route events to different endpoints" in {
        vertxSingleConfirmationSender(EndpointRouter.route {
          case ev: String if isEvenEvent(ev, "e") => endpoint1.address
          case ev: String if isOddEvent(ev, "e")  => endpoint2.address
        })
        writeEvents("e", 4)

        endpoint1.probe.expectVertxMsg(body = "e-2").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-4").confirm()

        endpoint2.probe.expectVertxMsg(body = "e-1").confirm()
        endpoint2.probe.expectVertxMsg(body = "e-3").confirm()
      }
      "deliver no events if the routing does not match" in {
        vertxSingleConfirmationSender(EndpointRouter.route {
          case "i-will-never-match" => endpoint1.address
        })
        writeEvents("e", 10)

        endpoint1.probe.expectNoMsg(1.second)
      }
      "send event metadata in event bus message headers" in {
        val event = writeEvents("e", 1).head
        vertxSingleConfirmationSender(EndpointRouter.routeAllTo(endpoint1.address))

        val msg = endpoint1.probe.expectVertxMsg(body = "e-1")

        msg.headers.get(EventMetadata.Headers.LocalLogId) mustBe event.localLogId
        msg.headers.get(EventMetadata.Headers.LocalSequenceNr).toLong mustBe event.localSequenceNr
        msg.headers.get(EventMetadata.Headers.EmitterId) mustBe event.emitterId
      }
      "send event metadata in event bus message headers readable from EventMetadata" in {
        val event = writeEvents("e", 1).head
        vertxSingleConfirmationSender(EndpointRouter.routeAllTo(endpoint1.address))

        val msg = endpoint1.probe.expectVertxMsg(body = "e-1")
        val metadata = EventMetadata.fromHeaders(msg.headers)

        metadata.map(_.localLogId) mustBe Some(event.localLogId)
        metadata.map(_.localSequenceNr) mustBe Some(event.localSequenceNr)
        metadata.map(_.emitterId) mustBe Some(event.emitterId)
      }
    }
  }
}
