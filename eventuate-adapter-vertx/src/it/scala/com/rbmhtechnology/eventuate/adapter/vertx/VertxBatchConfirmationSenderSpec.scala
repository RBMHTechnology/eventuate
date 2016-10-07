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

import akka.actor.{ActorRef, ActorSystem, Status}
import akka.testkit.{TestKit, TestProbe}
import com.rbmhtechnology.eventuate.SingleLocationSpecLeveldb
import com.rbmhtechnology.eventuate.adapter.vertx.api.{Confirmation, EndpointRouter}
import com.rbmhtechnology.eventuate.adapter.vertx.japi.ProcessingResult
import org.scalatest.{MustMatchers, WordSpecLike}

import scala.concurrent.duration._

class VertxBatchConfirmationSenderSpec extends TestKit(ActorSystem("test", TestConfig.default()))
  with WordSpecLike with MustMatchers with SingleLocationSpecLeveldb with StopSystemAfterAll
  with ActorStorage with EventWriter with VertxEnvironment with VertxEventBusProbes {

  import utilities._

  val confirmationTimeout = 2.seconds
  val storageTimeout = 500.millis
  val inboundLogId = "log_inbound_confirm"

  override def beforeEach(): Unit = {
    super.beforeEach()
    registerEventBusCodec(Confirmation.getClass)
  }

  def vertxBatchConfirmationSender(endpointRouter: EndpointRouter, batchSize: Int = 10): ActorRef =
    system.actorOf(VertxBatchConfirmationSender.props(inboundLogId, log, endpointRouter, vertx, actorStorageProvider(), batchSize, confirmationTimeout))

  def read: String = read(inboundLogId)

  def write: (Long) => String = write(inboundLogId)

  "A VertxBatchConfirmationSender" when {
    "reading events from an event log" must {
      "deliver events to a single consumer" in {
        writeEvents("e", 5)
        vertxBatchConfirmationSender(EndpointRouter.routeAllTo(endpoint1))

        storageProbe.expectMsg(read)
        storageProbe.reply(0L)

        endpoint1Probe.expectVertxMsg(body = "e-1")
        endpoint1Probe.expectVertxMsg(body = "e-2")
        endpoint1Probe.expectVertxMsg(body = "e-3")
        endpoint1Probe.expectVertxMsg(body = "e-4")
        endpoint1Probe.expectVertxMsg(body = "e-5")
      }
      "deliver events based on the replication progress" in {
        writeEvents("e", 5)
        vertxBatchConfirmationSender(EndpointRouter.routeAllTo(endpoint1))

        storageProbe.expectMsg(read)
        storageProbe.reply(2L)

        endpoint1Probe.expectVertxMsg(body = "e-3")
        endpoint1Probe.expectVertxMsg(body = "e-4")
        endpoint1Probe.expectVertxMsg(body = "e-5")
      }
      "persist event confirmations" in {
        writeEvents("e", 3)
        vertxBatchConfirmationSender(EndpointRouter.routeAllTo(endpoint1))

        storageProbe.expectMsg(read)
        storageProbe.reply(0L)
        storageProbe.expectNoMsg(storageTimeout)

        endpoint1Probe.expectVertxMsg(body = "e-1").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-2").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-3").confirm()

        storageProbe.expectMsg(write(3))
      }
      "persist event confirmations in batches" in {
        writeEvents("e", 4)
        vertxBatchConfirmationSender(EndpointRouter.routeAllTo(endpoint1), batchSize = 2)

        storageProbe.expectMsg(read)
        storageProbe.reply(0L)
        storageProbe.expectNoMsg(storageTimeout)

        endpoint1Probe.expectVertxMsg(body = "e-1").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-2").confirm()

        storageProbe.expectMsg(write(2))
        storageProbe.reply(2L)

        endpoint1Probe.expectVertxMsg(body = "e-3").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-4").confirm()

        storageProbe.expectMsg(write(4))
        storageProbe.reply(2L)
      }
      "persist event confirmations in batches of smaller size if no further events present" in {
        writeEvents("e", 3)
        vertxBatchConfirmationSender(EndpointRouter.routeAllTo(endpoint1), batchSize = 2)

        storageProbe.expectMsg(read)
        storageProbe.reply(0L)
        storageProbe.expectNoMsg(storageTimeout)

        endpoint1Probe.expectVertxMsg(body = "e-1").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-2").confirm()

        storageProbe.expectMsg(write(2))
        storageProbe.reply(2L)

        endpoint1Probe.expectVertxMsg(body = "e-3").confirm()

        storageProbe.expectMsg(write(3))
        storageProbe.reply(3L)
      }
      "redeliver whole batch if events are unconfirmed" in {
        writeEvents("e", 5)
        vertxBatchConfirmationSender(EndpointRouter.routeAllTo(endpoint1))

        storageProbe.expectMsg(read)
        storageProbe.reply(0L)

        endpoint1Probe.expectVertxMsg(body = "e-1")
        endpoint1Probe.expectVertxMsg(body = "e-2").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-3").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-4").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-5").confirm()

        storageProbe.expectMsg(read)
        storageProbe.reply(0L)

        endpoint1Probe.expectVertxMsg(body = "e-1")
        endpoint1Probe.expectVertxMsg(body = "e-2")
        endpoint1Probe.expectVertxMsg(body = "e-3")
        endpoint1Probe.expectVertxMsg(body = "e-4")
        endpoint1Probe.expectVertxMsg(body = "e-5")
      }
      "redeliver unconfirmed event batches while replaying events" in {
        writeEvents("e", 4)
        vertxBatchConfirmationSender(EndpointRouter.routeAllTo(endpoint1), batchSize = 2)

        storageProbe.expectMsg(read)
        storageProbe.reply(0L)

        endpoint1Probe.expectVertxMsg(body = "e-1").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-2").confirm()

        storageProbe.expectMsg(write(2))
        storageProbe.reply(2L)

        endpoint1Probe.expectVertxMsg(body = "e-3").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-4")

        storageProbe.expectMsg(read)
        storageProbe.reply(2L)

        endpoint1Probe.expectVertxMsg(body = "e-3").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-4").confirm()
      }
      "redeliver unconfirmed event batches while processing new events" in {
        writeEvents("e", 2)
        vertxBatchConfirmationSender(EndpointRouter.routeAllTo(endpoint1), batchSize = 2)

        storageProbe.expectMsg(read)
        storageProbe.reply(0L)

        endpoint1Probe.expectVertxMsg(body = "e-1").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-2").confirm()

        storageProbe.expectMsg(write(2))
        storageProbe.reply(2L)

        writeEvents("e", 1, start = 3)

        endpoint1Probe.expectVertxMsg(body = "e-3")

        storageProbe.expectMsg(read)
        storageProbe.reply(2L)

        endpoint1Probe.expectVertxMsg(body = "e-3").confirm()

        storageProbe.expectMsg(write(3))
        storageProbe.reply(3L)
      }
      "deliver selected events only" in {
        writeEvents("e", 10)
        vertxBatchConfirmationSender(EndpointRouter.route {
          case ev: String if isOddEvent(ev, "e") => endpoint1
        })

        storageProbe.expectMsg(read)
        storageProbe.reply(0L)

        endpoint1Probe.expectVertxMsg(body = "e-1").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-3").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-5").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-7").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-9").confirm()

        storageProbe.expectMsg(write(10))
        storageProbe.reply(10L)
      }
      "route events to different endpoints" in {
        writeEvents("e", 10)
        vertxBatchConfirmationSender(EndpointRouter.route {
          case ev: String if isEvenEvent(ev, "e") => endpoint1
          case ev: String if isOddEvent(ev, "e") => endpoint2
        })

        storageProbe.expectMsg(read)
        storageProbe.reply(0L)

        endpoint1Probe.expectVertxMsg(body = "e-2").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-4").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-6").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-8").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-10").confirm()

        endpoint2Probe.expectVertxMsg(body = "e-1").confirm()
        endpoint2Probe.expectVertxMsg(body = "e-3").confirm()
        endpoint2Probe.expectVertxMsg(body = "e-5").confirm()
        endpoint2Probe.expectVertxMsg(body = "e-7").confirm()
        endpoint2Probe.expectVertxMsg(body = "e-9").confirm()

        storageProbe.expectMsg(write(10))
        storageProbe.reply(10L)
      }
      "deliver no events if the routing does not match" in {
        writeEvents("e", 10)
        vertxBatchConfirmationSender(EndpointRouter.route {
          case "i-will-never-match" => endpoint1
        })

        storageProbe.expectMsg(read)
        storageProbe.reply(0L)

        endpoint1Probe.expectNoMsg(1.second)

        storageProbe.expectMsg(write(10))
        storageProbe.reply(10L)
      }
    }
    "encountering a write failure" must {
      "restart and start at the last position" in {
        writeEvents("e", 3)
        vertxBatchConfirmationSender(EndpointRouter.routeAllTo(endpoint1))

        storageProbe.expectMsg(read)
        storageProbe.reply(0L)

        endpoint1Probe.expectVertxMsg(body = "e-1").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-2").confirm()
        endpoint1Probe.expectVertxMsg(body = "e-3").confirm()

        storageProbe.expectMsg(write(3))
        storageProbe.reply(Status.Failure(new RuntimeException("err")))

        storageProbe.expectMsg(read)
        storageProbe.reply(0L)

        endpoint1Probe.expectVertxMsg(body = "e-1")
        endpoint1Probe.expectVertxMsg(body = "e-2")
        endpoint1Probe.expectVertxMsg(body = "e-3")
      }
    }
  }
}