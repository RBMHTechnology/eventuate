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
import com.rbmhtechnology.eventuate.adapter.vertx.api.EndpointRouter
import org.scalatest.{ MustMatchers, WordSpecLike }

import scala.concurrent.duration._

class VertxBatchConfirmationSenderSpec extends TestKit(ActorSystem("test", TestConfig.default()))
  with WordSpecLike with MustMatchers with SingleLocationSpecLeveldb with StopSystemAfterAll
  with EventWriter with VertxEnvironment with VertxEventBusProbes {

  import utilities._

  val confirmationTimeout = 2.seconds
  val adapterId = "adapter-1"
  var storage: ActorStorageProvider = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    storage = new ActorStorageProvider(adapterId)
    registerEventBusCodec(Confirmation.getClass)
  }

  def vertxBatchConfirmationSender(endpointRouter: EndpointRouter, batchSize: Int = 10): ActorRef =
    system.actorOf(VertxBatchConfirmationSender.props(adapterId, log, endpointRouter, vertx, storage, batchSize, confirmationTimeout))

  "A VertxBatchConfirmationSender" when {
    "reading events from an event log" must {
      "deliver events to a single consumer" in {
        writeEvents("e", 5)
        vertxBatchConfirmationSender(EndpointRouter.routeAllTo(endpoint1.address))

        storage.expectRead(replySequenceNr = 0)

        endpoint1.probe.expectVertxMsg(body = "e-1")
        endpoint1.probe.expectVertxMsg(body = "e-2")
        endpoint1.probe.expectVertxMsg(body = "e-3")
        endpoint1.probe.expectVertxMsg(body = "e-4")
        endpoint1.probe.expectVertxMsg(body = "e-5")
      }
      "deliver events based on the replication progress" in {
        writeEvents("e", 5)
        vertxBatchConfirmationSender(EndpointRouter.routeAllTo(endpoint1.address))

        storage.expectRead(replySequenceNr = 2)

        endpoint1.probe.expectVertxMsg(body = "e-3")
        endpoint1.probe.expectVertxMsg(body = "e-4")
        endpoint1.probe.expectVertxMsg(body = "e-5")
      }
      "persist event confirmations" in {
        writeEvents("e", 3)
        vertxBatchConfirmationSender(EndpointRouter.routeAllTo(endpoint1.address))

        storage.expectRead(replySequenceNr = 0)

        endpoint1.probe.expectVertxMsg(body = "e-1").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-2").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-3").confirm()

        storage.expectWrite(sequenceNr = 3)
      }
      "persist event confirmations in batches" in {
        writeEvents("e", 4)
        vertxBatchConfirmationSender(EndpointRouter.routeAllTo(endpoint1.address), batchSize = 2)

        storage.expectRead(replySequenceNr = 0)

        endpoint1.probe.expectVertxMsg(body = "e-1").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-2").confirm()

        storage.expectWrite(sequenceNr = 2)

        endpoint1.probe.expectVertxMsg(body = "e-3").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-4").confirm()

        storage.expectWrite(sequenceNr = 4)
      }
      "persist event confirmations in batches of smaller size if no further events present" in {
        writeEvents("e", 3)
        vertxBatchConfirmationSender(EndpointRouter.routeAllTo(endpoint1.address), batchSize = 2)

        storage.expectRead(replySequenceNr = 0)

        endpoint1.probe.expectVertxMsg(body = "e-1").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-2").confirm()

        storage.expectWrite(sequenceNr = 2)

        endpoint1.probe.expectVertxMsg(body = "e-3").confirm()

        storage.expectWrite(sequenceNr = 3)
      }
      "redeliver whole batch if events are unconfirmed" in {
        writeEvents("e", 5)
        vertxBatchConfirmationSender(EndpointRouter.routeAllTo(endpoint1.address))

        storage.expectRead(replySequenceNr = 0)

        endpoint1.probe.expectVertxMsg(body = "e-1")
        endpoint1.probe.expectVertxMsg(body = "e-2").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-3").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-4").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-5").confirm()

        storage.expectRead(replySequenceNr = 0)

        endpoint1.probe.expectVertxMsg(body = "e-1")
        endpoint1.probe.expectVertxMsg(body = "e-2")
        endpoint1.probe.expectVertxMsg(body = "e-3")
        endpoint1.probe.expectVertxMsg(body = "e-4")
        endpoint1.probe.expectVertxMsg(body = "e-5")
      }
      "redeliver unconfirmed event batches while replaying events" in {
        writeEvents("e", 4)
        vertxBatchConfirmationSender(EndpointRouter.routeAllTo(endpoint1.address), batchSize = 2)

        storage.expectRead(replySequenceNr = 0)

        endpoint1.probe.expectVertxMsg(body = "e-1").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-2").confirm()

        storage.expectWrite(sequenceNr = 2)

        endpoint1.probe.expectVertxMsg(body = "e-3").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-4")

        storage.expectRead(replySequenceNr = 2)

        endpoint1.probe.expectVertxMsg(body = "e-3").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-4").confirm()

        storage.expectWrite(sequenceNr = 4)
      }
      "redeliver unconfirmed event batches while processing new events" in {
        writeEvents("e", 2)
        vertxBatchConfirmationSender(EndpointRouter.routeAllTo(endpoint1.address), batchSize = 2)

        storage.expectRead(replySequenceNr = 0)

        endpoint1.probe.expectVertxMsg(body = "e-1").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-2").confirm()

        storage.expectWrite(sequenceNr = 2)

        writeEvents("e", 1, start = 3)

        endpoint1.probe.expectVertxMsg(body = "e-3")

        storage.expectRead(replySequenceNr = 2)

        endpoint1.probe.expectVertxMsg(body = "e-3").confirm()

        storage.expectWrite(sequenceNr = 3)
      }
      "deliver selected events only" in {
        writeEvents("e", 10)
        vertxBatchConfirmationSender(EndpointRouter.route {
          case ev: String if isOddEvent(ev, "e") => endpoint1.address
        })

        storage.expectRead(replySequenceNr = 0)

        endpoint1.probe.expectVertxMsg(body = "e-1").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-3").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-5").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-7").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-9").confirm()

        storage.expectWrite(sequenceNr = 10)
      }
      "route events to different endpoints" in {
        writeEvents("e", 10)
        vertxBatchConfirmationSender(EndpointRouter.route {
          case ev: String if isEvenEvent(ev, "e") => endpoint1.address
          case ev: String if isOddEvent(ev, "e")  => endpoint2.address
        })

        storage.expectRead(replySequenceNr = 0)

        endpoint1.probe.expectVertxMsg(body = "e-2").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-4").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-6").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-8").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-10").confirm()

        endpoint2.probe.expectVertxMsg(body = "e-1").confirm()
        endpoint2.probe.expectVertxMsg(body = "e-3").confirm()
        endpoint2.probe.expectVertxMsg(body = "e-5").confirm()
        endpoint2.probe.expectVertxMsg(body = "e-7").confirm()
        endpoint2.probe.expectVertxMsg(body = "e-9").confirm()

        storage.expectWrite(sequenceNr = 10)
      }
      "deliver no events if the routing does not match" in {
        writeEvents("e", 10)
        vertxBatchConfirmationSender(EndpointRouter.route {
          case "i-will-never-match" => endpoint1.address
        })

        storage.expectRead(replySequenceNr = 0)

        endpoint1.probe.expectNoMsg(1.second)

        storage.expectWrite(sequenceNr = 10)
      }
    }
    "encountering a write failure" must {
      "restart and start at the last position" in {
        writeEvents("e", 3)
        vertxBatchConfirmationSender(EndpointRouter.routeAllTo(endpoint1.address))

        storage.expectRead(replySequenceNr = 0)

        endpoint1.probe.expectVertxMsg(body = "e-1").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-2").confirm()
        endpoint1.probe.expectVertxMsg(body = "e-3").confirm()

        storage.expectWriteAndFail(sequenceNr = 3, failure = new RuntimeException("storage write failure."))
        storage.expectRead(replySequenceNr = 0)

        endpoint1.probe.expectVertxMsg(body = "e-1")
        endpoint1.probe.expectVertxMsg(body = "e-2")
        endpoint1.probe.expectVertxMsg(body = "e-3")
      }
    }
  }
}