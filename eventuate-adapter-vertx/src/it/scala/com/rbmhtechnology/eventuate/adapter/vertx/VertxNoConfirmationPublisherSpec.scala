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
import com.rbmhtechnology.eventuate.adapter.vertx.api.{EndpointRouter, EventMetadata}
import org.scalatest._

import scala.concurrent.duration._

class VertxNoConfirmationPublisherSpec extends TestKit(ActorSystem("test", TestConfig.withReplayBatchSize(50)))
  with WordSpecLike with MustMatchers with SingleLocationSpecLeveldb with BeforeAndAfterEach with StopSystemAfterAll with EventWriter
  with VertxEnvironment with VertxEventBusProbes {

  import utilities._

  val adapterId = "adapter-1"
  var storage: ActorStorageProvider = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    storage = new ActorStorageProvider(adapterId)
  }

  def vertxPublisher(endpointRouter: EndpointRouter): ActorRef =
    system.actorOf(VertxNoConfirmationPublisher.props(adapterId, log, endpointRouter, vertx, storage))

  "A VertxNoConfirmationPublisher" must {
    "publish events from the beginning of the event log" in {
      val writtenEvents = writeEvents("ev", 50)
      vertxPublisher(EndpointRouter.routeAllTo(endpoint1))

      storage.expectRead(replySequenceNr = 0)
      storage.expectWrite(sequenceNr = 50)
      storage.expectNoMsg(1.second)

      endpoint1Probe.receiveNVertxMsg[String](50).map(_.body) must be(writtenEvents.map(_.payload))
    }
    "publish events from a stored sequence number" in {
      val writtenEvents = writeEvents("ev", 50)
      vertxPublisher(EndpointRouter.routeAllTo(endpoint1))

      storage.expectRead(replySequenceNr = 10)
      storage.expectWrite(sequenceNr = 50)
      storage.expectNoMsg(1.second)

      endpoint1Probe.receiveNVertxMsg[String](40).map(_.body) must be(writtenEvents.drop(10).map(_.payload))
    }
    "publish events in batches" in {
      val writtenEvents = writeEvents("ev", 100)
      vertxPublisher(EndpointRouter.routeAllTo(endpoint1))

      storage.expectRead(replySequenceNr = 0)
      storage.expectWrite(sequenceNr = 50)
      storage.expectWrite(sequenceNr = 100)
      storage.expectNoMsg(1.second)

      endpoint1Probe.receiveNVertxMsg[String](100).map(_.body) must be(writtenEvents.map(_.payload))
    }
    "publish events to multiple consumers" in {
      val otherConsumer = eventBusProbe(endpoint1)

      writeEvents("e", 3)
      vertxPublisher(EndpointRouter.routeAllTo(endpoint1))

      storage.expectRead(replySequenceNr = 0)

      endpoint1Probe.expectVertxMsg(body = "e-1")
      endpoint1Probe.expectVertxMsg(body = "e-2")
      endpoint1Probe.expectVertxMsg(body = "e-3")

      otherConsumer.expectVertxMsg(body = "e-1")
      otherConsumer.expectVertxMsg(body = "e-2")
      otherConsumer.expectVertxMsg(body = "e-3")

      storage.expectWrite(sequenceNr = 3)
    }
    "publish selected events only" in {
      writeEvents("e", 10)
      vertxPublisher(EndpointRouter.route {
        case ev: String if isOddEvent(ev, "e") => endpoint1
      })

      storage.expectRead(replySequenceNr = 0)

      endpoint1Probe.expectVertxMsg(body = "e-1")
      endpoint1Probe.expectVertxMsg(body = "e-3")
      endpoint1Probe.expectVertxMsg(body = "e-5")
      endpoint1Probe.expectVertxMsg(body = "e-7")
      endpoint1Probe.expectVertxMsg(body = "e-9")

      storage.expectWrite(sequenceNr = 10)
    }
    "route events to different endpoints" in {
      writeEvents("e", 10)
      vertxPublisher(EndpointRouter.route {
        case ev: String if isEvenEvent(ev, "e") => endpoint1
        case ev: String if isOddEvent(ev, "e") => endpoint2
      })

      storage.expectRead(replySequenceNr = 0)

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

      storage.expectWrite(sequenceNr = 10)
    }
    "deliver no events if the routing does not match" in {
      writeEvents("e", 10)
      vertxPublisher(EndpointRouter.route {
        case "i-will-never-match" => endpoint1
      })

      storage.expectRead(replySequenceNr = 0)

      endpoint1Probe.expectNoMsg(1.second)

      storage.expectWrite(sequenceNr = 10)
    }
    "send event metadata in event bus message headers" in {
      val event = writeEvents("e", 1).head
      vertxPublisher(EndpointRouter.routeAllTo(endpoint1))

      storage.expectRead(replySequenceNr = 0)
      storage.expectWrite(sequenceNr = 1)

      val msg = endpoint1Probe.expectVertxMsg(body = "e-1")

      msg.headers.get(EventMetadata.Headers.LocalLogId) mustBe event.localLogId
      msg.headers.get(EventMetadata.Headers.LocalSequenceNr).toLong mustBe event.localSequenceNr
      msg.headers.get(EventMetadata.Headers.EmitterId) mustBe event.emitterId
    }
    "send event metadata in event bus message headers readable from EventMetadata" in {
      val event = writeEvents("e", 1).head
      vertxPublisher(EndpointRouter.routeAllTo(endpoint1))

      storage.expectRead(replySequenceNr = 0)
      storage.expectWrite(sequenceNr = 1)

      val msg = endpoint1Probe.expectVertxMsg(body = "e-1")
      val metadata = EventMetadata.fromHeaders(msg.headers)

      metadata.map(_.localLogId) mustBe Some(event.localLogId)
      metadata.map(_.localSequenceNr) mustBe Some(event.localSequenceNr)
      metadata.map(_.emitterId) mustBe Some(event.emitterId)
    }
  }
}
