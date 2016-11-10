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

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import com.rbmhtechnology.eventuate.adapter.vertx.utilities.EventBusMessage
import io.vertx.core.eventbus.Message
import org.scalatest.{ BeforeAndAfterEach, Suite }

trait VertxEventBusProbes extends BeforeAndAfterEach {
  this: TestKit with Suite with VertxEnvironment =>

  import VertxHandlerConverters._

  var endpoint1: EventBusEndpoint = _
  var endpoint2: EventBusEndpoint = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    endpoint1 = EventBusEndpoint.withId("1")
    endpoint2 = EventBusEndpoint.withId("2")
  }

  def eventBusProbe(endpoint: String): TestProbe = {
    val probe = TestProbe()
    val handler = (m: Message[String]) => probe.ref ! EventBusMessage(m.body(), m, endpoint)
    vertx.eventBus().consumer[String](endpoint, handler.asVertxHandler)
    probe
  }

  object EventBusEndpoint {
    def apply(address: String): EventBusEndpoint =
      new EventBusEndpoint(address, eventBusProbe(address))

    def withId(id: String): EventBusEndpoint =
      apply(endpointAddress(id))
  }

  case class EventBusEndpoint(address: String, probe: TestProbe)
}