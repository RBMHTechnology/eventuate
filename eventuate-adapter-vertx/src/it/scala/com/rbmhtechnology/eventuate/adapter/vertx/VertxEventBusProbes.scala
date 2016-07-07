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

import akka.testkit.{TestKit, TestProbe}
import io.vertx.core.eventbus.Message
import org.scalatest.{BeforeAndAfterEach, Suite}
import utilities.VertxEventBusMessage

trait VertxEventBusProbes extends BeforeAndAfterEach {
  this: TestKit with Suite with VertxEnvironment =>

  import VertxHandlerConverters._

  var endpoint1Probe: TestProbe = _
  var endpoint2Probe: TestProbe = _
  var endpoint3Probe: TestProbe = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    endpoint1Probe = eventBusProbe(endpoint1)
    endpoint2Probe = eventBusProbe(endpoint2)
    endpoint3Probe = eventBusProbe(endpoint3)
  }

  def eventBusProbe(endpoint: String): TestProbe = {
    val probe = TestProbe()
    val handler = (m: Message[String]) => probe.ref ! VertxEventBusMessage(m.body(), m)
    vertx.eventBus().consumer[String](endpoint, handler.asVertxHandler)
    probe
  }
}
