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

package com.rbmhtechnology.eventuate

import com.rbmhtechnology.eventuate.log.EventLogWriter
import com.rbmhtechnology.eventuate.utilities.AwaitHelper
import com.typesafe.config.ConfigFactory

import org.scalatest.{ Matchers, WordSpec }

object DeleteEventsSpecLeveldb {
  def emitter(endpoint: ReplicationEndpoint, logName: String): EventLogWriter =
    new EventLogWriter(s"${endpoint.id}_Emitter", endpoint.logs(logName))(endpoint.system)

  val config = ConfigFactory.parseString(
    """
      |eventuate.log.replication.retry-delay = 1s
      |eventuate.log.replication.remote-read-timeout = 2s
      |eventuate.log.recovery.remote-operation-retry-max = 10
      |eventuate.log.recovery.remote-operation-retry-delay = 1s
      |eventuate.log.recovery.remote-operation-timeout = 1s
    """.stripMargin)

  val L1 = "L1"
}

class DeleteEventsSpecLeveldb extends WordSpec with Matchers with MultiLocationSpecLeveldb {
  import DeleteEventsSpecLeveldb._
  import ReplicationIntegrationSpec.replicationConnection

  "Deleting events" must {
    "not replay deleted events on restart" in {
      def newLocationA = location("A", customConfig = DeleteEventsSpecLeveldb.config)
      def newEndpointA(l: Location) = l.endpoint(Set(L1), Set(), activate = false)

      val locationA1 = newLocationA
      val endpointA1 = newEndpointA(locationA1)

      val listenerA = locationA1.listener(endpointA1.logs(L1))
      val emitterA = emitter(endpointA1, L1)

      emitterA.write(0 to 5)
      listenerA.waitForMessage(5)

      endpointA1.delete(L1, 3, Set.empty).await shouldBe 3
      locationA1.terminate().await

      val locationA2 = newLocationA
      def endpointA2 = newEndpointA(locationA2)

      locationA2.listener(endpointA2.logs(L1)).expectMsgAllOf(3 to 5: _*)
    }
  }

  "Conditionally deleting events" must {
    "keep event available for corresponding remote log" in {
      val locationA = location("A", customConfig = DeleteEventsSpecLeveldb.config)
      val locationB = location("B", customConfig = DeleteEventsSpecLeveldb.config)
      val locationC = location("C", customConfig = DeleteEventsSpecLeveldb.config)

      val endpointA = locationA.endpoint(Set(L1), Set(replicationConnection(locationB.port), replicationConnection(locationC.port)), activate = false)
      val endpointB = locationB.endpoint(Set(L1), Set(replicationConnection(locationA.port)), activate = false)
      val endpointC = locationC.endpoint(Set(L1), Set(replicationConnection(locationA.port)), activate = false)

      val emitterA = emitter(endpointA, L1)

      val listenerA = locationA.listener(endpointA.logs(L1))
      val listenerB = locationB.listener(endpointB.logs(L1))
      val listenerC = locationC.listener(endpointC.logs(L1))

      emitterA.write(0 to 5)
      listenerA.waitForMessage(5)

      endpointA.delete(L1, 3, Set(endpointB.id, endpointC.id)).await shouldBe 3

      endpointA.activate()
      endpointB.activate()
      listenerB.expectMsgAllOf(0 to 5: _*)

      endpointC.activate()
      listenerC.expectMsgAllOf(0 to 5: _*)
    }
  }
}
