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

import akka.actor._

import org.scalatest._

import scala.concurrent.Future

trait ReplicationCycleSpec extends WordSpec with Matchers with MultiLocationSpec {
  import ReplicationIntegrationSpec._

  var locationA: Location = _
  var locationB: Location = _
  var locationC: Location = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    locationA = location("A")
    locationB = location("B")
    locationC = location("C")
  }

  def testReplication(endpointA: ReplicationEndpoint, endpointB: ReplicationEndpoint, endpointC: ReplicationEndpoint): Unit = {
    val actorA = locationA.system.actorOf(Props(new ReplicatedActor("pa", endpointA.logs("L1"), locationA.probe.ref)))
    val actorB = locationB.system.actorOf(Props(new ReplicatedActor("pb", endpointB.logs("L1"), locationB.probe.ref)))
    val actorC = locationC.system.actorOf(Props(new ReplicatedActor("pc", endpointC.logs("L1"), locationC.probe.ref)))

    actorA ! "a1"
    actorB ! "b1"
    actorC ! "c1"

    val expected = List("a1", "b1", "c1")

    val eventsA = locationA.probe.expectMsgAllOf(expected: _*)
    val eventsB = locationB.probe.expectMsgAllOf(expected: _*)
    val eventsC = locationC.probe.expectMsgAllOf(expected: _*)

    val num = 100
    val expectedA = 2 to num map { i => s"a$i" }
    val expectedB = 2 to num map { i => s"b$i" }
    val expectedC = 2 to num map { i => s"c$i" }
    val all = expectedA ++ expectedB ++ expectedC

    import endpointA.system.dispatcher

    Future(expectedA.foreach(s => actorA ! s))
    Future(expectedB.foreach(s => actorB ! s))
    Future(expectedC.foreach(s => actorC ! s))

    locationA.probe.expectMsgAllOf(all: _*)
    locationB.probe.expectMsgAllOf(all: _*)
    locationC.probe.expectMsgAllOf(all: _*)
  }

  "Event log replication" must {
    "support bi-directional cyclic replication networks" in {
      testReplication(
        locationA.endpoint(Set("L1"), Set(replicationConnection(locationB.port), replicationConnection(locationC.port))),
        locationB.endpoint(Set("L1"), Set(replicationConnection(locationA.port), replicationConnection(locationC.port))),
        locationC.endpoint(Set("L1"), Set(replicationConnection(locationA.port), replicationConnection(locationB.port))))

    }
    "support uni-directional cyclic replication networks" in {
      testReplication(
        locationA.endpoint(Set("L1"), Set(replicationConnection(locationB.port))),
        locationB.endpoint(Set("L1"), Set(replicationConnection(locationC.port))),
        locationC.endpoint(Set("L1"), Set(replicationConnection(locationA.port))))
    }
  }
}
