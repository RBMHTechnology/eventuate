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

import java.util.UUID

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.ReplicationIntegrationSpec.replicationConnection
import com.rbmhtechnology.eventuate.utilities._
import org.apache.commons.io.FileUtils
import org.scalatest.Matchers
import org.scalatest.WordSpec

import scala.concurrent.duration.DurationInt

object PersistOnEventWithRecoverySpecLeveldb {
  class OnBEmitRandomActor(val eventLog: ActorRef, probe: TestProbe) extends EventsourcedActor with PersistOnEvent {

    override def id = getClass.getName

    override def onCommand = Actor.emptyBehavior

    override def onEvent = {
      case "A"          =>
      case "B"          => persistOnEvent(UUID.randomUUID().toString)
      case uuid: String => probe.ref ! uuid
    }
  }

  def persistOnEventProbe(locationA1: Location, log: ActorRef) = {
    val probe = locationA1.probe
    locationA1.system.actorOf(Props(new OnBEmitRandomActor(log, probe)))
    probe
  }

  val noMsgTimeout = 100.millis
}

class PersistOnEventWithRecoverySpecLeveldb extends WordSpec with Matchers with MultiLocationSpecLeveldb {
  import RecoverySpecLeveldb._
  import PersistOnEventWithRecoverySpecLeveldb._

  override val logFactory: String => Props =
    id => SingleLocationSpecLeveldb.TestEventLog.props(id, batching = true)

  "An EventsourcedActor with PersistOnEvent" must {
    "not re-attempt persistence on successful write after reordering of events through disaster recovery" in {
      val locationB = location("B", customConfig = RecoverySpecLeveldb.config)
      def newLocationA = location("A", customConfig = RecoverySpecLeveldb.config)
      val locationA1 = newLocationA

      val endpointB = locationB.endpoint(Set("L1"), Set(replicationConnection(locationA1.port)))
      def newEndpointA(l: Location, activate: Boolean) = l.endpoint(Set("L1"), Set(replicationConnection(locationB.port)), activate = activate)
      val endpointA1 = newEndpointA(locationA1, activate = true)

      val targetA = endpointA1.target("L1")
      val logDirA = logDirectory(targetA)
      val targetB = endpointB.target("L1")
      val a1Probe = persistOnEventProbe(locationA1, targetA.log)

      write(targetA, List("A"))
      write(targetB, List("B"))
      val event = a1Probe.expectMsgClass(classOf[String])
      assertConvergence(Set("A", "B", event), endpointA1, endpointB)

      locationA1.terminate().await
      FileUtils.deleteDirectory(logDirA)

      val locationA2 = newLocationA
      val endpointA2 = newEndpointA(locationA2, activate = false)
      endpointA2.recover().await

      val a2Probe = persistOnEventProbe(locationA2, endpointA2.logs("L1"))
      a2Probe.expectMsg(event)
      a2Probe.expectNoMsg(noMsgTimeout)
      assertConvergence(Set("A", "B", event), endpointA2, endpointB)
    }
  }
}
