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

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import com.rbmhtechnology.eventuate.ReplicationIntegrationSpec.replicationConnection
import org.scalatest.Matchers
import org.scalatest.WordSpec

import scala.collection.immutable.Seq

trait EventsourcedProcessorWithReplicationIntegrationSpec extends WordSpec with Matchers with MultiLocationSpec {

  import EventsourcedProcessorWithReplicationIntegrationSpec._

  "EventsourcedProcessors" must {
    "support replication of input and output events if there are processors on each location" in {
      val locationA = location("A")
      val locationB = location("B")

      val endpointA = locationA.endpoint(Set("L1", "L2"), Set(replicationConnection(locationB.port)))
      val endpointB = locationB.endpoint(Set("L1", "L2"), Set(replicationConnection(locationA.port)))
      val logAL1 = endpointA.logs("L1")
      val logAL2 = endpointA.logs("L2")
      val logBL1 = endpointB.logs("L1")
      val logBL2 = endpointB.logs("L2")

      val writerAL1 = locationA.writer(logAL1)
      val writerBL1 = locationB.writer(logBL1)
      val listenerAL2 = locationA.listener(logAL2)
      val listenerBL1 = locationB.listener(logBL1)
      val listenerBL2 = locationB.listener(logBL2)
      localEventsProcessor("B_processor", logBL1, logBL2, endpointB.logId("L1"))(locationB.system)

      writerAL1.write(Seq("a1"))
      listenerBL1.waitForMessage("a1")
      writerBL1.write(Seq("b1"))
      listenerBL2.waitForMessage("b1")
      listenerAL2.waitForMessage("b1")

      localEventsProcessor("A_processor", logAL1, logAL2, endpointA.logId("L1"))(locationA.system)

      listenerAL2.waitForMessage("a1")
    }
  }
}

object EventsourcedProcessorWithReplicationIntegrationSpec {

  def localEventsProcessor(id: String, eventLog: ActorRef, targetEventLog: ActorRef, sourceProcessId: String)(implicit system: ActorSystem): ActorRef =
    system.actorOf(Props(new LocalEventsProcessor(id, eventLog, targetEventLog, sourceProcessId)))

  class LocalEventsProcessor(val id: String, val eventLog: ActorRef, val targetEventLog: ActorRef, val sourceProcessId: String) extends EventsourcedProcessor {
    override def processEvent: Process = {
      case event if lastProcessId == sourceProcessId => List(event)
    }

    override def onCommand: Receive = Actor.emptyBehavior
  }
}
