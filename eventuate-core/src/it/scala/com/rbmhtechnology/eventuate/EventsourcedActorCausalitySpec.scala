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
import akka.testkit.TestProbe

import org.scalatest._

import scala.collection.immutable.Seq
import scala.util._

object EventsourcedActorCausalitySpec {
  class Collaborator(val id: String, val eventLog: ActorRef, handles: Set[String], probe: ActorRef) extends EventsourcedActor {
    def onCommand = {
      case s: String => persist(s) {
        case Success(e) =>
        case Failure(e) => throw e
      }
    }

    def onEvent = {
      case s: String if handles.contains(s) =>
        probe ! ((s, lastVectorTimestamp, currentVectorTime))
    }
  }
}

trait EventsourcedActorCausalitySpec extends WordSpec with Matchers with MultiLocationSpec {
  import ReplicationIntegrationSpec.replicationConnection
  import EventsourcedActorCausalitySpec._

  def assertPartialOrder[A](events: Seq[A], sample: A*): Unit = {
    val indices = sample.map(events.indexOf)
    assert(indices == indices.sorted)
  }

  "Event-sourced actors" when {
    "located at multiple locations" can {
      "track causality" in {
        val logName = "L1"

        val locationA = location("A")
        val locationB = location("B")

        val endpointA = locationA.endpoint(Set(logName), Set(replicationConnection(locationB.port)))
        val endpointB = locationB.endpoint(Set(logName), Set(replicationConnection(locationA.port)))

        val logA = endpointA.logs(logName)
        val logB = endpointB.logs(logName)

        val logIdA = endpointA.logId(logName)
        val logIdB = endpointB.logId(logName)

        val probeA1 = new TestProbe(locationA.system)
        val probeA2 = new TestProbe(locationA.system)
        val probeA3 = new TestProbe(locationA.system)
        val probeB = new TestProbe(locationB.system)

        val actorA1 = locationA.system.actorOf(Props(new Collaborator("pa1", logA, Set("e1", "e2", "e5"), probeA1.ref)))
        val actorA2 = locationA.system.actorOf(Props(new Collaborator("pa2", logA, Set("e3", "e5", "e6"), probeA2.ref)))
        val actorA3 = locationA.system.actorOf(Props(new Collaborator("pa3", logA, Set("e4"), probeA3.ref)))
        val actorB = locationB.system.actorOf(Props(new Collaborator("pb", logB, Set("e1", "e6"), probeB.ref)))

        val expectedVectorTimeE1 = VectorTime.Zero.dotted(logIdB, 1L)
        val expectedVectorTimeE2 = VectorTime(logIdB -> 1L).dotted(logIdA, 2L)
        val expectedVectorTimeE3 = VectorTime.Zero.dotted(logIdA, 3L)
        val expectedVectorTimeE4 = VectorTime.Zero.dotted(logIdA, 4L)
        val expectedVectorTimeE5 = VectorTime(logIdA -> 2L, logIdB -> 1L).dotted(logIdA, 5L)
        val expectedVectorTimeE6 = VectorTime(logIdA -> 5L, logIdB -> 1L).dotted(logIdA, 6L)

        actorB ! "e1"
        probeA1.expectMsg(("e1", expectedVectorTimeE1, VectorTime(logIdB -> 1L)))
        probeB.expectMsg(("e1", expectedVectorTimeE1, VectorTime(logIdB -> 1L)))

        actorA1 ! "e2"
        probeA1.expectMsg(("e2", expectedVectorTimeE2, VectorTime(logIdA -> 2L, logIdB -> 1L)))

        actorA2 ! "e3"
        probeA2.expectMsg(("e3", expectedVectorTimeE3, VectorTime(logIdA -> 3L)))

        actorA3 ! "e4"
        probeA3.expectMsg(("e4", expectedVectorTimeE4, VectorTime(logIdA -> 4L)))

        actorA1 ! "e5"
        probeA1.expectMsg(("e5", expectedVectorTimeE5, VectorTime(logIdA -> 5L, logIdB -> 1L)))
        probeA2.expectMsg(("e5", expectedVectorTimeE5, VectorTime(logIdA -> 5L, logIdB -> 1L)))

        actorA2 ! "e6"
        probeA2.expectMsg(("e6", expectedVectorTimeE6, VectorTime(logIdA -> 6L, logIdB -> 1L)))
        probeB.expectMsg(("e6", expectedVectorTimeE6, VectorTime(logIdA -> 6L, logIdB -> 1L)))

        expectedVectorTimeE1 < expectedVectorTimeE2 should be(true)
        expectedVectorTimeE2 conc expectedVectorTimeE3 should be(true)
        expectedVectorTimeE3 conc expectedVectorTimeE4 should be(true)
        expectedVectorTimeE3 < expectedVectorTimeE4 should be(false)
      }
    }
    "located at a single location" can {
      "track causality" in {
        val logName = "L1"

        val locationA = location("A")
        val endpointA = locationA.endpoint(Set(logName), Set())

        val logA = endpointA.logs(logName)
        val logIdA = endpointA.logId(logName)

        val probeA = new TestProbe(locationA.system)
        val probeB = new TestProbe(locationA.system)
        val probeC = new TestProbe(locationA.system)

        val actorA = locationA.system.actorOf(Props(new Collaborator("PA", logA, Set("e1", "e3"), probeA.ref)))
        val actorB = locationA.system.actorOf(Props(new Collaborator("PB", logA, Set("e2", "e3"), probeB.ref)))
        val actorC = locationA.system.actorOf(Props(new Collaborator("PC", logA, Set("e1", "e2", "e3"), probeC.ref)))

        val expectedVectorTimeE1 = VectorTime.Zero.dotted(logIdA, 1L)
        val expectedVectorTimeE2 = VectorTime.Zero.dotted(logIdA, 2L)
        val expectedVectorTimeE3 = VectorTime(logIdA -> 2L).dotted(logIdA, 3L)

        actorA ! "e1"
        probeA.expectMsg(("e1", expectedVectorTimeE1, VectorTime(logIdA -> 1L)))
        probeC.expectMsg(("e1", expectedVectorTimeE1, VectorTime(logIdA -> 1L)))

        actorB ! "e2"
        probeB.expectMsg(("e2", expectedVectorTimeE2, VectorTime(logIdA -> 2L)))
        probeC.expectMsg(("e2", expectedVectorTimeE2, VectorTime(logIdA -> 2L)))

        actorC ! "e3"
        probeA.expectMsg(("e3", expectedVectorTimeE3, VectorTime(logIdA -> 3L)))
        probeB.expectMsg(("e3", expectedVectorTimeE3, VectorTime(logIdA -> 3L)))
        probeC.expectMsg(("e3", expectedVectorTimeE3, VectorTime(logIdA -> 3L)))

        expectedVectorTimeE1 conc expectedVectorTimeE2 should be(true)
        expectedVectorTimeE1 < expectedVectorTimeE3 should be(true)
        expectedVectorTimeE2 < expectedVectorTimeE3 should be(true)
      }
    }
  }
}
