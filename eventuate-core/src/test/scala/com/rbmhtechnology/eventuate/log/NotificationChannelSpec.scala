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

package com.rbmhtechnology.eventuate.log

import akka.actor._
import akka.testkit._

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.ReplicationFilter._
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.log.NotificationChannel._

import org.scalatest._

import scala.collection.immutable.Seq
import scala.concurrent.duration._

object NotificationChannelSpec {
  class PayloadFilter(accept: Any) extends ReplicationFilter {
    override def apply(event: DurableEvent): Boolean =
      event.payload == accept
  }

  val timeout = 0.2.seconds

  val sourceLogId = "slid"
  val targetLogId1 = "tlid1"
  val targetLogId2 = "tlid2"

  def event(payload: Any, vectorTimestamp: VectorTime): DurableEvent =
    DurableEvent(payload, "emitter", vectorTimestamp = vectorTimestamp)

  def vectorTime(s: Long, t1: Long, t2: Long) =
    VectorTime(sourceLogId -> s, targetLogId1 -> t1, targetLogId2 -> t2)
}

class NotificationChannelSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  import NotificationChannelSpec._

  private var channel: ActorRef = _
  private var probe: TestProbe = _

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  override def beforeEach(): Unit = {
    channel = system.actorOf(Props(new NotificationChannel(sourceLogId)))
    probe = TestProbe()
  }

  def sourceRead(targetLogId: String, targetVersionVector: VectorTime, filter: ReplicationFilter = NoFilter, probe: TestProbe = probe): Unit = {
    channel ! sourceReadMessage(targetLogId, targetVersionVector, filter, probe)
    channel ! sourceReadSuccessMessage(targetLogId)
  }

  def sourceReadMessage(targetLogId: String, targetVersionVector: VectorTime, filter: ReplicationFilter = NoFilter, probe: TestProbe = probe): ReplicationRead =
    ReplicationRead(1L, 10, 100, filter, targetLogId, probe.ref, targetVersionVector)

  def sourceReadSuccessMessage(targetLogId: String): ReplicationReadSuccess =
    ReplicationReadSuccess(Nil, 10L, 9L, targetLogId, VectorTime.Zero)

  def sourceUpdate(events: Seq[DurableEvent]): Unit =
    channel ! Updated(events)

  def replicaVersionUpdate(targetLogId: String, targetVersionVector: VectorTime): Unit =
    channel ! ReplicationWrite(Nil, Map(targetLogId -> ReplicationMetadata(10L, targetVersionVector)))

  "A notification channel" must {
    "send a notification if an update is not the causal past of the target" in {
      sourceRead(targetLogId1, vectorTime(0, 1, 0))
      sourceUpdate(Seq(event("a", vectorTime(1, 0, 0))))

      probe.expectMsg(ReplicationDue)
      probe.expectNoMsg(timeout)
    }
    "send a notification if part of an update is not in the causal past of the target" in {
      sourceRead(targetLogId1, vectorTime(0, 1, 0))
      sourceUpdate(Seq(
        event("a", vectorTime(0, 1, 0)),
        event("b", vectorTime(1, 0, 0))))

      probe.expectMsg(ReplicationDue)
      probe.expectNoMsg(timeout)
    }
    "not send a notification if an update is in the causal past of the target" in {
      sourceRead(targetLogId1, vectorTime(0, 2, 0))
      sourceUpdate(Seq(
        event("a", vectorTime(0, 1, 0)),
        event("b", vectorTime(0, 2, 0))))

      probe.expectNoMsg(timeout)
    }
    "not send a notification if an update does not pass the target's replication filter" in {
      sourceRead(targetLogId1, vectorTime(0, 1, 0), new PayloadFilter("a"))
      sourceUpdate(Seq(event("b", vectorTime(1, 0, 0))))

      probe.expectNoMsg(timeout)
    }
    "not send a notification if a target is currently running a replication read" in {
      channel ! sourceReadMessage(targetLogId1, vectorTime(0, 1, 0))
      sourceUpdate(Seq(
        event("a", vectorTime(1, 0, 0)),
        event("b", vectorTime(2, 0, 0))))

      probe.expectNoMsg(timeout)
    }
    "apply target version vector updates " in {
      sourceRead(targetLogId1, vectorTime(0, 1, 0))
      sourceUpdate(Seq(event("a", vectorTime(0, 2, 0))))

      probe.expectMsg(ReplicationDue)
      probe.expectNoMsg(timeout)

      replicaVersionUpdate(targetLogId1, vectorTime(0, 2, 0))
      sourceUpdate(Seq(event("a", vectorTime(0, 2, 0))))

      probe.expectNoMsg(timeout)
    }
    "send notification to several targets" in {
      sourceRead(targetLogId1, vectorTime(0, 1, 0))
      sourceRead(targetLogId2, vectorTime(0, 0, 1))
      sourceUpdate(Seq(event("a", vectorTime(1, 0, 0))))

      probe.expectMsg(ReplicationDue)
      probe.expectMsg(ReplicationDue)
      probe.expectNoMsg(timeout)
    }
  }
}
