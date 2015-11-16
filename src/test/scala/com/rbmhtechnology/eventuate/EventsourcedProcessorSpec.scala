/*
 * Copyright (C) 2015 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.eventuate

import akka.actor._
import akka.testkit._

import com.rbmhtechnology.eventuate.EventsourcedViewSpec._

import org.scalatest._

import scala.collection.immutable.Seq

object EventsourcedProcessorSpec {
  import DurableEvent._

  val eventA = event("a", 1)
  val eventB = event("b", 2)
  val eventC = event("c", 3)

  val eventA1 = update(eventA.copy("a-1"))
  val eventA2 = update(eventA.copy("a-2"))
  val eventB1 = update(eventB.copy("b-1"))
  val eventB2 = update(eventB.copy("b-2"))
  val eventC1 = update(eventC.copy("c-1"))
  val eventC2 = update(eventC.copy("c-2"))

  class TestProcessor(srcProbe: ActorRef, trgProbe: ActorRef, appProbe: ActorRef, sce: Boolean) extends EventsourcedProcessor {
    override val id = emitterIdB
    override val eventLog = srcProbe
    override val targetEventLog = trgProbe
    override val replayChunkSizeMax = 2
    override def sharedClockEntry = sce

    override val onCommand: Receive = {
      case cmd => appProbe ! cmd
    }

    override val processEvent: Process = {
      case evt: String => Seq(s"${evt}-1", s"${evt}-2")
    }

    override def writeSuccess(result: Long): Unit = {
      appProbe ! result
      super.writeSuccess(result)
    }

    override def writeFailure(cause: Throwable): Unit = {
      appProbe ! cause
      super.writeFailure(cause)
    }
  }

  def update(event: DurableEvent): DurableEvent =
    event.copy(emitterId = emitterIdB, processId = UndefinedLogId, localLogId = UndefinedLogId, localSequenceNr = UndefinedSequenceNr)
}

class EventsourcedProcessorSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  import EventsourcedProcessorSpec._
  import EventsourcingProtocol._
  import ReplicationProtocol._

  var instanceId: Int = _
  var srcProbe: TestProbe = _
  var trgProbe: TestProbe = _
  var appProbe: TestProbe = _

  override def beforeEach(): Unit = {
    instanceId = EventsourcedView.instanceIdCounter.get
    srcProbe = TestProbe()
    trgProbe = TestProbe()
    appProbe = TestProbe()
  }

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  def unrecoveredProcessor(stateful: Boolean = true, sharedClockEntry: Boolean = true): ActorRef =
    if (stateful) system.actorOf(Props(new TestProcessor(srcProbe.ref, trgProbe.ref, appProbe.ref, sharedClockEntry)))
    else          system.actorOf(Props(new TestProcessor(srcProbe.ref, trgProbe.ref, appProbe.ref, sharedClockEntry) with StatelessProcessor))

  def recoveredProcessor(stateful: Boolean = true, sharedClockEntry: Boolean = true): ActorRef =
    processRecover(unrecoveredProcessor(stateful, sharedClockEntry), stateful)

  def processRecover(actor: ActorRef, stateful: Boolean = true): ActorRef = {
    processRead(0)
    if (stateful) {
      processLoad(actor)
      processReplay(actor, 1)
    } else {
      processReplay(actor, 1)
    }
    actor
  }

  def processLoad(actor: ActorRef, instanceId: Int = instanceId): Unit = {
    srcProbe.expectMsg(LoadSnapshot(emitterIdB, actor, instanceId))
    actor ! LoadSnapshotSuccess(None, instanceId)
  }

  def processReplay(actor: ActorRef, fromSequenceNr: Long, instanceId: Int = instanceId): Unit =
    processReplay(actor, fromSequenceNr, fromSequenceNr - 1L, instanceId)

  def processReplay(actor: ActorRef, fromSequenceNr: Long, storedSequenceNr: Long, instanceId: Int): Unit = {
    srcProbe.expectMsg(Replay(fromSequenceNr, 2, actor, instanceId))
    actor ! ReplaySuccess(instanceId)
    appProbe.expectMsg(storedSequenceNr)
  }

  def processRead(progress: Long, success: Boolean = true): Unit = {
    trgProbe.expectMsg(GetReplicationProgress(emitterIdB))
    if (success) processResult(GetReplicationProgressSuccess(emitterIdB, progress, VectorTime()))
    else processResult(GetReplicationProgressFailure(boom))
  }

  def processWrite(progress: Long, events: Seq[DurableEvent], success: Boolean = true): Unit = {
    trgProbe.expectMsg(ReplicationWrite(events, emitterIdB, progress, VectorTime()))
    if (success) {
      processResult(ReplicationWriteSuccess(events.size, progress, VectorTime()))
      appProbe.expectMsg(progress)
    } else {
      processResult(ReplicationWriteFailure(boom))
      appProbe.expectMsg(boom)
    }
  }

  def processResult(result: Any): Unit =
    trgProbe.sender() ! Status.Success(result)

  "A stateful EventsourcedProcessor" must {
    "recover" in {
      recoveredProcessor()
    }
    "restart on failed read by default" in {
      val actor = unrecoveredProcessor()
      processRead(0, success = false)
      processRead(0)
      processLoad(actor, instanceId + 1)
      processReplay(actor, 1, instanceId + 1)
    }
    "recover on failed write by default" in {
      val actor = unrecoveredProcessor()
      processRead(0)
      processLoad(actor)
      processReplay(actor, 1)
      actor ! Written(eventA)
      processWrite(1, Seq(eventA1, eventA2), success = false)
      processRead(0)
      processLoad(actor, instanceId + 1)
      processReplay(actor, 1, instanceId + 1)
      actor ! Written(eventA)
      processWrite(1, Seq(eventA1, eventA2))
    }
    "write to target log during and after recovery" in {
      val actor = unrecoveredProcessor()
      processRead(0)
      processLoad(actor)
      srcProbe.expectMsg(Replay(1, 2, actor, instanceId))
      actor ! Replaying(eventA, instanceId)
      actor ! Replaying(eventB, instanceId)
      actor.tell(ReplaySuspended(instanceId), srcProbe.ref)
      processWrite(2, Seq(eventA1, eventA2, eventB1, eventB2))
      srcProbe.expectMsg(ReplayNext(2, instanceId))
      actor ! Replaying(eventC, instanceId)
      actor ! ReplaySuccess(instanceId)
      processWrite(3, Seq(eventC1, eventC2))
    }
    "write to target log and process concurrently" in {
      val actor = recoveredProcessor()
      actor ! Written(eventA)
      actor ! Written(eventB)
      actor ! Written(eventC)
      processWrite(1, Seq(eventA1, eventA2))
      processWrite(3, Seq(eventB1, eventB2, eventC1, eventC2))
    }
    "exclude events from write with sequenceNr <= storedSequenceNr" in {
      val actor = unrecoveredProcessor()
      processRead(3)
      processLoad(actor)
      processReplay(actor, 1, 3, instanceId)
      actor ! Written(eventA)
      appProbe.expectMsg(3)
    }
    "include events to write with sequenceNr > storedSequenceNr" in {
      val actor = unrecoveredProcessor()
      processRead(2)
      processLoad(actor)
      processReplay(actor, 1, 2, instanceId)
      actor ! Written(eventA)
      appProbe.expectMsg(2)
      actor ! Written(eventB)
      appProbe.expectMsg(2)
      actor ! Written(eventC)
      processWrite(3, Seq(eventC1, eventC2))
    }
  }

  "A stateful EventsourcedProcessor" when {
    "using its own vector clock entry" must {
      "update the process id and vector time of emitted events" in {
        val actor = recoveredProcessor(sharedClockEntry = false)
        actor ! Written(eventA)
        processWrite(1, Seq(
          eventA1.copy(processId = emitterIdB, vectorTimestamp = eventA.vectorTimestamp.merge(VectorTime(emitterIdB -> 2L))),
          eventA2.copy(processId = emitterIdB, vectorTimestamp = eventA.vectorTimestamp.merge(VectorTime(emitterIdB -> 3L)))))
      }
    }
  }

  "A stateless EventsourcedProcessor" must {
    "resume" in {
      recoveredProcessor(stateful = false)
    }
    "resume on failed read by default" in {
      val actor = unrecoveredProcessor(stateful = false)
      processRead(3)
      processReplay(actor, 4)
    }
    "resume on failed write by default" in {
      val actor = recoveredProcessor(stateful = false)
      actor ! Written(eventA)
      processWrite(1, Seq(eventA1, eventA2), success = false)
      processRead(3)
      processReplay(actor, 4, instanceId + 1)
    }
  }
}