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
import akka.testkit._

import com.rbmhtechnology.eventuate.EventsourcedViewSpec._
import com.typesafe.config.ConfigFactory

import org.scalatest._

import scala.collection.immutable.Seq

object EventsourcedProcessorSpec {
  import DurableEvent._

  val config = ConfigFactory.parseString("eventuate.log.write-batch-size = 10")

  val eventA = event("a", 1)
  val eventB = event("b", 2)
  val eventC = event("c", 3)

  val eventA1 = update(eventA.copy("a-1"))
  val eventA2 = update(eventA.copy("a-2"))
  val eventB1 = update(eventB.copy("b-1"))
  val eventB2 = update(eventB.copy("b-2"))
  val eventC1 = update(eventC.copy("c-1"))
  val eventC2 = update(eventC.copy("c-2"))

  class StatelessTestProcessor(srcProbe: ActorRef, trgProbe: ActorRef, appProbe: ActorRef) extends EventsourcedProcessor {
    override val id = emitterIdB
    override val eventLog = srcProbe
    override val targetEventLog = trgProbe
    override val replayBatchSize = 2

    private var processedEvents: Vector[String] = Vector.empty

    override def onCommand = {
      case "state" => appProbe ! processedEvents
    }

    override val processEvent: Process = {
      case "x" =>
        Vector.fill(4)("x")
      case "y" =>
        Vector.fill(5)("y")
      case "z" =>
        Vector.fill(11)("z")
      case evt: String =>
        processedEvents = processedEvents :+ evt
        Seq(s"${evt}-1", s"${evt}-2")
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

  class StatefulTestProcessor(srcProbe: ActorRef, trgProbe: ActorRef, appProbe: ActorRef)
    extends StatelessTestProcessor(srcProbe, trgProbe, appProbe) with StatefulProcessor

  def update(event: DurableEvent): DurableEvent =
    event.copy(emitterId = emitterIdB, processId = UndefinedLogId, localLogId = UndefinedLogId, localSequenceNr = UndefinedSequenceNr)
}

class EventsourcedProcessorSpec extends TestKit(ActorSystem("test", EventsourcedProcessorSpec.config)) with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
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

  def unrecoveredStatelessProcessor(): ActorRef =
    system.actorOf(Props(new StatelessTestProcessor(srcProbe.ref, trgProbe.ref, appProbe.ref)))

  def unrecoveredStatefulProcessor(): ActorRef =
    system.actorOf(Props(new StatefulTestProcessor(srcProbe.ref, trgProbe.ref, appProbe.ref)))

  def recoveredStatelessProcessor(): ActorRef = {
    val actor = unrecoveredStatelessProcessor()
    processRead(0)
    processReplay(actor, 1)
    actor
  }

  def recoveredStatefulProcessor(): ActorRef = {
    val actor = unrecoveredStatefulProcessor()
    processRead(0)
    processLoad(actor)
    processReplay(actor, 1)
    actor
  }

  def processLoad(actor: ActorRef, instanceId: Int = instanceId): Unit = {
    srcProbe.expectMsg(LoadSnapshot(emitterIdB, instanceId))
    srcProbe.sender() ! LoadSnapshotSuccess(None, instanceId)
  }

  def processReplay(actor: ActorRef, fromSequenceNr: Long, instanceId: Int = instanceId): Unit =
    processReplay(actor, fromSequenceNr, fromSequenceNr - 1L, instanceId)

  def processReplay(actor: ActorRef, fromSequenceNr: Long, storedSequenceNr: Long, instanceId: Int): Unit = {
    srcProbe.expectMsg(Replay(fromSequenceNr, 2, Some(actor), instanceId))
    srcProbe.sender() ! ReplaySuccess(Nil, 0L, instanceId)
  }

  def processRead(progress: Long, success: Boolean = true): Unit = {
    trgProbe.expectMsg(GetReplicationProgress(emitterIdB))
    if (success) processResult(GetReplicationProgressSuccess(emitterIdB, progress, VectorTime()))
    else processResult(GetReplicationProgressFailure(TestException))
  }

  def processPartialWrite(progress: Long, events: Seq[DurableEvent], success: Boolean = true): Unit = {
    trgProbe.expectMsg(ReplicationWrite(events, Map(emitterIdB -> ReplicationMetadata(progress, VectorTime.Zero))))
    if (success) {
      processResult(ReplicationWriteSuccess(events, Map(emitterIdB -> ReplicationMetadata(progress, VectorTime()))))
    } else {
      processResult(ReplicationWriteFailure(TestException))
    }
  }

  def processWrite(progress: Long, events: Seq[DurableEvent], success: Boolean = true): Unit = {
    processPartialWrite(progress, events, success)
    if (success) {
      appProbe.expectMsg(progress)
    } else {
      appProbe.expectMsg(TestException)
    }
  }

  def processResult(result: Any): Unit =
    trgProbe.sender() ! Status.Success(result)

  "A StatefulProcessor" must {
    "recover" in {
      recoveredStatefulProcessor()
    }
    "restart on failed read by default" in {
      val actor = unrecoveredStatefulProcessor()
      processRead(0, success = false)
      processRead(0)
      processLoad(actor, instanceId + 1)
      processReplay(actor, 1, instanceId + 1)
    }
    "recover on failed write by default" in {
      val actor = unrecoveredStatefulProcessor()
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
    "write to target log during recovery" in {
      val actor = unrecoveredStatefulProcessor()
      processRead(0)
      processLoad(actor)
      srcProbe.expectMsg(Replay(1, 2, Some(actor), instanceId))
      srcProbe.sender() ! ReplaySuccess(List(eventA, eventB), eventB.localSequenceNr, instanceId)
      processWrite(2, Seq(eventA1, eventA2, eventB1, eventB2))
      srcProbe.expectMsg(Replay(eventB.localSequenceNr + 1L, 2, None, instanceId))
      srcProbe.sender() ! ReplaySuccess(List(eventC), eventC.localSequenceNr, instanceId)
      processWrite(3, Seq(eventC1, eventC2))
      srcProbe.expectMsg(Replay(eventC.localSequenceNr + 1L, 2, None, instanceId))
      srcProbe.sender() ! ReplaySuccess(Nil, eventC.localSequenceNr, instanceId)
    }
    "write to target log and process concurrently" in {
      val actor = recoveredStatefulProcessor()
      actor ! Written(eventA)
      actor ! Written(eventB)
      actor ! Written(eventC)
      processWrite(1, Seq(eventA1, eventA2))
      processWrite(3, Seq(eventB1, eventB2, eventC1, eventC2))
    }
    "exclude events from write with sequenceNr <= storedSequenceNr" in {
      val actor = unrecoveredStatefulProcessor()
      processRead(3)
      processLoad(actor)
      processReplay(actor, 1, 3, instanceId)
      actor ! Written(eventA)
      appProbe.expectMsg(3)
    }
    "include events to write with sequenceNr > storedSequenceNr" in {
      val actor = unrecoveredStatefulProcessor()
      processRead(2)
      processLoad(actor)
      processReplay(actor, 1, 2, instanceId)
      actor ! Written(eventA)
      appProbe.expectMsg(2)
      actor ! Written(eventB)
      appProbe.expectMsg(2)
      actor ! Written(eventC)
      processWrite(3, Seq(eventC1, eventC2))
      actor ! "state"
      appProbe.expectMsg(Vector("a", "b", "c"))
    }
    "write events with current vector time" in {
      val actor = recoveredStatefulProcessor()
      actor ! Written(eventA.copy(vectorTimestamp = timestamp(1, 0)))
      actor ! Written(eventB.copy(vectorTimestamp = timestamp(0, 1)))
      processWrite(1, Seq(
        eventA1.copy(vectorTimestamp = timestamp(1, 0)),
        eventA2.copy(vectorTimestamp = timestamp(1, 0))))
      processWrite(2, Seq(
        eventB1.copy(vectorTimestamp = timestamp(1, 1)),
        eventB2.copy(vectorTimestamp = timestamp(1, 1))))
    }
  }

  "An EventsourcedProcessor" must {
    "resume" in {
      recoveredStatelessProcessor()
    }
    "resume on failed read by default" in {
      val actor = unrecoveredStatelessProcessor()
      processRead(3)
      processReplay(actor, 4)
    }
    "resume on failed write by default" in {
      val actor = recoveredStatelessProcessor()
      actor ! Written(eventA)
      processWrite(1, Seq(eventA1, eventA2), success = false)
      processRead(3)
      processReplay(actor, 4, instanceId + 1)
    }
    "write events with source event vector time" in {
      val actor = recoveredStatelessProcessor()
      actor ! Written(eventA.copy(vectorTimestamp = timestamp(1, 0)))
      actor ! Written(eventB.copy(vectorTimestamp = timestamp(0, 1)))
      processWrite(1, Seq(
        eventA1.copy(vectorTimestamp = timestamp(1, 0)),
        eventA2.copy(vectorTimestamp = timestamp(1, 0))))
      processWrite(2, Seq(
        eventB1.copy(vectorTimestamp = timestamp(0, 1)),
        eventB2.copy(vectorTimestamp = timestamp(0, 1))))
    }
    "write events in multiple batches if the number of generated events during processing since the last write is greater than settings.writeBatchSize" in {
      val actor = recoveredStatelessProcessor()

      val evt1 = event("x", 1).copy(vectorTimestamp = timestamp(1, 0))
      val evt2 = event("x", 2).copy(vectorTimestamp = timestamp(2, 0))
      val evt3 = event("x", 3).copy(vectorTimestamp = timestamp(3, 0))
      val evt4 = event("x", 4).copy(vectorTimestamp = timestamp(4, 0))

      actor ! Written(evt1) // ensure that a write is in progress when processing the next events
      actor ! Written(evt2)
      actor ! Written(evt3)
      actor ! Written(evt4)

      // process first write
      processWrite(1, Seq.fill(4)(update(evt1)))

      // process remaining writes
      processPartialWrite(1, Seq.fill(4)(update(evt2)) ++ Seq.fill(4)(update(evt3)))
      processWrite(4, Seq.fill(4)(update(evt4)))
    }
    "write events in a single batch if the number of generated events during processing since the last write is equal to settings.writeBatchSize" in {
      val actor = recoveredStatelessProcessor()

      val evt1 = event("x", 1).copy(vectorTimestamp = timestamp(1, 0))
      val evt2 = event("y", 2).copy(vectorTimestamp = timestamp(2, 0))
      val evt3 = event("y", 3).copy(vectorTimestamp = timestamp(3, 0))

      actor ! Written(evt1) // ensure that a write is in progress when processing the next events
      actor ! Written(evt2)
      actor ! Written(evt3)

      // process first write
      processWrite(1, Seq.fill(4)(update(evt1)))

      // process remaining writes
      processWrite(3, Seq.fill(5)(update(evt2)) ++ Seq.fill(5)(update(evt3)))
    }
    "allow batch sizes greater than settings.writeBatchSize if that batch was generated from a single input event (case 1)" in {
      val actor = recoveredStatelessProcessor()

      val evt1 = event("x", 1).copy(vectorTimestamp = timestamp(1, 0))
      val evt2 = event("x", 2).copy(vectorTimestamp = timestamp(2, 0))
      val evt3 = event("z", 3).copy(vectorTimestamp = timestamp(3, 0))

      actor ! Written(evt1) // ensure that a write is in progress when processing the next events
      actor ! Written(evt2)
      actor ! Written(evt3)

      // process first write
      processWrite(1, Seq.fill(4)(update(evt1)))

      // process remaining writes
      processPartialWrite(1, Seq.fill(4)(update(evt2)))
      processWrite(3, Seq.fill(11)(update(evt3)))
    }
    "allow batch sizes greater than settings.writeBatchSize if that batch was generated from a single input event (case 2)" in {
      val actor = recoveredStatelessProcessor()
      val evt1 = event("z", 1).copy(vectorTimestamp = timestamp(1, 0))

      actor ! Written(evt1)
      processWrite(1, Seq.fill(11)(update(evt1)))
    }
  }
}
