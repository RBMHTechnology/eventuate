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
import com.typesafe.config.ConfigFactory
import org.scalatest._

import scala.util._

object EventsourcedViewSpec {
  val emitterIdA = "A"
  val emitterIdB = "B"

  val logIdA = "logA"
  val logIdB = "logB"

  case class Ping(i: Int)
  case class Pong(i: Int)

  class TestEventsourcedView(
    val logProbe: ActorRef,
    val msgProbe: ActorRef,
    customReplayBatchSize: Option[Int],
    override val replayFromSequenceNr: Option[Long] = None) extends EventsourcedView {

    val id = emitterIdA
    val eventLog = logProbe

    override def replayBatchSize: Int = customReplayBatchSize match {
      case Some(i) => i
      case None    => super.replayBatchSize
    }

    override def onCommand = {
      case "boom"  => throw TestException
      case Ping(i) => msgProbe ! Pong(i)
    }

    override def onEvent = {
      case "boom" => throw TestException
      case evt    => msgProbe ! ((evt, lastVectorTimestamp, lastSequenceNr))
    }
  }

  class TestStashingView(
    val logProbe: ActorRef,
    val msgProbe: ActorRef) extends EventsourcedView {

    val id = emitterIdA
    val eventLog = logProbe

    var stashing = false

    override def onCommand = {
      case "boom" =>
        throw TestException
      case "stash-on" =>
        stashing = true
      case "stash-off" =>
        stashing = false
      case "unstash" =>
        unstashAll()
      case Ping(i) if stashing =>
        stash()
      case Ping(i) =>
        msgProbe ! Pong(i)
    }

    override def onEvent = {
      case "unstash" =>
        unstashAll()
    }
  }

  class TestCompletionView(
    val logProbe: ActorRef,
    val msgProbe: ActorRef) extends EventsourcedView {

    val id = emitterIdA
    val eventLog = logProbe

    override def onRecovery: Handler[Unit] = {
      case Success(_) => msgProbe ! "success"
      case Failure(e) => msgProbe ! e
    }

    override def onCommand: Receive = {
      case _ =>
    }

    override def onEvent: Receive = {
      case _ =>
    }
  }

  class TestBehaviorView(
    val logProbe: ActorRef,
    val msgProbe: ActorRef) extends EventsourcedView {

    val id = emitterIdA
    val eventLog = logProbe
    var total: Int = 0

    val add: Receive = {
      case i: Int =>
        total += i
        msgProbe ! total
    }

    val sub: Receive = {
      case i: Int =>
        total -= i
        msgProbe ! total
    }

    def change(context: => BehaviorContext): Receive = {
      case "add" => context.become(add orElse change(context))
      case "sub" => context.become(sub orElse change(context))
    }

    override def onCommand =
      add orElse change(commandContext)

    override def onEvent: Receive =
      add orElse change(eventContext)
  }

  class TestGuardingView(
    val logProbe: ActorRef,
    val msgProbe: ActorRef) extends EventsourcedView {

    val id = emitterIdA
    val eventLog = logProbe

    override def onCommand = {
      case "last" => msgProbe ! lastHandledEvent
    }

    override def onEvent = {
      case "e1" if lastEmitterId == "x" =>
        msgProbe ! "handled"
      case "e2" if lastEmitterId == "y" =>
        msgProbe ! "handled"
    }
  }

  val event1a = event("a", 1L)
  val event1b = event("b", 2L)
  val event1c = event("c", 3L)

  val event2a = DurableEvent("a", emitterIdA, None, Set(), 0L, timestamp(1, 0), logIdA, logIdA, 1L)
  val event2b = DurableEvent("b", emitterIdB, None, Set(), 0L, timestamp(0, 1), logIdB, logIdA, 2L)
  val event2c = DurableEvent("c", emitterIdB, None, Set(), 0L, timestamp(0, 2), logIdB, logIdA, 3L)
  val event2d = DurableEvent("d", emitterIdB, None, Set(), 0L, timestamp(0, 3), logIdB, logIdA, 4L)

  def timestamp(a: Long = 0L, b: Long = 0L) = (a, b) match {
    case (0L, 0L) => VectorTime()
    case (a, 0L)  => VectorTime(logIdA -> a)
    case (0L, b)  => VectorTime(logIdB -> b)
    case (a, b)   => VectorTime(logIdA -> a, logIdB -> b)
  }

  def event(payload: Any, sequenceNr: Long): DurableEvent =
    DurableEvent(payload, emitterIdA, None, Set(), 0L, timestamp(sequenceNr), logIdA, logIdA, sequenceNr)
}

class EventsourcedViewSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  import EventsourcedViewSpec._
  import EventsourcingProtocol._

  var instanceId: Int = _
  var logProbe: TestProbe = _
  var msgProbe: TestProbe = _

  override def beforeEach(): Unit = {
    instanceId = EventsourcedView.instanceIdCounter.get
    logProbe = TestProbe()
    msgProbe = TestProbe()
  }

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  def unrecoveredEventsourcedView(): ActorRef =
    system.actorOf(Props(new TestEventsourcedView(logProbe.ref, msgProbe.ref, None)))

  def unrecoveredEventsourcedView(customReplayBatchSize: Int): ActorRef =
    system.actorOf(Props(new TestEventsourcedView(logProbe.ref, msgProbe.ref, Some(customReplayBatchSize))))

  def unrecoveredCompletionView(): ActorRef =
    system.actorOf(Props(new TestCompletionView(logProbe.ref, msgProbe.ref)))

  def recoveredCompletionView(): ActorRef =
    processRecover(unrecoveredCompletionView())

  def recoveredStashingView(): ActorRef =
    processRecover(system.actorOf(Props(new TestStashingView(logProbe.ref, msgProbe.ref))))

  def recoveredBehaviorView(): ActorRef =
    processRecover(system.actorOf(Props(new TestBehaviorView(logProbe.ref, msgProbe.ref))))

  def recoveredGuardingView(): ActorRef =
    processRecover(system.actorOf(Props(new TestGuardingView(logProbe.ref, msgProbe.ref))))

  def replayControllingActor(snr: Option[Long]): ActorRef = {
    system.actorOf(Props(new TestEventsourcedView(logProbe.ref, msgProbe.ref, None, snr)))
  }

  def processRecover(actor: ActorRef, instanceId: Int = instanceId): ActorRef = {
    logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
    logProbe.sender() ! LoadSnapshotSuccess(None, instanceId)
    logProbe.expectMsg(Replay(1L, Some(actor), instanceId))
    actor ! ReplaySuccess(Nil, 0L, instanceId)
    actor
  }

  "An EventsourcedView" must {
    "recover from replayed events" in {
      val actor = unrecoveredEventsourcedView()

      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
      logProbe.sender() ! LoadSnapshotSuccess(None, instanceId)
      logProbe.expectMsg(Replay(1L, Some(actor), instanceId))
      logProbe.sender() ! ReplaySuccess(List(event1a, event1b), event1b.localSequenceNr, instanceId)

      msgProbe.expectMsg(("a", event1a.vectorTimestamp, event1a.localSequenceNr))
      msgProbe.expectMsg(("b", event1b.vectorTimestamp, event1b.localSequenceNr))
    }
    "recover from events that are replayed in batches" in {
      val actor = unrecoveredEventsourcedView(2)

      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
      logProbe.sender() ! LoadSnapshotSuccess(None, instanceId)
      logProbe.expectMsg(Replay(1L, 2, Some(actor), instanceId))
      logProbe.sender() ! ReplaySuccess(List(event1a, event1b), event1b.localSequenceNr, instanceId)
      logProbe.expectMsg(Replay(event1b.localSequenceNr + 1L, 2, None, instanceId))
      logProbe.sender() ! ReplaySuccess(List(event1c), event1c.localSequenceNr, instanceId)

      msgProbe.expectMsg(("a", event1a.vectorTimestamp, event1a.localSequenceNr))
      msgProbe.expectMsg(("b", event1b.vectorTimestamp, event1b.localSequenceNr))
      msgProbe.expectMsg(("c", event1c.vectorTimestamp, event1c.localSequenceNr))
    }
    "retry recovery on failure" in {
      val actor = unrecoveredEventsourcedView()

      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
      logProbe.sender() ! LoadSnapshotSuccess(None, instanceId)
      logProbe.expectMsg(Replay(1L, Some(actor), instanceId))
      logProbe.sender() ! ReplaySuccess(List(event1a, event1b.copy(payload = "boom"), event1c), event1c.localSequenceNr, instanceId)

      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId + 1))
      logProbe.sender() ! LoadSnapshotSuccess(None, instanceId + 1)
      logProbe.expectMsg(Replay(1L, Some(actor), instanceId + 1))
      logProbe.sender() ! ReplaySuccess(List(event1a, event1b, event1c), event1c.localSequenceNr, instanceId + 1)

      msgProbe.expectMsg(("a", event1a.vectorTimestamp, event1a.localSequenceNr))
      msgProbe.expectMsg(("a", event1a.vectorTimestamp, event1a.localSequenceNr))
      msgProbe.expectMsg(("b", event1b.vectorTimestamp, event1b.localSequenceNr))
      msgProbe.expectMsg(("c", event1c.vectorTimestamp, event1c.localSequenceNr))
    }
    "replay from application-defined sequence number (not load snapshot)" in {
      val actor = replayControllingActor(Some(2L))

      logProbe.expectMsg(Replay(2L, Some(actor), instanceId))
      logProbe.sender() ! ReplaySuccess(List(event1b, event1c), event1c.localSequenceNr, instanceId)

      msgProbe.expectMsg(("b", event1b.vectorTimestamp, event1b.localSequenceNr))
      msgProbe.expectMsg(("c", event1c.vectorTimestamp, event1c.localSequenceNr))
    }
    "not replay from application-defined sequence number (load snapshot)" in {
      processRecover(replayControllingActor(None))
    }
    "stash commands during recovery and handle them after initial recovery" in {
      val actor = unrecoveredEventsourcedView()

      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
      logProbe.sender() ! LoadSnapshotSuccess(None, instanceId)
      logProbe.expectMsg(Replay(1L, Some(actor), instanceId))

      actor ! Ping(1)
      actor ! Ping(2)
      actor ! Ping(3)

      logProbe.sender() ! ReplaySuccess(List(event1a, event1b), event1b.localSequenceNr, instanceId)
      logProbe.expectMsg(Replay(event1b.localSequenceNr + 1L, None, instanceId))
      logProbe.sender() ! ReplaySuccess(Nil, event1b.localSequenceNr, instanceId)

      msgProbe.expectMsg(("a", event1a.vectorTimestamp, event1a.localSequenceNr))
      msgProbe.expectMsg(("b", event1b.vectorTimestamp, event1b.localSequenceNr))
      msgProbe.expectMsg(Pong(1))
      msgProbe.expectMsg(Pong(2))
      msgProbe.expectMsg(Pong(3))
    }
    "stash commands during recovery and handle them after retried recovery" in {
      val actor = unrecoveredEventsourcedView()

      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
      logProbe.sender() ! LoadSnapshotSuccess(None, instanceId)
      logProbe.expectMsg(Replay(1L, Some(actor), instanceId))

      actor ! Ping(1)
      actor ! Ping(2)

      logProbe.sender() ! ReplaySuccess(List(event1a, event1b.copy(payload = "boom"), event1c), event1c.localSequenceNr, instanceId)

      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId + 1))
      logProbe.sender() ! LoadSnapshotSuccess(None, instanceId + 1)
      logProbe.expectMsg(Replay(1L, Some(actor), instanceId + 1))
      logProbe.sender() ! ReplaySuccess(List(event1a, event1b, event1c), event1c.localSequenceNr, instanceId + 1)
      logProbe.expectMsg(Replay(event1c.localSequenceNr + 1L, None, instanceId + 1))
      logProbe.sender() ! ReplaySuccess(Nil, event1c.localSequenceNr, instanceId + 1)

      msgProbe.expectMsg(("a", event1a.vectorTimestamp, event1a.localSequenceNr))
      msgProbe.expectMsg(("a", event1a.vectorTimestamp, event1a.localSequenceNr))
      msgProbe.expectMsg(("b", event1b.vectorTimestamp, event1b.localSequenceNr))
      msgProbe.expectMsg(("c", event1c.vectorTimestamp, event1c.localSequenceNr))
      msgProbe.expectMsg(Pong(1))
      msgProbe.expectMsg(Pong(2))
    }
    "stash live events consumed during recovery" in {
      val actor = unrecoveredEventsourcedView()
      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
      logProbe.sender() ! LoadSnapshotSuccess(None, instanceId)
      logProbe.expectMsg(Replay(1L, Some(actor), instanceId))

      actor ! Written(event2c) // live event
      actor ! Written(event2d) // live event

      logProbe.sender() ! ReplaySuccess(List(event2a, event2b), event2b.localSequenceNr, instanceId)
      logProbe.expectMsg(Replay(event2b.localSequenceNr + 1L, None, instanceId))
      logProbe.sender() ! ReplaySuccess(Nil, event2b.localSequenceNr, instanceId)

      msgProbe.expectMsg(("a", event2a.vectorTimestamp, event2a.localSequenceNr))
      msgProbe.expectMsg(("b", event2b.vectorTimestamp, event2b.localSequenceNr))
      msgProbe.expectMsg(("c", event2c.vectorTimestamp, event2c.localSequenceNr))
      msgProbe.expectMsg(("d", event2d.vectorTimestamp, event2d.localSequenceNr))
    }
    "ignore live events targeted at previous incarnations" in {
      val actor = unrecoveredEventsourcedView()
      val next = instanceId + 1

      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
      logProbe.sender() ! LoadSnapshotSuccess(None, instanceId)
      logProbe.expectMsg(Replay(1L, Some(actor), instanceId))

      logProbe.sender() ! ReplaySuccess(List(event2a, event2b), event2b.localSequenceNr, instanceId)
      logProbe.expectMsg(Replay(event2b.localSequenceNr + 1L, None, instanceId))
      logProbe.sender() ! ReplaySuccess(Nil, event2b.localSequenceNr, instanceId)

      actor ! "boom"
      actor ! Written(event2c) // live event

      msgProbe.expectMsg(("a", event2a.vectorTimestamp, event2a.localSequenceNr))
      msgProbe.expectMsg(("b", event2b.vectorTimestamp, event2b.localSequenceNr))

      logProbe.expectMsg(LoadSnapshot(emitterIdA, next))
      logProbe.sender() ! LoadSnapshotSuccess(None, next)
      logProbe.expectMsg(Replay(1L, Some(actor), next))
      logProbe.sender() ! ReplaySuccess(List(event2a, event2b, event2c), event2c.localSequenceNr, next)
      logProbe.expectMsg(Replay(event2c.localSequenceNr + 1L, None, next))
      logProbe.sender() ! ReplaySuccess(Nil, event2c.localSequenceNr, next)

      actor ! Written(event2d) // live event

      msgProbe.expectMsg(("a", event2a.vectorTimestamp, event2a.localSequenceNr))
      msgProbe.expectMsg(("b", event2b.vectorTimestamp, event2b.localSequenceNr))
      msgProbe.expectMsg(("c", event2c.vectorTimestamp, event2c.localSequenceNr))
      msgProbe.expectMsg(("d", event2d.vectorTimestamp, event2d.localSequenceNr))
    }
    "support user stash-unstash operations" in {
      val actor = recoveredStashingView()

      actor ! Ping(1)
      actor ! "stash-on"
      actor ! Ping(2)
      actor ! "stash-off"
      actor ! Ping(3)
      actor ! "unstash"
      actor ! Ping(4)

      msgProbe.expectMsg(Pong(1))
      msgProbe.expectMsg(Pong(3))
      msgProbe.expectMsg(Pong(2))
      msgProbe.expectMsg(Pong(4))
    }
    "support user unstash operations in event handler" in {
      val actor = recoveredStashingView()

      actor ! Ping(1)
      actor ! "stash-on"
      actor ! Ping(2)
      actor ! "stash-off"
      actor ! Ping(3)
      actor ! Written(event("unstash", 1))
      actor ! Ping(4)

      msgProbe.expectMsg(Pong(1))
      msgProbe.expectMsg(Pong(3))
      msgProbe.expectMsg(Pong(2))
      msgProbe.expectMsg(Pong(4))
    }
    "support user stash-unstash operations where unstash is the last operation" in {
      val actor = recoveredStashingView()

      actor ! Ping(1)
      actor ! "stash-on"
      actor ! Ping(2)
      actor ! "stash-off"
      actor ! Ping(3)
      actor ! "unstash"

      msgProbe.expectMsg(Pong(1))
      msgProbe.expectMsg(Pong(3))
      msgProbe.expectMsg(Pong(2))
    }
    "support user stash-unstash operations under failure conditions (failure before stash)" in {
      val actor = recoveredStashingView()

      actor ! Ping(1)
      actor ! "boom"
      actor ! "stash-on"
      actor ! Ping(2)
      actor ! "stash-off"
      actor ! Ping(3)
      actor ! "unstash"
      actor ! Ping(4)

      processRecover(actor, instanceId + 1)

      msgProbe.expectMsg(Pong(1))
      msgProbe.expectMsg(Pong(3))
      msgProbe.expectMsg(Pong(2))
      msgProbe.expectMsg(Pong(4))
    }
    "support user stash-unstash operations under failure conditions (failure after stash)" in {
      val actor = recoveredStashingView()

      actor ! Ping(1)
      actor ! "stash-on"
      actor ! Ping(2)
      actor ! "boom"
      actor ! Ping(3)
      actor ! "unstash"
      actor ! Ping(4)

      processRecover(actor, instanceId + 1)

      msgProbe.expectMsg(Pong(1))
      msgProbe.expectMsg(Pong(2))
      msgProbe.expectMsg(Pong(3))
      msgProbe.expectMsg(Pong(4))
    }
    "call the recovery completion handler with Success if recovery succeeds" in {
      recoveredCompletionView()
      msgProbe.expectMsg("success")
    }
    "call the recovery completion handler with Failure if recovery fails" in {
      val actor = unrecoveredCompletionView()
      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
      logProbe.sender() ! LoadSnapshotSuccess(None, instanceId)
      logProbe.expectMsg(Replay(1L, Some(actor), instanceId))
      actor ! ReplayFailure(TestException, 1L, instanceId)
      msgProbe.expectMsg(TestException)
    }
    "support command handler behavior changes" in {
      val actor = recoveredBehaviorView()

      actor ! 1
      actor ! 2
      actor ! "sub"
      actor ! 3

      msgProbe.expectMsg(1)
      msgProbe.expectMsg(3)
      msgProbe.expectMsg(0)
    }
    "support event handler behavior changes" in {
      val actor = recoveredBehaviorView()

      actor ! Written(event(1, 1L))
      actor ! Written(event(2, 2L))
      actor ! Written(event("sub", 3L))
      actor ! Written(event(3, 4L))

      msgProbe.expectMsg(1)
      msgProbe.expectMsg(3)
      msgProbe.expectMsg(0)
    }
    "stop during recovery if its event log is stopped" in {
      val actor = watch(unrecoveredEventsourcedView())
      system.stop(logProbe.ref)
      expectTerminated(actor)
    }
    "stop after recovery if its event log is stopped" in {
      val actor = watch(processRecover(unrecoveredEventsourcedView()))
      system.stop(logProbe.ref)
      expectTerminated(actor)
    }
    "support usage of last* methods in pattern guards when guard evaluates to true" in {
      val actor = recoveredGuardingView()
      val event1 = event("e1", 1L).copy(emitterId = "x")

      actor ! Written(event1)
      msgProbe.expectMsg("handled")

      actor ! "last"
      msgProbe.expectMsgType[DurableEvent].payload should be("e1")
    }
    "support usage of last* methods in pattern guards when guard evaluates to false" in {
      val actor = recoveredGuardingView()
      val event1 = event("e1", 1L).copy(emitterId = "x")
      val event2 = event("e2", 1L).copy(emitterId = "x")

      actor ! Written(event1)
      actor ! Written(event2)
      msgProbe.expectMsg("handled")

      actor ! "last"
      msgProbe.expectMsgType[DurableEvent].payload should be("e1")
    }
  }
}

object EventsourcedViewReplaySpec {
  val MaxRetries = 5
  val config = ConfigFactory.parseString(
    s"""
      |eventuate.log.replay-retry-max = $MaxRetries
      |eventuate.log.replay-retry-delay = 5ms
    """.stripMargin)
}

class EventsourcedViewReplayRetrySpec extends TestKit(ActorSystem("test", EventsourcedViewReplaySpec.config)) with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  import EventsourcedViewReplaySpec._
  import EventsourcedViewSpec._
  import EventsourcingProtocol._

  var instanceId: Int = _
  var logProbe: TestProbe = _
  var msgProbe: TestProbe = _

  override def beforeEach(): Unit = {
    instanceId = EventsourcedView.instanceIdCounter.get
    logProbe = TestProbe()
    msgProbe = TestProbe()
  }

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  def unrecoveredCompletionView(): ActorRef =
    system.actorOf(Props(new TestCompletionView(logProbe.ref, msgProbe.ref)))

  "An EventsourcedView" must {
    "retry replay on failure" in {
      val actor = unrecoveredCompletionView()

      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
      logProbe.sender() ! LoadSnapshotSuccess(None, instanceId)

      logProbe.expectMsg(Replay(1L, Some(actor), instanceId))
      actor ! ReplayFailure(TestException, 1L, instanceId)

      1 to MaxRetries foreach { _ =>
        logProbe.expectMsg(Replay(1L, None, instanceId))
        actor ! ReplayFailure(TestException, 1L, instanceId)
      }

      msgProbe.expectMsg(TestException)
    }
    "successfully finish recovery after replay retry" in {
      val actor = unrecoveredCompletionView()

      logProbe.expectMsg(LoadSnapshot(emitterIdA, instanceId))
      logProbe.sender() ! LoadSnapshotSuccess(None, instanceId)

      logProbe.expectMsg(Replay(1L, Some(actor), instanceId))
      actor ! ReplayFailure(TestException, 1L, instanceId)

      logProbe.expectMsg(Replay(1L, None, instanceId))
      actor ! ReplaySuccess(Nil, 0L, instanceId)

      msgProbe.expectMsg("success")
    }
  }
}
