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

package com.rbmhtechnology.eventuate.adapter.stream

import akka.actor._
import akka.stream._
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.scaladsl._
import akka.stream.testkit.TestSubscriber.Probe
import akka.stream.testkit.scaladsl._
import akka.testkit._

import com.rbmhtechnology.eventuate.adapter.stream.DurableEventSourceActor.Paused
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.typesafe.config.ConfigFactory

import org.scalatest._

import scala.collection.immutable.Seq
import scala.concurrent.duration._

object DurableEventSourceActorSpec {
  private class TestSourceActor(logProbe: ActorRef, msgProbe: ActorRef, fromSequenceNr: Long, aggregateId: Option[String])
    extends DurableEventSourceActor(logProbe, fromSequenceNr, aggregateId) {

    override def reading: Receive = {
      case State =>
        msgProbe ! State.Reading
      case r: ReplaySuccess =>
        msgProbe ! r
        super.reading(r)
      case r: ReplayFailure =>
        msgProbe ! r
        super.reading(r)
    }

    override def pausing: Receive = {
      case State =>
        msgProbe ! State.Pausing
      case Paused =>
        msgProbe ! Paused
        super.pausing(Paused)
      case m if super.pausing.isDefinedAt(m) =>
        super.pausing(m)
    }

    override def waiting: Receive = {
      case State =>
        msgProbe ! State.Waiting
      case m if super.waiting.isDefinedAt(m) =>
        super.waiting(m)
    }

    override def unhandled(message: Any): Unit = message match {
      case r: Request =>
        msgProbe ! r
      case m =>
        super.unhandled(m)
    }
  }

  object State extends Enumeration {
    val Reading, Pausing, Waiting = Value
  }

  val NoMsgTimeout = 10.millis

  val EmitterId = "emitter"
  val ProcessId = "process"
  val LogId = "log"

  val config = ConfigFactory.parseString("eventuate.log.replay-retry-delay = 500ms")

  val e1 = durableEvent("a", 1)
  val e2 = durableEvent("b", 2)
  val e3 = durableEvent("c", 3)
  val es = Seq(e1, e2, e3)

  def durableEvent(payload: String, sequenceNr: Long, aggregateId: Option[String] = None): DurableEvent =
    DurableEvent(payload, emitterId = EmitterId, emitterAggregateId = aggregateId, vectorTimestamp = VectorTime(LogId -> sequenceNr), processId = ProcessId, localLogId = LogId, localSequenceNr = sequenceNr)

  def testSource(logProbe: TestProbe, msgProbe: TestProbe, fromSequenceNr: Long, aggregateId: Option[String]): Source[DurableEvent, ActorRef] =
    Source.actorPublisher[DurableEvent](Props(new TestSourceActor(logProbe.ref, msgProbe.ref, fromSequenceNr, aggregateId)))
}

class DurableEventSourceActorSpec extends TestKit(ActorSystem("test", DurableEventSourceActorSpec.config)) with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  import DurableEventSourceActorSpec._
  import DurableEventSourceActorSpec.State._

  private val settings: DurableEventSourceSettings =
    new DurableEventSourceSettings(system.settings.config)

  implicit val materializer: Materializer =
    ActorMaterializer()

  private var log: TestProbe = _
  private var prb: TestProbe = _
  private var snk: Probe[DurableEvent] = _
  private var act: ActorRef = _

  override def beforeEach(): Unit = {
    val psa = testProbesAndActor()
    log = psa._1
    prb = psa._2
    snk = psa._3
    act = psa._4
  }

  override def afterEach(): Unit = {
    snk.cancel()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  def testProbesAndActor(fromSequenceNr: Long = 1L, aggregateId: Option[String] = None): (TestProbe, TestProbe, Probe[DurableEvent], ActorRef) = {
    val logProbe = TestProbe()
    val msgProbe = TestProbe()
    val (act, snk) = testSource(logProbe, msgProbe, fromSequenceNr, aggregateId).toMat(TestSink.probe[DurableEvent])(Keep.both).run()
    logProbe.expectMsg(Replay(fromSequenceNr, settings.replayBatchSize, Some(act), 1))
    (logProbe, msgProbe, snk, act)
  }

  def replaySuccess(events: Seq[DurableEvent], progress: Long): Unit = {
    val msg = ReplaySuccess(events, progress, 1)
    log.sender() ! msg
    prb.expectMsg(msg)
  }

  def replayFailure(cause: Throwable): Unit = {
    val msg = ReplayFailure(cause, 1L, 1)
    log.sender() ! msg
    prb.expectMsg(msg)
  }

  def expectState(expected: State.Value): Unit = {
    act ! State
    prb.expectMsg(expected)
  }

  def expectPaused(): Unit = {
    prb.expectMsg(Paused)
  }

  def requestInReading(n: Long): Unit = {
    snk.request(n)
    if (n > 0) prb.expectMsg(Request(n))
  }

  def switchToWaiting(numRequest: Int, numReplay: Int): Unit = {
    val events = es.take(numReplay)
    requestInReading(numRequest)
    replaySuccess(events, numReplay)
    events.take(numRequest).foreach(e => snk.expectNext() should be(e))
    snk.expectNoMsg(NoMsgTimeout)
    expectState(Waiting)
  }

  def switchToPausing(numRequest: Int): Unit = {
    requestInReading(numRequest)
    replaySuccess(Seq(), 0)
    expectState(Pausing)
  }

  "A DurableEventSourceActor" when {
    "in reading state" must {
      "emit n events if n have been requested and m replayed with n < m and switch to waiting state" in {
        requestInReading(2)
        replaySuccess(es, 3)
        snk.expectNext() should be(e1)
        snk.expectNext() should be(e2)
        snk.expectNoMsg(NoMsgTimeout)
        expectState(Waiting)
      }
      "emit n events if n have been requested and m replayed with n = m and switch to waiting state" in {
        requestInReading(3)
        replaySuccess(es, 3)
        snk.expectNext() should be(e1)
        snk.expectNext() should be(e2)
        snk.expectNext() should be(e3)
        snk.expectNoMsg(NoMsgTimeout)
        expectState(Waiting)
      }
      "emit m events if n have been requested and m replayed with n > m and stay in reading state" in {
        requestInReading(4)
        replaySuccess(es, 3)
        snk.expectNext() should be(e1)
        snk.expectNext() should be(e2)
        snk.expectNext() should be(e3)
        snk.expectNoMsg(NoMsgTimeout)
        log.expectMsg(Replay(4, settings.replayBatchSize, None, 1))
        expectState(Reading)
      }
      "switch to pausing state if no elements have been replayed" in {
        replaySuccess(Seq(), 0)
        expectState(Pausing)
      }
      "switch to pausing state if replay failed" in {
        replayFailure(TestException)
        expectState(Pausing)
      }
    }
    "in waiting state with empty buffer" must {
      "read elements on request then switch to reading state" in {
        switchToWaiting(1, 1)
        snk.request(1)
        expectState(Reading)
        log.expectMsg(Replay(2, settings.replayBatchSize, None, 1))
        replaySuccess(Seq(e2), 2)
        snk.expectNext() should be(e2)
        snk.expectNoMsg(NoMsgTimeout)
      }
    }
    "in waiting state with non-empty buffer" must {
      "emit events and stay in waiting state if buffer remains non-empty" in {
        switchToWaiting(1, 3)
        snk.request(1)
        snk.expectNext() should be(e2)
        expectState(Waiting)
      }
      "emit events and stay in waiting state if buffer becomes empty and there is no further demand" in {
        switchToWaiting(1, 3)
        snk.request(2)
        snk.expectNext() should be(e2)
        snk.expectNext() should be(e3)
        expectState(Waiting)
      }
      "emit events and switch to reading state if buffer becomes empty and there is further demand" in {
        switchToWaiting(1, 2)
        snk.request(2)
        snk.expectNext() should be(e2)
        expectState(Reading)
        log.expectMsg(Replay(3, settings.replayBatchSize, None, 1))
        replaySuccess(Seq(e3), 3)
        snk.expectNext() should be(e3)
      }
    }
    "in pausing state with no demand" must {
      "switch to waiting state on scheduled pause end" in {
        switchToPausing(0)
        expectPaused()
        expectState(Waiting)
      }
      "switch to waiting state on update notification" in {
        switchToPausing(0)
        act ! Written(null)
        expectState(Waiting)
      }
    }
    "in pausing state with existing demand" must {
      "switch to reading state on scheduled pause end" in {
        switchToPausing(3)
        expectPaused()
        expectState(Reading)
        log.expectMsg(Replay(1, settings.replayBatchSize, None, 1))
      }
      "switch to reading state on update notification" in {
        switchToPausing(3)
        act ! Written(null)
        expectState(Reading)
        log.expectMsg(Replay(1, settings.replayBatchSize, None, 1))
      }
    }
  }

  "A DurableEventSourceActor" must {
    "complete the source when the event log stops" in {
      snk.ensureSubscription()
      system.stop(log.ref)
      snk.expectComplete()
    }
    "stop when the source is cancelled" in {
      val probe = TestProbe()
      probe.watch(act)
      snk.cancel()
      probe.expectTerminated(act)
    }
  }
}
