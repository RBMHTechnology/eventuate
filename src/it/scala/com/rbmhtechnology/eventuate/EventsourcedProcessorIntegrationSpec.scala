/*
 * Copyright (C) 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
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

import com.rbmhtechnology.eventuate.log.EventLogLifecycleCassandra
import com.rbmhtechnology.eventuate.log.EventLogLifecycleLeveldb

import org.scalatest._

import scala.util._

object EventsourcedProcessorIntegrationSpec {
  class SampleActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {
    override def onCommand = {
      case s: String => persist(s) {
        case Success(_) =>
        case Failure(_) =>
      }
    }

    override def onEvent = {
      case s: String => probe ! ((s, lastVectorTimestamp))
    }
  }

  class StatelessSampleProcessor(val id: String, val eventLog: ActorRef, val targetEventLog: ActorRef, eventProbe: ActorRef, progressProbe: ActorRef) extends EventsourcedProcessor {
    override def onCommand = {
      case "boom" => throw boom
      case "snap" => save("") {
        case Success(_) => eventProbe ! "snapped"
        case Failure(_) =>
      }
    }

    override val processEvent: Process = {
      case s: String if !s.contains("processed") =>
        eventProbe ! s
        List(s"${s}-processed-1", s"${s}-processed-2")
    }

    override def onSnapshot = {
      case _ =>
    }

    override def writeSuccess(progress: Long): Unit = {
      super.writeSuccess(progress)
      progressProbe ! progress
    }
  }

  class StatefulSampleProcessor(id: String, eventLog: ActorRef, targetEventLog: ActorRef, override val sharedClockEntry: Boolean, eventProbe: ActorRef, progressProbe: ActorRef)
    extends StatelessSampleProcessor(id, eventLog, targetEventLog, eventProbe, progressProbe) with StatefulProcessor
}

abstract class EventsourcedProcessorIntegrationSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterEach {
  import EventsourcedProcessorIntegrationSpec._

  def log: ActorRef
  def logId: String
  def logProps(logId: String): Props

  def sourceLog = log
  def sourceLogId = logId

  var targetLog: ActorRef = _
  var targetLogId: String = _

  var sourceProbe: TestProbe = _
  var targetProbe: TestProbe = _

  var processorEventProbe: TestProbe = _
  var processorProgressProbe: TestProbe = _

  var a1: ActorRef = _
  var a2: ActorRef = _

  def init(): Unit = {
    targetLogId = s"${logId}_target"
    targetLog = system.actorOf(logProps(targetLogId))

    sourceProbe = TestProbe()
    targetProbe = TestProbe()

    processorEventProbe = TestProbe()
    processorProgressProbe = TestProbe()

    a1 = system.actorOf(Props(new SampleActor("a1", sourceLog, sourceProbe.ref)))
    a2 = system.actorOf(Props(new SampleActor("a2", targetLog, targetProbe.ref)))
  }

  def statelessProcessor(): ActorRef =
    system.actorOf(Props(new StatelessSampleProcessor("p", sourceLog, targetLog, processorEventProbe.ref, processorProgressProbe.ref)))

  def statefulProcessor(sourceLog: ActorRef, targetLog: ActorRef, sharedClockEntry: Boolean): ActorRef =
    system.actorOf(Props(new StatefulSampleProcessor("p", sourceLog, targetLog, sharedClockEntry, processorEventProbe.ref, processorProgressProbe.ref)))

  def waitForProgressWrite(progress: Long): Unit = {
    processorProgressProbe.fishForMessage() {
      case `progress` => true
      case n: Long    => false
    }
  }

  "A StatefulProcessor" must {
    "write processed events to a target log and recover from scratch" in {
      val p = statefulProcessor(sourceLog, targetLog, sharedClockEntry = true)

      a1 ! "a"
      a1 ! "b"
      a1 ! "c"

      processorEventProbe.expectMsg("a")
      processorEventProbe.expectMsg("b")
      processorEventProbe.expectMsg("c")

      targetProbe.expectMsg(("a-processed-1", VectorTime(sourceLogId -> 1L, targetLogId -> 1L)))
      targetProbe.expectMsg(("a-processed-2", VectorTime(sourceLogId -> 1L, targetLogId -> 2L)))
      targetProbe.expectMsg(("b-processed-1", VectorTime(sourceLogId -> 2L, targetLogId -> 3L)))
      targetProbe.expectMsg(("b-processed-2", VectorTime(sourceLogId -> 2L, targetLogId -> 4L)))
      targetProbe.expectMsg(("c-processed-1", VectorTime(sourceLogId -> 3L, targetLogId -> 5L)))
      targetProbe.expectMsg(("c-processed-2", VectorTime(sourceLogId -> 3L, targetLogId -> 6L)))

      waitForProgressWrite(3L)

      p ! "boom"
      a1 ! "d"

      processorEventProbe.expectMsg("d")

      targetProbe.expectMsg(("d-processed-1", VectorTime(sourceLogId -> 4L, targetLogId -> 7L)))
      targetProbe.expectMsg(("d-processed-2", VectorTime(sourceLogId -> 4L, targetLogId -> 8L)))
    }
    "write processed events to a target log and recover from snapshot" in {
      val p = statefulProcessor(sourceLog, targetLog, sharedClockEntry = true)

      a1 ! "a"
      a1 ! "b"

      processorEventProbe.expectMsg("a")
      processorEventProbe.expectMsg("b")

      p ! "snap"

      processorEventProbe.expectMsg("snapped")

      a1 ! "c"

      processorEventProbe.expectMsg("c")

      targetProbe.expectMsg(("a-processed-1", VectorTime(sourceLogId -> 1L, targetLogId -> 1L)))
      targetProbe.expectMsg(("a-processed-2", VectorTime(sourceLogId -> 1L, targetLogId -> 2L)))
      targetProbe.expectMsg(("b-processed-1", VectorTime(sourceLogId -> 2L, targetLogId -> 3L)))
      targetProbe.expectMsg(("b-processed-2", VectorTime(sourceLogId -> 2L, targetLogId -> 4L)))
      targetProbe.expectMsg(("c-processed-1", VectorTime(sourceLogId -> 3L, targetLogId -> 5L)))
      targetProbe.expectMsg(("c-processed-2", VectorTime(sourceLogId -> 3L, targetLogId -> 6L)))

      waitForProgressWrite(3L)

      p ! "boom"
      a1 ! "d"

      processorEventProbe.expectMsg("d")

      targetProbe.expectMsg(("d-processed-1", VectorTime(sourceLogId -> 4L, targetLogId -> 7L)))
      targetProbe.expectMsg(("d-processed-2", VectorTime(sourceLogId -> 4L, targetLogId -> 8L)))
    }
    "update event vector timestamps when having set sharedClockEntry to false" in {
      val p = statefulProcessor(sourceLog, targetLog, sharedClockEntry = false)

      a1 ! "a"
      a1 ! "b"
      a1 ! "c"

      processorEventProbe.expectMsg("a")
      processorEventProbe.expectMsg("b")

      targetProbe.expectMsg(("a-processed-1", VectorTime(sourceLogId -> 1L, "p" -> 2L)))
      targetProbe.expectMsg(("a-processed-2", VectorTime(sourceLogId -> 1L, "p" -> 3L)))
      targetProbe.expectMsg(("b-processed-1", VectorTime(sourceLogId -> 2L, "p" -> 5L)))
      targetProbe.expectMsg(("b-processed-2", VectorTime(sourceLogId -> 2L, "p" -> 6L)))
    }
  }

  "A stateful EventsourcedProcessor" when {
    "writing to the source event log" must {
      "have its own vector clock entry" in {
        a1 ! "a"
        a1 ! "b"
        a1 ! "c"

        sourceProbe.expectMsg(("a", VectorTime(sourceLogId -> 1L)))
        sourceProbe.expectMsg(("b", VectorTime(sourceLogId -> 2L)))
        sourceProbe.expectMsg(("c", VectorTime(sourceLogId -> 3L)))

        val p = statefulProcessor(sourceLog, sourceLog, sharedClockEntry = false)

        sourceProbe.expectMsg(("a-processed-1", VectorTime(sourceLogId -> 1L, "p" -> 2L)))
        sourceProbe.expectMsg(("a-processed-2", VectorTime(sourceLogId -> 1L, "p" -> 3L)))
        sourceProbe.expectMsg(("b-processed-1", VectorTime(sourceLogId -> 2L, "p" -> 5L)))
        sourceProbe.expectMsg(("b-processed-2", VectorTime(sourceLogId -> 2L, "p" -> 6L)))
        sourceProbe.expectMsg(("c-processed-1", VectorTime(sourceLogId -> 3L, "p" -> 8L)))
        sourceProbe.expectMsg(("c-processed-2", VectorTime(sourceLogId -> 3L, "p" -> 9L)))

        p ! "boom"
        a1 ! "d"

        sourceProbe.expectMsg(("d", VectorTime(sourceLogId -> 10L, "p" -> 9)))
        sourceProbe.expectMsg(("d-processed-1", VectorTime(sourceLogId -> 10L, "p" -> 11L)))
        sourceProbe.expectMsg(("d-processed-2", VectorTime(sourceLogId -> 10L, "p" -> 12L)))
      }
    }
  }

  "A stateless EventsourcedProcessor" must {
    "write processed events to a target log and resume from stored position" in {
      val p = statelessProcessor()

      a1 ! "a"
      a1 ! "b"
      a1 ! "c"

      processorEventProbe.expectMsg("a")
      processorEventProbe.expectMsg("b")
      processorEventProbe.expectMsg("c")

      targetProbe.expectMsg(("a-processed-1", VectorTime(sourceLogId -> 1L, targetLogId -> 1L)))
      targetProbe.expectMsg(("a-processed-2", VectorTime(sourceLogId -> 1L, targetLogId -> 2L)))
      targetProbe.expectMsg(("b-processed-1", VectorTime(sourceLogId -> 2L, targetLogId -> 3L)))
      targetProbe.expectMsg(("b-processed-2", VectorTime(sourceLogId -> 2L, targetLogId -> 4L)))
      targetProbe.expectMsg(("c-processed-1", VectorTime(sourceLogId -> 3L, targetLogId -> 5L)))
      targetProbe.expectMsg(("c-processed-2", VectorTime(sourceLogId -> 3L, targetLogId -> 6L)))

      waitForProgressWrite(3L)

      p ! "boom"
      a1 ! "d"

      processorEventProbe.expectMsg("d")

      targetProbe.expectMsg(("d-processed-1", VectorTime(sourceLogId -> 4L, targetLogId -> 7L)))
      targetProbe.expectMsg(("d-processed-2", VectorTime(sourceLogId -> 4L, targetLogId -> 8L)))
    }
  }
}

class EventsourcedProcessorIntegrationSpecLeveldb extends EventsourcedProcessorIntegrationSpec with EventLogLifecycleLeveldb {
  override def beforeEach(): Unit = {
    super.beforeEach()
    init()
  }
}

class EventsourcedProcessorIntegrationSpecCassandra extends EventsourcedProcessorIntegrationSpec with EventLogLifecycleCassandra {
  override def beforeEach(): Unit = {
    super.beforeEach()
    init()
  }
}