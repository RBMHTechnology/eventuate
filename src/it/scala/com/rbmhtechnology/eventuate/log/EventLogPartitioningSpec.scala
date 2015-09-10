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

package com.rbmhtechnology.eventuate.log

import akka.actor.ActorSystem
import akka.testkit.{TestProbe, TestKit}

import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationProtocol._

import com.typesafe.config._

import scala.collection.immutable.Seq

object EventLogPartitioningSpecCassandra {
  val config: Config = ConfigFactory.parseString(
    """
      |akka.loglevel = "ERROR"
      |akka.test.single-expect-default = 10s
      |
      |eventuate.snapshot.filesystem.dir = target/test-snapshot
      |
      |eventuate.log.batching.batch-size-limit = 3
      |eventuate.log.replication.batch-size-max = 3
      |eventuate.log.cassandra.partition-size-max = 5
      |eventuate.log.cassandra.default-port = 9142
    """.stripMargin)
}

class EventLogPartitioningSpecCassandra extends TestKit(ActorSystem("test", EventLogPartitioningSpecCassandra.config)) with EventLogSpecSupport with EventLogLifecycleCassandra {
  import EventLogSpec._

  def replay(fromSequenceNr: Long): Seq[(Any, Long)] = {
    val probe = TestProbe()
    log ! Replay(fromSequenceNr, probe.ref, None, 0)
    probe.receiveWhile[(Any, Long)]() {
      case r: Replaying => (r.event.payload, r.event.sequenceNr)
    }
  }

  "A Cassandra event log" must {
    "fill a partition with a single batch" in {
      writeEmittedEvents(List(event("a"), event("b"), event("c"), event("d"), event("e")))
      replay(1L) should be (List(("a", 1L), ("b", 2L), ("c", 3L), ("d", 4L), ("e", 5L)))
      replay(4L) should be (List(("d", 4L), ("e", 5L)))
      replay(5L) should be (List(("e", 5L)))
      replay(6L) should be (List())
    }
    "fill a partition with more than one batch" in {
      writeEmittedEvents(List(event("a"), event("b"), event("c")))
      writeEmittedEvents(List(event("d"), event("e")))
      replay(1L) should be (List(("a", 1L), ("b", 2L), ("c", 3L), ("d", 4L), ("e", 5L)))
      replay(5L) should be (List(("e", 5L)))
      replay(6L) should be (List())
    }
    "switch to the next partition if the current partition is full" in {
      writeEmittedEvents(List(event("a"), event("b"), event("c"), event("d"), event("e")))
      writeEmittedEvents(List(event("f"), event("g")))
      replay(1L) should be (List(("a", 1L), ("b", 2L), ("c", 3L), ("d", 4L), ("e", 5L), ("f", 6L), ("g", 7L)))
      replay(5L) should be (List(("e", 5L), ("f", 6L), ("g", 7L)))
      replay(6L) should be (List(("f", 6L), ("g", 7L)))
    }
    "switch to the next partition if the current partition isn't full but doesn't provide enough remaining space for a batch" in {
      val eventsA = List(event("a"), event("b"), event("c"), event("d"))
      val eventsB = List(event("f"), event("g"))

      log ! Write(eventsA, system.deadLetters, requestorProbe.ref, 0)
      log ! Write(eventsB, system.deadLetters, requestorProbe.ref, 0)

      val expectedA = eventsA.zipWithIndex.map {
        case (event, idx) => event.copy(
          vectorTimestamp = timestamp(1L + idx),
          processId = logId,
          sourceLogId = logId,
          targetLogId = logId,
          sourceLogSequenceNr = 1L + idx,
          targetLogSequenceNr = 1L + idx)
      }

      val expectedB = eventsB.zipWithIndex.map {
        case (event, idx) => event.copy(
          vectorTimestamp = timestamp(6L + idx),
          processId = logId,
          sourceLogId = logId,
          targetLogId = logId,
          sourceLogSequenceNr = 6L + idx,
          targetLogSequenceNr = 6L + idx)
      }

      notificationProbe.expectMsg(Updated(logId, expectedA))
      notificationProbe.expectMsg(Updated(logId, expectedB))

      expectedA.foreach(event => requestorProbe.expectMsg(WriteSuccess(event, 0)))
      expectedB.foreach(event => requestorProbe.expectMsg(WriteSuccess(event, 0)))

      replay(1L) should be (List(("a", 1L), ("b", 2L), ("c", 3L), ("d", 4L), ("f", 6L), ("g", 7L)))
      replay(5L) should be (List(("f", 6L), ("g", 7L)))
      replay(6L) should be (List(("f", 6L), ("g", 7L)))
    }
    "reject batches larger than the maximum partition size" in {
      val probe = TestProbe()
      val events = Vector(event("a"), event("b"), event("c"), event("d"), event("e"), event("f"))
      log ! Write(events, system.deadLetters, probe.ref, 0)
      1 to events.size foreach { _ => probe.expectMsgClass(classOf[WriteFailure]) }
    }
  }
}

