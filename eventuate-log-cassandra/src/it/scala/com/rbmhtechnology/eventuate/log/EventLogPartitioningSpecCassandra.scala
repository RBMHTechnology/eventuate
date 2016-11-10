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

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }

import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.SingleLocationSpecCassandra
import com.typesafe.config._

import scala.collection.immutable.Seq

object EventLogPartitioningSpecCassandra {
  val config: Config = ConfigFactory.parseString(
    """
      |akka.loglevel = "ERROR"
      |akka.test.single-expect-default = 20s
      |
      |eventuate.snapshot.filesystem.dir = target/test-snapshot
      |
      |eventuate.log.write-batch-size = 3
      |eventuate.log.cassandra.partition-size = 5
      |eventuate.log.cassandra.default-port = 9142
    """.stripMargin)
}

class EventLogPartitioningSpecCassandra extends TestKit(ActorSystem("test", EventLogPartitioningSpecCassandra.config)) with EventLogSpecSupport with SingleLocationSpecCassandra {
  import EventLogSpec._

  def replay(fromSequenceNr: Long): Seq[(Any, Long)] = {
    val probe = TestProbe()
    log.tell(Replay(fromSequenceNr, None, 0), replyToProbe.ref)
    replyToProbe.expectMsgClass(classOf[ReplaySuccess]).events.map { event =>
      (event.payload, event.localSequenceNr)
    }
  }

  "A Cassandra event log" must {
    "fill a partition with a single batch" in {
      writeEmittedEvents(List(event("a"), event("b"), event("c"), event("d"), event("e")))
      replay(1L) should be(List(("a", 1L), ("b", 2L), ("c", 3L), ("d", 4L), ("e", 5L)))
      replay(4L) should be(List(("d", 4L), ("e", 5L)))
      replay(5L) should be(List(("e", 5L)))
      replay(6L) should be(List())
    }
    "fill a partition with more than one batch" in {
      writeEmittedEvents(List(event("a"), event("b"), event("c")))
      writeEmittedEvents(List(event("d"), event("e")))
      replay(1L) should be(List(("a", 1L), ("b", 2L), ("c", 3L), ("d", 4L), ("e", 5L)))
      replay(5L) should be(List(("e", 5L)))
      replay(6L) should be(List())
    }
    "switch to the next partition if the current partition is full" in {
      writeEmittedEvents(List(event("a"), event("b"), event("c"), event("d"), event("e")))
      writeEmittedEvents(List(event("f"), event("g")))
      replay(1L) should be(List(("a", 1L), ("b", 2L), ("c", 3L), ("d", 4L), ("e", 5L), ("f", 6L), ("g", 7L)))
      replay(5L) should be(List(("e", 5L), ("f", 6L), ("g", 7L)))
      replay(6L) should be(List(("f", 6L), ("g", 7L)))
    }
    "switch to the next partition if the current partition isn't full but doesn't provide enough remaining space for a batch" in {
      val eventsA = List(event("a"), event("b"), event("c"), event("d"))
      val eventsB = List(event("f"), event("g"))

      log ! Write(eventsA, system.deadLetters, replyToProbe.ref, 0, 0)
      log ! Write(eventsB, system.deadLetters, replyToProbe.ref, 0, 0)

      val expectedA = eventsA.zipWithIndex.map {
        case (event, idx) => event.copy(vectorTimestamp = timestamp(1L + idx), processId = logId, localLogId = logId, localSequenceNr = 1L + idx)
      }

      val expectedB = eventsB.zipWithIndex.map {
        case (event, idx) => event.copy(vectorTimestamp = timestamp(6L + idx), processId = logId, localLogId = logId, localSequenceNr = 6L + idx)
      }

      replyToProbe.expectMsg(WriteSuccess(expectedA, 0, 0))
      replyToProbe.expectMsg(WriteSuccess(expectedB, 0, 0))

      replay(1L) should be(List(("a", 1L), ("b", 2L), ("c", 3L), ("d", 4L), ("f", 6L), ("g", 7L)))
      replay(5L) should be(List(("f", 6L), ("g", 7L)))
      replay(6L) should be(List(("f", 6L), ("g", 7L)))
    }
    "reject batches larger than the maximum partition size" in {
      val events = Vector(event("a"), event("b"), event("c"), event("d"), event("e"), event("f"))
      log ! Write(events, system.deadLetters, replyToProbe.ref, 0, 0)
      replyToProbe.expectMsgClass(classOf[WriteFailure])
    }
  }
}
