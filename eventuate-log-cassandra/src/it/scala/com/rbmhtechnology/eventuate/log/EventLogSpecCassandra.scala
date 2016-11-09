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
import akka.pattern.ask

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.DurableEvent._
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.SingleLocationSpecCassandra.TestFailureSpec
import com.rbmhtechnology.eventuate.log.cassandra.CassandraIndex
import com.rbmhtechnology.eventuate.utilities._
import com.typesafe.config._

object EventLogSpecCassandra {
  val config: Config = ConfigFactory.parseString(
    """
      |eventuate.log.cassandra.default-port = 9142
      |eventuate.log.cassandra.index-update-limit = 3
      |eventuate.log.cassandra.init-retry-delay = 1s
    """.stripMargin).withFallback(EventLogSpec.config)
}

class EventLogSpecCassandra extends TestKit(ActorSystem("test", EventLogSpecCassandra.config)) with EventLogSpec with SingleLocationSpecCassandra {
  import EventLogSpec._
  import CassandraIndex._

  var probe: TestProbe = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    probe = TestProbe()
  }

  def expectReplay(aggregateId: Option[String], payloads: String*): Unit =
    expectReplay(1L, aggregateId, payloads: _*)

  def expectReplay(fromSequenceNr: Long, aggregateId: Option[String], payloads: String*): Unit = {
    log.tell(Replay(fromSequenceNr, None, aggregateId, 0), replyToProbe.ref)
    val act = replyToProbe.expectMsgClass(classOf[ReplaySuccess]).events.map(_.payload)
    val exp = payloads.toVector
    act should be(exp)
  }

  "A Cassandra event log" must {
    "serve small replication reads efficiently" in {
      val num = 1000

      val events = 1 to num map { i =>
        event(s"e-$i", timestamp(i, 0), emitterIdA)
      }

      writeEmittedEvents(events)

      1 to num foreach { i =>
        log.tell(ReplicationRead(i, 1, 100, NoFilter, "test-target-log-id", dl, VectorTime()), probe.ref)
        probe.expectMsgClass(classOf[ReplicationReadSuccess])
      }
    }
  }

  "A Cassandra event log and its index" must {
    "recover the update counter and run an index update after reaching exactly the update limit" in {
      writeEmittedEvents(List(event("a"), event("b")))
      log ! "boom"
      writeEmittedEvents(List(event("d")))
      indexProbe.expectMsg(UpdateIndexSuccess(EventLogClock(sequenceNr = 3L, versionVector = timestamp(3L)), 1))
    }
    "recover the update counter and run an index update after exceeding the update limit" in {
      writeEmittedEvents(List(event("a"), event("b")))
      log ! "boom"
      writeEmittedEvents(List(event("d"), event("e"), event("f")))
      indexProbe.expectMsg(UpdateIndexSuccess(EventLogClock(sequenceNr = 5L, versionVector = timestamp(5L)), 2))
    }
    "retry state recovery if clock read fails" in {
      val failureLog = createLog(TestFailureSpec(failOnClockRead = true), indexProbe.ref)
      writeEmittedEvents(List(event("a"), event("b"), event("c")), failureLog)
      indexProbe.expectMsg(UpdateIndexSuccess(EventLogClock(sequenceNr = 3L, versionVector = timestamp(3L)), 1))
    }
    "run an index update after reaching the update limit with a single event batch" in {
      writeEmittedEvents(List(event("a"), event("b"), event("c"), event("d")))
      indexProbe.expectMsg(UpdateIndexSuccess(EventLogClock(sequenceNr = 4L, versionVector = timestamp(4L)), 2))
    }
    "run an index update after reaching the update limit with a several event batches" in {
      writeEmittedEvents(List(event("a"), event("b")))
      writeEmittedEvents(List(event("c"), event("d")))
      indexProbe.expectMsg(UpdateIndexSuccess(EventLogClock(sequenceNr = 4L, versionVector = timestamp(4L)), 2))
    }
    "return the initial value for a replication progress" in {
      log.tell(GetReplicationProgress(remoteLogId), probe.ref)
      probe.expectMsg(GetReplicationProgressSuccess(remoteLogId, 0L, VectorTime()))
    }
    "return the updated value for a replication progress" in {
      writeReplicatedEvents(List(event("a").remote, event("b").remote), 4L)
      log.tell(GetReplicationProgress(remoteLogId), probe.ref)
      probe.expectMsg(GetReplicationProgressSuccess(remoteLogId, 4L, VectorTime()))
    }
    "add events with emitter aggregate id to index" in {
      writeEmittedEvents(List(
        event("a", None),
        event("b", Some("a1")),
        event("c", None),
        event("d", Some("a1")),
        event("e", None),
        event("f", Some("a1"))))

      expectReplay(None, "a", "b", "c", "d", "e", "f")
      expectReplay(Some("a1"), "b", "d", "f")
    }
    "add events with custom routing destinations to index" in {
      writeEmittedEvents(List(
        event("a", None),
        event("b", None, Set("a1")),
        event("c", None),
        event("d", None, Set("a1")),
        event("e", None),
        event("f", None, Set("a1", "a2"))))

      expectReplay(None, "a", "b", "c", "d", "e", "f")
      expectReplay(Some("a1"), "b", "d", "f")
      expectReplay(Some("a2"), "f")
    }
    "add events with emitter aggregate id and custom routing destinations to index" in {
      writeEmittedEvents(List(
        event("a", None),
        event("b", Some("a1"), Set("a2")),
        event("c", None),
        event("d", Some("a1"), Set("a2")),
        event("e", None),
        event("f", Some("a1"), Set("a3"))))

      expectReplay(None, "a", "b", "c", "d", "e", "f")
      expectReplay(Some("a1"), "b", "d", "f")
      expectReplay(Some("a2"), "b", "d")
      expectReplay(Some("a3"), "f")
    }
    "replay aggregate events from log" in {
      writeEmittedEvents(List(
        event("a", Some("a1")),
        event("b", Some("a1"))))

      expectReplay(Some("a1"), "a", "b")
    }
    "replay aggregate events from index and log" in {
      writeEmittedEvents(List(
        event("a", Some("a1")),
        event("b", Some("a1")),
        event("c", Some("a1"))))

      indexProbe.expectMsg(UpdateIndexSuccess(EventLogClock(sequenceNr = 3L, versionVector = timestamp(3L)), 1))

      writeEmittedEvents(List(
        event("d", Some("a1"))))

      expectReplay(Some("a1"), "a", "b", "c", "d")
    }
    "replay aggregate events from index and log with a lower sequence number bound" in {
      writeEmittedEvents(List(
        event("a", Some("a1")),
        event("b", Some("a1")),
        event("c", Some("a1"))))

      indexProbe.expectMsg(UpdateIndexSuccess(EventLogClock(sequenceNr = 3L, versionVector = timestamp(3L)), 1))

      writeEmittedEvents(List(
        event("d", Some("a1"))))

      expectReplay(3L, Some("a1"), "c", "d")
    }
    "replay aggregate events from index and log in batches" in {
      writeEmittedEvents(List(
        event("a", Some("a1")),
        event("b", Some("a1")),
        event("c", Some("a1"))))

      indexProbe.expectMsg(UpdateIndexSuccess(EventLogClock(sequenceNr = 3L, versionVector = timestamp(3L)), 1))

      writeEmittedEvents(List(
        event("d", Some("a1"))))

      log.tell(Replay(1L, 2, None, Some("a1"), 0), replyToProbe.ref)
      replyToProbe.expectMsgClass(classOf[ReplaySuccess]).events.map(_.payload) should be(Seq("a", "b"))
      log.tell(Replay(3L, 2, None, Some("a1"), 0), replyToProbe.ref)
      replyToProbe.expectMsgClass(classOf[ReplaySuccess]).events.map(_.payload) should be(Seq("c", "d"))
    }
    "replication-read deleted (replicated or local) events" in {
      generateEmittedEvents()
      generateReplicatedEvents()
      (log ? Delete(generatedReplicatedEvents(1).localSequenceNr)).await
      log.tell(ReplicationRead(1, Int.MaxValue, Int.MaxValue, NoFilter, UndefinedLogId, dl, VectorTime()), replyToProbe.ref)
      replyToProbe.expectMsg(ReplicationReadSuccess(generatedEmittedEvents ++ generatedReplicatedEvents, 1, 6, UndefinedLogId, VectorTime(logId -> 3L, remoteLogId -> 9L)))
    }
  }
}
