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
import akka.pattern.ask
import akka.testkit._
import akka.util.Timeout

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.DurableEvent._
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.SingleLocationSpec._
import com.rbmhtechnology.eventuate.utilities.RestarterActor.restartActor
import com.rbmhtechnology.eventuate.utilities._
import com.typesafe.config._

import org.scalatest.time._
import org.scalatest.concurrent.Eventually

object EventLogSpecLeveldb {
  val config: Config = ConfigFactory.parseString(
    """
      |eventuate.log.leveldb.dir = target/test-log
      |eventuate.log.leveldb.index-update-limit = 6
      |eventuate.log.leveldb.deletion-batch-size = 2
      |eventuate.log.leveldb.deletion-retry-delay = 1ms
    """.stripMargin).withFallback(EventLogSpec.config)
}

class EventLogSpecLeveldb extends TestKit(ActorSystem("test", EventLogSpecLeveldb.config)) with EventLogSpec with SingleLocationSpecLeveldb {
  "A LeveldbEventLog" must {
    "not delete events required for restoring the EventLogClock" in {
      generateEmittedEvents()
      val currentEventLogClock = (log ? GetEventLogClock).mapTo[GetEventLogClockSuccess].await
      (log ? Delete(generatedEmittedEvents.last.localSequenceNr)).await
      val restartedLog = restartActor(log)
      val restoredEventLogClock = (restartedLog ? GetEventLogClock).mapTo[GetEventLogClockSuccess].await
      restoredEventLogClock should be(currentEventLogClock)
    }
  }
}

object EventLogWithImmediateEventLogClockSnapshotSpecLeveldb {
  val config = ConfigFactory.parseString("eventuate.log.leveldb.state-snapshot-limit = 1")
}

class EventLogWithImmediateEventLogClockSnapshotSpecLeveldb
  extends TestKit(ActorSystem("test", EventLogWithImmediateEventLogClockSnapshotSpecLeveldb.config.withFallback(EventLogSpecLeveldb.config)))
  with EventLogSpecSupport with SingleLocationSpecLeveldb with Eventually {

  import EventLogSpec._

  private val dl: ActorRef =
    system.deadLetters

  implicit val implicitTimeout =
    Timeout(timeoutDuration)

  override implicit def patienceConfig =
    PatienceConfig(Span(10, Seconds), Span(100, Millis))

  "A LeveldbEventLog" must {
    "actually delete events from the log and an index" in {
      generateEmittedEvents(customDestinationAggregateIds = Set("a"))
      generateEmittedEvents()
      (log ? Delete(generatedEmittedEvents(4).localSequenceNr)).await
      eventually {
        val probe = TestProbe()
        log.tell(Replay(IgnoreDeletedSequenceNr, None, 0), probe.ref)
        probe.expectMsg(ReplaySuccess(generatedEmittedEvents.slice(5, 6), generatedEmittedEvents(5).localSequenceNr, 0))
      }
      eventually {
        val probe = TestProbe()
        log.tell(Replay(IgnoreDeletedSequenceNr, None, Some("a"), 0), probe.ref)
        probe.expectMsg(ReplaySuccess(Nil, 0L, 0))
      }
    }
    "actually delete events from the log and an index when overlapping conditional delete and replication requests are sent" in {
      generateEmittedEvents(customDestinationAggregateIds = Set("a"), num = 50)
      generateEmittedEvents(num = 50)
      generatedEmittedEvents.foreach { event =>
        (log ? Delete(event.localSequenceNr, Set(remoteLogId))).await
        log ! ReplicationRead((event.localSequenceNr - 10) max 0, Int.MaxValue, NoFilter, remoteLogId, dl, VectorTime())
      }
      log ! ReplicationRead(generatedEmittedEvents.last.localSequenceNr + 1, Int.MaxValue, NoFilter, remoteLogId, dl, VectorTime())
      eventually {
        val probe = TestProbe()
        log.tell(Replay(IgnoreDeletedSequenceNr, None, 0), probe.ref)
        probe.expectMsg(ReplaySuccess(Nil, 0L, 0))
      }
      eventually {
        val probe = TestProbe()
        log.tell(Replay(IgnoreDeletedSequenceNr, None, Some("a"), 0), probe.ref)
        probe.expectMsg(ReplaySuccess(Nil, 0L, 0))
      }
    }
    "not replication-read unconditionally deleted (replicated or local) events" in {
      generateEmittedEvents()
      generateReplicatedEvents()
      (log ? Delete(generatedReplicatedEvents(1).localSequenceNr)).await
      eventually {
        val probe = TestProbe()
        log.tell(ReplicationRead(1, Int.MaxValue, NoFilter, UndefinedLogId, dl, VectorTime()), probe.ref)
        probe.expectMsg(ReplicationReadSuccess(List(generatedReplicatedEvents.last), 6, UndefinedLogId, VectorTime(logId -> 3L, remoteLogId -> 9L)))
      }
    }
    "continue with unfinished conditional deletion of replicated events after restart" in {
      generateEmittedEvents()
      (log ? Delete(generatedEmittedEvents.last.localSequenceNr, Set(remoteLogId))).await
      val restartedLog = restartActor(log)
      restartedLog ! ReplicationRead(generatedEmittedEvents.last.localSequenceNr + 1, Int.MaxValue, NoFilter, remoteLogId, dl, VectorTime())
      eventually {
        val probe = TestProbe()
        restartedLog.tell(Replay(IgnoreDeletedSequenceNr, None, 0), probe.ref)
        probe.expectMsg(ReplaySuccess(Nil, 0L, 0))
      }
    }
    "delete events after successful replication if conditionally deleted" in {
      generateEmittedEvents()
      (log ? Delete(generatedEmittedEvents(1).localSequenceNr, Set(remoteLogId))).await
      log.tell(ReplicationRead(1, Int.MaxValue, NoFilter, remoteLogId, dl, VectorTime()), replyToProbe.ref)
      replyToProbe.expectMsg(ReplicationReadSuccess(generatedEmittedEvents, 3, remoteLogId, VectorTime(logId -> 3L)))
      // indicate that remoteLogId has successfully replicated all events
      log.tell(ReplicationRead(generatedEmittedEvents.last.localSequenceNr + 1, Int.MaxValue, NoFilter, remoteLogId, dl, VectorTime()), ActorRef.noSender)

      eventually {
        val probe = TestProbe()
        log.tell(ReplicationRead(1, Int.MaxValue, NoFilter, "otherLog", dl, VectorTime()), probe.ref)
        probe.expectMsg(ReplicationReadSuccess(List(generatedEmittedEvents.last), 3, "otherLog", VectorTime(logId -> 3L)))
      }
    }
  }
}
