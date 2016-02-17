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

package com.rbmhtechnology.eventuate.log

import akka.actor._
import akka.pattern.ask
import akka.testkit.{TestProbe, TestKit}
import akka.util.Timeout

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.DurableEvent._
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.log.EventLogLifecycle._
import com.rbmhtechnology.eventuate.log.EventLogLifecycleCassandra.TestFailureSpec
import com.rbmhtechnology.eventuate.log.EventLogSpecLeveldb.immediateEventLogClockSnapshotConfig
import com.rbmhtechnology.eventuate.log.cassandra._
import com.rbmhtechnology.eventuate.utilities.RestarterActor.restartActor
import com.rbmhtechnology.eventuate.utilities._

import com.typesafe.config.{ConfigFactory, Config}

import org.scalatest._
import org.scalatest.time._
import org.scalatest.concurrent.Eventually

import scala.collection.immutable.Seq

object EventLogSpec {
  class ProcessIdFilter(processId: String) extends ReplicationFilter {
    override def apply(event: DurableEvent): Boolean =
      event.processId == processId
  }

  val config: Config = ConfigFactory.parseString(
    """
      |akka.loglevel = "ERROR"
      |akka.test.single-expect-default = 10s
      |
      |eventuate.log.leveldb.dir = target/test-log
      |eventuate.log.leveldb.index-update-limit = 6
      |eventuate.log.leveldb.deletion-batch-size = 2
      |eventuate.log.leveldb.deletion-retry-delay = 1ms
      |eventuate.log.cassandra.default-port = 9142
      |eventuate.log.cassandra.index-update-limit = 3
      |eventuate.log.cassandra.init-retry-delay = 1s
      |eventuate.snapshot.filesystem.dir = target/test-snapshot
    """.stripMargin)

  val emitterIdA = "A"
  val emitterIdB = "B"
  val remoteLogId = "R1"

  def event(payload: Any): DurableEvent =
    event(payload, VectorTime(), emitterIdA)

  def event(payload: Any, emitterAggregateId: Option[String]): DurableEvent =
    event(payload, VectorTime(), emitterIdA, emitterAggregateId, Set())

  def event(payload: Any, emitterAggregateId: Option[String], customDestinationAggregateIds: Set[String]): DurableEvent =
    event(payload, VectorTime(), emitterIdA, emitterAggregateId, customDestinationAggregateIds)

  def event(payload: Any, vectorTimestamp: VectorTime, emitterId: String, emitterAggregateId: Option[String] = None, customDestinationAggregateIds: Set[String] = Set()): DurableEvent =
    DurableEvent(payload, emitterId, emitterAggregateId, customDestinationAggregateIds, 0L, vectorTimestamp, UndefinedLogId, UndefinedLogId, UndefinedSequenceNr)

  implicit class RemoteDurableEvent(event: DurableEvent) {
    def remote: DurableEvent = event.copy(processId = remoteLogId)
  }
}

trait EventLogSpecSupport extends WordSpecLike with Matchers with BeforeAndAfterEach {
  import EventLogSpec._

  implicit val system: ActorSystem

  var _replyToProbe: TestProbe = _
  var _replicatorProbe: TestProbe = _
  var _notificationProbe: TestProbe = _

  var _generatedEmittedEvents: Vector[DurableEvent] = Vector.empty
  var _generatedReplicatedEvents: Vector[DurableEvent] = Vector.empty

  def replyToProbe: TestProbe = _replyToProbe
  def replicatorProbe: TestProbe = _replicatorProbe
  def notificationProbe: TestProbe = _notificationProbe

  def generatedEmittedEvents: Vector[DurableEvent] = _generatedEmittedEvents
  def generatedReplicatedEvents: Vector[DurableEvent] = _generatedReplicatedEvents

  def log: ActorRef
  def logId: String

  override def beforeEach(): Unit = {
    _replyToProbe = TestProbe()
    _replicatorProbe = TestProbe()
    _notificationProbe = TestProbe()
  }

  override def afterEach(): Unit = {
    _generatedEmittedEvents = Vector.empty
    _generatedReplicatedEvents = Vector.empty
  }

  def timestamp(a: Long = 0L, b: Long= 0L) = (a, b) match {
    case (0L, 0L) => VectorTime()
    case (a,  0L) => VectorTime(logId -> a)
    case (0L,  b) => VectorTime(remoteLogId -> b)
    case (a,   b) => VectorTime(logId -> a, remoteLogId -> b)
  }

  def currentSequenceNr: Long = {
    log.tell(GetEventLogClock, replyToProbe.ref)
    replyToProbe.expectMsgClass(classOf[GetEventLogClockSuccess]).clock.sequenceNr
  }

  def expectedEmittedEvents(events: Seq[DurableEvent], offset: Long = 0): Seq[DurableEvent] =
    events.zipWithIndex.map {
      case (event, idx) => event.copy(vectorTimestamp = timestamp(offset + idx), processId = logId, localLogId = logId, localSequenceNr = offset + idx)
    }

  def expectedReplicatedEvents(events: Seq[DurableEvent], offset: Long): Seq[DurableEvent] =
    events.zipWithIndex.map {
      case (event, idx) => event.copy(processId = remoteLogId, localLogId = logId, localSequenceNr = offset + idx)
    }

  def writeEmittedEvents(events: Seq[DurableEvent], log: ActorRef = log): Seq[DurableEvent] = {
    val offset = currentSequenceNr + 1L
    val expected = expectedEmittedEvents(events, offset)
    log ! Write(events, system.deadLetters, replyToProbe.ref, 0, 0)
    replyToProbe.expectMsg(WriteSuccess(expected, 0, 0))
    expected
  }

  def writeReplicatedEvents(events: Seq[DurableEvent], replicationProgress: Long, remoteLogId: String = remoteLogId): Seq[DurableEvent] = {
    val offset = currentSequenceNr + 1L
    val expected = expectedReplicatedEvents(events, offset)
    log.tell(ReplicationWrite(events, remoteLogId, replicationProgress, VectorTime()), replicatorProbe.ref)
    replicatorProbe.expectMsgPF() { case ReplicationWriteSuccess(_, _, `replicationProgress`, _) => }
    expected
  }

  def writeReplicationProgress(replicationProgress: Long, expectedStoredReplicationProgress: Long, remoteLogId: String = remoteLogId): Unit = {
    log.tell(ReplicationWrite(Seq(), remoteLogId, replicationProgress, VectorTime()), replicatorProbe.ref)
    replicatorProbe.expectMsgPF() { case ReplicationWriteSuccess(0, _, `expectedStoredReplicationProgress`, _) => }
  }

  def registerCollaborator(aggregateId: Option[String] = None, collaborator: TestProbe = TestProbe()): TestProbe = {
    log.tell(Replay(1L, 0, Some(collaborator.ref), aggregateId, 0), collaborator.ref)
    collaborator.expectMsg(ReplaySuccess(Nil, 0L, 0))
    collaborator
  }

  def generateEmittedEvents(emitterAggregateId: Option[String] = None, customDestinationAggregateIds: Set[String] = Set(), num: Int = 3): Unit = {
    _generatedEmittedEvents ++= writeEmittedEvents((1 to num).map { i =>
        DurableEvent(s"a-$i", emitterIdA, emitterAggregateId, customDestinationAggregateIds)
    })
  }

  def generateReplicatedEvents(emitterAggregateId: Option[String] = None, customDestinationAggregateIds: Set[String] = Set(), num: Int = 3): Unit = {
    _generatedReplicatedEvents ++= writeReplicatedEvents((1 to num).map { i =>
      DurableEvent(s"b-$i", emitterIdB, emitterAggregateId, customDestinationAggregateIds, 0L, timestamp(0, i + 6), remoteLogId, remoteLogId, i + 6)
    }, 17)
  }
}

abstract class EventLogSpec extends TestKit(ActorSystem("test", EventLogSpec.config)) with EventLogSpecSupport {
  import EventLogSpec._

  val dl = system.deadLetters
  implicit val implicitTimeout = Timeout(timeoutDuration)

  "An event log" must {
    "write local events" in {
      generateEmittedEvents()
      generateEmittedEvents()
    }
    "write local events with undefined defaultRoutingDestination and undefined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateEmittedEvents(emitterAggregateId = None, customDestinationAggregateIds = Set())
      collaborator.expectMsg(Written(generatedEmittedEvents(0)))
      collaborator.expectMsg(Written(generatedEmittedEvents(1)))
      collaborator.expectMsg(Written(generatedEmittedEvents(2)))
    }
    "write local events with undefined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateEmittedEvents(emitterAggregateId = None, customDestinationAggregateIds = Set("a1"))
      collaborator.expectMsg(Written(generatedEmittedEvents(0)))
      collaborator.expectMsg(Written(generatedEmittedEvents(1)))
      collaborator.expectMsg(Written(generatedEmittedEvents(2)))
    }
    "write local events with defined defaultRoutingDestination and undefined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateEmittedEvents(emitterAggregateId = Some("a1"), customDestinationAggregateIds = Set())
      collaborator.expectMsg(Written(generatedEmittedEvents(0)))
      collaborator.expectMsg(Written(generatedEmittedEvents(1)))
      collaborator.expectMsg(Written(generatedEmittedEvents(2)))
    }
    "write local events with defined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateEmittedEvents(emitterAggregateId = Some("a1"), customDestinationAggregateIds = Set("a2"))
      collaborator.expectMsg(Written(generatedEmittedEvents(0)))
      collaborator.expectMsg(Written(generatedEmittedEvents(1)))
      collaborator.expectMsg(Written(generatedEmittedEvents(2)))
    }
    "write local events with undefined defaultRoutingDestination and undefined customRoutingDestinations and not route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateEmittedEvents(emitterAggregateId = None, customDestinationAggregateIds = Set())
      collaborator.expectMsg(Written(generatedEmittedEvents(0)))
      collaborator.expectMsg(Written(generatedEmittedEvents(1)))
      collaborator.expectMsg(Written(generatedEmittedEvents(2)))
    }
    "write local events with undefined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateEmittedEvents(emitterAggregateId = None, customDestinationAggregateIds = Set("a1"))
      collaborator.expectMsg(Written(generatedEmittedEvents(0)))
      collaborator.expectMsg(Written(generatedEmittedEvents(0)))
      collaborator.expectMsg(Written(generatedEmittedEvents(1)))
      collaborator.expectMsg(Written(generatedEmittedEvents(1)))
      collaborator.expectMsg(Written(generatedEmittedEvents(2)))
      collaborator.expectMsg(Written(generatedEmittedEvents(2)))
    }
    "write local events with defined defaultRoutingDestination and undefined customRoutingDestinations and route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateEmittedEvents(emitterAggregateId = Some("a1"), customDestinationAggregateIds = Set())
      collaborator.expectMsg(Written(generatedEmittedEvents(0)))
      collaborator.expectMsg(Written(generatedEmittedEvents(0)))
      collaborator.expectMsg(Written(generatedEmittedEvents(1)))
      collaborator.expectMsg(Written(generatedEmittedEvents(1)))
      collaborator.expectMsg(Written(generatedEmittedEvents(2)))
      collaborator.expectMsg(Written(generatedEmittedEvents(2)))
    }
    "write local events with defined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = Some("a2"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateEmittedEvents(emitterAggregateId = Some("a1"), customDestinationAggregateIds = Set("a2"))
      collaborator.expectMsg(Written(generatedEmittedEvents(0)))
      collaborator.expectMsg(Written(generatedEmittedEvents(0)))
      collaborator.expectMsg(Written(generatedEmittedEvents(0)))
      collaborator.expectMsg(Written(generatedEmittedEvents(1)))
      collaborator.expectMsg(Written(generatedEmittedEvents(1)))
      collaborator.expectMsg(Written(generatedEmittedEvents(1)))
      collaborator.expectMsg(Written(generatedEmittedEvents(2)))
      collaborator.expectMsg(Written(generatedEmittedEvents(2)))
      collaborator.expectMsg(Written(generatedEmittedEvents(2)))
    }
    "reply with a failure message if write fails" in {
      val events = Vector(
        DurableEvent("boom", emitterIdA),
        DurableEvent("okay", emitterIdA))

      log ! Write(events, system.deadLetters, replyToProbe.ref, 0, 0)
      replyToProbe.expectMsg(WriteFailure(events, boom, 0, 0))
    }
    "write replicated events" in {
      generateReplicatedEvents()
    }
    "write replicated events with undefined defaultRoutingDestination and undefined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateReplicatedEvents(emitterAggregateId = None, customDestinationAggregateIds = Set())
      collaborator.expectMsg(Written(generatedReplicatedEvents(0)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(1)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(2)))
    }
    "write replicated events with undefined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateReplicatedEvents(emitterAggregateId = None, customDestinationAggregateIds = Set("a1"))
      collaborator.expectMsg(Written(generatedReplicatedEvents(0)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(1)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(2)))
    }
    "write replicated events with defined defaultRoutingDestination and undefined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateReplicatedEvents(emitterAggregateId = Some("a1"), customDestinationAggregateIds = Set())
      collaborator.expectMsg(Written(generatedReplicatedEvents(0)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(1)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(2)))
    }
    "write replicated events with defined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateReplicatedEvents(emitterAggregateId = Some("a1"), customDestinationAggregateIds = Set("a2"))
      collaborator.expectMsg(Written(generatedReplicatedEvents(0)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(1)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(2)))
    }
    "write replicated events with undefined defaultRoutingDestination and undefined customRoutingDestinations and not route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateReplicatedEvents(emitterAggregateId = None, customDestinationAggregateIds = Set())
      collaborator.expectMsg(Written(generatedReplicatedEvents(0)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(1)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(2)))
    }
    "write replicated events with undefined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateReplicatedEvents(emitterAggregateId = None, customDestinationAggregateIds = Set("a1"))
      collaborator.expectMsg(Written(generatedReplicatedEvents(0)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(0)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(1)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(1)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(2)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(2)))
    }
    "write replicated events with defined defaultRoutingDestination and undefined customRoutingDestinations and route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateReplicatedEvents(emitterAggregateId = Some("a1"), customDestinationAggregateIds = Set())
      collaborator.expectMsg(Written(generatedReplicatedEvents(0)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(0)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(1)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(1)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(2)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(2)))
    }
    "write replicated events with defined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = Some("a2"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateReplicatedEvents(emitterAggregateId = Some("a1"), customDestinationAggregateIds = Set("a2"))
      collaborator.expectMsg(Written(generatedReplicatedEvents(0)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(0)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(0)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(1)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(1)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(1)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(2)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(2)))
      collaborator.expectMsg(Written(generatedReplicatedEvents(2)))
    }
    "write replicated events and update the replication progress map" in {
      log.tell(GetReplicationProgresses, replyToProbe.ref)
      replyToProbe.expectMsg(GetReplicationProgressesSuccess(Map()))
      generateReplicatedEvents()
      log.tell(GetReplicationProgresses, replyToProbe.ref)
      replyToProbe.expectMsg(GetReplicationProgressesSuccess(Map(remoteLogId -> 17L)))
    }
    "reply with a failure message if replication fails" in {
      val events: Vector[DurableEvent] = Vector(
        DurableEvent("boom", emitterIdB, None, Set(), 0L, timestamp(0, 7), remoteLogId, remoteLogId, 7),
        DurableEvent("okay", emitterIdB, None, Set(), 0L, timestamp(0, 8), remoteLogId, remoteLogId, 8))

      log.tell(ReplicationWrite(events, remoteLogId, 8, VectorTime()), replicatorProbe.ref)
      replicatorProbe.expectMsg(ReplicationWriteFailure(boom))
    }
    "reply with a failure message if replication fails and not update the replication progress map" in {
      log.tell(GetReplicationProgresses, replyToProbe.ref)
      replyToProbe.expectMsg(GetReplicationProgressesSuccess(Map()))
      val events: Vector[DurableEvent] = Vector(
        DurableEvent("boom", emitterIdB, None, Set(), 0L, timestamp(0, 7), remoteLogId, remoteLogId, 7),
        DurableEvent("okay", emitterIdB, None, Set(), 0L, timestamp(0, 8), remoteLogId, remoteLogId, 8))

      log.tell(ReplicationWrite(events, remoteLogId, 8, VectorTime()), replicatorProbe.ref)
      replicatorProbe.expectMsg(ReplicationWriteFailure(boom))
      log.tell(GetReplicationProgresses, replyToProbe.ref)
      replyToProbe.expectMsg(GetReplicationProgressesSuccess(Map()))
    }
    "replay events from scratch" in {
      generateEmittedEvents()
      log.tell(Replay(1L, None, 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplaySuccess(generatedEmittedEvents, generatedEmittedEvents.last.localSequenceNr, 0))
    }
    "replay events in batches" in {
      generateEmittedEvents()
      generateEmittedEvents()
      log.tell(Replay(1L, 2, None, 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplaySuccess(generatedEmittedEvents.slice(0, 2), generatedEmittedEvents(1).localSequenceNr, 0))
      log.tell(Replay(generatedEmittedEvents(1).localSequenceNr + 1L, 2, None, 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplaySuccess(generatedEmittedEvents.slice(2, 4), generatedEmittedEvents(3).localSequenceNr, 0))
      log.tell(Replay(generatedEmittedEvents(3).localSequenceNr + 1L, 2, None, 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplaySuccess(generatedEmittedEvents.slice(4, 6), generatedEmittedEvents(5).localSequenceNr, 0))
    }
    "replay events from a custom position" in {
      generateEmittedEvents()
      log.tell(Replay(3L, None, 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplaySuccess(generatedEmittedEvents.slice(2, 3), generatedEmittedEvents(2).localSequenceNr, 0))
      // custom position > last sequence number
      log.tell(Replay(5L, None, 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplaySuccess(Nil, 4L, 0))
    }
    "replay events from the default log if request aggregateId is not defined" in {
      generateEmittedEvents(customDestinationAggregateIds = Set("a1"))
      log.tell(Replay(1L, None, 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplaySuccess(generatedEmittedEvents, generatedEmittedEvents.last.localSequenceNr, 0))
    }
    "replay events from the index if request aggregateId is defined" in {
      generateEmittedEvents(customDestinationAggregateIds = Set("a1"))
      log.tell(Replay(1L, None, Some("a1"), 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplaySuccess(generatedEmittedEvents, generatedEmittedEvents.last.localSequenceNr, 0))
    }
    "replay events from the index with proper isolation" in {
      generateEmittedEvents(customDestinationAggregateIds = Set("a1"))
      generateEmittedEvents(customDestinationAggregateIds = Set("a2"))
      log.tell(Replay(1L, None, Some("a1"), 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplaySuccess(generatedEmittedEvents.slice(0, 3), generatedEmittedEvents(2).localSequenceNr, 0))
    }
    "replay events from the index and from a custom position" in {
      generateEmittedEvents(customDestinationAggregateIds = Set("a1"))
      generateEmittedEvents(customDestinationAggregateIds = Set("a2"))
      log.tell(Replay(2L, None, Some("a1"), 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplaySuccess(generatedEmittedEvents.slice(1, 3), generatedEmittedEvents(2).localSequenceNr, 0))
      log.tell(Replay(5L, None, Some("a1"), 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplaySuccess(Nil, 4L, 0))
      log.tell(Replay(2L, None, Some("a2"), 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplaySuccess(generatedEmittedEvents.slice(3, 6), generatedEmittedEvents(5).localSequenceNr, 0))
      log.tell(Replay(5L, None, Some("a2"), 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplaySuccess(generatedEmittedEvents.slice(4, 6), generatedEmittedEvents(5).localSequenceNr, 0))
    }
    "not replay events with non-matching aggregateId if request aggregateId is defined" in {
      generateEmittedEvents(customDestinationAggregateIds = Set("a1"))
      log.tell(Replay(1L, None, Some("a2"), 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplaySuccess(Nil, 0L, 0))
    }
    "reply with a failure message if replay fails" in {
      log.tell(Replay(ErrorSequenceNr, None, 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplayFailure(boom, 0))
    }
    "replication-read local events" in {
      generateEmittedEvents()
      log.tell(ReplicationRead(1, Int.MaxValue, NoFilter, UndefinedLogId, dl, VectorTime()), replyToProbe.ref)
      replyToProbe.expectMsg(ReplicationReadSuccess(generatedEmittedEvents, 3, UndefinedLogId, VectorTime(logId -> 3L)))
    }
    "replication-read local and replicated events" in {
      generateEmittedEvents()
      generateReplicatedEvents()
      log.tell(ReplicationRead(1, Int.MaxValue, NoFilter, UndefinedLogId, dl, VectorTime()), replyToProbe.ref)
      replyToProbe.expectMsg(ReplicationReadSuccess(generatedEmittedEvents ++ generatedReplicatedEvents, 6, UndefinedLogId, VectorTime(logId -> 3L, remoteLogId -> 9L)))
    }
    "replication-read events with a batch size limit" in {
      generateEmittedEvents()
      log.tell(ReplicationRead(1, 2, NoFilter, UndefinedLogId, dl, VectorTime()), replyToProbe.ref)
      replyToProbe.expectMsg(ReplicationReadSuccess(generatedEmittedEvents.take(2), 2, UndefinedLogId, VectorTime(logId -> 3L)))
      log.tell(ReplicationRead(1, 0, NoFilter, UndefinedLogId, dl, VectorTime()), replyToProbe.ref)
      replyToProbe.expectMsg(ReplicationReadSuccess(Nil, 0, UndefinedLogId, VectorTime(logId -> 3L)))
    }
    "replication-read events from a custom position" in {
      generateEmittedEvents()
      log.tell(ReplicationRead(2, Int.MaxValue, NoFilter, UndefinedLogId, dl, VectorTime()), replyToProbe.ref)
      replyToProbe.expectMsg(ReplicationReadSuccess(generatedEmittedEvents.drop(1), 3, UndefinedLogId, VectorTime(logId -> 3L)))
    }
    "replication-read events from a custom position with a batch size limit" in {
      generateEmittedEvents()
      log.tell(ReplicationRead(2, 1, NoFilter, UndefinedLogId, dl, VectorTime()), replyToProbe.ref)
      replyToProbe.expectMsg(ReplicationReadSuccess(generatedEmittedEvents.slice(1, 2), 2, UndefinedLogId, VectorTime(logId -> 3L)))
    }
    "replication-read events with exclusion" in {
      generateEmittedEvents()
      generateReplicatedEvents()
      log.tell(ReplicationRead(1, Int.MaxValue, new ProcessIdFilter(remoteLogId), UndefinedLogId, dl, VectorTime()), replyToProbe.ref)
      replyToProbe.expectMsg(ReplicationReadSuccess(generatedReplicatedEvents, 6, UndefinedLogId, VectorTime(logId -> 3L, remoteLogId -> 9L)))
      log.tell(ReplicationRead(1, Int.MaxValue, new ProcessIdFilter(logId), UndefinedLogId, dl, VectorTime()), replyToProbe.ref)
      replyToProbe.expectMsg(ReplicationReadSuccess(generatedEmittedEvents, 6, UndefinedLogId, VectorTime(logId -> 3L, remoteLogId -> 9L)))
    }
    "not replication-read events from index" in {
      generateEmittedEvents(customDestinationAggregateIds = Set("a1"))
      generateEmittedEvents(customDestinationAggregateIds = Set())
      log.tell(ReplicationRead(1, Int.MaxValue, NoFilter, UndefinedLogId, dl, VectorTime()), replyToProbe.ref)
      replyToProbe.expectMsg(ReplicationReadSuccess(generatedEmittedEvents, 6, UndefinedLogId, VectorTime(logId -> 6L)))
    }
    "reply with a failure message if replication-read fails" in {
      log.tell(ReplicationRead(ErrorSequenceNr, Int.MaxValue, NoFilter, UndefinedLogId, dl, VectorTime()), replyToProbe.ref)
      replyToProbe.expectMsg(ReplicationReadFailure(boom.getMessage, UndefinedLogId))
    }
    "recover the current sequence number on (re)start" in {
      generateEmittedEvents()
      log.tell(GetEventLogClock, replyToProbe.ref)
      replyToProbe.expectMsgType[GetEventLogClockSuccess].clock.sequenceNr should be(3L)
      log ! "boom"
      log.tell(GetEventLogClock, replyToProbe.ref)
      replyToProbe.expectMsgType[GetEventLogClockSuccess].clock.sequenceNr should be(3L)
    }
    "recover the replication progress on (re)start" in {
      log.tell(SetReplicationProgress("x", 17), replyToProbe.ref)
      replyToProbe.expectMsg(SetReplicationProgressSuccess("x", 17))
      log.tell(SetReplicationProgress("y", 19), replyToProbe.ref)
      replyToProbe.expectMsg(SetReplicationProgressSuccess("y", 19))
      log.tell(GetReplicationProgresses, replyToProbe.ref)
      replyToProbe.expectMsg(GetReplicationProgressesSuccess(Map("x" -> 17, "y" -> 19)))
      log ! "boom"
      log.tell(GetReplicationProgresses, replyToProbe.ref)
      replyToProbe.expectMsg(GetReplicationProgressesSuccess(Map("x" -> 17, "y" -> 19)))
    }
    "update the replication progress if last read sequence nr > last replicated sequence nr" in {
      log.tell(GetReplicationProgresses, replyToProbe.ref)
      replyToProbe.expectMsg(GetReplicationProgressesSuccess(Map()))
      writeReplicationProgress(19, 19)
      log.tell(GetReplicationProgresses, replyToProbe.ref)
      replyToProbe.expectMsg(GetReplicationProgressesSuccess(Map(EventLogSpec.remoteLogId -> 19L)))
    }
    "update an event's system timestamp" in {
      log ! Write(List(event("a").copy(systemTimestamp = 3L)), system.deadLetters, replyToProbe.ref, 0, 0)
      replyToProbe.expectMsgType[WriteSuccess].events.head.systemTimestamp should be(0L)
    }
    "update an emitted event's process id and vector timestamp during if the process id is not defined" in {
      val evt = DurableEvent("a", emitterIdA, processId = UndefinedLogId)
      val exp = DurableEvent("a", emitterIdA, processId = logId, vectorTimestamp = VectorTime(logId -> 1L), localLogId = logId, localSequenceNr = 1)
      log ! Write(List(evt), system.deadLetters, replyToProbe.ref, 0, 0)
      replyToProbe.expectMsgType[WriteSuccess].events.head should be(exp)
    }
    "not update an emitted event's process id and vector timestamp during if the process id is defined" in {
      val evt = DurableEvent("a", emitterIdA, processId = emitterIdA, vectorTimestamp = VectorTime(emitterIdA -> 1L))
      val exp = DurableEvent("a", emitterIdA, processId = emitterIdA, vectorTimestamp = VectorTime(emitterIdA -> 1L), localLogId = logId, localSequenceNr = 1)
      log ! Write(List(evt), system.deadLetters, replyToProbe.ref, 0, 0)
      replyToProbe.expectMsgType[WriteSuccess].events.head should be(exp)
    }
    "update a replicated event's process id and vector timestamp during if the process id is not defined" in {
      val evt = DurableEvent("a", emitterIdA, processId = UndefinedLogId, vectorTimestamp = VectorTime(remoteLogId -> 1L))
      val exp = DurableEvent("a", emitterIdA, processId = logId, vectorTimestamp = VectorTime(remoteLogId -> 1L, logId -> 1L), localLogId = logId, localSequenceNr = 1)
      registerCollaborator(aggregateId = None, collaborator = replyToProbe)
      log ! ReplicationWrite(List(evt), remoteLogId, 5, VectorTime())
      replyToProbe.expectMsgType[Written].event should be(exp)
    }
    "not update a replicated event's process id and vector timestamp during if the process id is defined" in {
      val evt = DurableEvent("a", emitterIdA, processId = emitterIdA, vectorTimestamp = VectorTime(emitterIdA -> 1L))
      val exp = DurableEvent("a", emitterIdA, processId = emitterIdA, vectorTimestamp = VectorTime(emitterIdA -> 1L), localLogId = logId, localSequenceNr = 1)
      registerCollaborator(aggregateId = None, collaborator = replyToProbe)
      log ! ReplicationWrite(List(evt), remoteLogId, 5, VectorTime())
      replyToProbe.expectMsgType[Written].event should be(exp)
    }
    "not write events to the target log that are in causal past of the target log" in {
      val evt1 = DurableEvent("i", emitterIdB, vectorTimestamp = timestamp(0, 7), processId = remoteLogId)
      val evt2 = DurableEvent("j", emitterIdB, vectorTimestamp = timestamp(0, 8), processId = remoteLogId)
      val evt3 = DurableEvent("k", emitterIdB, vectorTimestamp = timestamp(0, 9), processId = remoteLogId)
      registerCollaborator(aggregateId = None, collaborator = replyToProbe)
      log ! ReplicationWrite(List(evt1, evt2), remoteLogId, 5, VectorTime())
      log ! ReplicationWrite(List(evt2, evt3), remoteLogId, 6, VectorTime())
      replyToProbe.expectMsgType[Written].event.payload should be("i")
      replyToProbe.expectMsgType[Written].event.payload should be("j")
      replyToProbe.expectMsgType[Written].event.payload should be("k")
    }
    "not read events from the source log that are in causal past of the target log (using the target time from the request)" in {
      generateEmittedEvents()
      log.tell(ReplicationRead(1, Int.MaxValue, NoFilter, remoteLogId, dl, timestamp(1)), replyToProbe.ref)
      replyToProbe.expectMsgType[ReplicationReadSuccess].events.map(_.payload) should be(Seq("a-2", "a-3"))
    }
    "not read events from the source log that are in causal past of the target log (using the target time from the cache)" in {
      generateEmittedEvents()
      log ! ReplicationWrite(Nil, remoteLogId, 5, timestamp(2)) // update time cache
      log.tell(ReplicationRead(1, Int.MaxValue, NoFilter, remoteLogId, dl, timestamp(1)), replyToProbe.ref)
      replyToProbe.expectMsgType[ReplicationReadSuccess].events.map(_.payload) should be(Seq("a-3"))
    }
    "delete all events when requested sequence nr is higher than current" in {
      generateEmittedEvents()
      (log ? Delete(generatedEmittedEvents(2).localSequenceNr + 1)).await
      log.tell(Replay(1L, None, 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplaySuccess(Nil, 3L, 0))
    }
    "not replay deleted events" in {
      generateEmittedEvents()
      (log ? Delete(generatedEmittedEvents(1).localSequenceNr)).await
      log.tell(Replay(1L, None, 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplaySuccess(generatedEmittedEvents.slice(2, 3), generatedEmittedEvents(2).localSequenceNr, 0))
    }
    "not replay deleted events from an index" in {
      generateEmittedEvents(customDestinationAggregateIds = Set("a"))
      (log ? Delete(generatedEmittedEvents(1).localSequenceNr)).await
      log.tell(Replay(1L, None, Some("a"), 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplaySuccess(generatedEmittedEvents.slice(2, 3), generatedEmittedEvents(2).localSequenceNr, 0))
    }
    "not delete future events when requested sequence nr is higher than current" in {
      (log ? Delete(10)).await
      generateEmittedEvents()
      log.tell(Replay(1L, None, 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplaySuccess(generatedEmittedEvents, generatedEmittedEvents.last.localSequenceNr, 0))
    }
    "not mark already deleted events as not deleted" in {
      generateEmittedEvents()
      log ! Delete(generatedEmittedEvents(2).localSequenceNr)
      (log ? Delete(generatedEmittedEvents(1).localSequenceNr)).await
      log.tell(Replay(1L, None, 0), replyToProbe.ref)
      replyToProbe.expectMsg(ReplaySuccess(Nil, 3L, 0))
    }
  }
}


object EventLogSpecLeveldb {
  val immediateEventLogClockSnapshotConfig =
    ConfigFactory.parseString("eventuate.log.leveldb.state-snapshot-limit = 1")
}

class EventLogSpecLeveldb extends EventLogSpec with EventLogLifecycleLeveldb {

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

class EventLogWithImmediateEventLogClockSnapshotSpecLeveldb
  extends TestKit(ActorSystem("test", immediateEventLogClockSnapshotConfig.withFallback(EventLogSpec.config)))
  with EventLogSpecSupport with EventLogLifecycleLeveldb with Eventually {

  import EventLogSpec._

  override implicit def patienceConfig = PatienceConfig(Span(10, Seconds), Span(100, Millis))

  val dl = system.deadLetters
  implicit val implicitTimeout = Timeout(timeoutDuration)

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

class EventLogSpecCassandra extends EventLogSpec with EventLogLifecycleCassandra {
  import EventLogSpec._
  import CassandraIndex._

  var probe: TestProbe = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    probe = TestProbe()
    indexProbe.expectMsg(UpdateIndexSuccess(EventLogClock(sequenceNr = 0L), 0))
  }

  def expectReplay(aggregateId: Option[String], payloads: String *): Unit =
    expectReplay(1L, aggregateId, payloads: _*)

  def expectReplay(fromSequenceNr: Long, aggregateId: Option[String], payloads: String *): Unit = {
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
        log.tell(ReplicationRead(i, 1, NoFilter, "test-target-log-id", dl, VectorTime()), probe.ref)
        probe.expectMsgClass(classOf[ReplicationReadSuccess])
      }
    }
  }

  "A Cassandra event log and its index" must {
    "run an index update on initialization" in {
      writeEmittedEvents(List(event("a"), event("b")))
      log ! "boom"
      indexProbe.expectMsg(UpdateIndexSuccess(EventLogClock(sequenceNr = 2L, versionVector = timestamp(2L)), 1))
    }
    "retry an index update on initialization if sequence number read fails" in {
      val failureLog = createLog(TestFailureSpec(failOnClockRead = true), indexProbe.ref)
      indexProbe.expectMsg(ReadClockFailure(boom))
      indexProbe.expectMsg(UpdateIndexSuccess(EventLogClock(sequenceNr = 0L, versionVector = VectorTime()), 0))
    }
    "retry an index update on initialization if index update fails" in {
      val failureLog = createLog(TestFailureSpec(failBeforeIndexIncrementWrite = true), indexProbe.ref)
      indexProbe.expectMsg(UpdateIndexFailure(boom))
      indexProbe.expectMsg(UpdateIndexSuccess(EventLogClock(sequenceNr = 0L, versionVector = VectorTime()), 0))
      writeEmittedEvents(List(event("a"), event("b")), failureLog)
      failureLog ! "boom"
      indexProbe.expectMsg(UpdateIndexFailure(boom))
      indexProbe.expectMsg(UpdateIndexSuccess(EventLogClock(sequenceNr = 2L, versionVector = timestamp(2L)), 1))
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
    "run an index update on initialization and after reaching the update limit" in {
      writeEmittedEvents(List(event("a"), event("b")))
      log ! "boom"
      indexProbe.expectMsg(UpdateIndexSuccess(EventLogClock(sequenceNr = 2L, versionVector = timestamp(2L)), 1))
      writeEmittedEvents(List(event("d"), event("e"), event("f")))
      indexProbe.expectMsg(UpdateIndexSuccess(EventLogClock(sequenceNr = 5L, versionVector = timestamp(5L)), 1))
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
      replyToProbe.expectMsgClass(classOf[ReplaySuccess]).events.map(_.payload) should be (Seq("a", "b"))
      log.tell(Replay(3L, 2, None, Some("a1"), 0), replyToProbe.ref)
      replyToProbe.expectMsgClass(classOf[ReplaySuccess]).events.map(_.payload) should be (Seq("c", "d"))
    }
    "replication-read deleted (replicated or local) events" in {
      generateEmittedEvents()
      generateReplicatedEvents()
      (log ? Delete(generatedReplicatedEvents(1).localSequenceNr)).await
      log.tell(ReplicationRead(1, Int.MaxValue, NoFilter, UndefinedLogId, dl, VectorTime()), replyToProbe.ref)
      replyToProbe.expectMsg(ReplicationReadSuccess(generatedEmittedEvents ++ generatedReplicatedEvents, 6, UndefinedLogId, VectorTime(logId -> 3L, remoteLogId -> 9L)))
    }
  }
}
