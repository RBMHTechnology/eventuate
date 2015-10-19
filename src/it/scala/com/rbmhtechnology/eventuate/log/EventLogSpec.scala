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

import scala.collection.immutable.Seq

import akka.actor._
import akka.testkit.{TestProbe, TestKit}

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.DurableEvent._
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.log.EventLogLifecycleCassandra.TestFailureSpec
import com.rbmhtechnology.eventuate.log.cassandra._

import com.typesafe.config.{ConfigFactory, Config}

import org.scalatest._

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
      |eventuate.snapshot.filesystem.dir = target/test-snapshot
      |
      |eventuate.log.leveldb.dir = target/test-log
      |eventuate.log.leveldb.index-update-limit = 6
      |eventuate.log.cassandra.default-port = 9142
      |eventuate.log.cassandra.index-update-limit = 3
      |eventuate.log.cassandra.init-retry-backoff = 1s
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

  var _requestorProbe: TestProbe = _
  var _replicatorProbe: TestProbe = _
  var _notificationProbe: TestProbe = _

  var _generatedEmittedEvents: Vector[DurableEvent] = Vector.empty
  var _generatedReplicatedEvents: Vector[DurableEvent] = Vector.empty

  def requestorProbe: TestProbe = _requestorProbe
  def replicatorProbe: TestProbe = _replicatorProbe
  def notificationProbe: TestProbe = _notificationProbe

  def generatedEmittedEvents: Vector[DurableEvent] = _generatedEmittedEvents
  def generatedReplicatedEvents: Vector[DurableEvent] = _generatedReplicatedEvents

  def log: ActorRef
  def logId: String

  override def beforeEach(): Unit = {
    _requestorProbe = TestProbe()
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
    log.tell(GetTimeTracker, requestorProbe.ref)
    requestorProbe.expectMsgClass(classOf[GetTimeTrackerSuccess]).tracker.sequenceNr
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
    log ! Write(events, system.deadLetters, requestorProbe.ref, 0)
    expected.foreach(event => requestorProbe.expectMsg(WriteSuccess(event, 0)))
    expected
  }

  def writeReplicatedEvents(events: Seq[DurableEvent], replicationProgress: Long, remoteLogId: String = remoteLogId): Seq[DurableEvent] = {
    val offset = currentSequenceNr + 1L
    val expected = expectedReplicatedEvents(events, offset)
    log.tell(ReplicationWrite(events, remoteLogId, replicationProgress, VectorTime()), replicatorProbe.ref)
    replicatorProbe.expectMsgPF() { case ReplicationWriteSuccess(_, `replicationProgress`, _) => }
    expected
  }

  def writeReplicationProgress(replicationProgress: Long, expectedStoredReplicationProgress: Long, remoteLogId: String = remoteLogId): Unit = {
    log.tell(ReplicationWrite(Seq(), remoteLogId, replicationProgress, VectorTime()), replicatorProbe.ref)
    replicatorProbe.expectMsgPF() { case ReplicationWriteSuccess(0, `expectedStoredReplicationProgress`, _) => }
  }

  def registerCollaborator(aggregateId: Option[String] = None, collaborator: TestProbe = TestProbe()): TestProbe = {
    log ! Replay(Long.MaxValue, collaborator.ref, aggregateId, 0)
    collaborator.expectMsg(ReplaySuccess(0))
    collaborator
  }

  def generateEmittedEvents(emitterAggregateId: Option[String] = None, customDestinationAggregateIds: Set[String] = Set()): Unit = {
    _generatedEmittedEvents ++= writeEmittedEvents(Vector(
      DurableEvent("a", emitterIdA, emitterAggregateId, customDestinationAggregateIds),
      DurableEvent("b", emitterIdA, emitterAggregateId, customDestinationAggregateIds),
      DurableEvent("c", emitterIdA, emitterAggregateId, customDestinationAggregateIds)))
  }

  def generateReplicatedEvents(emitterAggregateId: Option[String] = None, customDestinationAggregateIds: Set[String] = Set()): Unit = {
    _generatedReplicatedEvents ++= writeReplicatedEvents(Vector(
      DurableEvent("i", emitterIdB, emitterAggregateId, customDestinationAggregateIds, 0L, timestamp(0, 7), remoteLogId, remoteLogId, 7),
      DurableEvent("j", emitterIdB, emitterAggregateId, customDestinationAggregateIds, 0L, timestamp(0, 8), remoteLogId, remoteLogId, 8),
      DurableEvent("k", emitterIdB, emitterAggregateId, customDestinationAggregateIds, 0L, timestamp(0, 9), remoteLogId, remoteLogId, 9)), 17)
  }
}

abstract class EventLogSpec extends TestKit(ActorSystem("test", EventLogSpec.config)) with EventLogSpecSupport {
  import EventLogSpec._

  val dl = system.deadLetters

  "An event log" must {
    "write local events and send them to the requestor" in {
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

      log ! Write(events, system.deadLetters, requestorProbe.ref, 0)
      requestorProbe.expectMsg(WriteFailure(events(0), boom, 0))
      requestorProbe.expectMsg(WriteFailure(events(1), boom, 0))
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
      log.tell(GetReplicationProgresses, requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressesSuccess(Map()))
      generateReplicatedEvents()
      log.tell(GetReplicationProgresses, requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressesSuccess(Map(remoteLogId -> 17L)))
    }
    "reply with a failure message if replication fails" in {
      val events: Vector[DurableEvent] = Vector(
        DurableEvent("boom", emitterIdB, None, Set(), 0L, timestamp(0, 7), remoteLogId, remoteLogId, 7),
        DurableEvent("okay", emitterIdB, None, Set(), 0L, timestamp(0, 8), remoteLogId, remoteLogId, 8))

      log.tell(ReplicationWrite(events, remoteLogId, 8, VectorTime()), replicatorProbe.ref)
      replicatorProbe.expectMsg(ReplicationWriteFailure(boom))
    }
    "reply with a failure message if replication fails and not update the replication progress map" in {
      log.tell(GetReplicationProgresses, requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressesSuccess(Map()))
      val events: Vector[DurableEvent] = Vector(
        DurableEvent("boom", emitterIdB, None, Set(), 0L, timestamp(0, 7), remoteLogId, remoteLogId, 7),
        DurableEvent("okay", emitterIdB, None, Set(), 0L, timestamp(0, 8), remoteLogId, remoteLogId, 8))

      log.tell(ReplicationWrite(events, remoteLogId, 8, VectorTime()), replicatorProbe.ref)
      replicatorProbe.expectMsg(ReplicationWriteFailure(boom))
      log.tell(GetReplicationProgresses, requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressesSuccess(Map()))
    }
    "replay events from scratch" in {
      generateEmittedEvents()
      log ! Replay(1L, requestorProbe.ref, 0)
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(0), 0))
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(1), 0))
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(2), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "replay events from a custom position" in {
      generateEmittedEvents()
      log ! Replay(3L, requestorProbe.ref, 0)
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(2), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
      // custom position > last sequence number
      log ! Replay(5L, requestorProbe.ref, 0)
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "replay events from the default log if request aggregateId is not defined" in {
      generateEmittedEvents(customDestinationAggregateIds = Set("a1"))
      log ! Replay(1L, requestorProbe.ref, None, 0)
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(0), 0))
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(1), 0))
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(2), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "replay events from the index if request aggregateId is defined" in {
      generateEmittedEvents(customDestinationAggregateIds = Set("a1"))
      log ! Replay(1L, requestorProbe.ref, Some("a1"), 0)
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(0), 0))
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(1), 0))
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(2), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "replay events from the index and properly stop at the index classifier" in {
      generateEmittedEvents(customDestinationAggregateIds = Set("a1"))
      generateEmittedEvents(customDestinationAggregateIds = Set("a2"))
      log ! Replay(1L, requestorProbe.ref, Some("a1"), 0)
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(0), 0))
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(1), 0))
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(2), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "replay events from the index and from a custom position" in {
      generateEmittedEvents(customDestinationAggregateIds = Set("a1"))
      generateEmittedEvents(customDestinationAggregateIds = Set("a2"))
      log ! Replay(2L, requestorProbe.ref, Some("a1"), 0)
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(1), 0))
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(2), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
      log ! Replay(5L, requestorProbe.ref, Some("a1"), 0)
      requestorProbe.expectMsg(ReplaySuccess(0))
      log ! Replay(2L, requestorProbe.ref, Some("a2"), 0)
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(3), 0))
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(4), 0))
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(5), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
      log ! Replay(5L, requestorProbe.ref, Some("a2"), 0)
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(4), 0))
      requestorProbe.expectMsg(Replaying(generatedEmittedEvents(5), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "not replay events with non-matching aggregateId if request aggregateId is defined" in {
      generateEmittedEvents(customDestinationAggregateIds = Set("a1"))
      log ! Replay(1L, requestorProbe.ref, Some("a2"), 0)
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "reply with a failure message if replay fails" in {
      log ! Replay(-1, requestorProbe.ref, 0)
      requestorProbe.expectMsg(ReplayFailure(boom, 0))
    }
    "batch-read local events" in {
      generateEmittedEvents()
      log.tell(ReplicationRead(1, Int.MaxValue, NoFilter, UndefinedLogId, dl, VectorTime()), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(generatedEmittedEvents, 3, UndefinedLogId, VectorTime(logId -> 3L)))
    }
    "batch-read local and replicated events" in {
      generateEmittedEvents()
      generateReplicatedEvents()
      log.tell(ReplicationRead(1, Int.MaxValue, NoFilter, UndefinedLogId, dl, VectorTime()), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(generatedEmittedEvents ++ generatedReplicatedEvents, 6, UndefinedLogId, VectorTime(logId -> 3L, remoteLogId -> 9L)))
    }
    "batch-read events with a batch size limit" in {
      generateEmittedEvents()
      log.tell(ReplicationRead(1, 2, NoFilter, UndefinedLogId, dl, VectorTime()), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(generatedEmittedEvents.take(2), 2, UndefinedLogId, VectorTime(logId -> 3L)))
      log.tell(ReplicationRead(1, 0, NoFilter, UndefinedLogId, dl, VectorTime()), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(Nil, 0, UndefinedLogId, VectorTime(logId -> 3L)))
    }
    "batch-read events from a custom position" in {
      generateEmittedEvents()
      log.tell(ReplicationRead(2, Int.MaxValue, NoFilter, UndefinedLogId, dl, VectorTime()), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(generatedEmittedEvents.drop(1), 3, UndefinedLogId, VectorTime(logId -> 3L)))
    }
    "batch-read events from a custom position with a batch size limit" in {
      generateEmittedEvents()
      log.tell(ReplicationRead(2, 1, NoFilter, UndefinedLogId, dl, VectorTime()), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(generatedEmittedEvents.drop(1).take(1), 2, UndefinedLogId, VectorTime(logId -> 3L)))
    }
    "batch-read events with exclusion" in {
      generateEmittedEvents()
      generateReplicatedEvents()
      log.tell(ReplicationRead(1, Int.MaxValue, new ProcessIdFilter(remoteLogId), UndefinedLogId, dl, VectorTime()), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(generatedReplicatedEvents, 6, UndefinedLogId, VectorTime(logId -> 3L, remoteLogId -> 9L)))
      log.tell(ReplicationRead(1, Int.MaxValue, new ProcessIdFilter(logId), UndefinedLogId, dl, VectorTime()), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(generatedEmittedEvents, 6, UndefinedLogId, VectorTime(logId -> 3L, remoteLogId -> 9L)))
    }
    "not batch-read events from index" in {
      generateEmittedEvents(customDestinationAggregateIds = Set("a1"))
      generateEmittedEvents(customDestinationAggregateIds = Set())
      log.tell(ReplicationRead(1, Int.MaxValue, NoFilter, UndefinedLogId, dl, VectorTime()), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(generatedEmittedEvents, 6, UndefinedLogId, VectorTime(logId -> 6L)))
    }
    "reply with a failure message if batch-read fails" in {
      log.tell(ReplicationRead(-1, Int.MaxValue, NoFilter, UndefinedLogId, dl, VectorTime()), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadFailure(boom.getMessage, UndefinedLogId))
    }
    "recover the current sequence number on (re)start" in {
      generateEmittedEvents()
      log.tell(GetTimeTracker, requestorProbe.ref)
      requestorProbe.expectMsgType[GetTimeTrackerSuccess].tracker.sequenceNr should be(3L)
      log ! "boom"
      log.tell(GetTimeTracker, requestorProbe.ref)
      requestorProbe.expectMsgType[GetTimeTrackerSuccess].tracker.sequenceNr should be(3L)
    }
    "recover the replication progress on (re)start" in {
      log.tell(SetReplicationProgress("x", 17), requestorProbe.ref)
      requestorProbe.expectMsg(SetReplicationProgressSuccess("x", 17))
      log.tell(SetReplicationProgress("y", 19), requestorProbe.ref)
      requestorProbe.expectMsg(SetReplicationProgressSuccess("y", 19))
      log.tell(GetReplicationProgresses, requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressesSuccess(Map("x" -> 17, "y" -> 19)))
      log ! "boom"
      log.tell(GetReplicationProgresses, requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressesSuccess(Map("x" -> 17, "y" -> 19)))
    }
    "update the replication progress if last read sequence nr > last replicated sequence nr" in {
      log.tell(GetReplicationProgresses, requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressesSuccess(Map()))
      writeReplicationProgress(19, 19)
      log.tell(GetReplicationProgresses, requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressesSuccess(Map(EventLogSpec.remoteLogId -> 19L)))
    }
    "set an event's system timestamp if that event's processId is not defined" in {
      log ! Write(List(event("a").copy(systemTimestamp = 3L)), system.deadLetters, requestorProbe.ref, 0)
      requestorProbe.expectMsgType[WriteSuccess].event.systemTimestamp should be(0L)
    }
    "use an event's system timestamp if that event's processId is defined" in {
      log ! Write(List(event("a").copy(systemTimestamp = 3L, processId = "foo")), system.deadLetters, requestorProbe.ref, 0)
      requestorProbe.expectMsgType[WriteSuccess].event.systemTimestamp should be(3L)
    }
  }
}

class EventLogSpecLeveldb extends EventLogSpec with EventLogLifecycleLeveldb

class EventLogSpecCassandra extends EventLogSpec with EventLogLifecycleCassandra {
  import EventLogSpec._
  import CassandraIndex._

  var probe: TestProbe = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    probe = TestProbe()
    indexProbe.expectMsg(UpdateIndexSuccess(TimeTracker(sequenceNr = 0L), 0))
  }

  def expectReplay(aggregateId: Option[String], payloads: String *): Unit =
    expectReplay(1L, aggregateId, payloads: _*)

  def expectReplay(fromSequenceNr: Long, aggregateId: Option[String], payloads: String *): Unit = {
    log ! Replay(fromSequenceNr, requestorProbe.ref, aggregateId, 0)
    payloads.foreach(payload => requestorProbe.expectMsgClass(classOf[Replaying]).event.payload should be(payload))
    requestorProbe.expectMsg(ReplaySuccess(0))
  }

  "A Cassandra event log" must {
    "reuse source event iterators during replication" in {
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
      indexProbe.expectMsg(UpdateIndexSuccess(TimeTracker(sequenceNr = 2L, vectorTime = timestamp(2L)), 1))
    }
    "retry an index update on initialization if sequence number read fails" in {
      val failureLog = createLog(TestFailureSpec(failOnSequenceNrRead = true), indexProbe.ref)
      indexProbe.expectMsg(ReadTimeTrackerFailure(boom))
      indexProbe.expectMsg(UpdateIndexSuccess(TimeTracker(sequenceNr = 0L, vectorTime = VectorTime()), 0))
    }
    "retry an index update on initialization if index update fails" in {
      val failureLog = createLog(TestFailureSpec(failBeforeIndexIncrementWrite = true), indexProbe.ref)
      indexProbe.expectMsg(UpdateIndexFailure(boom))
      indexProbe.expectMsg(UpdateIndexSuccess(TimeTracker(sequenceNr = 0L, vectorTime = VectorTime()), 0))
      writeEmittedEvents(List(event("a"), event("b")), failureLog)
      failureLog ! "boom"
      indexProbe.expectMsg(UpdateIndexFailure(boom))
      indexProbe.expectMsg(UpdateIndexSuccess(TimeTracker(sequenceNr = 2L, vectorTime = timestamp(2L)), 1))
    }
    "run an index update after reaching the update limit with a single event batch" in {
      writeEmittedEvents(List(event("a"), event("b"), event("c"), event("d")))
      indexProbe.expectMsg(UpdateIndexSuccess(TimeTracker(sequenceNr = 4L, vectorTime = timestamp(4L)), 2))
    }
    "run an index update after reaching the update limit with a several event batches" in {
      writeEmittedEvents(List(event("a"), event("b")))
      writeEmittedEvents(List(event("c"), event("d")))
      indexProbe.expectMsg(UpdateIndexSuccess(TimeTracker(sequenceNr = 4L, vectorTime = timestamp(4L)), 2))
    }
    "run an index update on initialization and after reaching the update limit" in {
      writeEmittedEvents(List(event("a"), event("b")))
      log ! "boom"
      indexProbe.expectMsg(UpdateIndexSuccess(TimeTracker(sequenceNr = 2L, vectorTime = timestamp(2L)), 1))
      writeEmittedEvents(List(event("d"), event("e"), event("f")))
      indexProbe.expectMsg(UpdateIndexSuccess(TimeTracker(sequenceNr = 5L, vectorTime = timestamp(5L)), 1))
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

      indexProbe.expectMsg(UpdateIndexSuccess(TimeTracker(sequenceNr = 3L, vectorTime = timestamp(3L)), 1))

      writeEmittedEvents(List(
        event("d", Some("a1"))))

      expectReplay(Some("a1"), "a", "b", "c", "d")
    }
    "replay aggregate events from index and log with a lower sequence number bound" in {
      writeEmittedEvents(List(
        event("a", Some("a1")),
        event("b", Some("a1")),
        event("c", Some("a1"))))

      indexProbe.expectMsg(UpdateIndexSuccess(TimeTracker(sequenceNr = 3L, vectorTime = timestamp(3L)), 1))

      writeEmittedEvents(List(
        event("d", Some("a1"))))

      expectReplay(3L, Some("a1"), "c", "d")
    }
  }
}
