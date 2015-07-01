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
  case object GetReplicationProgress
  case class GetReplicationProgressSuccess(progress: Map[String, Long])
  case class GetReplicationProgressFailure(cause: Throwable)

  case class SetReplicationProgress(logId: String, progress: Long)

  case object GetSequenceNr
  case class GetSequenceNrSuccess(sequenceNr: Long)

  val config: Config = ConfigFactory.parseString(
    """
      |akka.loglevel = "ERROR"
      |akka.test.single-expect-default = 10s
      |
      |eventuate.snapshot.filesystem.dir = target/test-snapshot
      |
      |eventuate.log.leveldb.dir = target/test-log
      |eventuate.log.cassandra.default-port = 9142
      |eventuate.log.cassandra.index-update-limit = 3
      |eventuate.log.cassandra.init-retry-backoff = 1s
    """.stripMargin)

  val idA = "A"
  val idB = "B"
  val idC = "C"
  val remoteLogId = "R1"

  val remoteLogIdFilter = SourceLogIdExclusionFilter(remoteLogId)
  val undefinedLogIdFilter = SourceLogIdExclusionFilter(UndefinedLogId)

  def event(payload: Any): DurableEvent =
    event(payload, VectorTime(), idA)

  def event(payload: Any, emitterAggregateId: Option[String]): DurableEvent =
    event(payload, VectorTime(), idA, emitterAggregateId, Set())

  def event(payload: Any, emitterAggregateId: Option[String], customRoutingDestinations: Set[String]): DurableEvent =
    event(payload, VectorTime(), idA, emitterAggregateId, customRoutingDestinations)

  def event(payload: Any, timestamp: VectorTime, emitterId: String, emitterAggregateId: Option[String] = None, customRoutingDestinations: Set[String] = Set()): DurableEvent =
    DurableEvent(payload, 0L, timestamp, emitterId, emitterAggregateId, customRoutingDestinations)

  def timestampA(timeA: Long): VectorTime =
    VectorTime(idA -> timeA)

  def timestampAB(timeA: Long, timeB: Long): VectorTime =
    VectorTime(idA -> timeA, idB -> timeB)
}

trait EventLogSpecSupport extends WordSpecLike with Matchers with BeforeAndAfterEach {
  implicit val system: ActorSystem

  import EventLogSpec._

  var _requestorProbe: TestProbe = _
  var _replicatorProbe: TestProbe = _
  var _notificationProbe: TestProbe = _

  var _emittedEvents: Vector[DurableEvent] = Vector.empty
  var _replicatedEvents: Vector[DurableEvent] = Vector.empty

  def requestorProbe: TestProbe = _requestorProbe
  def replicatorProbe: TestProbe = _replicatorProbe
  def notificationProbe: TestProbe = _notificationProbe

  def emittedEvents: Vector[DurableEvent] = _emittedEvents
  def replicatedEvents: Vector[DurableEvent] = _replicatedEvents

  def logId: String
  def log: ActorRef

  override def beforeEach(): Unit = {
    _requestorProbe = TestProbe()
    _replicatorProbe = TestProbe()
    _notificationProbe = TestProbe()

    system.eventStream.subscribe(notificationProbe.ref, classOf[Updated])
  }

  override def afterEach(): Unit = {
    _emittedEvents = Vector.empty
    _replicatedEvents = Vector.empty

    system.eventStream.unsubscribe(notificationProbe.ref, classOf[Updated])
  }

  def registerCollaborator(aggregateId: Option[String] = None, collaborator: TestProbe = TestProbe()): TestProbe = {
    log ! Replay(Long.MaxValue, collaborator.ref, aggregateId, 0)
    collaborator.expectMsg(ReplaySuccess(0))
    collaborator
  }

  def writeEmittedEvents(events: DurableEvent *): Unit = {
    writeEmittedEvents(events.toVector)
  }

  def writeEmittedEvents(events: Seq[DurableEvent], log: ActorRef = log): Unit = {
    log ! Write(events, system.deadLetters, requestorProbe.ref, 0)
    notificationProbe.expectMsgClass(classOf[Updated]).events.foreach(event => requestorProbe.expectMsg(WriteSuccess(event, 0)))
  }

  def writeReplicatedEvents(events: Seq[DurableEvent], lastSourceLogSequenceNrRead: Long, remoteLogId: String = remoteLogId): Unit = {
    val updated = events.map(_.copy(sourceLogId = remoteLogId, targetLogId = remoteLogId))
    log.tell(ReplicationWrite(updated, remoteLogId, lastSourceLogSequenceNrRead), replicatorProbe.ref)
    replicatorProbe.expectMsg(ReplicationWriteSuccess(events.length, lastSourceLogSequenceNrRead))
    if (events.nonEmpty) notificationProbe.expectMsgClass(classOf[Updated])
  }

  def writeReplicationProgress(lastSourceLogSequenceNrRead: Long, expectedLastSourceLogSequenceNrReplicated: Long, remoteLogId: String = remoteLogId): Unit = {
    log.tell(ReplicationWrite(Seq(), remoteLogId, lastSourceLogSequenceNrRead), replicatorProbe.ref)
    replicatorProbe.expectMsg(ReplicationWriteSuccess(0, expectedLastSourceLogSequenceNrReplicated))
  }

  def generateEmittedEvents(offset: Long = 0L, emitterAggregateId: Option[String] = None, destinationAggregateIds: Set[String] = Set()): Unit = {
    val events: Vector[DurableEvent] = Vector(
      event("a", timestampAB(1 + offset, 0), idA, emitterAggregateId, destinationAggregateIds),
      event("b", timestampAB(2 + offset, 0), idA, emitterAggregateId, destinationAggregateIds),
      event("c", timestampAB(3 + offset, 0), idA, emitterAggregateId, destinationAggregateIds))

    val emitted = Vector(
      DurableEvent("a", 0L, timestampAB(1 + offset, 0), idA, emitterAggregateId, destinationAggregateIds, 0L, logId, logId, 1 + offset, 1 + offset),
      DurableEvent("b", 0L, timestampAB(2 + offset, 0), idA, emitterAggregateId, destinationAggregateIds, 0L, logId, logId, 2 + offset, 2 + offset),
      DurableEvent("c", 0L, timestampAB(3 + offset, 0), idA, emitterAggregateId, destinationAggregateIds, 0L, logId, logId, 3 + offset, 3 + offset))

    _emittedEvents ++= emitted

    log ! Write(events, system.deadLetters, requestorProbe.ref, 0)
    requestorProbe.expectMsg(WriteSuccess(emitted(0), 0))
    requestorProbe.expectMsg(WriteSuccess(emitted(1), 0))
    requestorProbe.expectMsg(WriteSuccess(emitted(2), 0))
    notificationProbe.expectMsg(Updated(logId, emitted))
  }

  def generateReplicatedEvents(offset: Long, remoteLogId: String = remoteLogId, emitterAggregateId: Option[String] = None, destinationAggregateIds: Set[String] = Set()): Unit = {
    val events: Vector[DurableEvent] = Vector(
      DurableEvent("i", 0L, timestampAB(0, 7 + offset), idB, emitterAggregateId, destinationAggregateIds, 0L, remoteLogId, remoteLogId, 7 + offset, 7 + offset),
      DurableEvent("j", 0L, timestampAB(0, 8 + offset), idB, emitterAggregateId, destinationAggregateIds, 0L, remoteLogId, remoteLogId, 8 + offset, 8 + offset),
      DurableEvent("k", 0L, timestampAB(0, 9 + offset), idB, emitterAggregateId, destinationAggregateIds, 0L, remoteLogId, remoteLogId, 9 + offset, 9 + offset))

    val replicated = Vector(
      DurableEvent("i", 0L, timestampAB(0, 7 + offset), idB, emitterAggregateId, destinationAggregateIds, 9 + offset, remoteLogId, logId, 7 + offset, 1 + offset),
      DurableEvent("j", 0L, timestampAB(0, 8 + offset), idB, emitterAggregateId, destinationAggregateIds, 9 + offset, remoteLogId, logId, 8 + offset, 2 + offset),
      DurableEvent("k", 0L, timestampAB(0, 9 + offset), idB, emitterAggregateId, destinationAggregateIds, 9 + offset, remoteLogId, logId, 9 + offset, 3 + offset))

    _replicatedEvents ++= replicated

    log.tell(ReplicationWrite(events, remoteLogId, 9 + offset), replicatorProbe.ref)
    replicatorProbe.expectMsg(ReplicationWriteSuccess(events.length, 9 + offset))
    notificationProbe.expectMsg(Updated(logId, replicated))
  }
}

abstract class EventLogSpec extends TestKit(ActorSystem("test", EventLogSpec.config)) with EventLogSpecSupport {
  import EventLogSpec._

  "An event log" must {
    "write local events and send them to the requestor" in {
      generateEmittedEvents()
    }
    "write local events with undefined defaultRoutingDestination and undefined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateEmittedEvents(offset = 0, emitterAggregateId = None, destinationAggregateIds = Set())
      collaborator.expectMsg(Written(emittedEvents(0)))
      collaborator.expectMsg(Written(emittedEvents(1)))
      collaborator.expectMsg(Written(emittedEvents(2)))
    }
    "write local events with undefined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateEmittedEvents(offset = 0, emitterAggregateId = None, destinationAggregateIds = Set("a1"))
      collaborator.expectMsg(Written(emittedEvents(0)))
      collaborator.expectMsg(Written(emittedEvents(1)))
      collaborator.expectMsg(Written(emittedEvents(2)))
    }
    "write local events with defined defaultRoutingDestination and undefined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateEmittedEvents(offset = 0, emitterAggregateId = Some("a1"), destinationAggregateIds = Set())
      collaborator.expectMsg(Written(emittedEvents(0)))
      collaborator.expectMsg(Written(emittedEvents(1)))
      collaborator.expectMsg(Written(emittedEvents(2)))
    }
    "write local events with defined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateEmittedEvents(offset = 0, emitterAggregateId = Some("a1"), destinationAggregateIds = Set("a2"))
      collaborator.expectMsg(Written(emittedEvents(0)))
      collaborator.expectMsg(Written(emittedEvents(1)))
      collaborator.expectMsg(Written(emittedEvents(2)))
    }
    "write local events with undefined defaultRoutingDestination and undefined customRoutingDestinations and not route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateEmittedEvents(offset = 0, emitterAggregateId = None, destinationAggregateIds = Set())
      collaborator.expectMsg(Written(emittedEvents(0)))
      collaborator.expectMsg(Written(emittedEvents(1)))
      collaborator.expectMsg(Written(emittedEvents(2)))
    }
    "write local events with undefined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateEmittedEvents(offset = 0, emitterAggregateId = None, destinationAggregateIds = Set("a1"))
      collaborator.expectMsg(Written(emittedEvents(0)))
      collaborator.expectMsg(Written(emittedEvents(0)))
      collaborator.expectMsg(Written(emittedEvents(1)))
      collaborator.expectMsg(Written(emittedEvents(1)))
      collaborator.expectMsg(Written(emittedEvents(2)))
      collaborator.expectMsg(Written(emittedEvents(2)))
    }
    "write local events with defined defaultRoutingDestination and undefined customRoutingDestinations and route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateEmittedEvents(offset = 0, emitterAggregateId = Some("a1"), destinationAggregateIds = Set())
      collaborator.expectMsg(Written(emittedEvents(0)))
      collaborator.expectMsg(Written(emittedEvents(0)))
      collaborator.expectMsg(Written(emittedEvents(1)))
      collaborator.expectMsg(Written(emittedEvents(1)))
      collaborator.expectMsg(Written(emittedEvents(2)))
      collaborator.expectMsg(Written(emittedEvents(2)))
    }
    "write local events with defined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = Some("a2"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateEmittedEvents(offset = 0, emitterAggregateId = Some("a1"), destinationAggregateIds = Set("a2"))
      collaborator.expectMsg(Written(emittedEvents(0)))
      collaborator.expectMsg(Written(emittedEvents(0)))
      collaborator.expectMsg(Written(emittedEvents(0)))
      collaborator.expectMsg(Written(emittedEvents(1)))
      collaborator.expectMsg(Written(emittedEvents(1)))
      collaborator.expectMsg(Written(emittedEvents(1)))
      collaborator.expectMsg(Written(emittedEvents(2)))
      collaborator.expectMsg(Written(emittedEvents(2)))
      collaborator.expectMsg(Written(emittedEvents(2)))
    }
    "reply with a failure message if write fails" in {
      val events = Vector(
        event("boom", timestampAB(1, 0), idA),
        event("okay", timestampAB(2, 0), idA))

      log ! Write(events, system.deadLetters, requestorProbe.ref, 0)
      requestorProbe.expectMsg(WriteFailure(DurableEvent("boom", 0L, timestampAB(1, 0), idA), boom, 0))
      requestorProbe.expectMsg(WriteFailure(DurableEvent("okay", 0L, timestampAB(2, 0), idA), boom, 0))
    }
    "write replicated events" in {
      generateReplicatedEvents(offset = 0)
    }
    "write replicated events with undefined defaultRoutingDestination and undefined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateReplicatedEvents(offset = 0, emitterAggregateId = None, destinationAggregateIds = Set())
      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(1)))
      collaborator.expectMsg(Written(replicatedEvents(2)))
    }
    "write replicated events with undefined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateReplicatedEvents(offset = 0, emitterAggregateId = None, destinationAggregateIds = Set("a1"))
      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(1)))
      collaborator.expectMsg(Written(replicatedEvents(2)))
    }
    "write replicated events with defined defaultRoutingDestination and undefined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateReplicatedEvents(offset = 0, emitterAggregateId = Some("a1"), destinationAggregateIds = Set())
      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(1)))
      collaborator.expectMsg(Written(replicatedEvents(2)))
    }
    "write replicated events with defined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateReplicatedEvents(offset = 0, emitterAggregateId = Some("a1"), destinationAggregateIds = Set("a2"))
      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(1)))
      collaborator.expectMsg(Written(replicatedEvents(2)))
    }
    "write replicated events with undefined defaultRoutingDestination and undefined customRoutingDestinations and not route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateReplicatedEvents(offset = 0, emitterAggregateId = None, destinationAggregateIds = Set())
      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(1)))
      collaborator.expectMsg(Written(replicatedEvents(2)))
    }
    "write replicated events with undefined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateReplicatedEvents(offset = 0, emitterAggregateId = None, destinationAggregateIds = Set("a1"))
      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(1)))
      collaborator.expectMsg(Written(replicatedEvents(1)))
      collaborator.expectMsg(Written(replicatedEvents(2)))
      collaborator.expectMsg(Written(replicatedEvents(2)))
    }
    "write replicated events with defined defaultRoutingDestination and undefined customRoutingDestinations and route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateReplicatedEvents(offset = 0, emitterAggregateId = Some("a1"), destinationAggregateIds = Set())
      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(1)))
      collaborator.expectMsg(Written(replicatedEvents(1)))
      collaborator.expectMsg(Written(replicatedEvents(2)))
      collaborator.expectMsg(Written(replicatedEvents(2)))
    }
    "write replicated events with defined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = Some("a2"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateReplicatedEvents(offset = 0, emitterAggregateId = Some("a1"), destinationAggregateIds = Set("a2"))
      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(1)))
      collaborator.expectMsg(Written(replicatedEvents(1)))
      collaborator.expectMsg(Written(replicatedEvents(1)))
      collaborator.expectMsg(Written(replicatedEvents(2)))
      collaborator.expectMsg(Written(replicatedEvents(2)))
      collaborator.expectMsg(Written(replicatedEvents(2)))
    }
    "write replicated events and update the replication progress map" in {
      log.tell(GetReplicationProgress, requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressSuccess(Map()))
      generateReplicatedEvents(offset = 0)
      log.tell(GetReplicationProgress, requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressSuccess(Map(remoteLogId -> 9L)))
    }
    "reply with a failure message if replication fails" in {
      val events: Vector[DurableEvent] = Vector(
        DurableEvent("boom", 0L, timestampAB(0, 7), idB, None, Set(), 0L, remoteLogId, remoteLogId, 7, 7),
        DurableEvent("okay", 0L, timestampAB(0, 8), idB, None, Set(), 0L, remoteLogId, remoteLogId, 8, 8))

      log.tell(ReplicationWrite(events, remoteLogId, 8), replicatorProbe.ref)
      replicatorProbe.expectMsg(ReplicationWriteFailure(boom))
    }
    "reply with a failure message if replication fails and not update the replication progress map" in {
      log.tell(GetReplicationProgress, requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressSuccess(Map()))
      val events: Vector[DurableEvent] = Vector(
        DurableEvent("boom", 0L, timestampAB(0, 7), idB, None, Set(), 0L, remoteLogId, remoteLogId, 7, 7),
        DurableEvent("okay", 0L, timestampAB(0, 8), idB, None, Set(), 0L, remoteLogId, remoteLogId, 8, 8))

      log.tell(ReplicationWrite(events, remoteLogId, 8), replicatorProbe.ref)
      replicatorProbe.expectMsg(ReplicationWriteFailure(boom))
      log.tell(GetReplicationProgress, requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressSuccess(Map()))
    }
    "replay events from scratch" in {
      generateEmittedEvents()
      log ! Replay(1L, requestorProbe.ref, 0)
      requestorProbe.expectMsg(Replaying(emittedEvents(0), 0))
      requestorProbe.expectMsg(Replaying(emittedEvents(1), 0))
      requestorProbe.expectMsg(Replaying(emittedEvents(2), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "replay events from a custom position" in {
      generateEmittedEvents()
      log ! Replay(3L, requestorProbe.ref, 0)
      requestorProbe.expectMsg(Replaying(emittedEvents(2), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
      // custom position > last sequence number
      log ! Replay(5L, requestorProbe.ref, 0)
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "replay events from the default log if request aggregateId is not defined" in {
      generateEmittedEvents(destinationAggregateIds = Set("a1"))
      log ! Replay(1L, requestorProbe.ref, None, 0)
      requestorProbe.expectMsg(Replaying(emittedEvents(0), 0))
      requestorProbe.expectMsg(Replaying(emittedEvents(1), 0))
      requestorProbe.expectMsg(Replaying(emittedEvents(2), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "replay events from the index if request aggregateId is defined" in {
      generateEmittedEvents(offset = 0, destinationAggregateIds = Set("a1"))
      log ! Replay(1L, requestorProbe.ref, Some("a1"), 0)
      requestorProbe.expectMsg(Replaying(emittedEvents(0), 0))
      requestorProbe.expectMsg(Replaying(emittedEvents(1), 0))
      requestorProbe.expectMsg(Replaying(emittedEvents(2), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "replay events from the index and properly stop at the index classifier" in {
      generateEmittedEvents(offset = 0, destinationAggregateIds = Set("a1"))
      generateEmittedEvents(offset = 3, destinationAggregateIds = Set("a2"))
      log ! Replay(1L, requestorProbe.ref, Some("a1"), 0)
      requestorProbe.expectMsg(Replaying(emittedEvents(0), 0))
      requestorProbe.expectMsg(Replaying(emittedEvents(1), 0))
      requestorProbe.expectMsg(Replaying(emittedEvents(2), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "replay events from the index and from a custom position" in {
      generateEmittedEvents(offset = 0, destinationAggregateIds = Set("a1"))
      generateEmittedEvents(offset = 3, destinationAggregateIds = Set("a2"))
      log ! Replay(2L, requestorProbe.ref, Some("a1"), 0)
      requestorProbe.expectMsg(Replaying(emittedEvents(1), 0))
      requestorProbe.expectMsg(Replaying(emittedEvents(2), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
      log ! Replay(5L, requestorProbe.ref, Some("a1"), 0)
      requestorProbe.expectMsg(ReplaySuccess(0))
      log ! Replay(2L, requestorProbe.ref, Some("a2"), 0)
      requestorProbe.expectMsg(Replaying(emittedEvents(3), 0))
      requestorProbe.expectMsg(Replaying(emittedEvents(4), 0))
      requestorProbe.expectMsg(Replaying(emittedEvents(5), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
      log ! Replay(5L, requestorProbe.ref, Some("a2"), 0)
      requestorProbe.expectMsg(Replaying(emittedEvents(4), 0))
      requestorProbe.expectMsg(Replaying(emittedEvents(5), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "not replay events with non-matching aggregateId if request aggregateId is defined" in {
      generateEmittedEvents(destinationAggregateIds = Set("a1"))
      log ! Replay(1L, requestorProbe.ref, Some("a2"), 0)
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "reply with a failure message if replay fails" in {
      log ! Replay(-1, requestorProbe.ref, 0)
      requestorProbe.expectMsg(ReplayFailure(boom, 0))
    }
    "batch-read local events" in {
      generateEmittedEvents()
      log.tell(ReplicationRead(1, Int.MaxValue, undefinedLogIdFilter, UndefinedLogId), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(emittedEvents, 3, UndefinedLogId))
    }
    "batch-read local and replicated events" in {
      generateEmittedEvents()
      generateReplicatedEvents(offset = 3)
      log.tell(ReplicationRead(1, Int.MaxValue, undefinedLogIdFilter, UndefinedLogId), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(emittedEvents ++ replicatedEvents, 6, UndefinedLogId))
    }
    "batch-read events with a batch size limit" in {
      generateEmittedEvents()
      log.tell(ReplicationRead(1, 2, undefinedLogIdFilter, UndefinedLogId), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(emittedEvents.take(2), 2, UndefinedLogId))
      log.tell(ReplicationRead(1, 0, undefinedLogIdFilter, UndefinedLogId), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(Nil, 0, UndefinedLogId))
    }
    "batch-read events from a custom position" in {
      generateEmittedEvents()
      log.tell(ReplicationRead(2, Int.MaxValue, undefinedLogIdFilter, UndefinedLogId), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(emittedEvents.drop(1), 3, UndefinedLogId))
    }
    "batch-read events from a custom position with a batch size limit" in {
      generateEmittedEvents()
      log.tell(ReplicationRead(2, 1, undefinedLogIdFilter, UndefinedLogId), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(emittedEvents.drop(1).take(1), 2, UndefinedLogId))
    }
    "batch-read events with exclusion" in {
      generateEmittedEvents()
      generateReplicatedEvents(offset = 3)
      log.tell(ReplicationRead(1, Int.MaxValue, SourceLogIdExclusionFilter(logId), UndefinedLogId), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(replicatedEvents, 6, UndefinedLogId))
      log.tell(ReplicationRead(1, Int.MaxValue, SourceLogIdExclusionFilter(remoteLogId), UndefinedLogId), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(emittedEvents, 6, UndefinedLogId))
    }
    "not batch-read events from index" in {
      generateEmittedEvents(offset = 0, destinationAggregateIds = Set("a1"))
      generateEmittedEvents(offset = 3, destinationAggregateIds = Set())
      log.tell(ReplicationRead(1, Int.MaxValue, undefinedLogIdFilter, UndefinedLogId), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(emittedEvents, 6, UndefinedLogId))
    }
    "reply with a failure message if batch-read fails" in {
      log.tell(ReplicationRead(-1, Int.MaxValue, undefinedLogIdFilter, UndefinedLogId), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadFailure(boom.getMessage, UndefinedLogId))
    }
    "recover the current sequence number on (re)start" in {
      generateEmittedEvents()
      log.tell(GetSequenceNr, requestorProbe.ref)
      requestorProbe.expectMsg(GetSequenceNrSuccess(3))
      log ! "boom"
      log.tell(GetSequenceNr, requestorProbe.ref)
      requestorProbe.expectMsg(GetSequenceNrSuccess(3))
    }
  }
}

class EventLogSpecLeveldb extends EventLogSpec with EventLogLifecycleLeveldb {
  import EventLogSpec._

  "A LevelDB event log" must {
    "recover the replication progress map on (re)start" in {
      log ! SetReplicationProgress("x", 17)
      log ! SetReplicationProgress("y", 19)
      log.tell(GetReplicationProgress, requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressSuccess(Map("x" -> 17, "y" -> 19)))
      log ! "boom"
      log.tell(GetReplicationProgress, requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressSuccess(Map("x" -> 17, "y" -> 19)))
    }
    "update the replication progress map if last read sequence nr > last replicated sequence nr" in {
      // -------------------------------------------------------------------------------------
      //  read-your-write consistency of *empty* updates only supported by LevelDB event log
      // -------------------------------------------------------------------------------------
      log.tell(GetReplicationProgress, requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressSuccess(Map()))
      writeReplicationProgress(19, 19)
      log.tell(GetReplicationProgress, requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressSuccess(Map(EventLogSpec.remoteLogId -> 19L)))
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
    indexProbe.expectMsg(UpdateIndexSuccess(0L, 0))
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
        event(s"e-$i", timestampAB(i, 0), idA)
      }

      writeEmittedEvents(events)

      1 to num foreach { i =>
        log.tell(ReplicationRead(i, 1, undefinedLogIdFilter, "test-target-log-id"), probe.ref)
        probe.expectMsgClass(classOf[ReplicationReadSuccess])
      }
    }
  }

  "A Cassandra event log and its index" must {
    "run an index update on initialization" in {
      writeEmittedEvents(event("a"), event("b"))
      log ! "boom"
      indexProbe.expectMsg(UpdateIndexSuccess(2L, 1))
    }
    "retry an index update on initialization if sequence number read fails" in {
      val failureLog = createLog(TestFailureSpec(failOnSequenceNrRead = true), indexProbe.ref)
      indexProbe.expectMsg(ReadSequenceNrFailure(boom))
      indexProbe.expectMsg(UpdateIndexSuccess(0L, 0))
    }
    "retry an index update on initialization if index update fails" in {
      val failureLog = createLog(TestFailureSpec(failBeforeIndexIncrementWrite = true), indexProbe.ref)
      indexProbe.expectMsg(UpdateIndexFailure(boom))
      indexProbe.expectMsg(UpdateIndexSuccess(0L, 0))
      writeEmittedEvents(List(event("a"), event("b")), failureLog)
      failureLog ! "boom"
      indexProbe.expectMsg(UpdateIndexFailure(boom))
      indexProbe.expectMsg(UpdateIndexSuccess(2L, 1))
    }
    "run an index update after reaching the update limit with a single event batch" in {
      writeEmittedEvents(event("a"), event("b"), event("c"), event("d"))
      indexProbe.expectMsg(UpdateIndexSuccess(4L, 2))
    }
    "run an index update after reaching the update limit with a several event batches" in {
      writeEmittedEvents(event("a"), event("b"))
      writeEmittedEvents(event("c"), event("d"))
      indexProbe.expectMsg(UpdateIndexSuccess(4L, 2))
    }
    "run an index update on initialization and after reaching the update limit" in {
      writeEmittedEvents(event("a"), event("b"))
      log ! "boom"
      indexProbe.expectMsg(UpdateIndexSuccess(2L, 1))
      writeEmittedEvents(event("d"), event("e"), event("f"))
      indexProbe.expectMsg(UpdateIndexSuccess(5L, 1))
    }
    "return the initial value for a replication progress" in {
      log.tell(GetLastSourceLogReadPosition(remoteLogId), probe.ref)
      probe.expectMsg(GetLastSourceLogReadPositionSuccess(remoteLogId, 0L))
    }
    "return the logged value for a replication progress" in {
      writeReplicatedEvents(List(event("a"), event("b")), 4L)
      log.tell(GetLastSourceLogReadPosition(remoteLogId), probe.ref)
      probe.expectMsg(GetLastSourceLogReadPositionSuccess(remoteLogId, 4L))
    }
    "return the index value for a replication progress" in {
      writeReplicatedEvents(List(event("a"), event("b"), event("c")), 4L)
      indexProbe.expectMsg(UpdateIndexSuccess(3L, 1))
      log.tell(GetLastSourceLogReadPosition(remoteLogId), probe.ref)
      probe.expectMsg(GetLastSourceLogReadPositionSuccess(remoteLogId, 4L))
    }
    "return the index value updated with the logged value for a replication progress" in {
      writeReplicatedEvents(List(event("a"), event("b"), event("c")), 4L)
      indexProbe.expectMsg(UpdateIndexSuccess(3L, 1))
      writeReplicatedEvents(List(event("d"), event("e")), 8L)
      log.tell(GetLastSourceLogReadPosition(remoteLogId), probe.ref)
      probe.expectMsg(GetLastSourceLogReadPositionSuccess(remoteLogId, 8L))
    }
    "buffer empty replication progress writes" in {
      writeReplicatedEvents(Nil, 4L, "extra")
      log.tell(GetLastSourceLogReadPosition("extra"), probe.ref)
      probe.expectMsg(GetLastSourceLogReadPositionSuccess("extra", 4L))
      log ! "boom"
      log.tell(GetLastSourceLogReadPosition("extra"), probe.ref)
      probe.expectMsg(GetLastSourceLogReadPositionSuccess("extra", 0L))
    }
    "persist empty replication progress writes" in {
      writeReplicatedEvents(Nil, 4L, "extra")
      writeReplicatedEvents(List(event("a"), event("b"), event("c")), 7L)
      indexProbe.expectMsg(UpdateIndexSuccess(3L, 1))
      log.tell(GetLastSourceLogReadPosition("extra"), probe.ref)
      probe.expectMsg(GetLastSourceLogReadPositionSuccess("extra", 4L))
      log ! "boom"
      log.tell(GetLastSourceLogReadPosition("extra"), probe.ref)
      probe.expectMsg(GetLastSourceLogReadPositionSuccess("extra", 4L))
    }
    "add events with emitter aggregate id to index" in {
      writeEmittedEvents(
        event("a", None),
        event("b", Some("a1")),
        event("c", None),
        event("d", Some("a1")),
        event("e", None),
        event("f", Some("a1")))

      expectReplay(None, "a", "b", "c", "d", "e", "f")
      expectReplay(Some("a1"), "b", "d", "f")
    }
    "add events with custom routing destinations to index" in {
      writeEmittedEvents(
        event("a", None),
        event("b", None, Set("a1")),
        event("c", None),
        event("d", None, Set("a1")),
        event("e", None),
        event("f", None, Set("a1", "a2")))

      expectReplay(None, "a", "b", "c", "d", "e", "f")
      expectReplay(Some("a1"), "b", "d", "f")
      expectReplay(Some("a2"), "f")
    }
    "add events with emitter aggregate id and custom routing destinations to index" in {
      writeEmittedEvents(
        event("a", None),
        event("b", Some("a1"), Set("a2")),
        event("c", None),
        event("d", Some("a1"), Set("a2")),
        event("e", None),
        event("f", Some("a1"), Set("a3")))

      expectReplay(None, "a", "b", "c", "d", "e", "f")
      expectReplay(Some("a1"), "b", "d", "f")
      expectReplay(Some("a2"), "b", "d")
      expectReplay(Some("a3"), "f")
    }
    "replay aggregate events from log" in {
      writeEmittedEvents(
        event("a", Some("a1")),
        event("b", Some("a1")))

      expectReplay(Some("a1"), "a", "b")
    }
    "replay aggregate events from index and log" in {
      writeEmittedEvents(
        event("a", Some("a1")),
        event("b", Some("a1")),
        event("c", Some("a1")))

      indexProbe.expectMsg(UpdateIndexSuccess(3L, 1))

      writeEmittedEvents(
        event("d", Some("a1")))

      expectReplay(Some("a1"), "a", "b", "c", "d")
    }
    "replay aggregate events from index and log with a lower sequence number bound" in {
      writeEmittedEvents(
        event("a", Some("a1")),
        event("b", Some("a1")),
        event("c", Some("a1")))

      indexProbe.expectMsg(UpdateIndexSuccess(3L, 1))

      writeEmittedEvents(
        event("d", Some("a1")))

      expectReplay(3L, Some("a1"), "c", "d")
    }
  }
}
