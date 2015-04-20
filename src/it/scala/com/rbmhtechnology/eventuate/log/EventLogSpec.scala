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
import com.rbmhtechnology.eventuate.log.EventLogSupport._
import com.typesafe.config.ConfigFactory

import org.scalatest._

object EventLogSpec {
  val replicaIdA = "A"
  val replicaIdB = "B"
  val replicaIdC = "C"
  val remoteLogId = "R1"

  val remoteLogIdFilter = SourceLogIdExclusionFilter(remoteLogId)
  val undefinedLogIdFilter = SourceLogIdExclusionFilter(UndefinedLogId)

  def event(payload: Any, timestamp: VectorTime, replicaId: String, emitterAggregateId: Option[String] = None, destinationAggregateIds: Set[String] = Set()): DurableEvent =
    DurableEvent(payload, 0L, timestamp, replicaId, emitterAggregateId, destinationAggregateIds)

  def timestampA(timeA: Long): VectorTime =
    VectorTime(processId(replicaIdA) -> timeA)

  def timestampAB(timeA: Long, timeB: Long): VectorTime =
    VectorTime(processId(replicaIdA) -> timeA, processId(replicaIdB) -> timeB)
}

class EventLogSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with EventLogSupport {
  import EventsourcingProtocol._
  import ReplicationProtocol._
  import EventLogSpec._

  var requestorProbe: TestProbe = _
  var replicatorProbe: TestProbe = _
  var notificationProbe: TestProbe = _

  var generatedEvents: Vector[DurableEvent] = Vector.empty
  var replicatedEvents: Vector[DurableEvent] = Vector.empty

  override def beforeEach(): Unit = {
    super.beforeEach()

    requestorProbe = TestProbe()
    replicatorProbe = TestProbe()
    notificationProbe = TestProbe()

    system.eventStream.subscribe(notificationProbe.ref, classOf[Updated])
  }

  override def afterEach(): Unit = {
    generatedEvents = Vector.empty
    replicatedEvents = Vector.empty
    system.eventStream.unsubscribe(notificationProbe.ref, classOf[Updated])
  }

  def registerCollaborator(aggregateId: Option[String] = None, collaborator: TestProbe = TestProbe()): TestProbe = {
    log ! Replay(Long.MaxValue, collaborator.ref, aggregateId, 0)
    collaborator.expectMsg(ReplaySuccess(0))
    collaborator
  }
  
  def generateEvents(offset: Long = 0L, emitterAggregateId: Option[String] = None, destinationAggregateIds: Set[String] = Set()): Unit = {
    val events: Vector[DurableEvent] = Vector(
      event("a", timestampAB(1 + offset, 0), replicaIdA, emitterAggregateId, destinationAggregateIds),
      event("b", timestampAB(2 + offset, 0), replicaIdA, emitterAggregateId, destinationAggregateIds),
      event("c", timestampAB(3 + offset, 0), replicaIdA, emitterAggregateId, destinationAggregateIds))

    val generated = Vector(
      DurableEvent("a", 0L, timestampAB(1 + offset, 0), replicaIdA, emitterAggregateId, destinationAggregateIds, logId, logId, 1 + offset, 1 + offset),
      DurableEvent("b", 0L, timestampAB(2 + offset, 0), replicaIdA, emitterAggregateId, destinationAggregateIds, logId, logId, 2 + offset, 2 + offset),
      DurableEvent("c", 0L, timestampAB(3 + offset, 0), replicaIdA, emitterAggregateId, destinationAggregateIds, logId, logId, 3 + offset, 3 + offset))

    generatedEvents ++= generated

    log ! Write(events, system.deadLetters, requestorProbe.ref, 0)
    requestorProbe.expectMsg(WriteSuccess(generated(0), 0))
    requestorProbe.expectMsg(WriteSuccess(generated(1), 0))
    requestorProbe.expectMsg(WriteSuccess(generated(2), 0))
    notificationProbe.expectMsg(Updated(generated))
  }

  def replicateEvents(offset: Long, remoteLogId: String = remoteLogId, emitterAggregateId: Option[String] = None, destinationAggregateIds: Set[String] = Set()): Unit = {
    val events: Vector[DurableEvent] = Vector(
      DurableEvent("i", 0L, timestampAB(0, 7 + offset), replicaIdB, emitterAggregateId, destinationAggregateIds, remoteLogId, remoteLogId, 7 + offset, 7 + offset),
      DurableEvent("j", 0L, timestampAB(0, 8 + offset), replicaIdB, emitterAggregateId, destinationAggregateIds, remoteLogId, remoteLogId, 8 + offset, 8 + offset),
      DurableEvent("k", 0L, timestampAB(0, 9 + offset), replicaIdB, emitterAggregateId, destinationAggregateIds, remoteLogId, remoteLogId, 9 + offset, 9 + offset))

    val replicated = Vector(
      DurableEvent("i", 0L, timestampAB(0, 7 + offset), replicaIdB, emitterAggregateId, destinationAggregateIds, remoteLogId, logId, 7 + offset, 1 + offset),
      DurableEvent("j", 0L, timestampAB(0, 8 + offset), replicaIdB, emitterAggregateId, destinationAggregateIds, remoteLogId, logId, 8 + offset, 2 + offset),
      DurableEvent("k", 0L, timestampAB(0, 9 + offset), replicaIdB, emitterAggregateId, destinationAggregateIds, remoteLogId, logId, 9 + offset, 3 + offset))

    replicatedEvents ++= replicated

    log.tell(ReplicationWrite(events, remoteLogId, 9 + offset, 0), replicatorProbe.ref)
    replicatorProbe.expectMsg(ReplicationWriteSuccess(events.length, 9 + offset, 0))
    notificationProbe.expectMsg(Updated(replicated))
  }

  def replicateNone(lastSourceLogSequenceNrRead: Long, expectedLastSourceLogSequenceNrReplicated: Long, remoteLogId: String = remoteLogId): Unit = {
    log.tell(ReplicationWrite(Seq(), remoteLogId, lastSourceLogSequenceNrRead, 0), replicatorProbe.ref)
    replicatorProbe.expectMsg(ReplicationWriteSuccess(0, expectedLastSourceLogSequenceNrReplicated, 0))
  }

  "An event log" must {
    "write local events and send them to the requestor" in {
      generateEvents()
    }
    "write local events with undefined defaultRoutingDestination and undefined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateEvents(offset = 0, emitterAggregateId = None, destinationAggregateIds = Set())
      collaborator.expectMsg(Written(generatedEvents(0)))
      collaborator.expectMsg(Written(generatedEvents(1)))
      collaborator.expectMsg(Written(generatedEvents(2)))
    }
    "write local events with undefined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateEvents(offset = 0, emitterAggregateId = None, destinationAggregateIds = Set("a1"))
      collaborator.expectMsg(Written(generatedEvents(0)))
      collaborator.expectMsg(Written(generatedEvents(1)))
      collaborator.expectMsg(Written(generatedEvents(2)))
    }
    "write local events with defined defaultRoutingDestination and undefined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateEvents(offset = 0, emitterAggregateId = Some("a1"), destinationAggregateIds = Set())
      collaborator.expectMsg(Written(generatedEvents(0)))
      collaborator.expectMsg(Written(generatedEvents(1)))
      collaborator.expectMsg(Written(generatedEvents(2)))
    }
    "write local events with defined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      generateEvents(offset = 0, emitterAggregateId = Some("a1"), destinationAggregateIds = Set("a2"))
      collaborator.expectMsg(Written(generatedEvents(0)))
      collaborator.expectMsg(Written(generatedEvents(1)))
      collaborator.expectMsg(Written(generatedEvents(2)))
    }
    "write local events with undefined defaultRoutingDestination and undefined customRoutingDestinations and not route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateEvents(offset = 0, emitterAggregateId = None, destinationAggregateIds = Set())
      collaborator.expectMsg(Written(generatedEvents(0)))
      collaborator.expectMsg(Written(generatedEvents(1)))
      collaborator.expectMsg(Written(generatedEvents(2)))
    }
    "write local events with undefined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateEvents(offset = 0, emitterAggregateId = None, destinationAggregateIds = Set("a1"))
      collaborator.expectMsg(Written(generatedEvents(0)))
      collaborator.expectMsg(Written(generatedEvents(0)))
      collaborator.expectMsg(Written(generatedEvents(1)))
      collaborator.expectMsg(Written(generatedEvents(1)))
      collaborator.expectMsg(Written(generatedEvents(2)))
      collaborator.expectMsg(Written(generatedEvents(2)))
    }
    "write local events with defined defaultRoutingDestination and undefined customRoutingDestinations and route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateEvents(offset = 0, emitterAggregateId = Some("a1"), destinationAggregateIds = Set())
      collaborator.expectMsg(Written(generatedEvents(0)))
      collaborator.expectMsg(Written(generatedEvents(0)))
      collaborator.expectMsg(Written(generatedEvents(1)))
      collaborator.expectMsg(Written(generatedEvents(1)))
      collaborator.expectMsg(Written(generatedEvents(2)))
      collaborator.expectMsg(Written(generatedEvents(2)))
    }
    "write local events with defined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = Some("a2"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      generateEvents(offset = 0, emitterAggregateId = Some("a1"), destinationAggregateIds = Set("a2"))
      collaborator.expectMsg(Written(generatedEvents(0)))
      collaborator.expectMsg(Written(generatedEvents(0)))
      collaborator.expectMsg(Written(generatedEvents(0)))
      collaborator.expectMsg(Written(generatedEvents(1)))
      collaborator.expectMsg(Written(generatedEvents(1)))
      collaborator.expectMsg(Written(generatedEvents(1)))
      collaborator.expectMsg(Written(generatedEvents(2)))
      collaborator.expectMsg(Written(generatedEvents(2)))
      collaborator.expectMsg(Written(generatedEvents(2)))
    }
    "reply with a failure message if write fails" in {
      val events = Vector(
        event("boom", timestampAB(1, 0), replicaIdA),
        event("okay", timestampAB(2, 0), replicaIdA))

      log ! Write(events, system.deadLetters, requestorProbe.ref, 0)
      requestorProbe.expectMsg(WriteFailure(DurableEvent("boom", 0L, timestampAB(1, 0), replicaIdA, None, Set(), logId, logId, 1, 1), boom, 0))
      requestorProbe.expectMsg(WriteFailure(DurableEvent("okay", 0L, timestampAB(2, 0), replicaIdA, None, Set(), logId, logId, 2, 2), boom, 0))
    }
    "write replicated events" in {
      replicateEvents(offset = 0)
    }
    "write replicated events with undefined defaultRoutingDestination and undefined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      replicateEvents(offset = 0, emitterAggregateId = None, destinationAggregateIds = Set())
      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(1)))
      collaborator.expectMsg(Written(replicatedEvents(2)))
    }
    "write replicated events with undefined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      replicateEvents(offset = 0, emitterAggregateId = None, destinationAggregateIds = Set("a1"))
      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(1)))
      collaborator.expectMsg(Written(replicatedEvents(2)))
    }
    "write replicated events with defined defaultRoutingDestination and undefined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      replicateEvents(offset = 0, emitterAggregateId = Some("a1"), destinationAggregateIds = Set())
      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(1)))
      collaborator.expectMsg(Written(replicatedEvents(2)))
    }
    "write replicated events with defined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with undefined aggregateId" in {
      val collaborator = registerCollaborator(aggregateId = None)
      replicateEvents(offset = 0, emitterAggregateId = Some("a1"), destinationAggregateIds = Set("a2"))
      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(1)))
      collaborator.expectMsg(Written(replicatedEvents(2)))
    }
    "write replicated events with undefined defaultRoutingDestination and undefined customRoutingDestinations and not route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      replicateEvents(offset = 0, emitterAggregateId = None, destinationAggregateIds = Set())
      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(1)))
      collaborator.expectMsg(Written(replicatedEvents(2)))
    }
    "write replicated events with undefined defaultRoutingDestination and defined customRoutingDestinations and route them to collaborators with defined aggregateId" in {
      val collaborator = TestProbe()
      registerCollaborator(aggregateId = Some("a1"), collaborator = collaborator)
      registerCollaborator(aggregateId = None, collaborator = collaborator)
      replicateEvents(offset = 0, emitterAggregateId = None, destinationAggregateIds = Set("a1"))
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
      replicateEvents(offset = 0, emitterAggregateId = Some("a1"), destinationAggregateIds = Set())
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
      replicateEvents(offset = 0, emitterAggregateId = Some("a1"), destinationAggregateIds = Set("a2"))
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
      log.tell(GetReplicationProgress(remoteLogId), requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressSuccess(0))
      replicateEvents(offset = 0)
      log.tell(GetReplicationProgress(remoteLogId), requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressSuccess(9))
    }
    "update the replication progress map if last read sequence nr > last replicated sequence nr" in {
      log.tell(GetReplicationProgress(remoteLogId), requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressSuccess(0))
      replicateNone(19, 19)
      log.tell(GetReplicationProgress(remoteLogId), requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressSuccess(19))
    }
    "not update the replication progress map if last read sequence nr <= last replicated sequence nr" in {
      replicateNone(19, 19)
      replicateNone(17, 19)
      log.tell(GetReplicationProgress(remoteLogId), requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressSuccess(19))
    }
    "support idempotent replication processing" in {
      val collaborator = registerCollaborator()

      val events: Vector[DurableEvent] = Vector(
        DurableEvent("i", 0L, timestampAB(0, 7), replicaIdB, None, Set(), remoteLogId, remoteLogId, 7, 7),
        DurableEvent("j", 0L, timestampAB(0, 8), replicaIdB, None, Set(), remoteLogId, remoteLogId, 8, 8),
        DurableEvent("k", 0L, timestampAB(0, 9), replicaIdB, None, Set(), remoteLogId, remoteLogId, 9, 9))

      val replicatedEvents = Vector(
        DurableEvent("i", 0L, timestampAB(0, 7), replicaIdB, None, Set(), remoteLogId, logId, 7, 1),
        DurableEvent("j", 0L, timestampAB(0, 8), replicaIdB, None, Set(), remoteLogId, logId, 8, 2),
        DurableEvent("k", 0L, timestampAB(0, 9), replicaIdB, None, Set(), remoteLogId, logId, 9, 3))

      // replicate first two events
      log.tell(ReplicationWrite(events.take(2), remoteLogId, 8, 0), replicatorProbe.ref)

      replicatorProbe.expectMsg(ReplicationWriteSuccess(2, 8, 0))
      notificationProbe.expectMsg(Updated(replicatedEvents.take(2)))

      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(1)))

      // replicate first two events again (= duplicate)
      log.tell(ReplicationWrite(events.take(2), remoteLogId, 8, 0), replicatorProbe.ref)

      replicatorProbe.expectMsg(ReplicationWriteSuccess(0, 8, 0))

      // replicate remaining events
      log.tell(ReplicationWrite(events.drop(2), remoteLogId, 9, 0), replicatorProbe.ref)

      replicatorProbe.expectMsg(ReplicationWriteSuccess(1, 9, 0))
      notificationProbe.expectMsg(Updated(replicatedEvents.drop(2)))

      collaborator.expectMsg(Written(replicatedEvents(2)))
    }
    "reply with a failure message if replication fails" in {
      val events: Vector[DurableEvent] = Vector(
        DurableEvent("boom", 0L, timestampAB(0, 7), replicaIdB, None, Set(), remoteLogId, remoteLogId, 7, 7),
        DurableEvent("okay", 0L, timestampAB(0, 8), replicaIdB, None, Set(), remoteLogId, remoteLogId, 8, 8))

      log.tell(ReplicationWrite(events, remoteLogId, 8, 0), replicatorProbe.ref)
      replicatorProbe.expectMsg(ReplicationWriteFailure(boom, 0))
    }
    "replay events from scratch" in {
      generateEvents()
      log ! Replay(1L, requestorProbe.ref, 0)
      requestorProbe.expectMsg(Replaying(generatedEvents(0), 0))
      requestorProbe.expectMsg(Replaying(generatedEvents(1), 0))
      requestorProbe.expectMsg(Replaying(generatedEvents(2), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "replay events from a custom position" in {
      generateEvents()
      log ! Replay(3L, requestorProbe.ref, 0)
      requestorProbe.expectMsg(Replaying(generatedEvents(2), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
      // custom position > last sequence number
      log ! Replay(5L, requestorProbe.ref, 0)
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "replay events from the default log if request replicaId is not defined" in {
      generateEvents(destinationAggregateIds = Set("a1"))
      log ! Replay(1L, requestorProbe.ref, None, 0)
      requestorProbe.expectMsg(Replaying(generatedEvents(0), 0))
      requestorProbe.expectMsg(Replaying(generatedEvents(1), 0))
      requestorProbe.expectMsg(Replaying(generatedEvents(2), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "replay events from the index if request replicaId is defined" in {
      generateEvents(offset = 0, destinationAggregateIds = Set("a1"))
      log ! Replay(1L, requestorProbe.ref, Some("a1"), 0)
      requestorProbe.expectMsg(Replaying(generatedEvents(0), 0))
      requestorProbe.expectMsg(Replaying(generatedEvents(1), 0))
      requestorProbe.expectMsg(Replaying(generatedEvents(2), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "replay events from the index and properly stop at the index classifier" in {
      generateEvents(offset = 0, destinationAggregateIds = Set("a1"))
      generateEvents(offset = 3, destinationAggregateIds = Set("a2"))
      log ! Replay(1L, requestorProbe.ref, Some("a1"), 0)
      requestorProbe.expectMsg(Replaying(generatedEvents(0), 0))
      requestorProbe.expectMsg(Replaying(generatedEvents(1), 0))
      requestorProbe.expectMsg(Replaying(generatedEvents(2), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "replay events from the index and from a custom position" in {
      generateEvents(offset = 0, destinationAggregateIds = Set("a1"))
      generateEvents(offset = 3, destinationAggregateIds = Set("a2"))
      log ! Replay(2L, requestorProbe.ref, Some("a1"), 0)
      requestorProbe.expectMsg(Replaying(generatedEvents(1), 0))
      requestorProbe.expectMsg(Replaying(generatedEvents(2), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
      log ! Replay(5L, requestorProbe.ref, Some("a1"), 0)
      requestorProbe.expectMsg(ReplaySuccess(0))
      log ! Replay(2L, requestorProbe.ref, Some("a2"), 0)
      requestorProbe.expectMsg(Replaying(generatedEvents(3), 0))
      requestorProbe.expectMsg(Replaying(generatedEvents(4), 0))
      requestorProbe.expectMsg(Replaying(generatedEvents(5), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
      log ! Replay(5L, requestorProbe.ref, Some("a2"), 0)
      requestorProbe.expectMsg(Replaying(generatedEvents(4), 0))
      requestorProbe.expectMsg(Replaying(generatedEvents(5), 0))
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "not replay events with non-matching replicaId if request replicaId is defined" in {
      generateEvents(destinationAggregateIds = Set("a1"))
      log ! Replay(1L, requestorProbe.ref, Some("a2"), 0)
      requestorProbe.expectMsg(ReplaySuccess(0))
    }
    "reply with a failure message if replay fails" in {
      log ! Replay(-1, requestorProbe.ref, 0)
      requestorProbe.expectMsg(ReplayFailure(boom, 0))
    }
    "batch-read local events" in {
      generateEvents()
      log.tell(ReplicationRead(1, Int.MaxValue, undefinedLogIdFilter, UndefinedLogId, 0), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(generatedEvents, 3, UndefinedLogId, 0))
    }
    "batch-read local and replicated events" in {
      generateEvents()
      replicateEvents(offset = 3)
      log.tell(ReplicationRead(1, Int.MaxValue, undefinedLogIdFilter, UndefinedLogId, 0), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(generatedEvents ++ replicatedEvents, 6, UndefinedLogId, 0))
    }
    "batch-read events with a batch size limit" in {
      generateEvents()
      log.tell(ReplicationRead(1, 2, undefinedLogIdFilter, UndefinedLogId, 0), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(generatedEvents.take(2), 2, UndefinedLogId, 0))
      log.tell(ReplicationRead(1, 0, undefinedLogIdFilter, UndefinedLogId, 0), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(Nil, 0, UndefinedLogId, 0))
    }
    "batch-read events from a custom position" in {
      generateEvents()
      log.tell(ReplicationRead(2, Int.MaxValue, undefinedLogIdFilter, UndefinedLogId, 0), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(generatedEvents.drop(1), 3, UndefinedLogId, 0))
    }
    "batch-read events from a custom position with a batch size limit" in {
      generateEvents()
      log.tell(ReplicationRead(2, 1, undefinedLogIdFilter, UndefinedLogId, 0), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(generatedEvents.drop(1).take(1), 2, UndefinedLogId, 0))
    }
    "batch-read events with exclusion" in {
      generateEvents()
      replicateEvents(offset = 3)
      log.tell(ReplicationRead(1, Int.MaxValue, SourceLogIdExclusionFilter(logId), UndefinedLogId, 0), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(replicatedEvents, 6, UndefinedLogId, 0))
      log.tell(ReplicationRead(1, Int.MaxValue, SourceLogIdExclusionFilter(remoteLogId), UndefinedLogId, 0), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(generatedEvents, 6, UndefinedLogId, 0))
    }
    "not batch-read events from index" in {
      generateEvents(offset = 0, destinationAggregateIds = Set("a1"))
      generateEvents(offset = 3, destinationAggregateIds = Set())
      log.tell(ReplicationRead(1, Int.MaxValue, undefinedLogIdFilter, UndefinedLogId, 0), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadSuccess(generatedEvents, 6, UndefinedLogId, 0))
    }
    "reply with a failure message if batch-read fails" in {
      log.tell(ReplicationRead(-1, Int.MaxValue, undefinedLogIdFilter, UndefinedLogId, 0), requestorProbe.ref)
      requestorProbe.expectMsg(ReplicationReadFailure(boom.getMessage, UndefinedLogId, 0))
    }
    "recover the current sequence number on (re)start" in {
      generateEvents()
      log.tell(GetSequenceNr, requestorProbe.ref)
      requestorProbe.expectMsg(GetSequenceNrSuccess(3))
      log ! "boom"
      log.tell(GetSequenceNr, requestorProbe.ref)
      requestorProbe.expectMsg(GetSequenceNrSuccess(3))
    }
    "recover the replication progress map on (re)start" in {
      log ! SetReplicationProgress("x", 17)
      log ! SetReplicationProgress("y", 19)
      log.tell(GetReplicationProgress("x"), requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressSuccess(17))
      log.tell(GetReplicationProgress("y"), requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressSuccess(19))
      log ! "boom"
      log.tell(GetReplicationProgress("x"), requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressSuccess(17))
      log.tell(GetReplicationProgress("y"), requestorProbe.ref)
      requestorProbe.expectMsg(GetReplicationProgressSuccess(19))
    }
  }
}
