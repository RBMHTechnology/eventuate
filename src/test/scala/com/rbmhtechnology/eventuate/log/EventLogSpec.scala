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
  val config = ConfigFactory.parseString("log.leveldb.dir = target/test")

  val processIdA = "A"
  val processIdB = "B"
  val processIdC = "C"
  val remoteLogId = "R1"

  val remoteLogIdFilter = SourceLogIdExclusionFilter(remoteLogId)
  val undefinedLogIdFilter = SourceLogIdExclusionFilter(UndefinedLogId)

  def event(payload: Any, timestamp: VectorTime, processId: String): DurableEvent =
    DurableEvent(payload, timestamp, processId, UndefinedLogId, UndefinedLogId, UndefinedSequenceNr, UndefinedSequenceNr)

  def timestampA(timeA: Long): VectorTime =
    VectorTime(processIdA -> timeA)

  def timestampAB(timeA: Long, timeB: Long): VectorTime =
    VectorTime(processIdA -> timeA, processIdB -> timeB)
}

import EventLogSpec._

class EventLogSpec extends TestKit(ActorSystem("test", config)) with WordSpecLike with Matchers with EventLogSupport {
  import EventLogProtocol._
  import ReplicationProtocol._

  var requestorProbe: TestProbe = _
  var replicatorProbe: TestProbe = _
  var notificationProbe: TestProbe = _

  var generatedEvents: Vector[DurableEvent] = _
  var replicatedEvents: Vector[DurableEvent] = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    requestorProbe = TestProbe()
    replicatorProbe = TestProbe()
    notificationProbe = TestProbe()

    system.eventStream.subscribe(notificationProbe.ref, classOf[Updated])
  }

  override def afterEach(): Unit = {
    system.eventStream.unsubscribe(notificationProbe.ref, classOf[Updated])
  }

  def registerCollaborator(collaborator: TestProbe = TestProbe()): TestProbe = {
    log ! Replay(Long.MaxValue, collaborator.ref, 0)
    collaborator.expectMsg(ReplaySuccess(0))
    collaborator
  }
  
  def generateEvents(): Unit = {
    val events: Vector[DurableEvent] = Vector(
      event("a", timestampAB(1, 0), processIdA),
      event("b", timestampAB(2, 0), processIdA),
      event("c", timestampAB(3, 0), processIdA))

    generatedEvents = Vector(
      DurableEvent("a", timestampAB(1, 0), processIdA, logId, logId, 1, 1),
      DurableEvent("b", timestampAB(2, 0), processIdA, logId, logId, 2, 2),
      DurableEvent("c", timestampAB(3, 0), processIdA, logId, logId, 3, 3))

    log ! Write(events, system.deadLetters, requestorProbe.ref, 0)
    requestorProbe.expectMsg(WriteSuccess(generatedEvents(0), 0))
    requestorProbe.expectMsg(WriteSuccess(generatedEvents(1), 0))
    requestorProbe.expectMsg(WriteSuccess(generatedEvents(2), 0))
    notificationProbe.expectMsg(Updated(generatedEvents))
  }

  def replicateEvents(offset: Long, remoteLogId: String = remoteLogId): Unit = {
    val events: Vector[DurableEvent] = Vector(
      DurableEvent("i", timestampAB(0, 7), processIdB, remoteLogId, remoteLogId, 7, 7),
      DurableEvent("j", timestampAB(0, 8), processIdB, remoteLogId, remoteLogId, 8, 8),
      DurableEvent("k", timestampAB(0, 9), processIdB, remoteLogId, remoteLogId, 9, 9))

    replicatedEvents = Vector(
      DurableEvent("i", timestampAB(0, 7), processIdB, remoteLogId, logId, 7, 1 + offset),
      DurableEvent("j", timestampAB(0, 8), processIdB, remoteLogId, logId, 8, 2 + offset),
      DurableEvent("k", timestampAB(0, 9), processIdB, remoteLogId, logId, 9, 3 + offset))

    log.tell(Replicate(events, remoteLogId, 9), replicatorProbe.ref)
    replicatorProbe.expectMsg(ReplicateSuccess(events.length, 9))
    notificationProbe.expectMsg(Updated(replicatedEvents))
  }

  def replicateNone(lastSourceLogSequenceNrRead: Long, expectedLastSourceLogSequenceNrReplicated: Long, remoteLogId: String = remoteLogId): Unit = {
    log.tell(Replicate(Seq(), remoteLogId, lastSourceLogSequenceNrRead), replicatorProbe.ref)
    replicatorProbe.expectMsg(ReplicateSuccess(0, expectedLastSourceLogSequenceNrReplicated))
    notificationProbe.expectMsg(Updated(Seq()))
  }

  "An event log" must {
    "write local events and send them to the requestor" in {
      generateEvents()
    }
    "write local events and send them to collaborators" in {
      val collaborator = registerCollaborator()
      generateEvents()
      collaborator.expectMsg(Written(generatedEvents(0)))
      collaborator.expectMsg(Written(generatedEvents(1)))
      collaborator.expectMsg(Written(generatedEvents(2)))
    }
    "reply with a failure message if write fails" in {
      val events = Vector(
        event("boom", timestampAB(1, 0), processIdA),
        event("okay", timestampAB(2, 0), processIdA))

      log ! Write(events, system.deadLetters, requestorProbe.ref, 0)
      requestorProbe.expectMsg(WriteFailure(DurableEvent("boom", timestampAB(1, 0), processIdA, logId, logId, 1, 1), boom, 0))
      requestorProbe.expectMsg(WriteFailure(DurableEvent("okay", timestampAB(2, 0), processIdA, logId, logId, 2, 2), boom, 0))
    }
    "write replicated events" in {
      replicateEvents(offset = 0)
    }
    "write replicated events and send them to collaborators" in {
      val collaborator = registerCollaborator()
      replicateEvents(offset = 0)
      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(1)))
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
        DurableEvent("i", timestampAB(0, 7), processIdB, remoteLogId, remoteLogId, 7, 7),
        DurableEvent("j", timestampAB(0, 8), processIdB, remoteLogId, remoteLogId, 8, 8),
        DurableEvent("k", timestampAB(0, 9), processIdB, remoteLogId, remoteLogId, 9, 9))

      val replicatedEvents = Vector(
        DurableEvent("i", timestampAB(0, 7), processIdB, remoteLogId, logId, 7, 1),
        DurableEvent("j", timestampAB(0, 8), processIdB, remoteLogId, logId, 8, 2),
        DurableEvent("k", timestampAB(0, 9), processIdB, remoteLogId, logId, 9, 3))

      // replicate first two events
      log.tell(Replicate(events.take(2), remoteLogId, 8), replicatorProbe.ref)

      replicatorProbe.expectMsg(ReplicateSuccess(2, 8))
      notificationProbe.expectMsg(Updated(replicatedEvents.take(2)))

      collaborator.expectMsg(Written(replicatedEvents(0)))
      collaborator.expectMsg(Written(replicatedEvents(1)))

      // replicate first two events again (= duplicate)
      log.tell(Replicate(events.take(2), remoteLogId, 8), replicatorProbe.ref)

      replicatorProbe.expectMsg(ReplicateSuccess(0, 8))
      notificationProbe.expectMsg(Updated(Seq()))

      // replicate remaining events
      log.tell(Replicate(events.drop(2), remoteLogId, 9), replicatorProbe.ref)

      replicatorProbe.expectMsg(ReplicateSuccess(1, 9))
      notificationProbe.expectMsg(Updated(replicatedEvents.drop(2)))

      collaborator.expectMsg(Written(replicatedEvents(2)))
    }
    "reply with a failure message if replication fails" in {
      val events: Vector[DurableEvent] = Vector(
        DurableEvent("boom", timestampAB(0, 7), processIdB, remoteLogId, remoteLogId, 7, 7),
        DurableEvent("okay", timestampAB(0, 8), processIdB, remoteLogId, remoteLogId, 8, 8))

      log.tell(Replicate(events, remoteLogId, 8), replicatorProbe.ref)
      replicatorProbe.expectMsg(ReplicateFailure(boom))
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
    "reply with a failure message if replay fails" in {
      log ! Replay(-1, requestorProbe.ref, 0)
      requestorProbe.expectMsg(ReplayFailure(boom, 0))
    }
    "batch-read local events" in {
      generateEvents()
      log.tell(Read(1, Int.MaxValue, undefinedLogIdFilter), requestorProbe.ref)
      requestorProbe.expectMsg(ReadSuccess(generatedEvents, 3))
    }
    "batch-read local and replicated events" in {
      generateEvents()
      replicateEvents(offset = 3)
      log.tell(Read(1, Int.MaxValue, undefinedLogIdFilter), requestorProbe.ref)
      requestorProbe.expectMsg(ReadSuccess(generatedEvents ++ replicatedEvents, 6))
    }
    "batch-read events with a batch size limit" in {
      generateEvents()
      log.tell(Read(1, 2, undefinedLogIdFilter), requestorProbe.ref)
      requestorProbe.expectMsg(ReadSuccess(generatedEvents.take(2), 2))
      log.tell(Read(1, 0, undefinedLogIdFilter), requestorProbe.ref)
      requestorProbe.expectMsg(ReadSuccess(Nil, 0))
    }
    "batch-read events from a custom position" in {
      generateEvents()
      log.tell(Read(2, Int.MaxValue, undefinedLogIdFilter), requestorProbe.ref)
      requestorProbe.expectMsg(ReadSuccess(generatedEvents.drop(1), 3))
    }
    "batch-read events from a custom position with a batch size limit" in {
      generateEvents()
      log.tell(Read(2, 1, undefinedLogIdFilter), requestorProbe.ref)
      requestorProbe.expectMsg(ReadSuccess(generatedEvents.drop(1).take(1), 2))
    }
    "batch-read events with exclusion" in {
      generateEvents()
      replicateEvents(offset = 3)
      log.tell(Read(1, Int.MaxValue, SourceLogIdExclusionFilter(logId)), requestorProbe.ref)
      requestorProbe.expectMsg(ReadSuccess(replicatedEvents, 6))
      log.tell(Read(1, Int.MaxValue, SourceLogIdExclusionFilter(remoteLogId)), requestorProbe.ref)
      requestorProbe.expectMsg(ReadSuccess(generatedEvents, 6))
    }
    "reply with a failure message if batch-read fails" in {
      log.tell(Read(-1, Int.MaxValue, undefinedLogIdFilter), requestorProbe.ref)
      requestorProbe.expectMsg(ReadFailure(boom))
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
