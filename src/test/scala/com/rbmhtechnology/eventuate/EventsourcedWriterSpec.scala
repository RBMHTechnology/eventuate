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

package com.rbmhtechnology.eventuate

import akka.actor._
import akka.pattern.ask
import akka.testkit._
import akka.util.Timeout

import com.rbmhtechnology.eventuate.EventsourcedViewSpec._

import org.scalatest._

import scala.concurrent._
import scala.concurrent.duration._
import scala.util._

object EventsourcedWriterSpec {
  class TestEventsourcedWriter(logProbe: ActorRef, appProbe: ActorRef, rwProbe: ActorRef, readSuccessResult: Option[Long]) extends EventsourcedWriter[String, String] {
    implicit val timeout = Timeout(3.seconds)

    override val id = emitterIdB
    override val eventLog = logProbe
    override val replayChunkSizeMax: Int = 2

    override def onCommand: Receive = {
      case cmd => appProbe ! cmd
    }

    override val onEvent: Receive = {
      case evt: String =>
        appProbe ! ((evt, lastSequenceNr))
    }

    override def read(): Future[String] =
      rwProbe.ask("r").mapTo[String]

    override def write(): Future[String] = {
      rwProbe.ask("w").mapTo[String]
    }

    override def readSuccess(result: String): Option[Long] = {
      appProbe ! result
      readSuccessResult
    }

    override def writeSuccess(result: String): Unit =
      appProbe ! result

    override def readFailure(cause: Throwable): Unit = {
      appProbe ! cause
      super.readFailure(cause)
    }

    override def writeFailure(cause: Throwable): Unit = {
      appProbe ! cause
      super.writeFailure(cause)
    }
  }
}

class EventsourcedWriterSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  import EventsourcedWriterSpec._
  import EventsourcingProtocol._

  var instanceId: Int = _
  var logProbe: TestProbe = _
  var appProbe: TestProbe = _
  var rwProbe: TestProbe = _

  override def beforeEach(): Unit = {
    instanceId = EventsourcedView.instanceIdCounter.get
    logProbe = TestProbe()
    appProbe = TestProbe()
    rwProbe = TestProbe()
  }

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  def unrecoveredEventsourcedWriter(readSuccessResult: Option[Long] = None): ActorRef =
    system.actorOf(Props(new TestEventsourcedWriter(logProbe.ref, appProbe.ref, rwProbe.ref, readSuccessResult)))

  def recoveredEventsourcedWriter(readSuccessResult: Option[Long] = None): ActorRef =
    processRecover(unrecoveredEventsourcedWriter(readSuccessResult), readSuccessResult)

  def processRecover(actor: ActorRef, readSuccessResult: Option[Long] = None): ActorRef = {
    processRead(Success("rs"))
    readSuccessResult match {
      case Some(snr) =>
        processReplay(actor, snr)
      case None =>
        processLoad(actor)
        processReplay(actor, 1)
    }
    processWrite(Success("ws"))
    actor
  }

  def processLoad(actor: ActorRef, instanceId: Int = instanceId): Unit = {
    logProbe.expectMsg(LoadSnapshot(emitterIdB, actor, instanceId))
    actor ! LoadSnapshotSuccess(None, instanceId)
  }

  def processReplay(actor: ActorRef, fromSequenceNr: Long): Unit = {
    logProbe.expectMsg(Replay(fromSequenceNr, 2, actor, instanceId))
    actor ! ReplaySuccess(instanceId)
  }

  def processRead(result: Try[String]): Unit = {
    rwProbe.expectMsg("r")
    processResult(result)
  }

  def processWrite(result: Try[String]): Unit = {
    rwProbe.expectMsg("w")
    processResult(result)
  }

  def processResult(result: Try[String]): Unit = {
    result match {
      case Success(s) =>
        rwProbe.sender() ! Status.Success(s)
        appProbe.expectMsg(s)
      case Failure(e) =>
        rwProbe.sender() ! Status.Failure(e)
        appProbe.expectMsg(e)
    }
  }

  "An EventsourcedWriter" when {
    "recovering" must {
      "recover after an initial read with an undefined return value" in {
        recoveredEventsourcedWriter(None)
      }
      "restart on failed read by default" in {
        val actor = unrecoveredEventsourcedWriter()
        processRead(Failure(boom))
        processRead(Success("rs"))
        processLoad(actor, instanceId + 1)
        logProbe.expectMsg(Replay(1, 2, actor, instanceId + 1))
        actor ! ReplaySuccess(instanceId + 1)
        processWrite(Success("ws"))
      }
      "restart on failed write by default" in {
        val actor = unrecoveredEventsourcedWriter()
        processRead(Success("rs"))
        processLoad(actor)
        logProbe.expectMsg(Replay(1, 2, actor, instanceId))
        actor ! Replaying(event("a", 1), instanceId)
        actor ! ReplaySuccess(instanceId)
        appProbe.expectMsg(("a", 1))
        processWrite(Failure(boom))
        processRead(Success("rs"))
        processLoad(actor, instanceId + 1)
        logProbe.expectMsg(Replay(1, 2, actor, instanceId + 1))
        actor ! Replaying(event("a", 1), instanceId + 1)
        actor ! ReplaySuccess(instanceId + 1)
        appProbe.expectMsg(("a", 1))
        processWrite(Success("ws"))
      }
      "trigger writes when recovery is suspended and completed" in {
        val actor = unrecoveredEventsourcedWriter()
        processRead(Success("rs"))
        processLoad(actor)
        logProbe.expectMsg(Replay(1, 2, actor, instanceId))
        actor ! Replaying(event("a", 1), instanceId)
        actor ! Replaying(event("b", 2), instanceId)
        actor.tell(ReplaySuspended(instanceId), logProbe.ref)
        appProbe.expectMsg(("a", 1))
        appProbe.expectMsg(("b", 2))
        processWrite(Success("ws"))
        logProbe.expectMsg(ReplayNext(2, instanceId))
        actor ! Replaying(event("c", 3), instanceId)
        actor ! ReplaySuccess(instanceId)
        appProbe.expectMsg(("c", 3))
        processWrite(Success("ws"))
      }
      "stash commands while read is in progress" in {
        val promise = Promise[String]()
        val actor = unrecoveredEventsourcedWriter()
        actor ! "cmd"
        processRead(Success("rs"))
        processLoad(actor)
        processReplay(actor, 1)
        appProbe.expectMsg("cmd")
        processWrite(Success("ws"))
      }
      "stash commands while write is in progress after suspended replay" in {
        val actor = unrecoveredEventsourcedWriter()
        processRead(Success("rs"))
        processLoad(actor)
        logProbe.expectMsg(Replay(1, 2, actor, instanceId))
        actor ! Replaying(event("a", 1), instanceId)
        actor ! Replaying(event("b", 2), instanceId)
        actor.tell(ReplaySuspended(instanceId), logProbe.ref)
        actor ! "cmd"
        appProbe.expectMsg(("a", 1))
        appProbe.expectMsg(("b", 2))
        processWrite(Success("ws"))
        logProbe.expectMsg(ReplayNext(2, instanceId))
        actor ! Replaying(event("c", 3), instanceId)
        actor ! ReplaySuccess(instanceId)
        appProbe.expectMsg(("c", 3))
        appProbe.expectMsg("cmd")
        processWrite(Success("ws"))
      }
      "handle commands while write is in progress after completed replay" in {
        val promise = Promise[String]()
        val actor = unrecoveredEventsourcedWriter()
        processRead(Success("rs"))
        processLoad(actor)
        processReplay(actor, 1)
        actor ! "cmd"
        appProbe.expectMsg("cmd")
        processWrite(Success("ws"))
      }
    }

    "resuming" must {
      "replay after an initial read using the defined return value as starting position" in {
        recoveredEventsourcedWriter(Some(3))
      }
    }

    "recovered" must {
      "handle commands while write is in progress" in {
        val promise = Promise[String]()
        val actor = processRecover(unrecoveredEventsourcedWriter())
        actor ! Written(event("a", 1)) // trigger write
        actor ! "cmd"
        appProbe.expectMsg(("a", 1))
        appProbe.expectMsg("cmd")
        processWrite(Success("ws"))
      }
      "handle events while write is in progress" in {
        val actor = processRecover(unrecoveredEventsourcedWriter())
        actor ! Written(event("a", 1)) // trigger write 1
        actor ! Written(event("b", 2)) // trigger write 2 (after write 1 completed)
        appProbe.expectMsg(("a", 1))
        appProbe.expectMsg(("b", 2))
        processWrite(Success("ws"))
        processWrite(Success("ws"))
      }
    }
  }
}