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

package com.rbmhtechnology.eventuate.adapter.vertx

import akka.actor.{ ActorSystem, Status }
import akka.pattern.ask
import akka.testkit.TestProbe
import akka.util.Timeout
import com.rbmhtechnology.eventuate.adapter.vertx.api.StorageProvider

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

class ActorStorageProvider(defaultId: String)(implicit system: ActorSystem) extends StorageProvider {
  implicit val timeout = Timeout(20.seconds)

  val probe = TestProbe()

  override def readProgress(id: String)(implicit executionContext: ExecutionContext): Future[Long] =
    probe.ref.ask(read(id)).mapTo[Long]

  override def writeProgress(id: String, sequenceNr: Long)(implicit executionContext: ExecutionContext): Future[Long] =
    probe.ref.ask(write(id, sequenceNr)).mapTo[Long]

  def expectRead(replySequenceNr: Long, id: String = defaultId): Unit = {
    probe.expectMsg(read(id))
    probe.reply(replySequenceNr)
  }

  def expectWrite(sequenceNr: Long, id: String = defaultId): Unit = {
    probe.expectMsg(write(id, sequenceNr))
    probe.reply(sequenceNr)
  }

  def expectWriteAndFail(sequenceNr: Long, failure: Throwable, id: String = defaultId): Unit = {
    probe.expectMsg(write(id, sequenceNr))
    probe.reply(Status.Failure(failure))
  }

  def expectWriteAnyOf(sequenceNrs: Seq[Long], id: String = defaultId): Unit = {
    probe.expectMsgAnyOf(sequenceNrs.map(write(id, _)): _*)
    probe.reply(sequenceNrs.max)
  }

  def expectNoMsg(duration: FiniteDuration): Unit = {
    probe.expectNoMsg(duration)
  }

  private def read(id: String): String =
    s"read[$id]"

  private def write(id: String, sequenceNr: Long): String =
    s"write[$id]-$sequenceNr"
}
