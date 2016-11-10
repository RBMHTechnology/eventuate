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

package com.rbmhtechnology.eventuate

import akka.pattern.ask
import akka.testkit.TestProbe
import akka.util.Timeout

import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationFilter.NoFilter
import com.rbmhtechnology.eventuate.ReplicationProtocol._

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration._

package object utilities {
  val timeoutDuration = 20.seconds

  implicit class AwaitHelper[T](awaitable: Awaitable[T]) {
    def await: T = Await.result(awaitable, timeoutDuration)
  }

  def write(target: ReplicationTarget, events: Seq[String]): Unit = {
    val system = target.endpoint.system
    val probe = TestProbe()(system)
    target.log ! Write(events.map(DurableEvent(_, target.logId)), system.deadLetters, probe.ref, 0, 0)
    probe.expectMsgClass(classOf[WriteSuccess])
  }

  def read(target: ReplicationTarget): Seq[String] = {
    import target.endpoint.system.dispatcher
    implicit val timeout = Timeout(3.seconds)

    def readEvents: Future[ReplicationReadSuccess] =
      target.log.ask(ReplicationRead(1L, Int.MaxValue, Int.MaxValue, NoFilter, DurableEvent.UndefinedLogId, target.endpoint.system.deadLetters, VectorTime())).mapTo[ReplicationReadSuccess]

    val reading = for {
      res <- readEvents
    } yield res.events.map(_.payload.asInstanceOf[String])

    reading.await
  }

  def replicate(from: ReplicationTarget, to: ReplicationTarget, num: Int = Int.MaxValue): Int = {
    import to.endpoint.system.dispatcher
    implicit val timeout = Timeout(3.seconds)

    def readProgress: Future[GetReplicationProgressSuccess] =
      to.log.ask(GetReplicationProgress(from.logId)).mapTo[GetReplicationProgressSuccess]

    def readEvents(reply: GetReplicationProgressSuccess): Future[ReplicationReadSuccess] =
      from.log.ask(ReplicationRead(reply.storedReplicationProgress + 1, num, Int.MaxValue, NoFilter, to.logId, to.endpoint.system.deadLetters, reply.currentTargetVersionVector)).mapTo[ReplicationReadSuccess]

    def writeEvents(reply: ReplicationReadSuccess): Future[ReplicationWriteSuccess] =
      to.log.ask(ReplicationWrite(reply.events, Map(from.logId -> ReplicationMetadata(reply.replicationProgress, VectorTime.Zero)))).mapTo[ReplicationWriteSuccess]

    val replication = for {
      rps <- readProgress
      res <- readEvents(rps)
      wes <- writeEvents(res)
    } yield wes.events.size

    replication.await
  }
}
