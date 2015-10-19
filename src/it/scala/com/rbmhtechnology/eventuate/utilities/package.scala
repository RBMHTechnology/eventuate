package com.rbmhtechnology.eventuate

import akka.pattern.ask
import akka.testkit.TestProbe
import akka.util.Timeout

import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationProtocol._

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration._

package object utilities {
  implicit class AwaitHelper[T](awaitable: Awaitable[T]) {
    def await: T = Await.result(awaitable, 10.seconds)
  }

  def write(target: ReplicationTarget, events: Seq[String]): Unit = {
    val system = target.endpoint.system
    val probe = TestProbe()(system)
    target.log ! Write(events.map(DurableEvent(_, target.logId)), system.deadLetters, probe.ref, 0)
    events.foreach(_ => probe.expectMsgClass(classOf[WriteSuccess]))
  }

  def read(target: ReplicationTarget): Seq[String] = {
    import target.endpoint.system.dispatcher
    implicit val timeout = Timeout(3.seconds)

    def readEvents: Future[ReplicationReadSuccess] =
      target.log.ask(ReplicationRead(1L, Int.MaxValue, NoFilter, DurableEvent.UndefinedLogId, target.endpoint.system.deadLetters, VectorTime())).mapTo[ReplicationReadSuccess]

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
      from.log.ask(ReplicationRead(reply.storedReplicationProgress + 1, num, NoFilter, to.logId, to.endpoint.system.deadLetters, reply.currentTargetVectorTime)).mapTo[ReplicationReadSuccess]

    def writeEvents(reply: ReplicationReadSuccess): Future[ReplicationWriteSuccess] =
      to.log.ask(ReplicationWrite(reply.events, from.logId, reply.replicationProgress, VectorTime())).mapTo[ReplicationWriteSuccess]

    val replication = for {
      rps <- readProgress
      res <- readEvents(rps)
      wes <- writeEvents(res)
    } yield wes.num

    replication.await
  }
}
