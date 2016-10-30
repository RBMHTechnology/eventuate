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

package com.rbmhtechnology.eventuate.adapter.stream

import akka.NotUsed
import akka.actor._
import akka.pattern.AskTimeoutException
import akka.stream._
import akka.stream.scaladsl.Keep
import akka.stream.testkit._
import akka.stream.testkit.scaladsl.{ TestSink, TestSource }
import akka.testkit._

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.DurableEvent._
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.typesafe.config.ConfigFactory

import org.scalatest._

import scala.collection.immutable.Seq

object DurableEventWriterSpec {
  val EmitterId = "emitter"

  val config = ConfigFactory.parseString(
    """
      |eventuate.log.write-batch-size = 3
      |eventuate.log.write-timeout = 500ms
    """.stripMargin)

  val e1 = DurableEvent("a")
  val e2 = DurableEvent("b")
  val e3 = DurableEvent("c")
  val e4 = DurableEvent("d")
  val e5 = DurableEvent("e")
  val e6 = DurableEvent("f")
  val e7 = DurableEvent("g")
  val e8 = DurableEvent("h")
}

abstract class DurableEventWriterSpec[I, O] extends TestKit(ActorSystem("test", DurableEventWriterSpec.config)) with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  private val settings: DurableEventWriterSettings =
    new DurableEventWriterSettings(system.settings.config)

  implicit val materializer: Materializer =
    ActorMaterializer()

  protected var src: TestPublisher.Probe[I] = _
  protected var snk: TestSubscriber.Probe[O] = _
  protected var log: TestProbe = _

  override def beforeEach(): Unit = {
    log = TestProbe()

    val probes = TestSource.probe[I].via(writer(log.ref)).toMat(TestSink.probe[O])(Keep.both).run()

    src = probes._1
    snk = probes._2
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  def writer(log: ActorRef): Graph[FlowShape[I, O], NotUsed]
}

class EmissionWriterSpec extends DurableEventWriterSpec[DurableEvent, DurableEvent] {
  import DurableEventWriterSpec._

  override def writer(log: ActorRef): Graph[FlowShape[DurableEvent, DurableEvent], NotUsed] =
    DurableEventWriter.emissionWriter(EmitterId, log)

  def expectWrite(events: Seq[DurableEvent]): Seq[DurableEvent] = {
    val written = events.map(_.copy(emitterId = EmitterId))
    log.expectMsg(Write(written, null, null, 0, 0))
    written
  }

  def writeSuccess(written: Seq[DurableEvent]): Unit = {
    log.sender() ! WriteSuccess(written, 0, 0)
  }

  def writeFailure(cause: Throwable): Unit = {
    log.sender() ! WriteFailure(Seq(), cause, 0, 0)
  }

  "An emission writer" must {
    "use EventsourcingProtocol.Write for writing event batches" in {
      snk.request(1)
      src.sendNext(e1)

      val written = expectWrite(Seq(e1))
      writeSuccess(written)
      written.foreach(snk.expectNext)
    }
    "fail on write timeouts" in {
      snk.request(1)
      src.sendNext(e1)

      val written = expectWrite(Seq(e1))
      snk.expectError should be(an[AskTimeoutException])
    }
    "fail on write failures" in {
      snk.request(1)
      src.sendNext(e1)

      val written = expectWrite(Seq(e1))
      writeFailure(TestException)
      snk.expectError(TestException)
    }
    "batch events while write is in progress" in {
      snk.request(6)
      src.sendNext(e1)

      val written1 = expectWrite(Seq(e1))

      src.sendNext(e2)
      src.sendNext(e3)
      src.sendNext(e4)
      src.sendNext(e5)
      src.sendNext(e6)

      writeSuccess(written1)
      written1.foreach(snk.expectNext)

      val written2 = expectWrite(Seq(e2, e3, e4))
      writeSuccess(written2)
      written2.foreach(snk.expectNext)

      val written3 = expectWrite(Seq(e5, e6))
      writeSuccess(written3)
      written3.foreach(snk.expectNext)
    }
  }
}

class ReplicationWriterSpec extends DurableEventWriterSpec[Seq[DurableEvent], DurableEvent] {
  import DurableEventWriterSpec._

  val undefinedMetadata: Map[String, ReplicationMetadata] =
    Map(UndefinedLogId -> ReplicationMetadata(UndefinedSequenceNr, VectorTime.Zero))

  override def writer(log: ActorRef): Graph[FlowShape[Seq[DurableEvent], DurableEvent], NotUsed] =
    DurableEventWriter.replicationWriter(EmitterId, log)

  def expectWrite(events: Seq[DurableEvent], metadata: Map[String, ReplicationMetadata] = undefinedMetadata): Seq[DurableEvent] = {
    val written = events.map(_.copy(emitterId = EmitterId, processId = UndefinedLogId))
    log.expectMsg(ReplicationWrite(written, metadata))
    written
  }

  def writeSuccess(written: Seq[DurableEvent]): Unit = {
    log.sender() ! ReplicationWriteSuccess(written, undefinedMetadata)
  }

  def writeFailure(cause: Throwable): Unit = {
    log.sender() ! ReplicationWriteFailure(cause)
  }

  "A replication writer" must {
    "use ReplicationProtocol.ReplicationWrite for writing event batches" in {
      snk.request(1)
      src.sendNext(Seq(e1))

      val written = expectWrite(Seq(e1))
      writeSuccess(written)
      written.foreach(snk.expectNext)
    }
    "fail on write timeouts" in {
      snk.request(1)
      src.sendNext(Seq(e1))

      val written = expectWrite(Seq(e1))
      snk.expectError should be(an[AskTimeoutException])
    }
    "fail on write failures" in {
      snk.request(1)
      src.sendNext(Seq(e1))

      val written = expectWrite(Seq(e1))
      writeFailure(TestException)
      snk.expectError(TestException)
    }
    "batch events while write is in progress" in {
      snk.request(6)
      src.sendNext(Seq(e1))

      val written1 = expectWrite(Seq(e1))

      src.sendNext(Seq(e2, e3))
      src.sendNext(Seq(e4))
      src.sendNext(Seq(e5))
      src.sendNext(Seq(e6))

      writeSuccess(written1)
      written1.foreach(snk.expectNext)

      val written2 = expectWrite(Seq(e2, e3, e4))
      writeSuccess(written2)
      written2.foreach(snk.expectNext)

      val written3 = expectWrite(Seq(e5, e6))
      writeSuccess(written3)
      written3.foreach(snk.expectNext)
    }
    "tolerate application-defined batches larger than settings.writeBatchSize" in {
      snk.request(8)
      src.sendNext(Seq(e1))

      val written1 = expectWrite(Seq(e1))

      src.sendNext(Seq(e2, e3, e4, e5, e6))
      src.sendNext(Seq(e7, e8))

      writeSuccess(written1)
      written1.foreach(snk.expectNext)

      val written2 = expectWrite(Seq(e2, e3, e4, e5, e6))
      writeSuccess(written2)
      written2.foreach(snk.expectNext)

      val written3 = expectWrite(Seq(e7, e8))
      writeSuccess(written3)
      written3.foreach(snk.expectNext)
    }
    "generate replication metadata for multiple sources" in {
      val srcLogIdA = "a"
      val srcLogIdB = "b"

      val e1a = e1.copy(localLogId = srcLogIdA, localSequenceNr = 1)
      val e2a = e1.copy(localLogId = srcLogIdA, localSequenceNr = 2)
      val e3a = e1.copy(localLogId = srcLogIdA, localSequenceNr = 3)
      val e4b = e1.copy(localLogId = srcLogIdB, localSequenceNr = 1)

      snk.request(4)
      src.sendNext(Seq(e1a))

      val written1 = expectWrite(Seq(e1a), Map(
        srcLogIdA -> ReplicationMetadata(1, VectorTime.Zero)))

      src.sendNext(Seq(e2a))
      src.sendNext(Seq(e3a))
      src.sendNext(Seq(e4b))

      writeSuccess(written1)
      written1.foreach(snk.expectNext)

      val written2 = expectWrite(Seq(e2a, e3a, e4b), Map(
        srcLogIdA -> ReplicationMetadata(3, VectorTime.Zero),
        srcLogIdB -> ReplicationMetadata(1, VectorTime.Zero)))

      writeSuccess(written2)
      written2.foreach(snk.expectNext)
    }
  }
}
