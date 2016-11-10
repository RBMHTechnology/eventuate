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

import akka.actor._
import akka.stream._
import akka.stream.scaladsl.Keep
import akka.stream.testkit._
import akka.stream.testkit.scaladsl.{ TestSink, TestSource }
import akka.pattern
import akka.testkit._

import com.rbmhtechnology.eventuate.DurableEvent

import org.scalatest._

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Random

class BatchWriteStageSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  import BatchWriteStage.BatchWriter

  private val settings: DurableEventWriterSettings =
    new DurableEventWriterSettings(system.settings.config)

  implicit val materializer: Materializer =
    ActorMaterializer()

  private var src: TestPublisher.Probe[Seq[DurableEvent]] = _
  private var snk: TestSubscriber.Probe[Seq[DurableEvent]] = _

  override def beforeEach(): Unit = {
    val probes = TestSource.probe[Seq[DurableEvent]]
      .via(new BatchWriteStage(ec => writer(ec)))
      .toMat(TestSink.probe[Seq[DurableEvent]])(Keep.both)
      .run()

    src = probes._1
    snk = probes._2
  }

  override def afterEach(): Unit = {
    snk.cancel()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  private def random: Int =
    Random.nextInt(100)

  private def writer(implicit ec: ExecutionContext): BatchWriter = events =>
    if (events.exists(_.payload == "boom")) Future(throw TestException)
    else pattern.after(random.millis, system.scheduler)(Future(events))

  "A BatchWriterStage" must {
    "write batches sequentially" in {
      val b1 = Seq("a", "b", "c").map(DurableEvent(_))
      val b2 = Seq("d", "e", "f").map(DurableEvent(_))
      val b3 = Seq("g", "h", "i").map(DurableEvent(_))

      snk.request(3)
      src.sendNext(b1)
      src.sendNext(b2)
      src.sendNext(b3)
      snk.expectNext() should be(b1)
      snk.expectNext() should be(b2)
      snk.expectNext() should be(b3)
    }
    "fail if the batch writer fails" in {
      val b = Seq("a", "boom", "c").map(DurableEvent(_))

      snk.request(3)
      src.sendNext(b)
      snk.expectError(TestException)
    }
  }
}
