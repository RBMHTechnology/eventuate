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

package com.rbmhtechnology.eventuate.adapter.spark

import akka.actor._
import akka.pattern
import akka.testkit._

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.log.EventLogWriter
import com.rbmhtechnology.eventuate.utilities._

import org.apache.spark._
import org.apache.spark.streaming._
import org.scalatest._

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.duration._

class SparkStreamAdapterSpec extends WordSpec with Matchers with MultiLocationSpecLeveldb {
  val logName = "L"
  val sparkConfig = new SparkConf(true)
    .setAppName("adapter")
    .setMaster("local[4]")

  var sparkContext: SparkContext = _
  var sparkStreamingContext: StreamingContext = _
  var sparkStreamAdapter: SparkStreamAdapter = _

  var port: Int = _
  var log: ActorRef = _
  var probe: TestProbe = _

  implicit var system: ActorSystem = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext = new SparkContext(sparkConfig)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    val loc = location("A")
    val ept = loc.endpoint(Set(logName), Set())

    system = loc.system

    log = ept.logs(logName)
    port = loc.port
    probe = TestProbe()

    sparkStreamingContext = new StreamingContext(sparkContext, Seconds(1))
    sparkStreamAdapter = new SparkStreamAdapter(sparkStreamingContext, system.settings.config)
  }

  override def afterEach(): Unit = {
    sparkStreamingContext.stop(stopSparkContext = false)
    super.afterEach()
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
    super.afterAll()
  }

  def writeEvents(prefix: String, num: Int = 100): Future[Seq[DurableEvent]] =
    new EventLogWriter("writer", log).write((0 until num).map(i => s"$prefix-$i"))

  def writeEventsWithInterval(prefix: String, num: Int = 100, interval: FiniteDuration = 100.millis): Future[Seq[DurableEvent]] = {
    val writer = new EventLogWriter("writer", log)
    implicit val dispatcher = system.dispatcher
    def go(i: Int): Future[List[DurableEvent]] = for {
      _ <- pattern.after(interval, system.scheduler)(Future.successful(Nil))
      es1 <- writer.write(Seq(s"$prefix-$i"))
      es2 <- if (i < num) go(i + 1) else Future.successful(Nil)
    } yield es1.head :: es2
    go(0)
  }

  "A Spark stream adapter" must {
    "create a DStream from an event log" in {
      val expected = writeEvents("a").await
      val stream = sparkStreamAdapter.eventStream("s1", "127.0.0.1", port, logName)
      stream.foreachRDD(rdd => rdd.collect().foreach(probe.ref ! _))
      sparkStreamingContext.start()
      probe.expectMsgAllOf(expected: _*)
    }
    "create a DStream from an event log starting from a given sequence number" in {
      val expected = writeEvents("b").await.drop(72)
      val stream = sparkStreamAdapter.eventStream("s2", "127.0.0.1", port, logName, fromSequenceNr = 73L)
      stream.foreachRDD(rdd => rdd.collect().foreach(probe.ref ! _))
      sparkStreamingContext.start()
      probe.expectMsgAllOf(expected: _*)
    }
    "create a live DStream from an event log" in {
      val stream = sparkStreamAdapter.eventStream("s3", "127.0.0.1", port, logName)
      stream.foreachRDD(rdd => rdd.collect().foreach(probe.ref ! _))
      sparkStreamingContext.start()
      val expected = writeEventsWithInterval("c", 50).await
      probe.expectMsgAllOf(expected: _*)
    }
  }
}
