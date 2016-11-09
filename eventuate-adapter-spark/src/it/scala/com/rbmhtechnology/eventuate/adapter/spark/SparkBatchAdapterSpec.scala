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
import akka.testkit._

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.utilities._
import com.rbmhtechnology.eventuate.log.EventLogWriter
import com.rbmhtechnology.eventuate.log.cassandra.CassandraEventLogSettings

import org.apache.spark._
import org.scalatest._

class SparkBatchAdapterSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with SingleLocationSpecCassandraAdapter {
  val cassandraSettings = new CassandraEventLogSettings(system.settings.config)
  val sparkConfig = new SparkConf(true)
    .set("spark.cassandra.connection.host", cassandraSettings.contactPoints.head.getHostName)
    .set("spark.cassandra.connection.port", cassandraSettings.contactPoints.head.getPort.toString)
    .set("spark.cassandra.auth.username", system.settings.config.getString("eventuate.log.cassandra.username"))
    .set("spark.cassandra.auth.password", system.settings.config.getString("eventuate.log.cassandra.password"))
    .setAppName("adapter")
    .setMaster("local[4]")

  var sparkContext: SparkContext = _
  var sparkAdapter: SparkBatchAdapter = _

  var probe: TestProbe = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    probe = TestProbe()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext = new SparkContext(sparkConfig)
    sparkAdapter = new SparkBatchAdapter(sparkContext, system.settings.config)
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
    super.afterAll()
  }

  def writeEvents(prefix: String, num: Int = 100): Seq[DurableEvent] =
    new EventLogWriter("w1", log).write((1 to num).map(i => s"$prefix-$i")).await

  "A Spark adapter" must {
    "read events from a local event log" in {
      val writtenEvents = writeEvents("a")
      val readEvents = sparkAdapter.eventBatch(logId)
      readEvents.sortBy(_.localSequenceNr).collect() should be(writtenEvents)
    }
    "read events from a local event log starting from a given sequence number" in {
      val writtenEvents = writeEvents("a")
      val readEvents = sparkAdapter.eventBatch(logId, 23L)
      readEvents.sortBy(_.localSequenceNr).collect() should be(writtenEvents.drop(22))
    }
  }
}
