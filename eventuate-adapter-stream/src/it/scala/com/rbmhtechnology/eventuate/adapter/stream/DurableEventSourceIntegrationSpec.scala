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
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl._
import akka.testkit._

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.log.EventLogWriter
import com.rbmhtechnology.eventuate.utilities._

import org.scalatest._

class DurableEventSourceIntegrationSpec extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with SingleLocationSpecLeveldb {
  implicit val materializer: Materializer = ActorMaterializer()

  var writer: EventLogWriter = _
  var writerA: EventLogWriter = _
  var writerB: EventLogWriter = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    writer = new EventLogWriter("writer", log)
    writerA = new EventLogWriter("writerA", log, Some("a"))
    writerB = new EventLogWriter("writerB", log, Some("b"))
  }

  override def afterEach(): Unit = {
    super.afterEach()
    writer.stop()
    writerA.stop()
    writerB.stop()
  }

  "A DurableEventSource" must {
    "emit events for all aggregate ids" in {
      val source = Source.fromGraph(DurableEventSource(log))
      val probe = source.take(4).runWith(TestSink.probe)

      val abc = writer.write(List("a", "b", "c")).await
      probe.request(3).expectNextN(abc)

      val xyz = writerA.write(List("x", "y", "z")).await
      probe.request(3).expectNext(xyz.head).expectComplete()
    }
    "emit events for given aggregateId" in {
      val source1 = Source.fromGraph(DurableEventSource(log, aggregateId = Some("a")))
      val source2 = Source.fromGraph(DurableEventSource(log, aggregateId = Some("b")))

      val probe1 = source1.take(1).runWith(TestSink.probe)
      val probe2 = source2.take(1).runWith(TestSink.probe)

      val x = writer.write(List("x")).await
      val y = writerA.write(List("y")).await
      val z = writerB.write(List("z")).await

      probe1.request(1).expectNext(y.head).expectComplete()
      probe2.request(1).expectNext(z.head).expectComplete()
    }
    "emit events from a given sequence number" in {
      val source = Source.fromGraph(DurableEventSource(log, fromSequenceNr = 2))
      val probe = source.take(2).runWith(TestSink.probe)

      val abc = writer.write(List("a", "b", "c")).await
      probe.request(2).expectNextN(abc.drop(1)).expectComplete()
    }
  }
}
