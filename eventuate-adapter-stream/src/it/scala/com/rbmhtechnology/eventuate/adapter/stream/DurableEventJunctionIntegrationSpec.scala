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
import akka.stream._
import akka.stream.scaladsl._

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.log.EventLogWriter
import com.rbmhtechnology.eventuate.utilities._

import org.scalatest._

import scala.collection.immutable.Seq

class DurableEventJunctionIntegrationSpec extends WordSpec with Matchers with MultiLocationSpecLeveldb {
  var logA: ActorRef = _
  var logB: ActorRef = _
  var logC: ActorRef = _

  implicit var system: ActorSystem = _
  implicit var materializer: ActorMaterializer = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    val location1 = location("1")
    val endpoint1 = location1.endpoint(Set("A", "B", "C"), Set())

    logA = endpoint1.logs("A")
    logB = endpoint1.logs("B")
    logC = endpoint1.logs("C")

    system = location1.system
    materializer = ActorMaterializer()
  }

  "A DurableEventProcessor" must {
    "be able to process a DurableEvent stream merged from multiple sources" in {
      val emittedA = Seq("a", "b", "c", "d", "e", "f")
      val emittedB = Seq("u", "v", "w", "x", "y", "z")

      val emitterA = new EventLogWriter("e1", logA)
      val emitterB = new EventLogWriter("e2", logB)

      val sourceA = Source.fromGraph(DurableEventSource(logA))
      val sourceB = Source.fromGraph(DurableEventSource(logB))

      val processorC = DurableEventProcessor.statefulProcessor[Int, String]("p1", logC)(0) {
        case (ctr, evt) => (ctr + 1, Seq(s"${evt.payload}-$ctr"))
      }

      emitterA.write(emittedA)
      emitterB.write(emittedB)

      val processedC = sourceA.merge(sourceB).via(processorC).map(_.payload.toString).take(12).toMat(Sink.seq[String])(Keep.right).run().await

      // test total order from stateful processing
      processedC.map(_.substring(2).toInt) should be(0 to 11)
      // test partial order from merging
      processedC.map(_.substring(0, 1)).partition(emittedA.contains) should be((emittedA, emittedB))
    }
    "be able to process a DurableEvent stream broadcast from a shared source" in {
      val emittedA = Seq("a", "b", "c")
      val emitterA = new EventLogWriter("e1", logA)

      val sourceA = Source.fromGraph(DurableEventSource(logA))

      val processorB = DurableEventProcessor.statefulProcessor[Int, String]("p1", logB)(0) {
        case (ctr, evt) => (ctr + 1, Seq(s"${evt.payload}-$ctr"))
      }

      val processorC = DurableEventProcessor.statefulProcessor[Int, String]("p2", logC)(0) {
        case (ctr, evt) => (ctr + 2, Seq(s"${evt.payload}-$ctr"))
      }

      def sink(processor: Graph[FlowShape[DurableEvent, DurableEvent], NotUsed]) =
        Flow.fromGraph(processor).map(_.payload.toString).take(3).toMat(Sink.seq[String])(Keep.right)

      val graph = RunnableGraph.fromGraph(GraphDSL.create(sink(processorB), sink(processorC))(Keep.both) { implicit b => (sink1, sink2) =>
        import GraphDSL.Implicits._

        val bcast = b.add(Broadcast[DurableEvent](2))

        sourceA ~> bcast
        bcast ~> sink1
        bcast ~> sink2

        ClosedShape
      })

      emitterA.write(emittedA)

      val (processedB, processedC) = graph.run()

      processedB.await should be(Seq("a-0", "b-1", "c-2"))
      processedC.await should be(Seq("a-0", "b-2", "c-4"))
    }
  }
}
