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

package com.rbmhtechnology.example.stream

//# durable-event-processor-stateless
import akka.stream.scaladsl.Source
import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.adapter.stream.DurableEventSource
import com.rbmhtechnology.eventuate.adapter.stream.DurableEventProcessor._
import scala.collection.immutable.Seq

//#
object DurableEventProcessorExample1 extends App with DurableEventLogs {
  //# durable-event-processor-stateless
  val processorId1 = "processor-1"

  def statelessProcessingLogic(event: DurableEvent): Seq[Any] = event.payload match {
    case "a" => Seq()
    case "b" => Seq(s"b-${event.localSequenceNr}")
    case "c" => Seq("c", "c")
    case evt => Seq(evt)
  }

  Source.fromGraph(DurableEventSource(logA))
    .via(statelessProcessor(processorId1, logB)(statelessProcessingLogic))
    .map(event => (event.payload, event.localSequenceNr))
    .runForeach(println)

  // prints (on first run):
  // (b-2,1)
  // (c,2)
  // (c,3)
  //#

  //# durable-event-processor-stateful
  val processorId2 = "processor-2"

  def statefulProcessingLogic(count: Int, event: DurableEvent): (Int, Seq[String]) = {
    event.payload match {
      case "b" =>
        val updated = count + 1
        (updated, Seq(s"b-$updated"))
      case evt =>
        (count, Seq(s"$evt-$count"))
    }
  }

  Source.fromGraph(DurableEventSource(logA))
    .via(statefulProcessor(processorId2, logC)(0)(statefulProcessingLogic))
    .map(event => (event.payload, event.localSequenceNr))
    .runForeach(println)

  // prints (on first run):
  // (a-0,1)
  // (b-1,2)
  // (c-1,3)
  //#
}

object DurableEventProcessorExample2 extends App with DurableEventLogs {
  def statelessProcessingLogic(event: DurableEvent): Seq[Any] = event.payload match {
    case "a" => Seq()
    case "b" => Seq(s"b-${event.localSequenceNr}")
    case "c" => Seq("c", "c")
    case evt => Seq(evt)
  }

  def statefulProcessingLogic(count: Int, event: DurableEvent): (Int, Seq[String]) = {
    event.payload match {
      case "b" =>
        val updated = count + 1
        (updated, Seq(s"b-$updated"))
      case evt =>
        (count, Seq(s"$evt-$count"))
    }
  }

  val processorId1 = "processor-1"
  val processorId2 = "processor-2"

  //# durable-event-processor-shared-source
  import akka.stream.ClosedShape
  import akka.stream.scaladsl._

  val source = DurableEventSource(logA)
  val sink = Flow[DurableEvent]
    .map(event => (event.payload, event.localSequenceNr))
    .to(Sink.foreach(println))

  val processor1 = statelessProcessor(processorId1, logB)(statelessProcessingLogic)
  val processor2 = statefulProcessor(processorId2, logC)(0)(statefulProcessingLogic)

  val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
    import GraphDSL.Implicits._

    val bcast = b.add(Broadcast[DurableEvent](2))

    source ~> bcast
    bcast ~> processor1 ~> sink
    bcast ~> processor2 ~> sink

    ClosedShape
  })

  graph.run()
  //#
}

object DurableEventProcessorExample3 extends App with DurableEventLogs {
  val processorId1 = "processor-1"

  def statelessProcessingLogic(event: DurableEvent): Seq[Any] = ???

  //# durable-event-processor-multiple-sources
  Source.fromGraph(DurableEventSource(logA))
    .merge(DurableEventSource(logB))
    .via(statelessProcessor(processorId1, logC)(statelessProcessingLogic))
    .map(event => (event.payload, event.localSequenceNr))
    .runForeach(println)
  //#
}
