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

//# durable-event-writer
import akka.stream.scaladsl.Source
import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.adapter.stream.DurableEventWriter

//#
object DurableEventWriterExample extends App with DurableEventLogs {
  //# durable-event-writer
  val writerId = "writer-1"

  Source(List("a", "b", "c"))
    .map(DurableEvent(_))
    .via(DurableEventWriter(writerId, logA))
    .map(event => (event.payload, event.localSequenceNr))
    .runForeach(println)

  // prints (on first run):
  // (a,1)
  // (b,2)
  // (c,3)
  //#
}
