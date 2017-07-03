/*
 * Copyright 2015 - 2017 Red Bull Media House GmbH <http://www.redbullmediahouse.com> and Mike Slinn - all rights reserved.
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

package sapi

/** Referenced `TrackingExample`. */
object ConcurrentExample extends App {
  //#detecting-concurrent-update
  import akka.actor._
  import com.rbmhtechnology.eventuate.{EventsourcedActor, VectorTime}

  class ExampleActor(
    override val id: String,
    override val aggregateId: Option[String],
    override val eventLog: ActorRef
  ) extends EventsourcedActor {
    private var currentState: Vector[String] = Vector.empty
    // This is the first time we see a VectorTime instance:
    private var updateTimestamp: VectorTime = VectorTime()

    def onCommand: PartialFunction[Any, Unit] = {
      case _ =>  // ...
  //#

      // todo add code here to make this example more realistic

  //#detecting-concurrent-update
    }

    def onEvent: PartialFunction[Any, Unit] = {
      case AppendedEvent(entry2) =>
        if (updateTimestamp < lastVectorTimestamp) { // regular update
          currentState = currentState :+ entry2
          updateTimestamp = lastVectorTimestamp
        } else if (updateTimestamp conc lastVectorTimestamp) {
          // concurrent update
          // TODO: track conflicting versions
        }
    }
  }
  //#
}
