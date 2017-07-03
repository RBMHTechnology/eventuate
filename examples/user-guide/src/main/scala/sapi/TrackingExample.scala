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

object TrackingExample extends App {
  import akka.actor._

  //#tracking-conflicting-versions
  import com.rbmhtechnology.eventuate.{ConcurrentVersions, EventsourcedActor, Versioned}
  import scala.collection.immutable.Seq

  class ExampleActor(
      override val id: String,
      override val aggregateId: Option[String],
      override val eventLog: ActorRef
    ) extends EventsourcedActor {
    // notice this is different:
    private var versionedState: ConcurrentVersions[Vector[String], String] =
      ConcurrentVersions(Vector.empty, (s, a) => s :+ a)

    override def onCommand: PartialFunction[Any, Unit] = {
      case _ =>  // ...
  //#

      // todo write something here to make this more realistic

  //#tracking-conflicting-versions
    }

    override def onEvent: PartialFunction[Any, Unit] = {
      case AppendedEvent(entry) =>
        versionedState = versionedState.update(entry, lastVectorTimestamp)
        if (versionedState.conflict) {
          val conflictingVersions: Seq[Versioned[Vector[String]]] =
            versionedState.all
          val conflictExists = conflictingVersions.size==1
          if (conflictExists) {
            // TODO: resolve conflicting versions
          }
        } else {
          val currentState: Vector[String] = versionedState.all.head.value
          // happily process causal event
        }
    }
  }
  //#
}
