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

object ResolveExample extends App {
  import akka.actor._
  import com.rbmhtechnology.eventuate._

  //#automated-conflict-resolution
  class ExampleActor(
    override val id: String,
    override val aggregateId: Option[String],
    override val eventLog: ActorRef
  ) extends EventsourcedActor {
    private var versionedState: ConcurrentVersions[Vector[String], String] =
      ConcurrentVersions(Vector.empty, (s, a) => s :+ a)

    override def onCommand: PartialFunction[Any, Unit] = {
      // ...
  //#
      case _ =>
  //#automated-conflict-resolution
    }

    override def onEvent: PartialFunction[Any, Unit] = {
      case AppendedEvent(entry) =>
        versionedState = versionedState.update(entry, lastVectorTimestamp, lastSystemTimestamp, lastEmitterId)
        if (versionedState.conflict) {
          val conflictingVersions = versionedState.all.sortWith { (v1, v2) =>
            if (v1.systemTimestamp == v2.systemTimestamp) v1.creator < v2.creator
            else v1.systemTimestamp > v2.systemTimestamp
          }
          val winnerTimestamp: VectorTime = conflictingVersions.head.vectorTimestamp
          versionedState = versionedState.resolve(winnerTimestamp)
        }
    }
  }
  //#
}
