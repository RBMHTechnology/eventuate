/*
 * Copyright (C) 2015 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.eventuate.log

import akka.actor.Actor

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.ReplicationProtocol._

import scala.collection.immutable.Seq

private object NotificationChannel {
  case class Updated(events: Seq[DurableEvent])
}

/**
 * Notifies registered [[Replicator]]s about source log updates.
 */
private class NotificationChannel(logId: String) extends Actor {
  import NotificationChannel._

  // targetLogId -> subscription
  var registry: Map[String, ReplicationRead] = Map.empty

  // targetLogIds for which a read operation is in progress
  var reading: Set[String] = Set.empty

  def receive = {
    case Updated(events) =>
      registry.foreach {
        case (targetLogId, reg) =>
          if (!reading.contains(targetLogId) && events.exists(_.replicable(reg.currentTargetVectorTime, reg.filter))) {
            reg.replicator ! ReplicationDue
          }
      }
    case r: ReplicationRead =>
      registry += (r.targetLogId -> r)
      reading += r.targetLogId
    case r: ReplicationReadSuccess =>
      reading -= r.targetLogId
    case r: ReplicationReadFailure =>
      reading -= r.targetLogId
    case w: ReplicationWrite =>
      registry.get(w.sourceLogId) match {
        case Some(reg) => registry += (w.sourceLogId -> reg.copy(currentTargetVectorTime = w.currentSourceVectorTime))
        case None      =>
      }
  }
}
