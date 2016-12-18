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

package com.rbmhtechnology.eventuate.log

import akka.testkit.TestKitBase
import com.rbmhtechnology.eventuate.EventsourcingProtocol.Write
import com.rbmhtechnology.eventuate.EventsourcingProtocol.WriteSuccess
import com.rbmhtechnology.eventuate.EventsourcingProtocol.Written
import com.rbmhtechnology.eventuate.ReplicationProtocol.ReplicationMetadata
import com.rbmhtechnology.eventuate.ReplicationProtocol.ReplicationWrite
import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.VectorTime

trait EventLogSystemTimeSpec extends TestKitBase with EventLogSpecSupport {

  import EventLogSpec._

  val UndefinedTimestamp = 0L
  val SystemTimestamp = 123L

  override def currentSystemTime: Long = SystemTimestamp

  "An event log" must {
    "update an event's system timestamp" in {
      log ! Write(List(event("a").copy(systemTimestamp = 3L)), system.deadLetters, replyToProbe.ref, 0, 0)
      replyToProbe.expectMsgType[WriteSuccess].events.head.systemTimestamp should be(SystemTimestamp)
    }
    "update a replicated events system timestamp if not defined (systemTimestamp == 0L, cf. #371)" in {
      val evt = DurableEvent("a", emitterIdA, processId = logId, vectorTimestamp = VectorTime(remoteLogId -> 1L))
      val collaborator = registerCollaborator()
      log ! ReplicationWrite(List(evt), Map(remoteLogId -> ReplicationMetadata(5, VectorTime.Zero)))
      collaborator.expectMsgType[Written].event.systemTimestamp should not be UndefinedTimestamp
    }
    "not update a replicated events system timestamp if defined (systemTimestamp != 0L, cf. #371)" in {
      val evt = DurableEvent("a", emitterIdA, processId = logId, vectorTimestamp = VectorTime(remoteLogId -> 1L), systemTimestamp = SystemTimestamp)
      val collaborator = registerCollaborator()
      log ! ReplicationWrite(List(evt), Map(remoteLogId -> ReplicationMetadata(5, VectorTime.Zero)))
      collaborator.expectMsgType[Written].event.systemTimestamp should be(SystemTimestamp)
    }
  }
}
