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

import akka.stream.scaladsl.Source
import com.rbmhtechnology.eventuate.ReplicationProtocol.SetReplicationProgress
import com.rbmhtechnology.eventuate.adapter.stream.DurableEventSource
//# progress-source
import com.rbmhtechnology.eventuate.adapter.stream.ProgressSource

//#

object ProgressSourceExample extends App with DurableEventLogs {
  logC ! SetReplicationProgress(logAId, 17)
  logC ! SetReplicationProgress(logBId, 22)

  Thread.sleep(1000)

  //# progress-source
  val progressSourceA = Source.fromGraph(ProgressSource(logAId, logC))
  val progressSourceB = Source.fromGraph(ProgressSource(logBId, logC))

  val sourceA = progressSourceA.flatMapConcat { progress =>
    Source.fromGraph(DurableEventSource(logA, fromSequenceNr = progress))
  }

  val sourceB = progressSourceB.flatMapConcat { progress =>
    Source.fromGraph(DurableEventSource(logB, fromSequenceNr = progress))
  }
  //#
}
