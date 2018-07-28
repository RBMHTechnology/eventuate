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

package com.rbmhtechnology.eventuate.crdt.pure

import com.rbmhtechnology.eventuate.VectorTime

object StabilityProtocol {

  case class StabilityConf(localPartition: String, partitions: Set[String])

  object RTM {
    def apply(conf: StabilityConf): RTM = RTM(conf.localPartition, (conf.partitions - conf.localPartition).map(_ -> VectorTime.Zero).toMap)
  }

  case class RTM(private val localPartition: String, private val timestamps: Map[String, VectorTime]) {

    def update(partition: String, timestamp: VectorTime): RTM = Option(partition).filterNot(_ equals localPartition).flatMap(timestamps.get) match {
      case Some(oldTimestamp) => copy(timestamps = timestamps + (partition -> oldTimestamp.merge(timestamp)))
      case _                  => this
    }

    def stable(): Option[TCStable] = {
      val processIds = timestamps.values.flatMap(_.value.keys).toSet
      Option(TCStable(VectorTime(processIds.map(processId => (processId, timestamps.values.map(_.localTime(processId)).reduce[Long](Math.min))).toMap))).filterNot(_.isZero)
    }
  }

  case class TCStable(private val stableVector: VectorTime) {
    def isStable(that: VectorTime): Boolean = that <= stableVector

    def isZero: Boolean = stableVector.value.forall(_._2 equals 0L)

    def equiv(that: TCStable): Boolean = stableVector.equiv(that.stableVector)
  }

}
