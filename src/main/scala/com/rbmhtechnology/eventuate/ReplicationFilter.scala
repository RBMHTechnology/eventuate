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

package com.rbmhtechnology.eventuate

import scala.collection.immutable.Seq

/**
 * Serializable and composable replication filter.
 */
trait ReplicationFilter extends Serializable {
  def apply(event: DurableEvent): Boolean

  def compose(filter: ReplicationFilter): ReplicationFilter = this match {
    case f @ CompositeFilter(filters) => f.copy(filter +: filters)
    case _ => CompositeFilter(Seq(filter, this))
  }
}

case class CompositeFilter(filters: Seq[ReplicationFilter]) extends ReplicationFilter {
  def apply(event: DurableEvent): Boolean = {
    @annotation.tailrec
    def go(filters: Seq[ReplicationFilter]): Boolean = filters match {
      case Nil => true
      case f +: fs => if (f(event)) go(fs) else false
    }
    go(filters)
  }
}

case class SourceLogIdExclusionFilter(sourceLogId: String) extends ReplicationFilter {
  def apply(event: DurableEvent): Boolean =
    event.sourceLogId != sourceLogId
}
