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

object ReplicationFilter {
  /**
   * Marker trait for protobuf-serializable replication filters.
   */
  trait Format extends Serializable
}

/**
 * Serializable and composable replication filter.
 */
trait ReplicationFilter extends Serializable {
  /**
   * Evaluates this filter on the given `event`.
   */
  def apply(event: DurableEvent): Boolean

  /**
   * Returns a composed replication filter that represents a logical AND of
   * this filter and the given `filter`.
   */
  def and(filter: ReplicationFilter): ReplicationFilter = this match {
    case f @ AndFilter(filters) => f.copy(filter +: filters)
    case _ => AndFilter(Seq(filter, this))
  }

  /**
   * Returns a composed replication filter that represents a logical OR of
   * this filter and the given `filter`.
   */
  def or(filter: ReplicationFilter): ReplicationFilter = this match {
    case f @ OrFilter(filters) => f.copy(filter +: filters)
    case _ => OrFilter(Seq(filter, this))
  }
}

/**
 * Serializable logical AND of given `filters`.
 */
private[eventuate] case class AndFilter(filters: Seq[ReplicationFilter]) extends ReplicationFilter with ReplicationFilter.Format {
  /**
   * Evaluates to `true` if all `filters` evaluate to `true`, `false` otherwise.
   */
  def apply(event: DurableEvent): Boolean = {
    @annotation.tailrec
    def go(filters: Seq[ReplicationFilter]): Boolean = filters match {
      case Nil => true
      case f +: fs => if (f(event)) go(fs) else false
    }
    go(filters)
  }
}

/**
 * Serializable logical OR of given `filters`.
 */
private[eventuate] case class OrFilter(filters: Seq[ReplicationFilter]) extends ReplicationFilter with ReplicationFilter.Format {
  /**
   * Evaluates to `true` if any of `filters` evaluate to `true`, `false` otherwise.
   */
  def apply(event: DurableEvent): Boolean = {
    @annotation.tailrec
    def go(filters: Seq[ReplicationFilter]): Boolean = filters match {
      case Nil => false
      case f +: fs => if (f(event)) true else go(fs)
    }
    go(filters)
  }
}

/**
 * Default replication filter.
 */
private[eventuate] case class SourceLogIdExclusionFilter(sourceLogId: String) extends ReplicationFilter with ReplicationFilter.Format {
  /**
   * Evaluates to `true` if `event.sourceLogId` does not equal `sourceLogId`.
   */
  def apply(event: DurableEvent): Boolean =
    event.sourceLogId != sourceLogId
}

/**
 * Replication filter that evaluates to `true` for all events.
 */
private[eventuate] object NoFilter extends ReplicationFilter {
  /**
   * Evaluates to `true`.
   */
  def apply(event: DurableEvent): Boolean = true
}

/**
 * Replication filter that evaluates to `true` for non-replicated events.
 */
private[eventuate] case object NonReplicatedFilter extends ReplicationFilter {
  def apply(event: DurableEvent): Boolean =
    !event.replicated
}