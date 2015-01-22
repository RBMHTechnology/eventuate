/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
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