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

package com.rbmhtechnology.eventuate

import scala.annotation.tailrec
import scalaz._
import Scalaz._

/**
 * Vector time, represented as ''process id'' -> ''logical time'' map.
 */
case class VectorTime(value: Map[String, Long] = Map.empty) {
  import VectorTime._

  /**
   * Sets the local time of `processId`.
   */
  def setLocalTime(processId: String, localTime: Long): VectorTime =
    copy(value.updated(processId, localTime))

  /**
   * Returns the local time of `processId`.
   */
  def localTime(processId: String): Long =
    value.getOrElse(processId, 0L)

  /**
   * Returns the local time of `processId` represented as vector time.
   */
  def localCopy(processId: String): VectorTime = value.get(processId) match {
    case Some(t) => VectorTime(processId -> t)
    case None    => VectorTime.Zero
  }

  /**
   * Increments local time of given `processId` by `count`.
   */
  def increment(processId: String): VectorTime = value.get(processId) match {
    case Some(v) => copy(value + (processId -> (v + 1L)))
    case None    => copy(value + (processId -> 1L))
  }

  /**
   * Merges this vector time with `that` by taking the max of
   * the corresponding local times.
   */
  def merge(that: VectorTime): VectorTime =
    copy(value.unionWith(that.value)(math.max))

  /**
   * Returns `true` if this vector time is equivalent (equal) to `that`.
   */
  def equiv(that: VectorTime): Boolean =
    Ordering.equiv(this, that)

  /**
   * Returns `true` if this vector time is concurrent to `that`.
   */
  def conc(that: VectorTime): Boolean =
    Ordering.tryCompare(this, that).isEmpty

  /**
   * Returns `true` if this vector time is concurrent to `that`.
   */
  def <->(that: VectorTime): Boolean =
    conc(that)

  /**
   * Returns `true` if this vector time is less than or equal to `that`.
   */
  def <=(that: VectorTime): Boolean =
    Ordering.lteq(this, that)

  /**
   * Returns `true` if this vector time is greater than or equal to `that`.
   */
  def >=(that: VectorTime): Boolean =
    Ordering.gteq(this, that)

  /**
   * Returns `true` if this vector time is less than `that` (= this happened before `that`).
   */
  def <(that: VectorTime): Boolean =
    Ordering.lt(this, that)

  /**
   * Returns `true` if this vector time is greater than `that` (= `that` happened before this).
   */
  def >(that: VectorTime): Boolean =
    Ordering.gt(this, that)

  /**
   * Java API
   *
   * Returns `true` if this vector time is less than or equal to `that`.
   */
  def lte(that: VectorTime): Boolean =
    <=(that)

  /**
   * Java API
   *
   * Returns `true` if this vector time is greater than or equal to `that`.
   */
  def gte(that: VectorTime): Boolean =
    >=(that)

  /**
   * Java API
   *
   * Returns `true` if this vector time is less than `that` (= this happened before `that`).
   */
  def lt(that: VectorTime): Boolean =
    <(that)

  /**
   * Java API
   *
   * Returns `true` if this vector time is greater than `that` (= `that` happened before this).
   */
  def gt(that: VectorTime): Boolean =
    >(that)

  override def toString: String =
    s"VectorTime(${value.mkString(",")})"
}

object VectorTime {
  val Zero: VectorTime =
    VectorTime()

  def apply(entries: (String, Long)*): VectorTime =
    VectorTime(Map(entries: _*))

  val Ordering = new PartialOrdering[VectorTime] {
    def lteq(x: VectorTime, y: VectorTime): Boolean = {
      tryCompare(x, y) match {
        case None             => false
        case Some(r) if r > 0 => false
        case other            => true
      }
    }

    def tryCompare(x: VectorTime, y: VectorTime): Option[Int] = {
      val xValue = x.value.withDefaultValue(0L)
      val yValue = y.value.withDefaultValue(0L)

      @tailrec
      def go(keys: List[String], current: Long): Option[Long] = keys match {
        case Nil => Some(current)
        case k :: ks =>
          val s = math.signum(xValue(k) - yValue(k))

          if (current == 0)
            go(ks, s)
          else if (current == -1)
            if (s == +1) None else go(ks, current)
          else // current == +1
          if (s == -1) None else go(ks, current)
      }

      go(xValue.keySet.union(yValue.keySet).toList, 0).map(_.toInt)
    }
  }
}
