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

sealed abstract class VectorTime {
  import VectorTime._

  def localTimes: Map[String, Long]
  def processIds: Set[String]
  def merge(that: VectorTime): DefaultVectorTime

  def hasDot(processId: String): Boolean
  def dotOption(processId: String): Option[Dot]
  def dotted(processId: String, localTime: Long, localTimeGap: Long = 0L): DottedVectorTime

  /**
   * Returns `true` if this vector time is equivalent to `that`.
   */
  def equiv(that: VectorTime): Boolean =
    Ordering.equiv(this, that)

  /**
   * Returns `true` if this vector time is concurrent to `that`.
   */
  def conc(that: VectorTime): Boolean =
    Ordering.tryCompare(this, that).isEmpty

  /**
   * Returns `true` if this vector time is less than or equivalent to `that`.
   */
  def <=(that: VectorTime): Boolean =
    Ordering.lteq(this, that)

  /**
   * Returns `true` if this vector time is greater than or equivalent to `that`.
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
   * Returns `true` if this vector time is less than or equivalent to `that`.
   */
  def lte(that: VectorTime): Boolean =
    <=(that)

  /**
   * Java API
   *
   * Returns `true` if this vector time is greater than or equivalent to `that`.
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
}

object VectorTime {
  val Zero: DefaultVectorTime =
    DefaultVectorTime()

  def apply(entries: (String, Long)*): DefaultVectorTime =
    DefaultVectorTime(Map(entries: _*))

  val Ordering = new PartialOrdering[VectorTime] {
    def lteq(x: VectorTime, y: VectorTime): Boolean = {
      tryCompare(x, y) match {
        case None             => false
        case Some(r) if r > 0 => false
        case other            => true
      }
    }

    def tryCompare(x: VectorTime, y: VectorTime): Option[Int] = {
      val xlts = x.localTimes.withDefaultValue(0L)
      val ylts = y.localTimes.withDefaultValue(0L)

      @tailrec
      def go(keys: List[String], current: Long): Option[Long] = {
        keys match {
          case Nil =>
            Some(current)
          case k :: ks =>
            tryCompareLocalTime(xlts(k), x.dotOption(k), ylts(k), y.dotOption(k)) match {
              case None =>
                None
              case Some(s) =>
                if (current == 0)
                  go(ks, s)
                else if (current == -1)
                  if (s == +1) None else go(ks, current)
                else // current == +1
                if (s == -1) None else go(ks, current)
            }
        }
      }
      go(x.processIds.union(y.processIds).toList, 0).map(_.toInt)
    }
  }

  def tryCompareLocalTime(xlt: Long, xdo: Option[Dot], ylt: Long, ydo: Option[Dot]): Option[Long] = (xdo, ydo) match {
    case (None, None) =>
      Some(math.signum(xlt - ylt))
    case (None, Some(yd)) =>
      if (xlt <= ylt)
        Some(-1L)
      else if (yd.localTime == xlt && !yd.localTimeGap)
        Some(0L)
      else if (yd.localTime <= xlt)
        Some(+1L)
      else None
    case (Some(_), None) =>
      tryCompareLocalTime(ylt, ydo, xlt, xdo).map(_ * -1L)
    case (Some(xd), Some(yd)) =>
      if (xd.localTime == yd.localTime)
        Some(math.signum(xlt - ylt))
      else if (xd.localTime <= ylt)
        Some(-1L)
      else if (yd.localTime <= xlt)
        Some(+1L)
      else None
  }
}

case class DefaultVectorTime(localTimes: Map[String, Long] = Map.empty) extends VectorTime {
  def processIds: Set[String] =
    localTimes.keySet

  def merge(that: VectorTime): DefaultVectorTime = that match {
    case _: DefaultVectorTime => copy(localTimes.unionWith(that.localTimes)(math.max))
    case t: DottedVectorTime  => merge(t.mergeDot)
  }

  def hasDot(processId: String): Boolean =
    false

  def dotOption(processId: String): Option[Dot] =
    None

  def dotted(processId: String, localTime: Long, localTimeGap: Long = 0L): DottedVectorTime =
    withDot(processId, localTime, localTimes.getOrElse(processId, 0L) != localTime - 1L - localTimeGap)

  private def withDot(processId: String, localTime: Long, localTimeGap: Boolean): DottedVectorTime =
    DottedVectorTime(localTimes, Dot(processId, localTime, localTimeGap))
}

case class DottedVectorTime(localTimes: Map[String, Long] = Map.empty, dot: Dot) extends VectorTime {
  val localTime = localTimes.getOrElse(dot.processId, 0L)

  if (dot.localTime <= localTime)
    throw new IllegalArgumentException(s"local time of dot must be > $localTime but was ${dot.localTime}")

  def processIds: Set[String] =
    localTimes.keySet + dot.processId

  def mergeDot: DefaultVectorTime =
    DefaultVectorTime(localTimes.updated(dot.processId, dot.localTime))

  def merge(that: VectorTime): DefaultVectorTime = that match {
    case t: DefaultVectorTime => t.merge(this)
    case t: DottedVectorTime  => t.mergeDot.merge(this)
  }

  def hasDot(processId: String): Boolean =
    dot.processId == processId

  def dotOption(processId: String): Option[Dot] =
    if (hasDot(processId)) Some(dot) else None

  def dotted(processId: String, localTime: Long, localTimeGap: Long = 0L): DottedVectorTime =
    throw new UnsupportedOperationException("Only one dot per vector time os possible")
}

case class Dot(processId: String, localTime: Long, localTimeGap: Boolean)