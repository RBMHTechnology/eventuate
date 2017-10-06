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

package com.rbmhtechnology.eventuate.crdt

import akka.actor._
import com.rbmhtechnology.eventuate._

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.collection.immutable.Vector
import scala.util.{ Failure, Success, Try }

/**
 * Operation-based Replicated Growable Array CRDT.
 *
 * @tparam A Entry value type.
 *
 * @see [[http://hal.upmc.fr/docs/00/55/55/88/PDF/techreport.pdf A comprehensive study of Convergent and Commutative Replicated Data Types]], specification 15
 */
case class RGArray[A](vertexes: Vector[Vertex[A]] = Vector.empty[Vertex[A]]) extends CRDTFormat {
  def value: Vector[A] = vertexes.filterNot(_.isTombstoned).map(_.value)

  def insertRight(value: A, newPos: Position): RGArray[A] = insertRight(Position.head, value, newPos)

  def insertRight(index: Position, value: A, newPos: Position): RGArray[A] = {
    val newVertex = Vertex(value, newPos)
    if (vertexes.isEmpty) {
      RGArray(vertexes :+ newVertex)
    } else {
      val found = if (index == Position.head) 0 else vertexes.indexWhere((v: Vertex[A]) => v.pos == index) + 1
      val idx: Int = skipOver(newPos, found)
      RGArray((vertexes.take(idx) :+ newVertex) ++: vertexes.drop(idx))
    }
  }

  def delete(index: Position) = {
    this.copy(vertexes = vertexes.map((v: Vertex[A]) => if (v.pos == index) v.copy(isTombstoned = true) else v))
  }

  def removeTombstones() = this.copy(vertexes = vertexes.filterNot(_.isTombstoned))

  @tailrec
  private def skipOver(pos: Position, idx: Int): Int = {
    if (idx < vertexes.length && vertexes(idx).pos > pos) skipOver(pos, idx + 1) else idx
  }
}

object RGArray {

  def apply[A]: RGArray[A] =
    new RGArray[A]()

  implicit def RGArrayServiceOps[A] = new CRDTServiceOps[RGArray[A], Vector[A]] {
    override def zero: RGArray[A] = RGArray.apply[A]

    override def value(crdt: RGArray[A]): Vector[A] = crdt.value

    override def prepare(crdt: RGArray[A], operation: Any): Try[Option[Any]] = operation match {
      case InsertOp(pos, value, None) => Success {
        Some(InsertOp(pos, value, Some(crdt.vertexes.length + 1)))
      }
      case op =>
        super.prepare(crdt, op)
    }

    override def effect(crdt: RGArray[A], operation: Any, event: DurableEvent): RGArray[A] = operation match {
      case InsertOp(after, value, Some(pos)) => crdt.insertRight(after, value.asInstanceOf[A], Position(pos, event.emitterId))
      case DeleteOp(pos)                     => crdt.delete(pos)
    }
  }
}

class RGArrayService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTServiceOps[RGArray[A], Vector[A]])
  extends CRDTService[RGArray[A], Vector[A]] {

  /**
   * Adds `entry` to the RGA identified by `id` on the right side of the provided
   * position and returns an updated entry set.
   */
  def insertRight(id: String, index: Position, value: A): Future[Vector[A]] = {
    op(id, InsertOp(index, value))
  }

  /**
   * Removes `entry` from the RGA identified by `id` and returns the updated entry set.
   */
  def delete(id: String, pos: Position): Future[Vector[A]] = op(id, DeleteOp(pos))

  start()
}

/**
 * Persistent insert operation used for [[RGArray]].
 */
case class InsertOp(after: Position, value: Any, pos: Option[Int] = None) extends CRDTFormat

/**
 * Persistent delete operation used for [[RGArray]].
 */
case class DeleteOp(pos: Position) extends CRDTFormat

/**
 * Position of a particular element in relation to others within provided [[RGArray]].
 * @param order Monotonically increasing order incremented with each insert.
 * @param emitterId Identifier of a particular replica making an update.
 */
case class Position(order: Int, emitterId: String) extends Ordered[Position] {
  override def compare(that: Position): Int = order compareTo that.order match {
    case 0 => emitterId compareTo that.emitterId
    case n => n
  }
}

object Position {
  val head: Position = Position(0, "")
}

/**
 * Bucket for values stored inside an [[RGArray]].
 * @param value Assigned value.
 * @param pos Position of that value in relation to others within [[RGArray]].
 * @param isTombstoned Has a value been removed.
 * @tparam A Type of a stored value.
 */
case class Vertex[A](value: A, pos: Position, isTombstoned: Boolean = false)
