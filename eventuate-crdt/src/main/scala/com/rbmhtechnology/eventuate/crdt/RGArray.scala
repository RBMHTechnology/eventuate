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
 * @see [[http://hal.upmc.fr/docs/00/55/55/88/PDF/techreport.pdf A comprehensive study of Convergent and Commutative Replicated Data Types]], specification 19
 */
case class RGArray[A](vertices: Vector[Vertex[A]] = Vector.empty[Vertex[A]]) extends CRDTFormat {
  def value: Vector[A] = vertices.filter(_.deletionTime.isEmpty).map(_.value)

  def vertexAt(index: Int): Vertex[A] = vertices.iterator.filter(_.deletionTime.isEmpty).drop(index).next

  def insertRight(value: A, newPos: Position): RGArray[A] = insertRight(Position.head, value, newPos)

  def insertRight(after: Position, value: A, pos: Position): RGArray[A] = {
    val newVertex = Vertex(value, pos)
    if (vertices.isEmpty) {
      RGArray(vertices :+ newVertex)
    } else {
      val found = if (after == Position.head) 0 else vertices.indexWhere((v: Vertex[A]) => v.pos == after) + 1
      val index: Int = skipOver(pos, found)
      RGArray((vertices.take(index) :+ newVertex) ++: vertices.drop(index))
    }
  }

  /**
   * Returns a new RGArray with vertex identified by provided `index` marked as tombstoned at provided `time`.
   *
   * @param pos An absolute position of an element to remove.
   * @param time A timestamp at which removal has happened.
   */
  def delete(pos: Position, time: VectorTime) = {
    this.copy(vertices = vertices.map((v: Vertex[A]) => if (v.pos == pos) v.copy(deletionTime = Some(time)) else v))
  }

  /**
   * Prunes all removed values, that have been tombstoned in `timestamp`s causual past.
   *
   * @param timestamp Last stable timestamp, reached by all corresponding replicas.
   * @return A new RGArray without tombstoned values, that had happened before `timestamp`.
   */
  def prune(timestamp: VectorTime) = {
    val pruned = vertices.filter(x => {
      x.deletionTime match {
        case Some(time) if time < timestamp => false
        case _                              => true
      }
    })
    this.copy(vertices = pruned)
  }

  @tailrec
  private def skipOver(pos: Position, index: Int): Int = {
    if (index < vertices.length && vertices(index).pos > pos) skipOver(pos, index + 1) else index
  }
}

object RGArray {

  def apply[A]: RGArray[A] =
    new RGArray[A]()

  implicit def RGArrayServiceOps[A] = new CRDTServiceOps[RGArray[A], Vector[A]] {
    override def zero: RGArray[A] = RGArray.apply[A]

    override def value(crdt: RGArray[A]): Vector[A] = crdt.value

    override def prepare(crdt: RGArray[A], operation: Any): Try[Option[Any]] = operation match {
      case InsertOp(pos, value) => Success {
        Some(InsertOp(pos, value))
      }
      case op =>
        super.prepare(crdt, op)
    }

    override def effect(crdt: RGArray[A], operation: Any, event: DurableEvent): RGArray[A] = operation match {
      case InsertOp(after, value) => crdt.insertRight(after, value.asInstanceOf[A], Position(event.localSequenceNr, event.emitterId))
      case DeleteOp(pos)          => crdt.delete(pos, event.vectorTimestamp)
    }
  }
}

class RGArrayService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTServiceOps[RGArray[A], Vector[A]])
  extends CRDTService[RGArray[A], Vector[A]] {

  /**
   * Adds `value` to the RGA on the right side of the provided position and returns an
   * updated entry set.
   */
  def insertRight(id: String, after: Position, value: A): Future[Vector[A]] = {
    op(id, InsertOp(after, value))
  }

  /**
   * Removes `entry` from the RGA identified by `pos` and returns the updated entry set.
   */
  def delete(id: String, pos: Position): Future[Vector[A]] = op(id, DeleteOp(pos))

  start()
}

case class PrepareInsert(afterIndex: Int, value: Any)
case class PrepareDelete(atIndex: Int)

/**
 * Persistent insert operation used for [[RGArray]].
 */
case class InsertOp(after: Position, value: Any) extends CRDTFormat

/**
 * Persistent delete operation used for [[RGArray]].
 */
case class DeleteOp(pos: Position) extends CRDTFormat

/**
 * Position of a particular element in relation to others within provided [[RGArray]].
 * @param seqNr Monotonically increasing order incremented with each insert.
 * @param emitterId Identifier of a particular replica making an update.
 */
case class Position(seqNr: Long, emitterId: String) extends Ordered[Position] {
  override def compare(that: Position): Int = seqNr compareTo that.seqNr match {
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
 * @param deletionTime Determines a local causual time, at which current value has been removed.
 * @tparam A Type of a stored value.
 */
case class Vertex[A](value: A, pos: Position, deletionTime: Option[VectorTime] = None)
