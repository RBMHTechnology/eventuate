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
import scala.collection.immutable.TreeMap
import scala.util.{ Failure, Success, Try }

/**
 * Operation-based Replicated Growable Array CRDT.
 *
 * @tparam A Entry value type.
 *
 * @see [[http://hal.upmc.fr/docs/00/55/55/88/PDF/techreport.pdf A comprehensive study of Convergent and Commutative Replicated Data Types]], specification 19
 */
case class RGArray[A](vertexTree: TreeMap[Position, Vertex[A]] = TreeMap[Position, Vertex[A]]((Position.head, Vertex.head[A]))) extends CRDTFormat {

  /**
   * Returns raw iterator of all inserted vertices.
   *
   * @return
   */
  def vertices: Iterator[Vertex[A]] = vertexTree(Position.head).next.iterator

  /**
   * A materialized linear indexed sequence of elements represented by the current RGArray.
   *
   * @return
   */
  def value: Vector[A] = vertices.filter(_.deletionTime.isEmpty).map(_.value).toVector

  /**
   * Return a non-deleted Vertex at the given index number. Index of the vertex
   * is related to a [[RGArray[A].value]] collection generated from the current RGArray.
   *
   * @param index
   * @return
   */
  def vertexAt(index: Int): Vertex[A] = vertices.filter(_.deletionTime.nonEmpty).drop(index).next()

  /**
   * Returns a new RGArray with an element inserted at the beginning of an existing RGArray.
   *
   * @param element value to insert into RGArray.
   * @param pos a new unique position identifier given to a provided `element`.
   * @return A new updated instance of RGArray.
   */
  def insertRight(element: A, pos: Position): RGArray[A] = insertRight(Position.head, element, pos)

  /**
   * Returns a new RGArray with an element inserted on the right of the provided position.
   *
   * @param after position after which an `element` will be inserted.
   * @param element value to insert into RGArray.
   * @param pos a new unique position identifier given to a provided `element`.
   * @return A new updated instance of RGArray.
   */
  def insertRight(after: Position, element: A, pos: Position): RGArray[A] = {
    val prev = vertexTree(after)
    updateTree(element, pos, prev)
  }

  @tailrec
  private def updateTree(elem: A, pos: Position, prev: Vertex[A]): RGArray[A] = {
    prev.next match {
      case Some(vertex) if vertex.pos > pos =>
        updateTree(elem, pos, vertex)
      case Some(vertex) =>
        val curr = Vertex(elem, pos, next = Some(vertex))
        val updated = prev.copy(next = Some(curr))
        RGArray[A](vertexTree = vertexTree.updated(updated.pos, updated).updated(pos, curr))
      case None =>
        val curr = Vertex(elem, pos)
        val updated = prev.copy(next = Some(curr))
        RGArray[A](vertexTree = vertexTree.updated(updated.pos, updated).updated(pos, curr))
    }
  }

  /**
   * Returns a new RGArray with vertex identified by provided `index` marked as tombstoned at provided `time`.
   *
   * @param pos An absolute position of an element to remove.
   * @param time A timestamp at which removal has happened.
   */
  def delete(pos: Position, time: VectorTime) = {
    val vertex = this.vertexTree(pos).copy(deletionTime = Some(time))
    this.copy(vertexTree = this.vertexTree.updated(pos, vertex))
  }

  /**
   * Prunes all removed values, that have been tombstoned in `timestamp`s causal past.
   *
   * @param timestamp Last stable timestamp, reached by all corresponding replicas.
   * @return A new RGArray without tombstoned values, that had happened before `timestamp`.
   */
  def prune(timestamp: VectorTime) = {
    val pruned = vertexTree.filter({
      case (pos, vertex) => {
        vertex.deletionTime match {
          case Some(time) if time < timestamp => false
          case _                              => true
        }
      }
    })
    this.copy(vertexTree = pruned)
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
  val head: Position = Position(-1, "")
}

/**
 * Bucket for values stored inside an [[RGArray]].
 * @param value Assigned value.
 * @param pos Position of that value in relation to others within [[RGArray]].
 * @param next Next vertex in a linear sequence.
 * @param deletionTime Determines a local causal time, at which current value has been removed.
 * @tparam A Type of a stored value.
 */
case class Vertex[A](value: A, pos: Position, next: Option[Vertex[A]] = None, deletionTime: Option[VectorTime] = None) extends Iterable[Vertex[A]] {

  case class VertexIterator(var current: Option[Vertex[A]]) extends Iterator[Vertex[A]] {
    override def hasNext: Boolean = current.nonEmpty
    override def next(): Vertex[A] = current match {
      case Some(vertex) =>
        current = vertex.next
        vertex
      case None => throw new NoSuchElementException("Next on empty iterator")
    }
  }

  override def iterator: Iterator[Vertex[A]] = VertexIterator(Some(this))
}

object Vertex {
  def head[A]: Vertex[A] = Vertex[A](null.asInstanceOf[A], Position.head)
}
