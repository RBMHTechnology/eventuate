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

import java.util.function.BiFunction
import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

/**
 * A versioned value.
 *
 * @param value The value.
 * @param version The version vector of the value.
 * @param processId Id of the process that emitted the corresponding
 *                  event that caused this version.
 */
case class Versioned[A](value: A, version: VectorTime, processId: String = "")

/**
 * Tracks concurrent [[Versioned]] values which arise from concurrent updates.
 *
 * @tparam A Versioned value type
 * @tparam B Update type
 */
trait ConcurrentVersions[A, B] {
  /**
   * Updates that [[Versioned]] value with `b` that is a predecessor of `eventTimestamp`. If
   * there is no such predecessor, a new concurrent version is created (optionally derived
   * from an older entry in the version history, in case of incremental updates).
   */
  def update(b: B, eventTimestamp: VectorTime, eventProcessId: String = ""): ConcurrentVersions[A, B]

  /**
   * Resolves multiple concurrent versions to a single version. For the resolution to be
   * successful, one of the concurrent versions must have a version vector that is equal to
   * `selectedTimestamp`. Only those concurrent versions with a version vector less than the
   * `eventTimestamp` participate in the resolution process (which allows for resolutions to
   * be concurrent to other updates).
   */
  def resolve(selectedTimestamp: VectorTime, eventTimestamp: VectorTime): ConcurrentVersions[A, B]

  /**
   * Experimental ...
   */
  def resolve(selectedTimestamp: VectorTime): ConcurrentVersions[A, B] =
    resolve(selectedTimestamp, all.map(_.version).foldLeft(VectorTime())(_.merge(_)))

  /**
   * Returns all (un-resolved) concurrent versions.
   */
  def all: Seq[Versioned[A]]

  /**
   * Java API.
   *
   * Returns all (un-resolved) concurrent versions.
   */
  def getAll: JList[Versioned[A]] = all.asJava

  /**
   * Returns `true` if there is more than one version available i.e. if there are multiple
   * concurrent (= conflicting) versions.
   */
  def conflict: Boolean = all.length > 1

  /**
   * Owner of versioned values.
   */
  def owner: String

  /**
   * Updates the owner.
   */
  def withOwner(owner: String): ConcurrentVersions[A, B]
}

object ConcurrentVersions {
  /**
   * Creates a new [[ConcurrentVersionsTree]] that uses projection function `f` to compute
   * new (potentially concurrent) versions from a parent version.
   *
   * @param initial Value of the initial version.
   * @param f Projection function for updates.
   * @tparam A Versioned value type
   * @tparam B Update type
   */
  def apply[A, B](initial: A, f: (A, B) => A): ConcurrentVersions[A, B] =
    ConcurrentVersionsTree[A, B](initial, f)
}

/**
 * A [[ConcurrentVersions]] implementation that shall be used if updates replace current
 * versioned values (= full updates). `ConcurrentVersionsList` is an immutable data structure.
 */
class ConcurrentVersionsList[A](vs: List[Versioned[A]], val owner: String = "") extends ConcurrentVersions[A, A] {
  def update(na: A, eventTimestamp: VectorTime, eventProcessId: String = ""): ConcurrentVersionsList[A] = {
    val r = vs.foldRight[(List[Versioned[A]], Boolean)]((Nil, false)) {
      case (a, (acc, true))  => (a :: acc, true)
      case (a, (acc, false)) =>
        if (eventTimestamp > a.version)
          // regular update on that version
          (Versioned(na, eventTimestamp, eventProcessId) :: acc, true)
        else if (eventTimestamp < a.version)
          // conflict already resolved, ignore
          (a :: acc, true)
        else
          // conflicting update, try next
          (a :: acc, false)
    }
    r match {
      case (updates, true)   => new ConcurrentVersionsList(updates, owner)
      case (original, false) => new ConcurrentVersionsList((Versioned(na, eventTimestamp, eventProcessId) :: original), owner)
    }
  }

  def resolve(selectedTimestamp: VectorTime, eventTimestamp: VectorTime): ConcurrentVersionsList[A] = {
    new ConcurrentVersionsList(vs.foldRight(List.empty[Versioned[A]]) {
      case (v, acc) if v.version == selectedTimestamp => v.copy(version = eventTimestamp) :: acc
      case (v, acc) if v.version.conc(eventTimestamp) => v :: acc
      case (v, acc)                                   => acc
    })
  }

  def all: List[Versioned[A]] = vs.reverse

  def withOwner(owner: String) = new ConcurrentVersionsList(vs, owner)
}

case object ConcurrentVersionsList {
  /**
   * Creates an empty [[ConcurrentVersionsList]].
   */
  def apply[A]: ConcurrentVersionsList[A] =
    new ConcurrentVersionsList(Nil)

  /**
   * Creates a new [[ConcurrentVersionsList]] with a single [[Versioned]] value from `a` and `timestamp`.
   */
  def apply[A](a: A, timestamp: VectorTime): ConcurrentVersionsList[A] =
    new ConcurrentVersionsList(List(Versioned(a, timestamp, "")))
}

/**
 * A [[ConcurrentVersions]] implementation that shall be used if updates modify
 * current versioned values (= incremental updates). `ConcurrentVersionsTree` is a
 * mutable data structure.
 *
 * '''Please note:''' This implementation is not optimized and leaks memory at the
 * moment. Also, future versions of `ConcurrentVersionsTree` will likely be based
 * on immutable data structures.
 *
 * @param f Projection function for updates.
 */
class ConcurrentVersionsTree[A, B](f: (A, B) => A, initial: A = null.asInstanceOf[A] /* FIXME */) extends ConcurrentVersions[A, B] {
  import ConcurrentVersionsTree._

  private var _owner: String = ""

  private val root: Node[A] =
    new Node(Versioned(initial, VectorTime(), ""))

  override def update(b: B, eventTimestamp: VectorTime, eventProcessId: String = ""): ConcurrentVersionsTree[A, B] = {
    val p = pred(eventTimestamp)
    p.addChild(new Node(Versioned(f(p.versioned.value, b), eventTimestamp, eventProcessId)))
    this
  }

  override def resolve(selectedTimestamp: VectorTime, eventTimestamp: VectorTime): ConcurrentVersionsTree[A, B] = {
    leaves.foreach {
      case n if n.rejected                               => // ignore rejected leaf
      case n if n.versioned.version.conc(eventTimestamp) => // ignore concurrent update
      case n if n.versioned.version == selectedTimestamp => n.stamp(eventTimestamp)
      case n                                             => n.reject()
    }
    this
  }

  override def all: Seq[Versioned[A]] =
    leaves.filterNot(_.rejected).map(_.versioned)

  override def owner: String =
    _owner

  override def withOwner(owner: String): ConcurrentVersions[A, B] = {
    _owner = owner
    this
  }

  private[eventuate] def nodes: Seq[Node[A]] = foldLeft(root, Vector.empty[Node[A]]) {
    case (acc, n) => acc :+ n
  }

  private[eventuate] def leaves: Seq[Node[A]] = foldLeft(root, Vector.empty[Node[A]]) {
    case (leaves, n) => if (n.leaf) leaves :+ n else leaves
  }

  private[eventuate] def pred(timestamp: VectorTime): Node[A] = foldLeft(root, root) {
    case (candidate, n) => if (timestamp > n.versioned.version && n.versioned.version > candidate.versioned.version) n else candidate
  }

  // TODO: make foldLeft tail recursive or create a trampolined version
  private[eventuate] def foldLeft[C](node: Node[A], acc: C)(f: (C, Node[A]) => C): C = {
    val acc2 = f(acc, node)
    node.children match {
      case Seq() => acc2
      case ns => ns.foldLeft(acc2) {
        case (acc, n) => foldLeft(n, acc)(f)
      }
    }
  }
}

object ConcurrentVersionsTree {
  /**
   * Creates a new [[ConcurrentVersionsTree]] that uses projection function `f` to compute
   * new (potentially concurrent) versions from a parent version.
   *
   * @param f Projection function for updates.
   * @tparam A Versioned value type
   * @tparam B Update type
   */
  def apply[A, B](f: (A, B) => A): ConcurrentVersionsTree[A, B] =
    new ConcurrentVersionsTree[A, B](f)

  /**
   * Creates a new [[ConcurrentVersionsTree]] that uses projection function `f` to compute
   * new (potentially concurrent) versions from a parent version.
   *
   * @param initial Value of the initial version.
   * @param f Projection function for updates.
   * @tparam A Versioned value type
   * @tparam B Update type
   */
  def apply[A, B](initial: A, f: (A, B) => A): ConcurrentVersionsTree[A, B] =
    new ConcurrentVersionsTree[A, B](f, initial)

  /**
   * Java API.
   *
   * Creates a new [[ConcurrentVersionsTree]] that uses projection function `f` to compute
   * new (potentially concurrent) versions from a parent version.
   *
   * @param f Projection function for updates.
   * @tparam A Versioned value type
   * @tparam B Update type
   */
  def create[A, B](f: BiFunction[A, B, A]): ConcurrentVersionsTree[A, B] =
    new ConcurrentVersionsTree[A, B](f.apply)

  private[eventuate] class Node[A](var versioned: Versioned[A]) {
    var rejected: Boolean = false
    var children: Vector[Node[A]] = Vector.empty
    var parent: Node[A] = this

    def leaf: Boolean = children.isEmpty
    def root: Boolean = parent == this

    def addChild(node: Node[A]): Unit = {
      node.parent = this
      children = children :+ node
    }

    def reject(): Unit = {
      rejected = true
      if (parent.children.size == 1) parent.reject()
    }

    def stamp(t: VectorTime): Unit = {
      versioned = versioned.copy(version = t)
    }
  }
}
