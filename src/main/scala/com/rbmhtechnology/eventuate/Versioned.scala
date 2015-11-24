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
 * @param updateTimestamp Timestamp of the event that caused this version.
 * @param creator Creator of the event that caused this version.
 */
case class Versioned[A](value: A, updateTimestamp: VectorTime, creator: String = "")

/**
 * Tracks concurrent [[Versioned]] values which arise from concurrent updates.
 *
 * @tparam A Versioned value type
 * @tparam B Update type
 */
trait ConcurrentVersions[A, B] extends Serializable {
  /**
   * Updates that [[Versioned]] value with `b` that is a predecessor of `updateTimestamp`. If
   * there is no such predecessor, a new concurrent version is created (optionally derived
   * from an older entry in the version history, in case of incremental updates).
   */
  def update(b: B, updateTimestamp: VectorTime, creator: String = ""): ConcurrentVersions[A, B]

  /**
   * Resolves multiple concurrent versions to a single version. For the resolution to be
   * successful, one of the concurrent versions must have a `creationTimestamp` that is equal to
   * `selectedTimestamp`. Only those concurrent versions with a `creationTimestamp` less than the
   * `updateTimestamp` participate in the resolution process (which allows for resolutions to
   * be concurrent to other updates).
   */
  def resolve(selectedTimestamp: VectorTime, updateTimestamp: VectorTime): ConcurrentVersions[A, B]

  /**
   * Experimental ...
   */
  def resolve(selectedTimestamp: VectorTime): ConcurrentVersions[A, B] =
    resolve(selectedTimestamp, all.map(_.updateTimestamp).foldLeft(VectorTime.Zero)(_.merge(_)))

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
    ConcurrentVersionsTree[A, B](initial)(f)
}

/**
 * A [[ConcurrentVersions]] implementation that shall be used if updates replace current
 * versioned values (= full updates). `ConcurrentVersionsList` is an immutable data structure.
 */
class ConcurrentVersionsList[A](vs: List[Versioned[A]], val owner: String = "") extends ConcurrentVersions[A, A]{
  def update(na: A, updateTimestamp: VectorTime, creator: String = ""): ConcurrentVersionsList[A] = {
    val r = vs.foldRight[(List[Versioned[A]], Boolean)]((Nil, false)) {
      case (a, (acc, true))  => (a :: acc, true)
      case (a, (acc, false)) =>
        if (updateTimestamp > a.updateTimestamp)
          // regular update on that version
          (Versioned(na, updateTimestamp, creator) :: acc, true)
        else if (updateTimestamp < a.updateTimestamp)
          // conflict already resolved, ignore
          (a :: acc, true)
        else
          // conflicting update, try next
          (a :: acc, false)
    }
    r match {
      case (updates, true)   => new ConcurrentVersionsList(updates, owner)
      case (original, false) => new ConcurrentVersionsList(Versioned(na, updateTimestamp, creator) :: original, owner)
    }
  }

  def resolve(selectedTimestamp: VectorTime, updateTimestamp: VectorTime): ConcurrentVersionsList[A] = {
    new ConcurrentVersionsList(vs.foldRight(List.empty[Versioned[A]]) {
      case (v, acc) if v.updateTimestamp == selectedTimestamp => v.copy(updateTimestamp = updateTimestamp) :: acc
      case (v, acc) if v.updateTimestamp.conc(updateTimestamp) => v :: acc
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
 * A [[ConcurrentVersions]] implementation that shall be used if updates are incremental.
 * `ConcurrentVersionsTree` is a mutable data structure. Therefore, it is recommended not
 * to share instances of `ConcurrentVersionsTree` directly but rather the [[Versioned]]
 * sequence returned by [[ConcurrentVersionsTree#all]]. Later releases will be based on
 * an immutable data structure.
 *
 * '''Please note:''' This implementation does not purge old versions at the moment (which
 * shouldn't be a problem if the number of incremental updates to a versioned aggregate is
 * rather small). In later releases, manual and automated purging of old versions will be
 * supported.
 */
class ConcurrentVersionsTree[A, B] (private[eventuate] val root: ConcurrentVersionsTree.Node[A]) extends ConcurrentVersions[A, B] {
  import ConcurrentVersionsTree._

  @transient
  private var _projection: (A, B) => A = (s, _) => s
  private var _owner: String = ""

  override def update(b: B, updateTimestamp: VectorTime, creator: String = ""): ConcurrentVersionsTree[A, B] = {
    val p = pred(updateTimestamp)
    p.addChild(new Node(Versioned(_projection(p.versioned.value, b), updateTimestamp, creator)))
    this
  }

  override def resolve(selectedTimestamp: VectorTime, updateTimestamp: VectorTime): ConcurrentVersionsTree[A, B] = {
    leaves.foreach {
      case n if n.rejected                                       => // ignore rejected leaf
      case n if n.versioned.updateTimestamp.conc(updateTimestamp) => // ignore concurrent update
      case n if n.versioned.updateTimestamp == selectedTimestamp => n.stamp(updateTimestamp)
      case n                                                     => n.reject()
    }
    this
  }

  override def all: Seq[Versioned[A]] =
    leaves.filterNot(_.rejected).map(_.versioned)

  override def owner: String =
    _owner

  override def withOwner(owner: String): ConcurrentVersionsTree[A, B] = {
    _owner = owner
    this
  }

  def withProjection(f: (A, B) => A): ConcurrentVersionsTree[A, B] = {
    _projection = f
    this
  }

  def withProjection(f: BiFunction[A, B, A]): ConcurrentVersionsTree[A, B] =
    withProjection((a, b) => f.apply(a, b))

  private[eventuate] def copy(): ConcurrentVersionsTree[A, B] =
    new ConcurrentVersionsTree[A, B](root.copy()).withOwner(_owner).withProjection(_projection)

  private[eventuate] def nodes: Seq[Node[A]] = foldLeft(root, Vector.empty[Node[A]]) {
    case (acc, n) => acc :+ n
  }

  private[eventuate] def leaves: Seq[Node[A]] = foldLeft(root, Vector.empty[Node[A]]) {
    case (leaves, n) => if (n.leaf) leaves :+ n else leaves
  }

  private[eventuate] def pred(timestamp: VectorTime): Node[A] = foldLeft(root, root) {
    case (candidate, n) => if (timestamp > n.versioned.updateTimestamp && n.versioned.updateTimestamp > candidate.versioned.updateTimestamp) n else candidate
  }

  // TODO: make tail recursive or create a trampolined version
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
   * @param initial Value of the initial version.
   * @param f Projection function for updates.
   * @tparam A Versioned value type
   * @tparam B Update type
   */
  def apply[A, B](initial: A)(f: (A, B) => A): ConcurrentVersionsTree[A, B] =
    new ConcurrentVersionsTree[A, B](new ConcurrentVersionsTree.Node(Versioned(initial, VectorTime.Zero, ""))).withProjection(f)

  /**
   * Creates a new [[ConcurrentVersionsTree]] that uses projection function `f` to compute
   * new (potentially concurrent) versions from a parent version.
   *
   * @param f Projection function for updates.
   * @tparam A Versioned value type
   * @tparam B Update type
   */
  def apply[A, B](f: (A, B) => A): ConcurrentVersionsTree[A, B] =
    apply(null.asInstanceOf[A] /* FIXME: use Monoid[A].zero */)(f).withProjection(f)

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
    apply(null.asInstanceOf[A] /* FIXME: use Monoid[A].zero */)((a, b) => f.apply(a, b))

  private[eventuate] class Node[A](var versioned: Versioned[A]) extends Serializable {
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
      versioned = versioned.copy(updateTimestamp = t)
    }

    // TODO: make tail recursive or create a trampolined version
    def copy(): Node[A] = {
      val cn = new Node[A](versioned)
      cn.rejected = rejected
      cn.children = children.map(_.copy())
      cn
    }
  }
}
