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

import org.scalatest._

abstract class ConcurrentVersionsSpec extends WordSpec with Matchers with BeforeAndAfterEach {
  var versions: ConcurrentVersions[String, String] = null

  override def beforeEach(): Unit =
    versions = create

  def create: ConcurrentVersions[String, String]

  def vectorTime(t1: Int, t2: Int, t3: Int): VectorTime =
    VectorTime("p1" -> t1, "p2" -> t2, "p3" -> t3)

  "A ConcurrentVersions instance" must {
    "track causal updates" in {
      val result = versions
        .update("a", vectorTime(1, 0, 0))
        .update("b", vectorTime(2, 0, 0))

      result.conflict should be(false)
      result.all(0) should be(Versioned("b", vectorTime(2, 0, 0)))
    }
    "track concurrent updates" in {
      val result = versions
        .update("a", vectorTime(1, 0, 0))
        .update("b", vectorTime(0, 1, 0))

      result.conflict should be(true)
      result.all(0) should be(Versioned("a", vectorTime(1, 0, 0)))
      result.all(1) should be(Versioned("b", vectorTime(0, 1, 0)))
    }
    "resolve concurrent updates" in {
      val result = versions
        .update("a", vectorTime(1, 0, 0))
        .update("b", vectorTime(0, 1, 0))
        .resolve(
          vectorTime(1, 0, 0),
          vectorTime(2, 1, 0))

      result.conflict should be(false)
      result.all(0) should be(Versioned("a", vectorTime(2, 1, 0)))
    }
    "resolve concurrent updates with implicit event timestamp" in {
      val result = versions
        .update("a", vectorTime(1, 0, 0))
        .update("b", vectorTime(0, 1, 0))
        .resolve(vectorTime(1, 0, 0))

      result.conflict should be(false)
      result.all(0) should be(Versioned("a", vectorTime(1, 1, 0)))
    }
    "resolve concurrent updates (advanced)" in {
      val updated = versions
        .update("a", vectorTime(1, 0, 0))
        .update("b", vectorTime(0, 1, 0))
        .update("c", vectorTime(0, 1, 4))
        .update("d", vectorTime(0, 3, 0))
        .update("e", vectorTime(0, 1, 5))

      updated.all.length should be(3)
      updated.all(0) should be(Versioned("a", vectorTime(1, 0, 0)))
      updated.all(1) should be(Versioned("e", vectorTime(0, 1, 5)))
      updated.all(2) should be(Versioned("d", vectorTime(0, 3, 0)))

      val result = updated.resolve(
        vectorTime(0, 3, 0),
        vectorTime(3, 4, 8))

      result.conflict should be(false)
      result.all(0) should be(Versioned("d", vectorTime(3, 4, 8)))
    }
    "only resolve concurrent updates that happened before the resolve" in {
      val result = versions
        .update("a", vectorTime(1, 0, 0))
        .update("b", vectorTime(0, 1, 0))
        .update("c", vectorTime(0, 0, 1))
        .resolve(
          vectorTime(1, 0, 0),
          vectorTime(2, 1, 0))

      result.all.length should be(2)
      result.all(0) should be(Versioned("a", vectorTime(2, 1, 0)))
      result.all(1) should be(Versioned("c", vectorTime(0, 0, 1)))
    }
  }
}

object ConcurrentVersionsTreeSpec {
  implicit class ConcurrentVersionsTreeHelper(tree: ConcurrentVersionsTree[String, String]) {
    def nodeTuples = tree.nodes.map { node => (node.versioned, node.rejected) }
  }
}

class ConcurrentVersionsTreeSpec extends ConcurrentVersionsSpec {
  type Projection = (String, String) => String

  val replace: Projection = (a, b) => b
  val append: Projection = (a, b) => if (a == null) b else a + b

  override def create: ConcurrentVersions[String, String] = ConcurrentVersionsTree(replace)

  "A ConcurrentVersionsTree instance" must {
    "support updates on rejected versions (append to leaf)" in {
      val result = ConcurrentVersionsTree(append)
        .update("a", vectorTime(1, 0, 0))
        .update("b", vectorTime(1, 1, 0))
        .update("c", vectorTime(1, 0, 1))
        .resolve(
          vectorTime(1, 0, 1),
          vectorTime(1, 2, 1))
        .update("d", vectorTime(2, 1, 0))
        .update("e", vectorTime(3, 1, 0))

      result.all.length should be(2)
      result.all(0) should be(Versioned("abde", vectorTime(3, 1, 0)))
      result.all(1) should be(Versioned("ac", vectorTime(1, 2, 1)))
    }
    "support updates on rejected versions (append to non-leaf)" in {
      val result = ConcurrentVersionsTree(append)
        .update("a", vectorTime(1, 0, 0))
        .update("b", vectorTime(1, 1, 0))
        .update("x", vectorTime(1, 2, 0))
        .update("c", vectorTime(1, 0, 1))
        .resolve(
          vectorTime(1, 0, 1),
          vectorTime(1, 3, 1))
        .update("d", vectorTime(2, 1, 0))
        .update("e", vectorTime(3, 1, 0))

      result.all.length should be(2)
      result.all(0) should be(Versioned("abde", vectorTime(3, 1, 0)))
      result.all(1) should be(Versioned("ac", vectorTime(1, 3, 1)))
    }
    "append updates to the closest predecessor" in {
      val result = ConcurrentVersionsTree(append)
        .update("a", vectorTime(1, 0, 0))
        .update("b", vectorTime(2, 0, 0))
        .update("c", vectorTime(1, 1, 0))
        .resolve(
          vectorTime(2, 0, 0),
          vectorTime(2, 2, 0))
        .update("d", vectorTime(3, 2, 0))

      result.conflict should be(false)
      result.all(0) should be(Versioned("abd", vectorTime(3, 2, 0)))
    }
    "create a deep copy of itself" in {
      val tree = ConcurrentVersionsTree(append)
        .update("a", vectorTime(1, 0, 0))
        .update("b", vectorTime(2, 0, 0))
        .update("c", vectorTime(1, 1, 0))

      val upd1 = tree.copy().resolve(
        vectorTime(2, 0, 0),
        vectorTime(2, 2, 0))

      val upd2 = tree.copy().resolve(
        vectorTime(1, 1, 0),
        vectorTime(2, 2, 0))

      tree.conflict should be(true)
      upd1.conflict should be(false)
      upd2.conflict should be(false)

      tree.all should be(Seq(
        Versioned("ab", vectorTime(2, 0, 0)),
        Versioned("ac", vectorTime(1, 1, 0))))

      upd1.all should be(Seq(Versioned("ab", vectorTime(2, 2, 0))))
      upd2.all should be(Seq(Versioned("ac", vectorTime(2, 2, 0))))
    }
  }
}

class ConcurrentVersionsListSpec extends ConcurrentVersionsSpec {
  override def create: ConcurrentVersions[String, String] = ConcurrentVersionsList[String]
}
