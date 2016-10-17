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

import com.rbmhtechnology.eventuate.VectorTime

import org.scalatest._

class CRDTSpec extends WordSpec with Matchers with BeforeAndAfterEach {
  val mvReg = MVRegister[Int]()
  val lwwReg = LWWRegister[Int]()
  val orSet = ORSet[Int]()
  val orShoppingCart = ORCart[String]()

  def vectorTime(t1: Long, t2: Long): VectorTime =
    VectorTime("p1" -> t1, "p2" -> t2)

  "An MVRegister" must {
    "not have set a value by default" in {
      mvReg.value should be('empty)
    }
    "store a single value" in {
      mvReg
        .assign(1, vectorTime(1, 0))
        .value should be(Set(1))
    }
    "store multiple values in case of concurrent writes" in {
      mvReg
        .assign(1, vectorTime(1, 0))
        .assign(2, vectorTime(0, 1))
        .value should be(Set(1, 2))
    }
    "mask duplicate concurrent writes" in {
      mvReg
        .assign(1, vectorTime(1, 0))
        .assign(1, vectorTime(0, 1))
        .value should be(Set(1))
    }
    "replace a value if it happened before a new write" in {
      mvReg
        .assign(1, vectorTime(1, 0))
        .assign(2, vectorTime(2, 0))
        .value should be(Set(2))
    }
    "replace multiple concurrent values if they happened before a new write" in {
      mvReg
        .assign(1, vectorTime(1, 0))
        .assign(2, vectorTime(0, 1))
        .assign(3, vectorTime(1, 1))
        .value should be(Set(3))
    }
  }

  "An LWWRegister" must {
    "not have a value by default" in {
      lwwReg.value should be('empty)
    }
    "store a single value" in {
      lwwReg
        .assign(1, vectorTime(1, 0), 0, "source-1")
        .value should be(Some(1))
    }
    "accept a new value if was set after the current value according to the vector clock" in {
      lwwReg
        .assign(1, vectorTime(1, 0), 1, "emitter-1")
        .assign(2, vectorTime(2, 0), 0, "emitter-2")
        .value should be(Some(2))
    }
    "fallback to the wall clock if the values' vector clocks are concurrent" in {
      lwwReg
        .assign(1, vectorTime(1, 0), 0, "emitter-1")
        .assign(2, vectorTime(0, 1), 1, "emitter-2")
        .value should be(Some(2))
      lwwReg
        .assign(1, vectorTime(1, 0), 1, "emitter-1")
        .assign(2, vectorTime(0, 1), 0, "emitter-2")
        .value should be(Some(1))
    }
    "fallback to the greatest emitter if the values' vector clocks and wall clocks are concurrent" in {
      lwwReg
        .assign(1, vectorTime(1, 0), 0, "emitter-1")
        .assign(2, vectorTime(0, 1), 0, "emitter-2")
        .value should be(Some(2))
      lwwReg
        .assign(1, vectorTime(1, 0), 0, "emitter-2")
        .assign(2, vectorTime(0, 1), 0, "emitter-1")
        .value should be(Some(1))
    }
  }

  "An ORSet" must {
    "be empty by default" in {
      orSet.value should be('empty)
    }
    "add an entry" in {
      orSet
        .add(1, vectorTime(1, 0))
        .value should be(Set(1))
    }
    "mask sequential duplicates" in {
      orSet
        .add(1, vectorTime(1, 0))
        .add(1, vectorTime(2, 0))
        .value should be(Set(1))
    }
    "mask concurrent duplicates" in {
      orSet
        .add(1, vectorTime(1, 0))
        .add(1, vectorTime(0, 1))
        .value should be(Set(1))
    }
    "remove a pair" in {
      orSet
        .add(1, vectorTime(1, 0))
        .remove(Set(vectorTime(1, 0)))
        .value should be(Set())
    }
    "remove an entry by removing all pairs" in {
      val tmp = orSet
        .add(1, vectorTime(1, 0))
        .add(1, vectorTime(2, 0))

      tmp
        .remove(tmp.prepareRemove(1))
        .value should be(Set())
    }
    "keep an entry if not all pairs are removed" in {
      orSet
        .add(1, vectorTime(1, 0))
        .add(1, vectorTime(2, 0))
        .remove(Set(vectorTime(1, 0)))
        .value should be(Set(1))
    }
    "add an entry if concurrent to remove" in {
      orSet
        .add(1, vectorTime(1, 0))
        .remove(Set(vectorTime(1, 0)))
        .add(1, vectorTime(0, 1))
        .value should be(Set(1))
    }
    "prepare a remove-set if it contains the given entry" in {
      orSet
        .add(1, vectorTime(1, 0))
        .prepareRemove(1) should be(Set(vectorTime(1, 0)))
    }
    "not prepare a remove-set if it doesn't contain the given entry" in {
      orSet.prepareRemove(1) should be('empty)
    }
  }

  "An ORCart" must {
    "be empty by default" in {
      orShoppingCart.value should be('empty)
    }
    "set initial entry quantities" in {
      orShoppingCart
        .add("a", 2, vectorTime(1, 0))
        .add("b", 3, vectorTime(2, 0))
        .value should be(Map("a" -> 2, "b" -> 3))
    }
    "increment existing entry quantities" in {
      orShoppingCart
        .add("a", 1, vectorTime(1, 0))
        .add("b", 3, vectorTime(2, 0))
        .add("a", 1, vectorTime(3, 0))
        .add("b", 1, vectorTime(4, 0))
        .value should be(Map("a" -> 2, "b" -> 4))
    }
    "remove observed entries" in {
      orShoppingCart
        .add("a", 2, vectorTime(1, 0))
        .add("b", 3, vectorTime(2, 0))
        .add("a", 1, vectorTime(3, 0))
        .add("b", 1, vectorTime(4, 0))
        .remove(Set(vectorTime(1, 0), vectorTime(2, 0), vectorTime(3, 0)))
        .value should be(Map("b" -> 1))
    }
  }
}
