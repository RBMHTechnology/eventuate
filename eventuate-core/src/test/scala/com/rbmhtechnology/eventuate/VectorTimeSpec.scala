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

class VectorTimeSpec extends WordSpec with Matchers {
  val T = true
  val F = false

  def vectorTime(a: Long = 0L, b: Long = 0L, c: Long = 0L): DefaultVectorTime = {
    DefaultVectorTime(Map("a" -> a, "b" -> b, "c" -> c).filter { case ((_, v)) => v > 0L })
  }

  def assert(t1: VectorTime, t2: VectorTime, equiv: Boolean, conc: Boolean, lt: Boolean, lteq: Boolean, gt: Boolean, gteq: Boolean): Unit = {
    t1 equiv t2 should be(equiv)
    t1 conc t2 should be(conc)
    t1 < t2 should be(lt)
    t1 <= t2 should be(lteq)
    t1 > t2 should be(gt)
    t1 >= t2 should be(gteq)
  }

  "DefaultVectorTime" must {
    "be comparable to DefaultVectorTime" in {
      val vt = vectorTime(1, 2)

      assert(vt, vectorTime(1, 2), equiv = T, conc = F, lt = F, lteq = T, gt = F, gteq = T)
      assert(vt, vectorTime(1, 3), equiv = F, conc = F, lt = T, lteq = T, gt = F, gteq = F)
      assert(vt, vectorTime(2, 3), equiv = F, conc = F, lt = T, lteq = T, gt = F, gteq = F)
      assert(vt, vectorTime(0, 1), equiv = F, conc = F, lt = F, lteq = F, gt = T, gteq = T)
      assert(vt, vectorTime(0, 2), equiv = F, conc = F, lt = F, lteq = F, gt = T, gteq = T)
      assert(vt, vectorTime(0, 3), equiv = F, conc = T, lt = F, lteq = F, gt = F, gteq = F)
      assert(vt, vectorTime(c = 2), equiv = F, conc = T, lt = F, lteq = F, gt = F, gteq = F)
    }
    "be comparable to DottedVectorTime" in {
      val vt = vectorTime(a = 3)

      assert(vt, vectorTime(a = 1).dotted("a", 2), equiv = F, conc = F, lt = F, lteq = F, gt = T, gteq = T)
      assert(vt, vectorTime(a = 1).dotted("a", 3), equiv = F, conc = F, lt = F, lteq = F, gt = T, gteq = T)
      assert(vt, vectorTime(a = 1).dotted("a", 3, 1), equiv = T, conc = F, lt = F, lteq = T, gt = F, gteq = T)
      assert(vt, vectorTime(a = 1).dotted("a", 4), equiv = F, conc = T, lt = F, lteq = F, gt = F, gteq = F)
      assert(vt, vectorTime(a = 2).dotted("a", 3), equiv = T, conc = F, lt = F, lteq = T, gt = F, gteq = T)
      assert(vt, vectorTime(a = 2).dotted("a", 4), equiv = F, conc = T, lt = F, lteq = F, gt = F, gteq = F)
      assert(vt, vectorTime(a = 3).dotted("a", 4), equiv = F, conc = F, lt = T, lteq = T, gt = F, gteq = F)
      assert(vt, vectorTime(a = 3).dotted("a", 5), equiv = F, conc = F, lt = T, lteq = T, gt = F, gteq = F)
      assert(vt, vectorTime(a = 4).dotted("a", 5), equiv = F, conc = F, lt = T, lteq = T, gt = F, gteq = F)
      assert(vt, vectorTime(a = 4).dotted("a", 6), equiv = F, conc = F, lt = T, lteq = T, gt = F, gteq = F)
    }
    "be comparable to DottedVectorTime (multiple entries)" in {
      val vt = vectorTime(a = 1, b = 2)

      assert(vt, vectorTime(a = 1, b = 1).dotted("b", 2), equiv = T, conc = F, lt = F, lteq = T, gt = F, gteq = T)
      assert(vt, vectorTime(a = 1, b = 1).dotted("b", 3), equiv = F, conc = T, lt = F, lteq = F, gt = F, gteq = F)

      // ...
    }
    "be mergeable with DefaultVectorTime" in {
      val vt1 = vectorTime(a = 1, b = 2, c = 2)
      val vt2 = vectorTime(a = 4, c = 1)

      vt1.merge(vt2) should be(vectorTime(a = 4, b = 2, c = 2))
    }
    "be mergeable with DottedVectorTime" in {
      val vt1 = vectorTime(a = 1, b = 2, c = 2)
      val vt2 = vectorTime(a = 4).dotted("c", 5)

      vt1.merge(vt2) should be(vectorTime(a = 4, b = 2, c = 5))
    }
  }

  "DottedVectorTime" must {
    "be comparable to DefaultVectorTime" in {
      val vt = vectorTime(a = 3)

      assert(vectorTime(a = 1).dotted("a", 2), vt, equiv = F, conc = F, lt = T, lteq = T, gt = F, gteq = F)
      assert(vectorTime(a = 1).dotted("a", 3), vt, equiv = F, conc = F, lt = T, lteq = T, gt = F, gteq = F)
      assert(vectorTime(a = 1).dotted("a", 3, 1), vt, equiv = T, conc = F, lt = F, lteq = T, gt = F, gteq = T)
      assert(vectorTime(a = 1).dotted("a", 4), vt, equiv = F, conc = T, lt = F, lteq = F, gt = F, gteq = F)
      assert(vectorTime(a = 2).dotted("a", 3), vt, equiv = T, conc = F, lt = F, lteq = T, gt = F, gteq = T)
      assert(vectorTime(a = 2).dotted("a", 4), vt, equiv = F, conc = T, lt = F, lteq = F, gt = F, gteq = F)
      assert(vectorTime(a = 3).dotted("a", 4), vt, equiv = F, conc = F, lt = F, lteq = F, gt = T, gteq = T)
      assert(vectorTime(a = 3).dotted("a", 5), vt, equiv = F, conc = F, lt = F, lteq = F, gt = T, gteq = T)
      assert(vectorTime(a = 4).dotted("a", 5), vt, equiv = F, conc = F, lt = F, lteq = F, gt = T, gteq = T)
      assert(vectorTime(a = 4).dotted("a", 6), vt, equiv = F, conc = F, lt = F, lteq = F, gt = T, gteq = T)
    }
    "be comparable to DottedVectorTime" in {
      val vt = vectorTime(a = 3).dotted("a", 5)

      assert(vt, vectorTime(a = 2).dotted("a", 3), equiv = F, conc = F, lt = F, lteq = F, gt = T, gteq = T)
      assert(vt, vectorTime(a = 2).dotted("a", 4), equiv = F, conc = T, lt = F, lteq = F, gt = F, gteq = F)
      assert(vt, vectorTime(a = 3).dotted("a", 5), equiv = T, conc = F, lt = F, lteq = T, gt = F, gteq = T)
      assert(vt, vectorTime(a = 3).dotted("a", 6), equiv = F, conc = T, lt = F, lteq = F, gt = F, gteq = F)
      assert(vt, vectorTime(a = 4).dotted("a", 6), equiv = F, conc = T, lt = F, lteq = F, gt = F, gteq = F)
      assert(vt, vectorTime(a = 5).dotted("a", 7), equiv = F, conc = F, lt = T, lteq = T, gt = F, gteq = F)
    }
    "be comparable to DottedVectorTime (multiple entries)" in {
      val vt = vectorTime(a = 1, b = 2).dotted("b", 4)

      assert(vt, vectorTime(a = 1, b = 1).dotted("b", 2), equiv = F, conc = F, lt = F, lteq = F, gt = T, gteq = T)
      assert(vt, vectorTime(a = 1, b = 2).dotted("b", 4), equiv = T, conc = F, lt = F, lteq = T, gt = F, gteq = T)
      assert(vt, vectorTime(a = 1, b = 2).dotted("b", 5), equiv = F, conc = T, lt = F, lteq = F, gt = F, gteq = F)

      // ...
    }
    "be mergeable with DefaultVectorTime" in {
      val vt1 = vectorTime(a = 4).dotted("c", 5)
      val vt2 = vectorTime(a = 1, b = 2, c = 2)

      vt1.merge(vt2) should be(vectorTime(a = 4, b = 2, c = 5))
    }
    "be mergeable with DottedVectorTime" in {
      val vt1 = vectorTime(a = 4).dotted("b", 3)
      val vt2 = vectorTime(a = 5).dotted("c", 4)

      vt1.merge(vt2) should be(vectorTime(a = 5, b = 3, c = 4))
    }
    "support dot merge" in {
      val vt1 = vectorTime(a = 4).dotted("b", 3)

      vt1.mergeDot should be(vectorTime(a = 4, b = 3))
    }
    "be equivalent to its dot merge result if the dot immediately follows the corresponding local time in the past" in {
      val vt1 = vectorTime(a = 4, c = 2).dotted("b", 1)
      val vt2 = vectorTime(a = 4, b = 1, c = 2).dotted("b", 3, 1)

      assert(vt1, vt1.mergeDot, equiv = T, conc = F, lt = F, lteq = T, gt = F, gteq = T)
      assert(vt2, vt2.mergeDot, equiv = T, conc = F, lt = F, lteq = T, gt = F, gteq = T)
    }
  }
}
