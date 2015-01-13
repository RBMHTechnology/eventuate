/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
 */

package com.rbmhtechnology.eventuate

import org.scalatest._

class VectorTimeSpec extends WordSpec with Matchers {
  "Vector times" must {
    "have a partial ordering" in {
      val t1 = VectorTime("a" -> 1, "b" -> 2)
      val t2 = VectorTime("a" -> 1, "b" -> 1)
      val t3 = VectorTime("a" -> 2, "b" -> 1)
      val t4 = VectorTime("a" -> 1, "b" -> 2, "c" -> 2)
      val t5 = VectorTime("a" -> 1, "c" -> 2)
      val t6 = VectorTime("a" -> 1, "c" -> 0)

      assert(t1, t1, equiv = true, conc = false, lt = false, lteq = true, gt = false, gteq = true)
      assert(t1, t2, equiv = false, conc = false, lt = false, lteq = false, gt = true, gteq = true)
      assert(t2, t1, equiv = false, conc = false, lt = true, lteq = true, gt = false, gteq = false)
      assert(t1, t3, equiv = false, conc = true, lt = false, lteq = false, gt = false, gteq = false)
      assert(t3, t1, equiv = false, conc = true, lt = false, lteq = false, gt = false, gteq = false)
      assert(t1, t4, equiv = false, conc = false, lt = true, lteq = true, gt = false, gteq = false)
      assert(t4, t1, equiv = false, conc = false, lt = false, lteq = false, gt = true, gteq = true)
      assert(t1, t5, equiv = false, conc = true, lt = false, lteq = false, gt = false, gteq = false)
      assert(t5, t1, equiv = false, conc = true, lt = false, lteq = false, gt = false, gteq = false)
      assert(t1, t6, equiv = false, conc = false, lt = false, lteq = false, gt = true, gteq = true)
      assert(t6, t1, equiv = false, conc = false, lt = true, lteq = true, gt = false, gteq = false)
    }
    "be mergeable" in {
      val t1 = VectorTime("a" -> 1, "b" -> 2, "c" -> 2)
      val t2 = VectorTime("a" -> 4, "c" -> 1)

      t1.merge(t2) should be(VectorTime("a" -> 4, "b" -> 2, "c" -> 2))
      t2.merge(t1) should be(VectorTime("a" -> 4, "b" -> 2, "c" -> 2))
    }
  }

  private def assert(t1: VectorTime, t2: VectorTime, equiv: Boolean, conc: Boolean, lt: Boolean, lteq: Boolean, gt: Boolean, gteq: Boolean): Unit = {
    t1 equiv t2 should be(equiv)
    t1 conc t2 should be(conc)
    t1 < t2 should be(lt)
    t1 <= t2 should be(lteq)
    t1 > t2 should be(gt)
    t1 >= t2 should be(gteq)
  }
}
