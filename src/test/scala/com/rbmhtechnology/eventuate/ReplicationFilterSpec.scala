/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
 */

package com.rbmhtechnology.eventuate

import org.scalatest._

object ReplicationFilterSpec {
  class TestFilter(prefix: String) extends ReplicationFilter {
    override def apply(event: DurableEvent): Boolean = event.payload match {
      case s: String => s.startsWith(prefix)
      case _         => false
    }
  }

  def filter(prefix: String): ReplicationFilter =
    new TestFilter(prefix)

  def event(payload: String): DurableEvent =
    DurableEvent(payload, VectorTime(), "")
}

class ReplicationFilterSpec extends WordSpec with Matchers {
  import ReplicationFilterSpec._

  "A replication filter" must {
    "be composable" in {
      val f1 = filter("a").compose(filter("ab"))

      assert(f1(event("abc")))
      assert(!f1(event("a")))
      assert(!f1(event("b")))

      val f2 = filter("a").compose(filter("ab")).compose(filter("abc"))

      assert(f2(event("abc")))
      assert(!f2(event("a")))

      val f3 = filter("a").compose(filter("b"))

      assert(!f3(event("abc")))
      assert(!f3(event("a")))

      val f4 = f1.compose(f2)

      assert(f4(event("abc")))
      assert(!f4(event("a")))
    }
  }
}
