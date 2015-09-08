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
    DurableEvent(payload, "")
}

class ReplicationFilterSpec extends WordSpec with Matchers {
  import ReplicationFilterSpec._

  "A replication filter" must {
    "be composable with a logical AND" in {
      val f1 = filter("a").and(filter("ab"))

      assert(f1(event("abc")))
      assert(!f1(event("a")))
      assert(!f1(event("b")))

      val f2 = filter("a").and(filter("ab")).and(filter("abc"))

      assert(f2(event("abc")))
      assert(!f2(event("a")))

      val f3 = filter("a").and(filter("b"))

      assert(!f3(event("abc")))
      assert(!f3(event("a")))

      val f4 = f1.and(f2)

      assert(f4(event("abc")))
      assert(!f4(event("a")))
    }
    "be composable with a logical OR" in {
      val f1 = filter("ab").or(filter("xy"))

      assert(f1(event("ab")))
      assert(f1(event("xy")))
      assert(!f1(event("ij")))
    }
  }
}
