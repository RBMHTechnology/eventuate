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

class VectorClockSpec extends WordSpec with Matchers with BeforeAndAfterEach {
  var clock: VectorClock = _

  override protected def beforeEach(): Unit = {
    clock = VectorClock("p1")
  }

  def vectorTime(t1: Long = 0L, t2: Long = 0L, t3: Long = 0L, t4: Long = 0L): VectorTime =
    VectorTime(clock.processId -> t1, "p2" -> t2, "p3" -> t3, "p4" -> t4)

  "A vector clock" must {
    "increment local time on tick" in {
      clock.currentLocalTime should be(0L)
      clock
        .tick()
        .currentLocalTime should be(1L)
    }
    "update itself with tick and merge" in {
      clock
        .copy(currentTime = vectorTime(3, 1, 2, 0))
        .update(vectorTime(1, 0, 2, 8))
        .currentTime should be(vectorTime(4, 1, 2, 8))
    }
    "not modify remote times on tick" in {
      clock
        .copy(currentTime = vectorTime(3, 1, 2))
        .tick()
        .currentTime should be(vectorTime(4, 1, 2))
    }
    "be able to tell whether it agrees with a sender process about states of all other processes" in {
      clock.covers(vectorTime(1, 0, 0), "p1") should be(true)
      clock.covers(vectorTime(0, 1, 0), "p2") should be(true)
      clock.covers(vectorTime(0, 1, 1), "p2") should be(false)

      clock.update(vectorTime(0, 0, 1))
        .covers(vectorTime(0, 1, 1), "p2") should be(true)
      clock.update(vectorTime(0, 0, 1))
        .covers(vectorTime(0, 1, 2), "p2") should be(false)
    }
  }
}
