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
    clock = VectorClock("test")
  }

  "A vector clock" must {
    "increment local time on tick" in {
      clock.currentLocalTime should be(0L)
      clock
        .tick()
        .currentLocalTime should be(1L)
    }
    "update itself with tick and merge" in {
      clock
        .copy(currentTime =    VectorTime(clock.processId -> 3L, "p2" -> 1L, "p3" -> 2L))
        .update(               VectorTime(clock.processId -> 1L,             "p3" -> 2L, "p4" -> 8L))
        .currentTime should be(VectorTime(clock.processId -> 4L, "p2" -> 1L, "p3" -> 2L, "p4" -> 8L))
    }
    "not modify remote times on tick" in {
      clock
        .copy(currentTime =    VectorTime(clock.processId -> 3L, "p2" -> 1L, "p3" -> 2L))
        .tick()
        .currentTime should be(VectorTime(clock.processId -> 4L, "p2" -> 1L, "p3" -> 2L))
    }
  }
}
