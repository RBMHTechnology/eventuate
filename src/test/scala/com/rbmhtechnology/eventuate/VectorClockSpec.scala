/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
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
        .copy(currentTime = VectorTime(clock.processId -> 3L, "p2" -> 1L, "p3" -> 2L))
        .update(VectorTime(clock.processId -> 1L, "p3" -> 2L, "p4" -> 8L))
        .currentTime should be(VectorTime(clock.processId -> 4L, "p2" -> 1L, "p3" -> 2L, "p4" -> 8L))
    }
    "not modify remote times on tick" in {
      clock
        .copy(currentTime = VectorTime(clock.processId -> 3L, "p2" -> 1L, "p3" -> 2L))
        .tick()
        .currentTime should be(VectorTime(clock.processId -> 4L, "p2" -> 1L, "p3" -> 2L))
    }
  }
}
