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

package com.rbmhtechnology.eventuate.crdt.pure

import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.crdt.pure.StabilityProtocol._
import org.scalatest.Matchers
import org.scalatest.WordSpecLike

class StabilityProtocolSpec extends WordSpecLike with Matchers {

  val A = "A"
  val B = "B"
  val C = "C"

  def partitions: Set[String] = Set(A, B, C)

  def initialRTM = RTM(StabilityConf(A, partitions))

  def vt(a: Long, b: Long, c: Long) = VectorTime(A -> a, B -> b, C -> c)

  def tcstable(a: Long, b: Long, c: Long) = Some(TCStable(vt(a, b, c)))

  "Stability" should {
    "drop updates from local partition" in {
      initialRTM
        .update(A, vt(1, 1, 1))
        .update(B, vt(2, 2, 2))
        .update(C, vt(2, 2, 2))
        .stable shouldBe tcstable(2, 2, 2)
    }
    "not emit tcstable when B = (1,1,1), C = unknown " in {
      initialRTM
        .update(B, vt(1, 1, 1))
        .stable shouldBe None
    }
    "emit TCStable(0,1) when B = (0,1,1), C = (0,0,1) " in {
      initialRTM
        .update(B, vt(0, 1, 1))
        .update(C, vt(0, 0, 1))
        .stable shouldBe tcstable(0, 0, 1)
    }
    "emit TCStable(1,1) when A = (1,1,1), B = (1,1,1)" in {
      initialRTM
        .update(B, vt(1, 1, 1))
        .update(C, vt(1, 1, 1))
        .stable shouldBe tcstable(1, 1, 1)
    }
    "emit TCStable(1,1) when A = (2,1), B = (1,2)" in {
      initialRTM
        .update(B, vt(2, 2, 1))
        .update(C, vt(1, 1, 2))
        .stable shouldBe tcstable(1, 1, 1)
    }
  }

}
