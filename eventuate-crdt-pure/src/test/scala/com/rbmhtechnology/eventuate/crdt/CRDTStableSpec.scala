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

import com.rbmhtechnology.eventuate.crdt.pure.CRDTTestDSL.VectorTimeControl
import org.scalatest.Matchers
import org.scalatest.WordSpec

class CRDTStableSpec extends WordSpec with Matchers {
  val crdt = CRDT.zero
  val awSet = AWSetService.zero[Int]

  "An AWSet" should {
    import CRDTTestDSL.AWSetCRDT._
    "discard stable operations" in new VectorTimeControl {
      val updated = awSet
        .add(1, vt(1, 0))
        .add(2, vt(2, 0))
        .add(3, vt(2, 1))
        .add(4, vt(3, 1))
        .add(5, vt(3, 2))
        .stable(stableVT(2, 1))
      updated.value shouldBe Set(1, 2, 3, 4, 5)
      updated.polog.log.size shouldBe 2
      updated.state.size shouldBe 3
    }
    "remove stable values" in new VectorTimeControl {
      val updated = awSet
        .add(1, vt(1, 0))
        .stable(stableVT(1, 0))
        .remove(1, vt(2, 0))
      updated.value shouldBe Set()
    }
    "remove only stable values" in new VectorTimeControl {
      val updated = awSet
        .add(1, vt(1, 0))
        .add(2, vt(2, 0))
        .remove(1, vt(3, 0))
        .stable(stableVT(1, 0))
      updated.value shouldBe Set(2)
    }
    "clear stable values" in new VectorTimeControl {
      val updated = awSet
        .add(1, vt(1, 0))
        .add(2, vt(0, 1))
        .stable(stableVT(1, 0))
        .clear(vt(2, 0))
      updated.value shouldBe Set(2)
    }
  }

  "A MVRegister" should {
    import CRDTTestDSL.MVRegisterCRDT._
    "discard stable operations" in new VectorTimeControl {
      val updated = crdt
        .assign(1, vt(1, 0))
        .assign(2, vt(0, 1))
        .stable(stableVT(1, 1))
      updated.value should be(Set(1, 2))
      updated.polog.log.size shouldBe 0
      updated.state.size shouldBe 2
    }
    "clear stable operations" in new VectorTimeControl {
      crdt
        .assign(1, vt(1, 0))
        .assign(2, vt(0, 1))
        .stable(stableVT(1, 1))
        .clear(vt(2, 1))
        .value shouldBe Set()

    }
  }

  "A LWWRegister" should {
    import CRDTTestDSL.LWWRegisterCRDT._
    "discard stable operations" in new VectorTimeControl {
      val updated = crdt
        .assign(1, vt(1, 0), 0, "emitter1")
        .assign(2, vt(0, 1), 1, "emitter2")
        .assign(3, vt(2, 0), 2, "emitter2")
        .stable(stableVT(1, 1))
      updated.value should be(Some(3))
      updated.polog.log.size shouldBe 1
      updated.state.size shouldBe 1
    }
    "clear stable operations" in new VectorTimeControl {
      val updated = crdt
        .assign(1, vt(1, 0), 0, "emitter1")
        .assign(2, vt(0, 1), 1, "emitter2")
        .stable(stableVT(0, 1))
        .clear(vt(0, 2))
      updated.value shouldBe Some(1)
      updated.polog.log.size shouldBe 1
      updated.state.size shouldBe 0
    }
  }

  "An AWCart" should {
    import CRDTTestDSL.AWCartCRDT._
    "discard stable operations" in new VectorTimeControl {
      val updated = crdt
        .add("a", 1, vt(1, 0))
        .add("b", 2, vt(2, 0))
        .add("a", 5, vt(0, 1))
        .stable(stableVT(1, 1))
      updated.value should be(Map("a" -> 6, "b" -> 2))
      updated.polog.log.size shouldBe 1
      updated.state.size shouldBe 2
    }
    "clear stable operations" in new VectorTimeControl {
      val updated = crdt
        .add("a", 1, vt(1, 0))
        .add("b", 2, vt(2, 0))
        .stable(stableVT(2, 0))
        .clear(vt(3, 0))
      updated.value should be(Map())
      updated.polog.log.size shouldBe 0
      updated.state.size shouldBe 0
    }
  }

}
