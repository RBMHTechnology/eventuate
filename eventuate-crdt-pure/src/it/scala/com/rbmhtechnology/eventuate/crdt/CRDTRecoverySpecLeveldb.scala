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

import akka.actor._
import akka.testkit._
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.crdt.pure.AWSetService.AWSet
import com.rbmhtechnology.eventuate.crdt.pure.CRDTTypes.Operation
import com.rbmhtechnology.eventuate.utilities._
import org.scalatest._

class CRDTRecoverySpecLeveldb extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with SingleLocationSpecLeveldb {

  var probe: TestProbe = _

  def service(serviceId: String) = new AWSetService[Int](serviceId, log) {
    override def onChange(crdt: AWSet[Int], operation: Option[Operation]): Unit = probe.ref ! ops.value(crdt)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    probe = TestProbe()
  }

  "A CRDTService" must {
    "recover CRDT instances" in {
      val service1 = service("a")
      service1.add("x", 1)
      service1.add("x", 2)
      probe.expectMsg(Set(1))
      probe.expectMsg(Set(1, 2))
      service1.value("x").await should be(Set(1, 2))

      val service2 = service("b")
      // CRDT lazily recovered on read request
      service2.value("x").await should be(Set(1, 2))
      probe.expectMsg(Set(1))
      probe.expectMsg(Set(1, 2))
    }
    "recover CRDT instances from snapshots" in {
      val service1 = service("a")
      service1.add("x", 1)
      // Await before save to not depend on actor's stateSync flag
      service1.add("x", 2).await
      service1.save("x").await
      service1.add("x", 3)
      probe.expectMsg(Set(1))
      probe.expectMsg(Set(1, 2))
      probe.expectMsg(Set(1, 2, 3))
      service1.value("x").await should be(Set(1, 2, 3))

      val service2 = service("a")
      // CRDT lazily recovered on read request
      service2.value("x").await should be(Set(1, 2, 3))
      // snapshot only exists in scope of service a
      probe.expectMsg(Set(1, 2))
      probe.expectMsg(Set(1, 2, 3))

      val service3 = service("b")
      // CRDT lazily recovered on read request
      service3.value("x").await should be(Set(1, 2, 3))
      // snapshot doesn't exist in scope of service b
      probe.expectMsg(Set(1))
      probe.expectMsg(Set(1, 2))
      probe.expectMsg(Set(1, 2, 3))
    }
  }
}
