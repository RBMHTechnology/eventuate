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

package com.rbmhtechnology.eventuate.crdt

import akka.actor._
import akka.testkit._

import com.rbmhtechnology.eventuate.SingleLocationSpecLeveldb
import com.rbmhtechnology.eventuate.utilities._

import org.scalatest._

class CRDTServiceSpecLeveldb extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with SingleLocationSpecLeveldb {
  "A CRDTService" must {
    "manage multiple CRDTs identified by name" in {
      val service = new CounterService[Int]("a", log)
      service.update("a", 1).await should be(1)
      service.update("b", 2).await should be(2)
      service.value("a").await should be(1)
      service.value("b").await should be(2)
    }
    "ignore events from CRDT services of different type" in {
      val service1 = new CounterService[Int]("a", log)
      val service2 = new MVRegisterService[Int]("b", log)
      val service3 = new LWWRegisterService[Int]("c", log)
      service1.update("a", 1).await should be(1)
      service2.set("a", 1).await should be(Set(1))
      service3.set("a", 1).await should be(Some(1))
    }
  }

  "A CounterService" must {
    "return the default value of a Counter" in {
      val service = new CounterService[Int]("a", log)
      service.value("a").await should be(0)
    }
    "increment a Counter" in {
      val service = new CounterService[Int]("a", log)
      service.update("a", 3).await should be(3)
      service.update("a", 2).await should be(5)
      service.value("a").await should be(5)
    }
    "decrement a Counter" in {
      val service = new CounterService[Int]("a", log)
      service.update("a", -3).await should be(-3)
      service.update("a", -2).await should be(-5)
      service.value("a").await should be(-5)
    }
  }

  "An MVRegisterService" must {
    "return the default value of an MVRegister" in {
      val service = new MVRegisterService[Int]("a", log)
      service.value("a").await should be(Set())
    }
    "return the written value of an MVRegister" in {
      val service = new MVRegisterService[Int]("a", log)
      service.set("a", 1).await should be(Set(1))
      service.value("a").await should be(Set(1))
    }
  }

  "An LWWRegisterService" must {
    "return the default value of an LWWRegister" in {
      val service = new LWWRegisterService[Int]("a", log)
      service.value("a").await should be(None)
    }
    "return the written value of an LWWRegister" in {
      val service = new LWWRegisterService[Int]("a", log)
      service.set("a", 1).await should be(Some(1))
      service.value("a").await should be(Some(1))
    }
  }

  "An ORSetService" must {
    "return the default value of an ORSet" in {
      val service = new ORSetService[Int]("a", log)
      service.value("a").await should be(Set())
    }
    "add an entry" in {
      val service = new ORSetService[Int]("a", log)
      service.add("a", 1).await should be(Set(1))
      service.value("a").await should be(Set(1))
    }
    "mask duplicates" in {
      val service = new ORSetService[Int]("a", log)
      service.add("a", 1).await should be(Set(1))
      service.add("a", 1).await should be(Set(1))
      service.value("a").await should be(Set(1))
    }
    "remove an entry" in {
      val service = new ORSetService[Int]("a", log)
      service.add("a", 1).await should be(Set(1))
      service.remove("a", 1).await should be(Set())
      service.value("a").await should be(Set())
    }
    "remove duplicates" in {
      val service = new ORSetService[Int]("a", log)
      service.add("a", 1).await should be(Set(1))
      service.add("a", 1).await should be(Set(1))
      service.remove("a", 1).await should be(Set())
      service.value("a").await should be(Set())
    }
  }

  "An ORCartService" must {
    "return the default value of an ORCart" in {
      val service = new ORCartService[String]("a", log)
      service.value("a").await should be(Map())
    }
    "set initial entry quantities" in {
      val service = new ORCartService[String]("a", log)
      service.add("a", "123", 1).await should be(Map("123" -> 1))
      service.add("a", "124", 1).await should be(Map("123" -> 1, "124" -> 1))
      service.value("a").await should be(Map("123" -> 1, "124" -> 1))
    }
    "increment existing entry quantities" in {
      val service = new ORCartService[String]("a", log)
      service.add("a", "123", 1).await should be(Map("123" -> 1))
      service.add("a", "123", 1).await should be(Map("123" -> 2))
    }
    "remove entries" in {
      val service = new ORCartService[String]("a", log)
      service.add("a", "123", 1).await should be(Map("123" -> 1))
      service.add("a", "124", 1).await should be(Map("123" -> 1, "124" -> 1))
      service.remove("a", "123").await should be(Map("124" -> 1))
      service.value("a").await should be(Map("124" -> 1))
    }
    "reject non-positive quantities" in {
      val service = new ORCartService[String]("a", log)
      intercept[IllegalArgumentException](service.add("a", "123", 0).await)
      intercept[IllegalArgumentException](service.add("a", "123", -1).await)
    }
  }
}
