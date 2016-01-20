/*
 * Copyright (C) 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
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

import akka.actor.Actor.Receive

import org.scalatest._

class BehaviorContextSpec extends WordSpec with Matchers with BeforeAndAfterEach {
  var context: BehaviorContext = _
  var state: Int = 0

  val b1: Receive = {
    case _ => state = 1
  }

  val b2: Receive = {
    case _ => state = 2
  }

  val b3: Receive = {
    case _ => state = 3
  }

  implicit class RunnableBehavior(behavior: Receive) {
    def run: Int = {
      behavior.apply(())
      state
    }
  }

  override protected def beforeEach(): Unit = {
    context = new DefaultBehaviorContext(b1)
  }

  "A DefaultBehaviorContext" must {
    "have an initial behavior" in {
      context.current.run should be(1)
    }
    "add and remove behavior" in {
      context.become(b2, replace = false)
      context.current.run should be(2)
      context.become(b3, replace = false)
      context.current.run should be(3)
      context.unbecome()
      context.current.run should be(2)
      context.unbecome()
      context.current.run should be(1)
    }
    "replace behavior and revert to initial behavior" in {
      context.become(b2)
      context.current.run should be(2)
      context.become(b3)
      context.current.run should be(3)
      context.unbecome() // revert to initial behavior
      context.current.run should be(1)
    }
  }
}
