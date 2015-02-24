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

import akka.actor._
import akka.testkit._

import com.typesafe.config.ConfigFactory

import org.scalatest._

import ReplicationEndpoint._
import ReplicationServerFailureDetector._
import ReplicationFailureDetectorSpec._

object ReplicationFailureDetectorSpec {
  val config = ConfigFactory.parseString("log.replication.failure-detection-limit = 1s")
}

class ReplicationFailureDetectorSpec extends TestKit(ActorSystem("test", config)) with WordSpecLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  var failureDetector: ActorRef = _

  override def beforeEach(): Unit = {
    failureDetector = system.actorOf(Props(new ReplicationServerFailureDetector(InstanceId("A", 0), "L1")))
  }

  override def afterEach(): Unit = {
    system.stop(failureDetector)
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A replication failure detector" must {
    "publish availability events to the actor system's event stream" in {
      val probe = TestProbe()

      system.eventStream.subscribe(probe.ref, classOf[Available])
      system.eventStream.subscribe(probe.ref, classOf[Unavailable])

      failureDetector ! Ping
      probe.expectMsg(Available("A", "L1"))
      // time passes ...
      probe.expectMsg(Unavailable("A", "L1"))
      failureDetector ! Ping
      failureDetector ! Ping // second Tick within limit doesn't publish another Available
      probe.expectMsg(Available("A", "L1"))
      // time passes ...
      probe.expectMsg(Unavailable("A", "L1"))
    }
  }
}
