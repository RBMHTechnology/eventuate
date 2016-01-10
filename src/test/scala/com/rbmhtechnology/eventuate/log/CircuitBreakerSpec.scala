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

package com.rbmhtechnology.eventuate.log

import akka.actor._
import akka.pattern.ask
import akka.testkit._
import akka.util.Timeout

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.typesafe.config.ConfigFactory

import org.scalatest._

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration._

object CircuitBreakerSpec {
  implicit val timeout = Timeout(3.seconds)

  implicit class AwaitHelper[T](awaitable: Awaitable[T]) {
    def await: T = Await.result(awaitable, timeout.duration)
  }

  class TestLog extends Actor {
    def receive = {
      case msg => sender() ! s"re-$msg"
    }
  }
}

class CircuitBreakerSpec extends TestKit(ActorSystem("test", ConfigFactory.parseString("eventuate.log.circuit-breaker.open-after-retries = 1")))
  with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  import CircuitBreakerSpec._
  import CircuitBreaker._

  private var breaker: ActorRef = _
  private var probe: TestProbe = _

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  override def beforeEach(): Unit = {
    probe = TestProbe()
    breaker = system.actorOf(Props(new CircuitBreaker(Props(new TestLog), batching = false)))
  }

  "A circuit breaker" must {
    "be closed after initialization" in {
      breaker.ask("a").await should be("re-a")
    }
    "be closed after initial failure" in {
      breaker ! ServiceFailed(0)
      breaker.ask("a").await should be("re-a")
    }
    "open after first failed retry" in {
      breaker ! ServiceFailed(1)
      intercept[UnavailableException] {
        breaker.ask("a").await
      }
    }
    "close again after service success" in {
      breaker ! ServiceFailed(1)
      intercept[UnavailableException] {
        breaker.ask("a").await
      }
      breaker ! ServiceNormal
      breaker.ask("a").await should be("re-a")
    }
    "close again after service initialization" in {
      breaker ! ServiceFailed(1)
      intercept[UnavailableException] {
        breaker.ask("a").await
      }
      breaker ! ServiceInitialized
      breaker.ask("a").await should be("re-a")
    }
    "reply with a special failure message on Write requests if open" in {
      val events = Seq(DurableEvent("a", "emitter"))
      breaker ! ServiceFailed(1)
      breaker ! Write(events, probe.ref, probe.ref, 1, 2)
      probe.expectMsg(WriteFailure(events, Exception, 1, 2))
      probe.sender() should be(probe.ref)
    }
  }
}
