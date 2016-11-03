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

package com.rbmhtechnology.eventuate.adapter.stream

import akka.actor.ActorSystem
import akka.pattern.AskTimeoutException
import akka.stream.scaladsl._
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream._
import akka.testkit._

import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.VectorTime
import com.typesafe.config.ConfigFactory

import org.scalatest._

object ProgressSourceSpec {
  val SrcLogId = "A"

  val config = ConfigFactory.parseString("eventuate.log.read-timeout = 500ms")
}

class ProgressSourceSpec extends TestKit(ActorSystem("test", ProgressSourceSpec.config)) with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  import ProgressSourceSpec._

  implicit val materializer: Materializer =
    ActorMaterializer()

  private var log: TestProbe = _
  private var snk: TestSubscriber.Probe[Long] = _

  override def beforeEach(): Unit = {
    log = TestProbe()
    snk = Source.fromGraph(ProgressSource(SrcLogId, log.ref)).toMat(TestSink.probe[Long])(Keep.right).run()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  "A ProgressSource" must {
    "complete after emitting a stored replication progress" in {
      snk.request(1)
      log.expectMsg(GetReplicationProgress(SrcLogId))
      log.sender() ! GetReplicationProgressSuccess(SrcLogId, 17, VectorTime.Zero)
      snk.expectNext(17)
      snk.expectComplete()
    }
    "fail if replication progress reading fails" in {
      snk.request(1)
      log.expectMsg(GetReplicationProgress(SrcLogId))
      log.sender() ! GetReplicationProgressFailure(TestException)
      snk.expectError(TestException)
    }
    "fail on replication progress reading timeout" in {
      snk.request(1)
      log.expectMsg(GetReplicationProgress(SrcLogId))
      snk.expectError should be(an[AskTimeoutException])
    }
  }
}
