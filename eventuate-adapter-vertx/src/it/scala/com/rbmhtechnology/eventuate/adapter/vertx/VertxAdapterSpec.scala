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

package com.rbmhtechnology.eventuate.adapter.vertx

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.rbmhtechnology.eventuate.adapter.vertx.api.{ EventProducer, VertxAdapterConfig }
import com.rbmhtechnology.eventuate.log.EventLogWriter
import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog
import com.rbmhtechnology.eventuate.utilities._
import com.rbmhtechnology.eventuate.{ LocationCleanupLeveldb, ReplicationEndpoint }
import com.typesafe.config.Config
import org.scalatest.{ BeforeAndAfterAll, MustMatchers, WordSpecLike }

import scala.collection.immutable.Seq

object VertxAdapterSpec {
  case class Event(id: String)

  val Config = TestConfig.withReplayBatchSize(10)
}

class VertxAdapterSpec extends TestKit(ActorSystem("test", VertxAdapterSpec.Config))
  with WordSpecLike with MustMatchers with BeforeAndAfterAll with StopSystemAfterAll with LocationCleanupLeveldb
  with VertxEnvironment with VertxEventBusProbes {

  import VertxAdapterSpec._
  import utilities._

  val logName = "logA"
  val adapterId = "adapter1"
  var storage: ActorStorageProvider = _
  var endpoint: ReplicationEndpoint = _

  override def config: Config = VertxAdapterSpec.Config

  override def beforeAll(): Unit = {
    super.beforeAll()
    storage = new ActorStorageProvider(adapterId)
    endpoint = new ReplicationEndpoint(id = "1", logNames = Set(logName), logFactory = logId => LeveldbEventLog.props(logId), connections = Set())
  }

  "A VertxAdapter" must {
    "read events from an inbound log and deliver them to the Vert.x eventbus" in {
      val log = endpoint.logs(logName)
      val adapterConfig = VertxAdapterConfig()
        .addProducer(EventProducer.fromLog(log)
          .publishTo {
            case _ => endpoint1.address
          }
          .as("adapter1"))
        .registerDefaultCodecFor(classOf[Event])

      val vertxAdapter = VertxAdapter(adapterConfig, vertx, storage)
      val logWriter = new EventLogWriter("w1", endpoint.logs(logName))

      endpoint.activate()
      vertxAdapter.start()

      logWriter.write(Seq(Event("1"))).await.head

      storage.expectRead(replySequenceNr = 0)
      storage.expectWrite(sequenceNr = 1)

      endpoint1.probe.expectVertxMsg(body = Event("1"))

      logWriter.write(Seq(Event("2"))).await

      storage.expectWrite(sequenceNr = 2)

      endpoint1.probe.expectVertxMsg(body = Event("2"))

      logWriter.write(Seq(Event("3"), Event("4"))).await

      storage.expectWriteAnyOf(sequenceNrs = Seq(3, 4))

      endpoint1.probe.expectVertxMsg(body = Event("3"))
      endpoint1.probe.expectVertxMsg(body = Event("4"))
    }
  }
}
