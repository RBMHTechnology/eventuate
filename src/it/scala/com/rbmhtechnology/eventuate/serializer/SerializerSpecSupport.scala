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

package com.rbmhtechnology.eventuate.serializer

import akka.actor._
import akka.serialization.SerializationExtension
import akka.testkit.TestProbe

import com.typesafe.config.Config

import scala.concurrent.Await
import scala.concurrent.duration._

object SerializerSpecSupport {
  class SenderActor(receiver: ActorSelection) extends Actor {
    def receive = {
      case msg => receiver ! msg
    }
  }

  class ReceiverActor(probe: ActorRef) extends Actor {
    def receive = {
      case msg => probe ! msg
    }
  }
}

class SerializerSpecSupport(config1: Config, config2: Config) {
  import SerializerSpecSupport._

  val system1 = ActorSystem("test-system-1", config1)
  val system2 = ActorSystem("test-system-2", config2)

  val serialization1 = SerializationExtension(system1)
  val serialization2 = SerializationExtension(system2)

  val receiverProbe = new TestProbe(system2)
  val receiverActor = system2.actorOf(Props(new ReceiverActor(receiverProbe.ref)), "receiver")
  val senderActor = system1.actorOf(Props(new SenderActor(system1.actorSelection("akka.tcp://test-system-2@127.0.0.1:2553/user/receiver"))))

  def shutdown(): Unit = {
    Await.result(system1.terminate(), 10.seconds)
    Await.result(system2.terminate(), 10.seconds)
  }
}
