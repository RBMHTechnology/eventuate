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

package com.rbmhtechnology.eventuate.serializer

import akka.actor._
import akka.serialization.Serialization
import akka.serialization.SerializationExtension

import com.typesafe.config.Config

import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.duration._

object SerializationContext {
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

class SerializationContext(configs: Config*) {
  val systems: Seq[ActorSystem] = configs.toList.zipWithIndex.map {
    case (config, idx) => ActorSystem(s"test-system-${idx + 1}", config)
  }

  val serializations: Seq[Serialization] =
    systems.map(SerializationExtension(_))

  val ports: Seq[Int] =
    systems.map(port)

  def port(system: ActorSystem): Int =
    system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress.port.get

  def shutdown(): Unit =
    systems.foreach(system => Await.result(system.terminate(), 10.seconds))
}
