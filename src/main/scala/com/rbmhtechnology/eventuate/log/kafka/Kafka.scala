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

package com.rbmhtechnology.eventuate.log.kafka

import java.util.{Map => JMap}

import akka.actor._
import akka.persistence.kafka.BrokerWatcher
import akka.persistence.kafka.BrokerWatcher.BrokersUpdated
import akka.persistence.kafka.MetadataConsumer.Broker
import akka.serialization.SerializationExtension

import com.rbmhtechnology.eventuate.DurableEventBatch

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization._

object Kafka extends ExtensionId[Kafka] with ExtensionIdProvider {
  def createExtension(system: ExtendedActorSystem): Kafka = new Kafka(system)

  def lookup() = Kafka
}

class Kafka(val system: ExtendedActorSystem) extends Extension { extension =>
  implicit val readDispatcher = system.dispatchers.lookup("eventuate.log.kafka.read-dispatcher")

  val config = new KafkaConfig(system.settings.config.getConfig("eventuate.log.kafka"))
  val watcher = new BrokerWatcher(config.zookeeperConfig, system.actorOf(Props(new BrokerListener)))

  @volatile var brokers: List[Broker] = watcher.start()

  private class BrokerListener extends Actor {
    def receive = {
      // FIXME: replace this evil hack with an updated BrokerWatcher
      case BrokersUpdated(brokers) => extension.brokers = brokers
    }
  }

  val producer = new KafkaProducer[String, DurableEventBatch](
    config.producerConfig(brokers),
    new StringSerializer,
    new EventSerializer(system))

  system.registerOnTermination {
    producer.close()
    watcher.stop()
  }
}

private class EventSerializer(system: ActorSystem) extends Serializer[DurableEventBatch] {
  val serialization = SerializationExtension(system)

  override def configure(configs: JMap[String, _], isKey: Boolean): Unit = ()

  override def serialize(topic: String, data: DurableEventBatch): Array[Byte] =
    serialization.serialize(data).get

  override def close(): Unit = ()
}