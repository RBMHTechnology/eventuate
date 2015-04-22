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

import akka.actor._
import akka.event.Logging
import akka.persistence.kafka.MetadataConsumer.Broker
import akka.persistence.kafka._
import akka.serialization.SerializationExtension

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.log.AggregateRegistry

import org.apache.kafka.clients.producer._

import scala.collection.immutable.Seq
import scala.concurrent.{Future, Promise}
import scala.util._

class KafkaEventLog(id: String) extends Actor with Stash with MetadataConsumer {
  import KafkaEventLog._

  val serialization = SerializationExtension(context.system)
  val eventStream = context.system.eventStream
  val syslog = Logging(context.system, this)

  val kafka = Kafka(context.system)
  val config = kafka.config
  val producer = kafka.producer

  // registry for event-sourced actors/views that have no aggregate id defined
  var defaultRegistry: Set[ActorRef] = Set.empty
  // registry for event-sourced actors/views that have an aggregate id defined
  var aggregateRegistry: AggregateRegistry = AggregateRegistry()

  var sequenceNr = 0L

  val initializing: Receive = {
    case Scan =>
      import kafka.readDispatcher
      Future {
        eventIterator(1L).toStream.lastOption match {
          case Some(event) => self ! Scanned(event.sequenceNr)
          case None        => self ! Scanned(0L)
        }
      }
    case Scanned(snr) =>
      sequenceNr = snr
      unstashAll()
      context.become(initialized)
    case cmd =>
      stash()
  }

  val initialized: Receive = {
    case Replay(from, requestor, None, iid) =>
      defaultRegistry = defaultRegistry + context.watch(requestor)

      import kafka.readDispatcher
      replayAsync(from)(event => requestor ! Replaying(event, iid) ) onComplete {
        case Success(_) => requestor ! ReplaySuccess(iid)
        case Failure(e) => requestor ! ReplayFailure(e, iid); syslog.error(e, "Replay failure")
      }
    case Replay(from, requestor, emitterAggregateId, iid) =>
      aggregateRegistry = aggregateRegistry.add(context.watch(requestor), emitterAggregateId.get)

      import kafka.readDispatcher
      replayAsync(from)(event => if (event.emitterAggregateId == emitterAggregateId) requestor ! Replaying(event, iid)) onComplete {
        case Success(_) => requestor ! ReplaySuccess(iid)
        case Failure(e) => requestor ! ReplayFailure(e, iid); syslog.error(e, "Replay failure")
      }
    case Write(events, eventsSender, requestor, iid) =>
      val updated = prepareWrite(events)

      val dreg = defaultRegistry
      val areg = aggregateRegistry

      // -----------------------------------------------------------
      //  IMPORTANT: context.dispatcher must be a pinned dispatcher
      // -----------------------------------------------------------

      import context.dispatcher

      writeAsync(updated) onComplete {
        case Success(_) =>
          pushWriteSuccess(dreg, areg, updated, eventsSender, requestor, iid)
          publishUpdateNotification(updated)
        case Failure(e) =>
          pushWriteFailure(updated, eventsSender, requestor, iid, e)
      }
    case Terminated(requestor) =>
      aggregateRegistry.aggregateId(requestor) match {
        case Some(aggregateId) => aggregateRegistry = aggregateRegistry.remove(requestor, aggregateId)
        case None              => defaultRegistry = defaultRegistry - requestor
      }
  }

  def receive = initializing

  // ----------------------------------------------------------------------------------
  //  Writes
  // ----------------------------------------------------------------------------------

  def writeAsync(events: Seq[DurableEvent]): Future[Seq[DurableEvent]] = {
    val promise = Promise[Seq[DurableEvent]]
    producer.send(new ProducerRecord(id, config.partition, "", DurableEventBatch(events)), new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
        if (exception == null) promise.success(events) else promise.failure(exception)
    })
    promise.future
  }

  // ----------------------------------------------------------------------------------
  //  Reads
  // ----------------------------------------------------------------------------------

  def replayAsync(fromSequenceNr: Long)(f: DurableEvent => Unit): Future[Unit] = {
    import kafka.readDispatcher
    Future(replay(fromSequenceNr)(f))
  }

  def replay(fromSequenceNr: Long)(f: DurableEvent => Unit): Unit =
    eventIterator(fromSequenceNr).foreach(f)

  def eventIterator(fromSequenceNr: Long): Iterator[DurableEvent] =
    leaderFor(id, kafka.brokers) match {
      case Some(Broker(host, port)) => eventIterator(host, port, id, 0L /* FIXME: compute offset from sequence number */).filter(_.sequenceNr >= fromSequenceNr)
      case None => Iterator.empty
    }

  def eventIterator(host: String, port: Int, topic: String, offset: Long): Iterator[DurableEvent] = {
    new MessageIterator(host, port, topic, config.partition, offset, config.consumerConfig).map { m =>
      serialization.deserialize(MessageUtil.payloadBytes(m), classOf[DurableEventBatch]).get
    }.flatMap(_.events)
  }

  // ----------------------------------------------------------------------------------
  //  Notifications
  // ----------------------------------------------------------------------------------

  def publishUpdateNotification(events: Seq[DurableEvent] = Seq()): Unit =
    if (events.nonEmpty) eventStream.publish(Updated(events))

  def pushWriteSuccess(defaultRegistry: Set[ActorRef], aggregateRegistry: AggregateRegistry, events: Seq[DurableEvent], eventsSender: ActorRef, requestor: ActorRef, instanceId: Int): Unit =
    events.foreach { event =>
      requestor.tell(WriteSuccess(event, instanceId), eventsSender)
      // in any case, notify all default subscribers (except requestor)
      defaultRegistry.foreach(r => if (r != requestor) r ! Written(event))
      // notify subscribers with matching aggregate id (except requestor)
      for {
        aggregateId <- event.routingDestinations
        aggregate <- aggregateRegistry(aggregateId) if aggregate != requestor
      } aggregate ! Written(event)
    }

  def pushWriteFailure(events: Seq[DurableEvent], eventsSender: ActorRef, requestor: ActorRef, instanceId: Int, cause: Throwable): Unit =
    events.foreach { event =>
      requestor.tell(WriteFailure(event, cause, instanceId), eventsSender)
    }

  override def preStart(): Unit = {
    self ! Scan
  }

  private def prepareWrite(events: Seq[DurableEvent]): Seq[DurableEvent] = {
    events.map { event =>
      val snr = nextSequenceNr()
      event.copy(
        sourceLogId = id,
        targetLogId = id,
        sourceLogSequenceNr = snr,
        targetLogSequenceNr = snr)
    }
  }

  private def prepareReplicate(events: Seq[DurableEvent]): Seq[DurableEvent] = {
    events.map { event =>
      val snr = nextSequenceNr()
      event.copy(
        sourceLogId = event.targetLogId,
        targetLogId = id,
        sourceLogSequenceNr = event.targetLogSequenceNr,
        targetLogSequenceNr = snr)
    }
  }

  private def nextSequenceNr(): Long = {
    sequenceNr += 1L
    sequenceNr
  }
}

object KafkaEventLog extends App {
  private case object Scan
  private case class Scanned(sequenceNr: Long)

  def props(id: String): Props =
    Props(new KafkaEventLog(id)).withDispatcher("eventuate.log.kafka.write-dispatcher")
}
