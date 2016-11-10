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

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.Props
import akka.testkit.TestProbe

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.crdt.CRDTService.ValueUpdated
import com.rbmhtechnology.eventuate.log._
import com.rbmhtechnology.eventuate.log.leveldb._
import com.typesafe.config.ConfigFactory

import org.scalatest._

import scala.collection.immutable.Seq
import scala.concurrent.{ ExecutionContext, Future }

object CRDTChaosSpecLeveldb {
  val crdtId = "1"

  def randomNr(): String =
    ThreadLocalRandom.current.nextInt(1, 10).toString
}

class CRDTChaosSpecLeveldb extends WordSpec with Matchers with MultiLocationSpecLeveldb {
  import ReplicationIntegrationSpec.replicationConnection
  import CRDTChaosSpecLeveldb._

  class TestEventLog(id: String) extends LeveldbEventLog(id, "log-test") {
    override def write(events: Seq[DurableEvent], partition: Long, clock: EventLogClock): Unit =
      if (events.map(_.payload).contains(ValueUpdated(AddOp(randomNr())))) throw IntegrationTestException else super.write(events, partition, clock)
  }

  val customConfig = ConfigFactory.parseString(
    """
      |eventuate.log.write-batch-size = 3
      |eventuate.log.replication.retry-delay = 100ms
    """.stripMargin)

  override val logFactory: String => Props =
    id => logProps(id)

  def logProps(logId: String, batching: Boolean = true): Props = {
    val logProps = Props(new TestEventLog(logId)).withDispatcher("eventuate.log.dispatchers.write-dispatcher")
    if (batching) Props(new BatchingLayer(logProps)) else logProps
  }

  def service(endpoint: ReplicationEndpoint): (ORSetService[String], TestProbe) = {
    implicit val system = endpoint.system

    val probe = TestProbe()
    val service = new ORSetService[String](endpoint.id, endpoint.logs("L1")) {
      val startCounter = new AtomicInteger()
      val stopCounter = new AtomicInteger()

      override private[crdt] def onChange(crdt: ORSet[String], operation: Any): Unit = {
        operation match {
          case AddOp(entry: String) if entry.startsWith("start") => startCounter.incrementAndGet()
          case AddOp(entry: String) if entry.startsWith("stop") => stopCounter.incrementAndGet()
          case _ =>
        }

        if (startCounter.get == 4) {
          probe.ref ! "started"
          startCounter.set(0)
        }

        if (stopCounter.get == 4) {
          probe.ref ! crdt.value.filterNot(s => s.startsWith("start") || s.startsWith("stop"))
          stopCounter.set(0)
        }
      }
    }

    (service, probe)
  }

  "A replicated ORSet" must {
    "converge under concurrent updates and write failures" in {
      val numUpdates = 100

      val locationA = location("A", customConfig = customConfig)
      val locationB = location("B", customConfig = customConfig)
      val locationC = location("C", customConfig = customConfig)
      val locationD = location("D", customConfig = customConfig)

      val endpointA = locationA.endpoint(Set("L1"), Set(replicationConnection(locationB.port), replicationConnection(locationC.port), replicationConnection(locationD.port)))
      val endpointB = locationB.endpoint(Set("L1"), Set(replicationConnection(locationA.port)))
      val endpointC = locationC.endpoint(Set("L1"), Set(replicationConnection(locationA.port)))
      val endpointD = locationD.endpoint(Set("L1"), Set(replicationConnection(locationA.port)))

      val (serviceA, probeA) = service(endpointA)
      val (serviceB, probeB) = service(endpointB)
      val (serviceC, probeC) = service(endpointC)
      val (serviceD, probeD) = service(endpointD)

      serviceA.add(crdtId, s"start-${serviceA.serviceId}")
      serviceB.add(crdtId, s"start-${serviceB.serviceId}")
      serviceC.add(crdtId, s"start-${serviceC.serviceId}")
      serviceD.add(crdtId, s"start-${serviceD.serviceId}")

      probeA.expectMsg("started")
      probeB.expectMsg("started")
      probeC.expectMsg("started")
      probeD.expectMsg("started")

      def singleUpdate(service: ORSetService[String])(implicit executionContext: ExecutionContext): Future[Unit] = {
        Future.sequence(List(
          service.add(crdtId, randomNr()).recover { case _ => () },
          service.remove(crdtId, randomNr())
        )).map(_ => ())
      }

      def batchUpdate(service: ORSetService[String]): Future[Unit] = {
        import scala.concurrent.ExecutionContext.Implicits.global
        1.to(numUpdates).foldLeft(Future.successful(())) {
          case (acc, _) => acc.flatMap(_ => singleUpdate(service))
        }.flatMap(_ => service.add(crdtId, s"stop-${service.serviceId}").map(_ => ()))
      }

      batchUpdate(serviceA)
      batchUpdate(serviceB)
      batchUpdate(serviceC)
      batchUpdate(serviceD)

      val sA = probeA.expectMsgClass(classOf[Set[_]])
      val sB = probeB.expectMsgClass(classOf[Set[_]])
      val sC = probeC.expectMsgClass(classOf[Set[_]])
      val sD = probeD.expectMsgClass(classOf[Set[_]])

      sA should be(sB)
      sA should be(sC)
      sA should be(sD)
    }
  }
}
