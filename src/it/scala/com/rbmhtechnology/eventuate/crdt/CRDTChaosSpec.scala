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

package com.rbmhtechnology.eventuate.crdt

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.Props
import akka.testkit.TestProbe

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.crdt.CRDTService.ValueUpdated
import com.rbmhtechnology.eventuate.log._
import com.rbmhtechnology.eventuate.log.cassandra._
import com.rbmhtechnology.eventuate.log.leveldb._

import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest._

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}

object CRDTChaosSpec {
  val crdtId = "1"

  def randomNr(): String =
    ThreadLocalRandom.current.nextInt(1, 10).toString
}

abstract class CRDTChaosSpec extends WordSpec with Matchers with ReplicationNodeRegistry {
  import ReplicationIntegrationSpec.replicationConnection
  import CRDTChaosSpec._

  implicit def logFactory: String => Props

  def config =
    ReplicationConfig.create()

  def node(nodeName: String, port: Int, connections: Set[ReplicationConnection]): ReplicationNode =
    register(new ReplicationNode(nodeName, Set(ReplicationEndpoint.DefaultLogName), port, connections, "eventuate.log.write-batch-size = 3"))

  def service(node: ReplicationNode): (ORSetService[String], TestProbe) = {
    implicit val system = node.system

    val probe = TestProbe()
    val service = new ORSetService[String](node.id, node.logs(ReplicationEndpoint.DefaultLogName)) {
      val startCounter = new AtomicInteger()
      val stopCounter =  new AtomicInteger()

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

      val nodeA = node("A", 2552, Set(replicationConnection(2553), replicationConnection(2554), replicationConnection(2555)))
      val nodeB = node("B", 2553, Set(replicationConnection(2552)))
      val nodeC = node("C", 2554, Set(replicationConnection(2552)))
      val nodeD = node("D", 2555, Set(replicationConnection(2552)))

      val (serviceA, probeA) = service(nodeA)
      val (serviceB, probeB) = service(nodeB)
      val (serviceC, probeC) = service(nodeC)
      val (serviceD, probeD) = service(nodeD)

      serviceA.add(crdtId, s"start-${serviceA.serviceId}")
      serviceB.add(crdtId, s"start-${serviceB.serviceId}")
      serviceC.add(crdtId, s"start-${serviceC.serviceId}")
      serviceD.add(crdtId, s"start-${serviceD.serviceId}")

      probeA.expectMsg("started")
      probeB.expectMsg("started")
      probeC.expectMsg("started")
      probeD.expectMsg("started")

      import scala.concurrent.ExecutionContext.Implicits.global

      def singleUpdate(service: ORSetService[String])(implicit executionContext: ExecutionContext): Future[Unit] = {
        Future.sequence(List(
          service.add(crdtId, randomNr()).recover { case _ => () },
          service.remove(crdtId, randomNr())
        )).map(_ => ())
      }

      def batchUpdate(service: ORSetService[String]): Future[Unit] = {
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

class CRDTChaosSpecLeveldb extends CRDTChaosSpec with EventLogCleanupLeveldb {
  import CRDTChaosSpec._

  class TestEventLog(id: String) extends LeveldbEventLog(id, "log-test") {
    override def write(events: Seq[DurableEvent], partition: Long, clock: EventLogClock): Unit =
      if (events.map(_.payload).contains(ValueUpdated(crdtId, AddOp(randomNr())))) throw boom else super.write(events, partition, clock)
  }

  val logFactory: String => Props =
    id => logProps(id)

  def logProps(logId: String, batching: Boolean = true): Props = {
    val logProps = Props(new TestEventLog(logId)).withDispatcher("eventuate.log.dispatchers.write-dispatcher")
    if (batching) Props(new BatchingLayer(logProps)) else logProps
  }
}

class CRDTChaosSpecCassandra  extends CRDTChaosSpec with EventLogCleanupCassandra {
  import CRDTChaosSpec._

  class TestEventLog(id: String) extends CassandraEventLog(id) {
    override def write(events: Seq[DurableEvent], partition: Long, clock: EventLogClock): Unit =
      if (events.map(_.payload).contains(ValueUpdated(crdtId, AddOp(randomNr())))) throw boom else super.write(events, partition, clock)
  }

  override val logFactory: String => Props =
    id => logProps(id)

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(60000)
  }

  override def node(nodeName: String, port: Int, connections: Set[ReplicationConnection]): ReplicationNode = {
    val node = super.node(nodeName, port, connections)
    Cassandra(node.system) // enforce keyspace/schema setup
    node
  }

  def logProps(logId: String, batching: Boolean = true): Props = {
    val logProps = Props(new TestEventLog(logId)).withDispatcher("eventuate.log.dispatchers.write-dispatcher")
    Props(new CircuitBreaker(logProps, batching))
  }
}
