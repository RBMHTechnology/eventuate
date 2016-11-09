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

package com.rbmhtechnology.eventuate

import java.io.File

import akka.actor._
import akka.testkit._
import com.rbmhtechnology.eventuate.EndpointFilters.NoFilters
import com.rbmhtechnology.eventuate.ReplicationEndpoint._
import com.rbmhtechnology.eventuate.log._
import com.typesafe.config._
import org.apache.commons.io.FileUtils
import org.scalatest._

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration._

trait LocationCleanup extends Suite with BeforeAndAfterAll {
  def config: Config

  def storageLocations: List[File]

  override def beforeAll(): Unit = {
    storageLocations.foreach(FileUtils.deleteDirectory)
    storageLocations.foreach(_.mkdirs())
  }

  override def afterAll(): Unit = {
    storageLocations.foreach(FileUtils.deleteDirectory)
  }
}

object SingleLocationSpec {
  val ErrorSequenceNr = -1L
  val IgnoreDeletedSequenceNr = -2L

  trait TestEventLog[A <: EventLogState] extends EventLog[A] {
    override def currentSystemTime: Long = 0L

    abstract override def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int): Future[BatchReadResult] =
      if (fromSequenceNr == ErrorSequenceNr) Future.failed(IntegrationTestException) else super.read(fromSequenceNr, toSequenceNr, max)

    abstract override def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int, aggregateId: String): Future[BatchReadResult] =
      if (fromSequenceNr == ErrorSequenceNr) Future.failed(IntegrationTestException) else super.read(fromSequenceNr, toSequenceNr, max, aggregateId)

    abstract override def replicationRead(fromSequenceNr: Long, toSequenceNr: Long, max: Int, scanLimit: Int, filter: (DurableEvent) => Boolean): Future[BatchReadResult] =
      if (fromSequenceNr == ErrorSequenceNr) Future.failed(IntegrationTestException) else super.replicationRead(fromSequenceNr, toSequenceNr, max, scanLimit, filter)

    abstract override def write(events: Seq[DurableEvent], partition: Long, clock: EventLogClock): Unit =
      if (events.map(_.payload).contains("boom")) throw IntegrationTestException else super.write(events, partition, clock)

    override private[eventuate] def adjustFromSequenceNr(seqNr: Long) = seqNr match {
      case ErrorSequenceNr         => seqNr
      case IgnoreDeletedSequenceNr => 0
      case s                       => super.adjustFromSequenceNr(s)
    }
  }
}

trait SingleLocationSpec extends LocationCleanup with BeforeAndAfterEach {
  private var _logCtr: Int = 0

  implicit val system: ActorSystem

  override def beforeEach(): Unit = {
    super.beforeEach()
    generateLogId()
  }

  override def afterEach(): Unit = {
    system.stop(log)
    super.afterEach()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  def config: Config =
    system.settings.config

  def batching: Boolean =
    true

  def generateLogId(): Unit =
    _logCtr += 1

  def logId: String =
    _logCtr.toString

  def log: ActorRef
}

object MultiLocationConfig {
  def create(port: Int = 0, customConfig: Config = ConfigFactory.empty()): Config = {
    val defaultConfig = ConfigFactory.parseString(
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.enabled-transports = ["akka.remote.netty.tcp"]
         |akka.remote.netty.tcp.hostname = "127.0.0.1"
         |akka.remote.netty.tcp.port = ${port}
         |akka.remote.retry-gate-closed-for = 300ms
         |akka.test.single-expect-default = 20s
         |akka.loglevel = "ERROR"
         |
         |eventuate.log.write-batch-size = 3
         |eventuate.log.replication.retry-delay = 1s
         |eventuate.log.replication.remote-read-timeout = 1s
         |eventuate.log.replication.failure-detection-limit = 3s
         |eventuate.snapshot.filesystem.dir = target/test-snapshot
       """.stripMargin)

    customConfig.withFallback(defaultConfig)
  }
}

trait MultiLocationSpec extends LocationCleanup with BeforeAndAfterEach {
  private var locations: List[Location] = Nil
  private var ctr: Int = 0

  override def beforeEach(): Unit =
    ctr += 1

  override def afterEach(): Unit =
    locations.foreach(location => Await.result(location.terminate(), 10.seconds))

  override def config =
    MultiLocationConfig.create(port = 0, providerConfig)

  def providerConfig: Config

  def logFactory: String => Props

  def location(name: String, customPort: Int = 0, customConfig: Config = ConfigFactory.empty(), logFactory: String => Props = logFactory): Location = {
    val location = new Location(locationId(name), logFactory, customPort, customConfig.withFallback(providerConfig))
    registerLocation(location)
    location
  }

  def locationId(locationName: String): String =
    s"${locationName}_${ctr}"

  private def registerLocation(location: Location): Location = {
    locations = location :: locations
    location
  }
}

class Location(val id: String, logFactory: String => Props, customPort: Int, customConfig: Config) {
  import Location._

  val system: ActorSystem =
    ActorSystem(ReplicationConnection.DefaultRemoteSystemName, MultiLocationConfig.create(customPort, customConfig))

  val port: Int =
    if (customPort != 0) customPort else system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress.port.get

  val probe: TestProbe =
    new TestProbe(system)

  def listener(eventLog: ActorRef): EventListener =
    new EventListener(id, eventLog)(system)

  def endpoint(
    logNames: Set[String],
    connections: Set[ReplicationConnection],
    endpointFilters: EndpointFilters = NoFilters,
    applicationName: String = DefaultApplicationName,
    applicationVersion: ApplicationVersion = DefaultApplicationVersion,
    activate: Boolean = true): ReplicationEndpoint = {

    val endpoint = new ReplicationEndpoint(id, logNames, logFactory, connections, endpointFilters, applicationName, applicationVersion)(system)
    if (activate) endpoint.activate()
    endpoint
  }

  def terminate(): Future[Terminated] =
    system.terminate()
}

object Location {
  class EventListener(locationId: String, eventLog: ActorRef)(implicit system: ActorSystem) extends TestProbe(system, s"EventListener-$locationId") { listener =>
    private class EventListenerView extends EventsourcedView {
      override val id =
        testActorName

      override val eventLog =
        listener.eventLog

      override def onCommand =
        Actor.emptyBehavior

      override def onEvent = {
        case event => ref ! event
      }
    }

    system.actorOf(Props(new EventListenerView))

    def waitForMessage(msg: Any): Any =
      fishForMessage(hint = msg.toString) {
        case `msg` => true
        case _     => false
      }
  }
}
