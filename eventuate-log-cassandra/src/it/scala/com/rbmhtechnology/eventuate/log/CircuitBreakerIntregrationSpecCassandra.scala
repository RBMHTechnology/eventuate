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

package com.rbmhtechnology.eventuate.log

import java.util.concurrent.CountDownLatch

import akka.actor._
import akka.pattern.ask
import akka.testkit._
import akka.util.Timeout

import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.log.cassandra._
import com.rbmhtechnology.eventuate.utilities.AwaitHelper
import com.typesafe.config._

import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{ Millis, Seconds, Span }

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration._
import scala.util._
import scala.util.control.NoStackTrace

object CircuitBreakerIntregrationSpecCassandra {
  val config: Config = ConfigFactory.parseString(
    """
      |akka.loglevel = "ERROR"
      |akka.test.single-expect-default = 20s
      |
      |eventuate.log.circuit-breaker.open-after-retries = 2
      |eventuate.log.cassandra.write-retry-max = 3
      |eventuate.log.cassandra.default-port = 9142
      |eventuate.log.cassandra.index-update-limit = 3
      |eventuate.log.cassandra.init-retry-delay = 1s
      |eventuate.snapshot.filesystem.dir = target/test-snapshot
    """.stripMargin)

  case class TestFailureSpec(
    failingAttempts: Int,
    appLatch: CountDownLatch,
    logLatch: CountDownLatch)

  class TestEventLog(id: String, failureSpec: TestFailureSpec) extends CassandraEventLog(id) {
    override private[eventuate] def createEventLogStore(cassandra: Cassandra, logId: String) =
      new TestEventLogStore(cassandra, logId, failureSpec)
  }

  class TestEventLogStore(cassandra: Cassandra, logId: String, failureSpec: TestFailureSpec) extends CassandraEventLogStore(cassandra, logId) {
    import failureSpec._

    var numAttempts = 0

    override def write(events: Seq[DurableEvent], partition: Long): Unit =
      if (numAttempts < failingAttempts && events.map(_.payload).contains("timeout")) {
        numAttempts += 1
        appLatch.countDown()
        throw new TimeoutException("test") with NoStackTrace
      } else if (numAttempts == failingAttempts) {
        logLatch.await()
      } else {
        super.write(events, partition)
      }
  }

  class TestActor(val id: String, val eventLog: ActorRef, override val stateSync: Boolean) extends EventsourcedActor {
    override val aggregateId = Some(id)

    override def onCommand = {
      case s: String =>
        persist(s) {
          case Success(_) => sender() ! s
          case Failure(e) => sender() ! Status.Failure(e)
        }
    }

    override def onEvent = {
      case e: String =>
    }
  }
}

class CircuitBreakerIntregrationSpecCassandra extends TestKit(ActorSystem("test", CircuitBreakerIntregrationSpecCassandra.config))
  with WordSpecLike with Matchers with BeforeAndAfterEach with LocationCleanupCassandra with Eventually {

  import CircuitBreakerIntregrationSpecCassandra._
  import system.dispatcher

  implicit val implicitTimeout = Timeout(10.seconds)

  var log: ActorRef = _
  var logCtr: Int = 0

  override def beforeEach(): Unit = {
    super.beforeEach()
    logCtr += 1
  }

  override def afterEach(): Unit = {
    system.stop(log)
    super.afterEach()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedCassandra.start(EmbeddedCassandra.DefaultCassandraDir)
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  override implicit def patienceConfig =
    PatienceConfig(Span(10, Seconds), Span(100, Millis))

  def config: Config =
    system.settings.config

  def logId: String =
    logCtr.toString

  def logProps(logId: String, failureSpec: TestFailureSpec): Props = {
    val logProps = Props(new TestEventLog(logId, failureSpec)).withDispatcher("eventuate.log.dispatchers.write-dispatcher")
    Props(new CircuitBreaker(logProps, batching = true))
  }

  def createLog(failureSpec: TestFailureSpec = this.failureSpec(-1)): Unit = {
    log = system.actorOf(logProps(logId, failureSpec))
  }

  def failureSpec(failingAttempts: Int): TestFailureSpec =
    TestFailureSpec(failingAttempts, new CountDownLatch(failingAttempts max 0), new CountDownLatch(1))

  "A circuit breaker" must {
    "be initially closed" in {
      createLog()

      write("a", 1L).await should be(1)
      write("b", 2L).await should be(1)
    }
    "remain closed if retry count < 2" in {
      val spec = failureSpec(2)
      createLog(spec)

      val op1 = write("timeout", 1L)
      spec.appLatch.await()
      val op2 = write("b", 2L)
      spec.logLatch.countDown()

      op1.await should be(1)
      op2.await should be(1)
    }
    "open if retry count == 2 and close again after subsequent successful retry" in {
      val spec = failureSpec(3)
      createLog(spec)

      write("timeout", 1L)
      spec.appLatch.await()
      assertUnavailable()
      spec.logLatch.countDown()

      eventually {
        write("b", 2L).await should be(1)
      }
    }
    "stop if the underlying log stops" in {
      val probe = TestProbe()
      val spec = failureSpec(10)
      createLog(spec)

      write("timeout", 1L)

      probe.watch(log)
      probe.expectTerminated(log)
    }
  }

  "A circuit breaker" when {
    "open" must {
      "reply with an UnavailableException failure to an event-sourced actor with stateSync = false in any case" in {
        implicit val implicitTimeout = Timeout(10.seconds)
        val spec = failureSpec(3)
        createLog(spec)

        val actor1 = system.actorOf(Props(new TestActor("1", log, stateSync = false)))
        //val actor2 = system.actorOf(Props(new TestActor("2", log, stateSync = true)))

        request(actor1, "timeout")
        spec.appLatch.await()

        eventually {
          intercept[EventLogUnavailableException] { request(actor1, "a").await }
        }

        spec.logLatch.countDown()

        eventually {
          request(actor1, "b").await should be("b")
        }
      }
      "reply with UnavailableException failure to an event-sourced actor with stateSync = true if that actor was idle at the time of opening" in {
        implicit val implicitTimeout = Timeout(10.seconds)
        val spec = failureSpec(3)
        createLog(spec)

        val actor1 = system.actorOf(Props(new TestActor("1", log, stateSync = false)))
        val actor2 = system.actorOf(Props(new TestActor("2", log, stateSync = true)))

        request(actor1, "timeout")
        spec.appLatch.await()

        eventually {
          intercept[EventLogUnavailableException] { request(actor1, "a").await }
        }

        // The circuit breaker opened when actor2 (stateSync = true) was
        // idle. A later request from that actor will therefore receive
        // an UnavailableException failure reply.
        intercept[EventLogUnavailableException] { request(actor2, "a").await }

        spec.logLatch.countDown()

        eventually {
          request(actor1, "b").await should be("b")
          request(actor2, "b").await should be("b")
        }
      }
    }
  }

  def assertUnavailable(): Unit = {
    val w = ReplicationWrite(Seq(), Map("source" -> ReplicationMetadata(0L, VectorTime.Zero)))
    eventually {
      intercept[EventLogUnavailableException] { log.ask(w).await }
    }
  }

  def write(payload: Any, sequenceNr: Long)(implicit executor: ExecutionContext): Future[Int] = {
    val e = DurableEvent(payload, "emitter", vectorTimestamp = VectorTime("source" -> sequenceNr))
    val w = ReplicationWrite(Seq(e), Map("source" -> ReplicationMetadata(sequenceNr, VectorTime.Zero)))

    log.ask(w) flatMap {
      case s: ReplicationWriteSuccess => Future.successful(s.events.size)
      case f: ReplicationWriteFailure => Future.failed(f.cause)
    }
  }

  def request(actor: ActorRef, command: String): Future[String] =
    actor.ask(command).mapTo[String]
}
