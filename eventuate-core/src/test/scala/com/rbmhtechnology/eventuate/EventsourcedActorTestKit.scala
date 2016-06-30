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

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.TestActorRef
import akka.testkit.TestKit
import akka.testkit.TestProbe
import akka.util.Timeout
import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.EventSourcedActorTestKit.Persist
import com.rbmhtechnology.eventuate.EventsourcedActor
import com.rbmhtechnology.eventuate.EventsourcingProtocol.WriteSuccess
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.FeatureSpecLike
import org.scalatest.Matchers
import org.scalatest.words.ShouldVerb

import scala.concurrent.Await
import scala.concurrent.Awaitable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object EventSourcedActorTestKit {

  /** actor system all actors under test run in */
  val actorSystem = ActorSystem("test", ConfigFactory.parseString("eventuate.log.write-timeout = 1s"))

  /** Simulate persisting 'event'. Actors this is sent to must implement [[ExplicitOnEventCall]] */
  case class Persist(event: Any)

}

/**
  * Actor tests need to implement this trait as well as [[akka.testkit.TestKit]]. A default [[ActorSystem]] is given
  * by companion object.  Example for usage of this trait: <br>
  *
  * {{{ class MyActorTest extends TestKit(ActorTest.actorSystem)  with ActorTest with GivenWhenThen { ... } }}}
  *
  * Usage: <br>
  * - instantiate actors to test via initEventSourcedActorAsTestActorRef or initEventSourcedActor. Former returns an
  * [[TestActorRef]] and thus allows to test _.underlyingActor. <br>
  * - either tell returned [[ActorRef]]/[[TestActorRef]] a command or explicitly call onEvent via #callOnEvent <br>
  * - query 'peristed' events via #expectEvent or #expectEvents <br><br>
  *
  * All actor's under test must have #eventLog as their eventLog. Provided event log can be viewed as a stash. Events
  * persisted by actors using provided eventLog are stashed. By calling #expectEvent or #expectEvents one or several
  * events are popped from stash while asserting equality to user defined output. <br>
  * By calling #cleanEventLog the whole stash can be cleaned. <br>
  * If eventLogs content should not be asserted, but simply queried #queryEventlog can be used. Cleans the whole stash
  * as well.
  *
  * fixme: move to reasonable package.
  */
trait EventSourcedActorTestKit
  extends FeatureSpecLike with BeforeAndAfterAll with BeforeAndAfterEach with ShouldVerb with Matchers {
  this: TestKit =>

  import EventSourcedActorTestKit._

  /** defines duration to wait, when expecting events on event log */
  private val expectEventTimeOut = 100 millisecond

  /** defines how long to wait, when expecting responses in ask patterns. */
  implicit val askPatternTimeOut = Timeout(FiniteDuration(60, "seconds"))

  // ec for future mapping
  implicit val ec = system.dispatcher

  /** TestProbe for event log */
  var logProbe: TestProbe = _

  /** cleans Eventlog before each test */
  override def beforeEach(): Unit = {
    logProbe = TestProbe()
  }

  /** shuts down actor system, once tests are finished */
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  /** get [[ActorRef]] of event log. Must be used as event log for actor's under test. */
  def eventlog = logProbe.ref

  // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  // +++++++++++ EventsourcedActor initialization +++++++++++
  // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  /** starts given EventSourcedActor and  returns an [[TestActorRef]] */
  def initEventSourcedActorAsTestActorRef[T <: EventsourcedActor : ClassTag](creator: ⇒ T) = {
    val testActor = TestActorRef[T](Props(creator))
    expectEABootMsgExchange(testActor)
    testActor
  }

  /** start an EventSourcedActor and return its actorRef */
  def initEventSourcedActor[T <: EventsourcedActor : ClassTag](creator: ⇒ T) = {
    val actor = system.actorOf(Props(creator))
    expectEABootMsgExchange(actor)
    actor
  }

  /**
    * When an [[EventsourcedActor]] is started it persists events and consumes answers. This method simulates this
    * behavior. <br><br>
    *
    * Warning: this method will fail, if given [[ActorRef]] is not an [[EventsourcedActor]].
    */
  def expectEABootMsgExchange(actorRef: ActorRef) = {
    val loadSnapshot = logProbe.expectMsgClass(classOf[LoadSnapshot])
    val actorId = loadSnapshot.emitterId
    val instanceId = loadSnapshot.instanceId
    logProbe.sender() ! LoadSnapshotSuccess(None, instanceId)
    logProbe.expectMsg(Replay(1L, Some(actorRef), instanceId))
    logProbe.sender() ! ReplaySuccess(Nil, 0L, instanceId)
  }

  // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  // +++++++++++++ sending and expecting events +++++++++++++
  // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  /**
    * makes given actor end up in onEvent. Eventuate-specific events are popped from stash of logProbes events via
    * expectEvent.
    *
    * NOTE: actor ref'd by ActorRef MUST implement {{ExplicitOnEventCall}}
    */
  def callOnEvent(actor: ActorRef, event: Any, max: FiniteDuration = expectEventTimeOut) = {
    actor ! Persist(event)
    expectEvent(event, max)
  }

  /**
    * checks if given events were written to eventLog within one [[Write]]. Fails if not.
    *
    */
  def expectEvents[T](expected: Seq[T], max: FiniteDuration = expectEventTimeOut) {
    processEventlog(Some(expected), max)
  }

  /**
    * checks if given events were written to eventLog, each in seperate [[Write]]. Fails if any of given events was
    * not written, or order of given events differs from persisted events.
    *
    */
  def expectEventsOneAtATime[T](expected: Seq[T], max: FiniteDuration = expectEventTimeOut) {
    expected foreach { ev =>
      expectEvent(ev, max)
    }
  }

  /** checks if given event was written to eventLog. fails, if not (see expectEvents) */
  def expectEvent[T](expected: T, max: FiniteDuration = expectEventTimeOut) {
    processEventlog(Some(Seq(expected)), max)
  }


  // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  // ++++++++++++ cleaning one or multiple events +++++++++++
  // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++


  /** Pops one event from event log without returning it */
  def popFromEventLog(max: FiniteDuration = expectEventTimeOut) {
    processEventlog(None, max)
  }

  /**
    * Pops all events from event log. Can be used to simply clean current event log in order not to explicitly expect
    * messages that are not of interest and furthermore to 'unblock' actor, which waits for [[WriteSuccess]] messages
    * before doing anything but persisting again. <br>
    *
    * Note: handle with care. In order to be sure that sending events to logProbe is not blocked by waiting for
    * WriteSuccess in logProbe (which happens if events to be persisted are stashed in persisting actor), logProbe
    * has to be queried until no more messages can be expected (see processEventlog). Which means this takes at least
    * max to terminate.
    */
  def cleanEventLog(max: FiniteDuration = expectEventTimeOut): Unit = {

    while (Try(processEventlog(None, max)).isInstanceOf[Success[Seq[Any]]]) {

    }
  }

  // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  // +++++++++++ Querying events currently stashed ++++++++++
  // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  /** Returns single event currently stashed in event log (which is than popped) */
  def queryNextEvent(max: FiniteDuration = expectEventTimeOut) = processEventlog(None, max)

  /**
    * Cleans all events from event log. <br>
    *
    * Note: handle with care. In order to be sure that sending events to logProbe is not blocked by waiting for
    * WriteSuccess in logProbe (which happens if events to be persisted are stashed in persisting actor), logProbe
    * has to be queried until no more messages can be expected (see processEventlog). Which means this takes at least
    * max to terminate.
    *
    * @param max
    * @return
    */
  def queryAllEvents(max: FiniteDuration = expectEventTimeOut): Seq[Any] = {

    var events: Seq[Any] = Seq.empty
    var continue = true

    while (continue) {
      Try(processEventlog(None, max)) match {
        case Success(nextEvents) =>
          events = events ++ nextEvents
        case Failure(_) =>
          continue = false
      }
    }

    events
  }

  /**
    * Simulates Eventlog behaviour. Expects an [[Write]] on 'logProbe'. If 'expected' event is defined equality to
    * written event(s) is asserted. Sender of event is told about [[WriteSuccess]] allowing him to continue processing.
    *
    * @return events that were contained by [[Write]]
    */
  private def processEventlog[T](expected: Option[Seq[T]], max: FiniteDuration = expectEventTimeOut) = {
    val write = logProbe.expectMsgClass(max, classOf[Write])
    val instanceId = write.instanceId
    val persistedEvents = write.events.map(_.payload)

    if (expected.isDefined) {
      persistedEvents should be(expected.get)
    }

    logProbe.sender() ! WriteSuccess(write.events, write.correlationId, instanceId)

    persistedEvents
  }

  // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  // ++++++++++++ convenience for request sending +++++++++++
  // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++

  /** Asserts that given future responses with expected response within timeout */
  def expectCommand[T](future: Awaitable[T], response: T, timeout: FiniteDuration = askPatternTimeOut.duration) = {
    val awaited = Await.result(future, askPatternTimeOut.duration)
    assert(awaited === response)
  }
}

/**
  * enables events to be delegated from command to eventHandler, thus simulating an onEvent receive of given event. <br>
  *
  * In order to use this functionality the Actor under test needs to be extended like:
  *
  * {{{class MyActorUnderTest extends ActorUnderTest with ExplicitOnEventCall {
  *   override def onCommand: Receive = super.onCommand orElse persistRecieve
  * }
  * }}}
  */
trait ExplicitOnEventCall {
  this: EventsourcedActor =>

  val persistRecieve: Receive = {
    case Persist(event) =>
      persist(event) {
        case Success(e) =>
        case Failure(ex) =>
      }
  }
}
