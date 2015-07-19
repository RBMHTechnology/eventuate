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

package com.rbmhtechnology.eventuate

import java.util.function.BiConsumer
import java.util.{Optional => JOption}
import java.util.concurrent.atomic.AtomicInteger

import akka.actor._

import com.rbmhtechnology.eventuate.EventsourcingProtocol._

import scala.util._

private object EventsourcedView {
  val instanceIdCounter = new AtomicInteger(0)
}

/**
 * An actor that derives internal state from events stored in an event log. Events are pushed from
 * the `eventLog` actor to this actor and handled with the `onEvent` event handler. An event handler
 * defines how internal state is updated from events.
 *
 * An `EventsourcedView` can also store snapshots of internal state with its `save` method. During
 * (re-)start the latest snapshot saved by this actor (if any) is passed as argument to the `onSnapshot`
 * handler, if the handler is defined at that snapshot. If the `onSnapshot` handler is not defined at
 * that snapshot or is not overridden at all, event replay starts from scratch. Newer events that are
 * not covered by the snapshot are handled by `onEvent` after `onSnapshot` returns.
 *
 * By default, an `EventsourcedView` does not define an `aggregateId`. In this case, the `eventLog`
 * pushes all events to this actor. If it defines an `aggregateId`, the `eventLog` actor only pushes
 * those events that contain that `aggregateId` value in their `routingDestinations` set.
 *
 * An `EventsourcedView` can only consume events from its `eventLog` but cannot produce new events.
 * Commands sent to an `EventsourcedView` during recovery are delayed until recovery completes.
 *
 * @see [[EventsourcedActor]]
 * @see [[DurableEvent]]
 */
trait EventsourcedView extends Actor with ConditionalCommands with Stash with ActorLogging {
  import EventsourcedView._

  type Handler[A] = Try[A] => Unit

  val instanceId: Int = instanceIdCounter.getAndIncrement()

  // -- Transient internal state --
  private var saveRequests: Map[SnapshotMetadata, Handler[SnapshotMetadata]] = Map.empty
  private var _recovering: Boolean = true
  // ------------------------------

  // -- Persistent internal state --
  private var clock: VectorClock = _
  private var _highestReceivedEvent: DurableEvent = _
  private var _lastDeliveredEvent: DurableEvent = _
  // -------------------------------

  /**
   * Internal API.
   */
  private[eventuate] val commandStash = new CommandStash()

  /**
   * Internal API.
   */
  private[eventuate] val eventStash = new EventStash(instanceId)

  /**
   * Internal API.
   */
  private[eventuate] def currentTime: VectorTime =
    clock.currentTime

  /**
   * Internal API.
   */
  private[eventuate] def advanceClock[A](f: VectorClock => A): A = {
    clock = clock.tick()
    f(clock)
  }

  /**
   * Whether to deliver received events in causal order to the event handler. Defaults to `false`
   * and can be overriden by implementations.
   */
  def causalDelivery: Boolean =
    false

  /**
   * Optional aggregate id. It is used for routing [[DurableEvent]]s to event-sourced destinations
   * which can be [[EventsourcedView]]s or [[EventsourcedActor]]s. By default, an event is routed
   * to an event-sourced destination with an undefined `aggregateId`. If a destination's `aggregateId`
   * is defined it will only receive events with a matching aggregate id in
   * [[DurableEvent#destinationAggregateIds]].
   */
  def aggregateId: Option[String] =
    None

  /**
   * Global unique actor id.
   */
  def id: String

  /**
   * Event log actor.
   */
  def eventLog: ActorRef

  /**
   * Command handler.
   */
  def onCommand: Receive

  /**
   * Event handler.
   */
  def onEvent: Receive

  /**
   * Snapshot handler.
   */
  def onSnapshot: Receive =
    Actor.emptyBehavior

  /**
   * Called after recovery successfully completed. Can be overridden by implementations.
   */
  def onRecovered(): Unit =
    ()

  /**
   * Returns `true` if this actor is currently recovering internal state by consuming
   * replayed events from the event log. Returns `false` after recovery completed and
   * the actor switches to consuming live events.
   */
  final def recovering: Boolean =
    _recovering

  /**
   * Internal API.
   */
  private[eventuate] def recovered(): Unit = {
    _recovering = false
    onRecovered()
  }

  /**
   * Sequence number of the last handled event.
   */
  final def lastSequenceNr: Long =
    lastDeliveredEvent.sequenceNr

  /**
   * Wall-clock timestamp of the last handled event.
   */
  final def lastSystemTimestamp: Long =
    lastDeliveredEvent.systemTimestamp

  /**
   * Vector timestamp of the last handled event.
   */
  final def lastVectorTimestamp: VectorTime =
    lastDeliveredEvent.vectorTimestamp

  /**
   * Emitter aggregate id of the last handled event.
   */
  final def lastEmitterAggregateId: Option[String] =
    lastDeliveredEvent.emitterAggregateId

  /**
   * Emitter id of the last handled event.
   */
  final def lastEmitterId: String =
    lastDeliveredEvent.emitterId

  /**
   * Internal API.
   */
  private[eventuate] def lastDeliveredEvent: DurableEvent =
    _lastDeliveredEvent

  /**
   * Internal API.
   */
  private[eventuate] def lastDeliveredEvent_=(event: DurableEvent): Unit =
    _lastDeliveredEvent = event

  /**
   * Internal API.
   */
  private[eventuate] def highestReceivedEvent: DurableEvent =
    _highestReceivedEvent

  /**
   * Internal API.
   */
  private[eventuate] def highestReceivedEvent_=(event: DurableEvent): Unit =
    if (_highestReceivedEvent.sequenceNr < event.sequenceNr) _highestReceivedEvent = event

  /**
   * Asynchronously saves the given `snapshot` and calls `handler` with the generated
   * snapshot metadata. The `handler` can also obtain a reference to the initial message
   * sender via `sender()`.
   */
  def save(snapshot: Any)(handler: Handler[SnapshotMetadata]): Unit = {
    val payload = snapshot match {
      case tree: ConcurrentVersionsTree[_, _] => tree.copy()
      case other                              => other
    }

    val prototype = Snapshot(payload, id, highestReceivedEvent, lastDeliveredEvent, eventStash.events, timestamp = clock.currentTime)
    val metadata = prototype.metadata

    if (saveRequests.contains(metadata)) {
      handler(Failure(new IllegalStateException(s"snapshot with metadata ${metadata} is currently being saved")))
    } else {
      saveRequests += (metadata -> handler)
      val snapshot = capturedSnapshot(prototype)
      eventLog ! SaveSnapshot(snapshot, sender(), self, instanceId)
    }
  }

  /**
   * Internal API.
   */
  private[eventuate] def capturedSnapshot(snapshot: Snapshot): Snapshot =
    snapshot

  /**
   * Internal API.
   */
  private[eventuate] def loadedSnapshot(snapshot: Snapshot): Unit = {
    clock = clock.copy(currentTime = snapshot.timestamp)
    highestReceivedEvent = snapshot.highestReceivedEvent
    lastDeliveredEvent = snapshot.lastDeliveredEvent
    snapshot.stashedEvents.foreach(eventStash.stash)
  }

  /**
   * Internal API.
   */
  private[eventuate] def unhandledMessage(msg: Any): Unit =
    onCommand(msg)

  /**
   * Sends a [[EventsourcingProtocol#LoadSnapshot LoadSnapshot]] command to the event log.
   */
  private def load(): Unit =
    eventLog ! LoadSnapshot(id, self, instanceId)

  /**
   * Sends a [[EventsourcingProtocol#Replay Replay]] command to the event log.
   */
  //#replay
  private def replay(fromSequenceNr: Long = 1L): Unit =
    eventLog ! Replay(fromSequenceNr, self, aggregateId, instanceId)
  //#

  private def initiating: Receive = {
    case LoadSnapshotSuccess(Some(snapshot), iid) => if (iid == instanceId) {
      if (onSnapshot.isDefinedAt(snapshot.payload)) {
        loadedSnapshot(snapshot)
        onSnapshot(snapshot.payload)
        replay(snapshot.metadata.sequenceNr + 1L)
      } else {
        log.warning(s"snapshot loaded (metadata = ${snapshot.metadata}) but onSnapshot doesn't handle it, replaying from scratch")
        replay()
      }
    }
    case LoadSnapshotSuccess(None, iid) => if (iid == instanceId) {
      replay()
    }
    case LoadSnapshotFailure(cause, iid) => if (iid == instanceId) {
      log.error(cause, s"snapshot loading failed, replaying from scratch")
      replay()
    }
    case Replaying(event, iid) => if (iid == instanceId) {
      receiveEvent(event)
    }
    case Unstashed(event, iid) => if (iid == instanceId) {
      receiveEvent(event)
    }
    case ReplaySuccess(iid) => if (iid == instanceId) {
      context.become(initiated)
      conditionChanged(lastVectorTimestamp)
      commandStash.unstashAll()
      recovered()
    }
    case ReplayFailure(cause, iid) => if (iid == instanceId) {
      log.error(cause, s"replay failed, stopping self")
      context.stop(self)
    }
    case other =>
      commandStash.stash()
  }

  private def initiated: Receive = {
    case Written(event) => if (event.sequenceNr > lastSequenceNr) {
      receiveEvent(event)
    }
    case Unstashed(event, iid) => if (iid == instanceId) {
      receiveEvent(event)
    }
    case ConditionalCommand(condition, cmd) =>
      conditionalSend(condition, cmd)
    case SaveSnapshotSuccess(metadata, iid) => if (iid == instanceId) {
      saveRequests.get(metadata).foreach(handler => handler(Success(metadata)))
      saveRequests = saveRequests - metadata
    }
    case SaveSnapshotFailure(metadata, cause, iid) => if (iid == instanceId) {
      saveRequests.get(metadata).foreach(handler => handler(Failure(cause)))
      saveRequests = saveRequests - metadata
    }
    case msg =>
      unhandledMessage(msg)
  }

  private def receiveEvent(event: DurableEvent): Unit = {
    highestReceivedEvent = event
    if (onEvent.isDefinedAt(event.payload)) {
      processEvent(event)
    } else if (event.emitter(id)) {
      // Event not handled but it has been previously emitted by an
      // EventsourcedActor with same id. So we need to recover local
      // time, otherwise, we could end up in the past after recovery ....
      clock = clock.merge(event.vectorTimestamp.localCopy(id))
    }
  }

  private def processEvent(event: DurableEvent): Unit = {
    if (!causalDelivery || clock.covers(event.vectorTimestamp, event.emitterId)) {
      clock = clock.update(event.vectorTimestamp)

      lastDeliveredEvent = event
      onEvent(event.payload)

      if (!recovering) {
        conditionChanged(lastVectorTimestamp)
      }

      // -----------------------------------------------
      // TODO: optimize this inefficient implementation
      //
      // Current non-functional issues:
      // - events are not stashed consistent with partial order (consider topological sorting)
      // - events might be repeatedly stashed and unstashed in some corner cases
      //
      if (!event.emitter(id)) eventStash.unstashAll()
      // -----------------------------------------------

    } else eventStash.stash(event)
  }

  /**
   * Initialization behavior.
   */
  final def receive = initiating

  /**
   * Initiates recovery.
   */
  override def preStart(): Unit = {
    _highestReceivedEvent = DurableEvent(id)
    _lastDeliveredEvent = DurableEvent(id)
    clock = VectorClock(id)
    load()
  }

  /**
   * Unstashes all commands from internal stash and calls `super.preRestart`.
   */
  override def preRestart(reason: Throwable, message: Option[Any]): Unit =
    try commandStash.unstashAll() finally super.preRestart(reason, message)

  /**
   * Unstashes all commands from internal stash and calls `super.postStop`.
   */
  override def postStop(): Unit =
    try commandStash.unstashAll() finally super.postStop()
}

/**
 * Java API.
 *
 * @see [[EventsourcedView]]
 */
abstract class AbstractEventsourcedView(val id: String, val eventLog: ActorRef) extends EventsourcedView {
  private var _onCommand: Receive = Actor.emptyBehavior
  private var _onEvent: Receive = Actor.emptyBehavior
  private var _onSnapshot: Receive = Actor.emptyBehavior

  final override def onCommand: Receive = _onCommand
  final override def onEvent: Receive = _onEvent
  final override def onSnapshot: Receive = _onSnapshot

  override def aggregateId: Option[String] =
    Option(getAggregateId.orElse(null))

  /**
   * Optional aggregate id. Not defined by default.
   */
  def getAggregateId: JOption[String] =
    JOption.empty()

  /**
   * Asynchronously saves the given `snapshot`.
   */
  def save(snapshot: Any, handler: BiConsumer[SnapshotMetadata, Throwable]): Unit = save(snapshot) {
    case Success(a) => handler.accept(a, null)
    case Failure(e) => handler.accept(null.asInstanceOf[SnapshotMetadata], e)
  }

  /**
   * Sets this actor's command handler.
   */
  protected def onReceiveCommand(handler: Receive): Unit =
    _onCommand = handler

  /**
   * Sets this actor's event handler.
   */
  protected def onReceiveEvent(handler: Receive) =
    _onEvent = handler

  /**
   * Sets this actor's snapshot handler.
   */
  protected def onReceiveSnapshot(handler: Receive) =
    _onSnapshot = handler
}
