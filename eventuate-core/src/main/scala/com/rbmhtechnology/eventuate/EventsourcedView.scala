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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.event.{ Logging, LoggingAdapter }
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.typesafe.config.Config

import scala.concurrent.duration._
import scala.util._

private class EventsourcedViewSettings(config: Config) {
  val replayBatchSize =
    config.getInt("eventuate.log.replay-batch-size")

  val replayRetryMax =
    config.getInt("eventuate.log.replay-retry-max")

  val replayRetryDelay =
    config.getDuration("eventuate.log.replay-retry-delay", TimeUnit.MILLISECONDS).millis

  val readTimeout =
    config.getDuration("eventuate.log.read-timeout", TimeUnit.MILLISECONDS).millis

  val loadTimeout =
    config.getDuration("eventuate.snapshot.load-timeout", TimeUnit.MILLISECONDS).millis

  val saveTimeout =
    config.getDuration("eventuate.snapshot.save-timeout", TimeUnit.MILLISECONDS).millis
}

object EventsourcedView {
  /**
   * Callback handler invoked on an actor's dispatcher thread.
   */
  type Handler[A] = Try[A] => Unit

  object Handler {
    def empty[A]: Handler[A] = (_: Try[A]) => Unit
  }

  /**
   * Internal API.
   */
  private[eventuate] val instanceIdCounter = new AtomicInteger(0)
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
 * Event replay is subject to backpressure. After a configurable number of events
 * (see `eventuate.log.replay-batch-size` configuration parameter), replay is suspended until these
 * events have been handled by `onEvent` and then resumed again. There's no backpressure mechanism
 * for live event processing yet (but will come in future releases).
 *
 * @see [[DurableEvent]]
 * @see [[EventsourcedActor]]
 * @see [[EventsourcedWriter]]
 * @see [[EventsourcedProcessor]]
 */
trait EventsourcedView extends Actor with Stash {
  import EventsourcedView._
  import context.dispatcher

  type Handler[A] = EventsourcedView.Handler[A]

  val instanceId: Int = instanceIdCounter.getAndIncrement()

  private var _recovering: Boolean = true
  private var _eventHandling: Boolean = false
  private var _lastHandledEvent: DurableEvent = _
  private var _lastReceivedSequenceNr = 0L

  private val settings = new EventsourcedViewSettings(context.system.settings.config)

  private var saveRequests: Map[SnapshotMetadata, Handler[SnapshotMetadata]] = Map.empty

  private lazy val _commandContext: BehaviorContext = new DefaultBehaviorContext(onCommand)
  private lazy val _eventContext: BehaviorContext = new DefaultBehaviorContext(onEvent)
  private lazy val _snapshotContext: BehaviorContext = new DefaultBehaviorContext(onSnapshot)

  /**
   * This actor's logging adapter.
   */
  val logger: LoggingAdapter =
    Logging(context.system, this)

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
   * Maximum number of events to be replayed to this actor before replaying is suspended. A suspended replay
   * is resumed automatically after all replayed events haven been handled by this actor's event handler
   * (= backpressure). The default value for the maximum replay batch size is given by configuration item
   * `eventuate.log.replay-batch-size`. Configured values can be overridden by overriding this method.
   */
  def replayBatchSize: Int =
    settings.replayBatchSize

  /**
   * Global unique actor id.
   */
  def id: String

  /**
   * Event log actor.
   */
  def eventLog: ActorRef

  /**
   * Returns the command [[BehaviorContext]].
   */
  def commandContext: BehaviorContext =
    _commandContext

  /**
   * Returns the event [[BehaviorContext]].
   */
  def eventContext: BehaviorContext =
    _eventContext

  /**
   * Returns the snapshot [[BehaviorContext]].
   */
  def snapshotContext: BehaviorContext =
    _snapshotContext

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
   * Recovery completion handler. If called with a `Failure`, the actor will be stopped in
   * any case, regardless of the action taken by the returned handler. The default handler
   * implementation does nothing and can be overridden by implementations.
   */
  def onRecovery: Handler[Unit] =
    Handler.empty[Unit]

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
  private[eventuate] def eventHandling: Boolean =
    _eventHandling

  /**
   * Internal API.
   */
  private[eventuate] def recovered(): Unit = {
    _recovering = false
    onRecovery(Success(()))
  }

  /**
   * Internal API.
   */
  private[eventuate] def receiveEvent(event: DurableEvent): Unit = {
    val behavior = _eventContext.current
    val previous = lastHandledEvent

    _lastHandledEvent = event
    if (behavior.isDefinedAt(event.payload)) {
      _eventHandling = true
      receiveEventInternal(event)
      behavior(event.payload)
      if (!recovering) versionChanged(currentVersion)
      _eventHandling = false
    } else _lastHandledEvent = previous

    _lastReceivedSequenceNr = event.localSequenceNr
  }

  /**
   * Internal API.
   */
  private[eventuate] def receiveEventInternal(event: DurableEvent): Unit = {
    _lastHandledEvent = event
  }

  /**
   * Internal API.
   */
  private[eventuate] def receiveEventInternal(event: DurableEvent, failure: Throwable): Unit = {
    _lastHandledEvent = event
  }

  /**
   * Internal API.
   */
  private[eventuate] def lastHandledEvent: DurableEvent =
    _lastHandledEvent

  /**
   * Internal API.
   */
  private[eventuate] def currentVersion: VectorTime =
    VectorTime.Zero

  /**
   * Internal API.
   */
  private[eventuate] def conditionalSend(condition: VectorTime, cmd: Any): Unit =
    throw new ConditionalRequestException("Actor must extend ConditionalRequests to support ConditionalRequest processing")

  /**
   * Internal API.
   */
  private[eventuate] def versionChanged(condition: VectorTime): Unit =
    ()

  /**
   * Sequence number of the last handled event.
   */
  final def lastSequenceNr: Long =
    lastHandledEvent.localSequenceNr

  /**
   * Wall-clock timestamp of the last handled event.
   */
  final def lastSystemTimestamp: Long =
    lastHandledEvent.systemTimestamp

  /**
   * Vector timestamp of the last handled event.
   */
  final def lastVectorTimestamp: VectorTime =
    lastHandledEvent.vectorTimestamp

  /**
   * Emitter aggregate id of the last handled event.
   */
  final def lastEmitterAggregateId: Option[String] =
    lastHandledEvent.emitterAggregateId

  /**
   * Emitter id of the last handled event.
   */
  final def lastEmitterId: String =
    lastHandledEvent.emitterId

  /**
   * Asynchronously saves the given `snapshot` and calls `handler` with the generated
   * snapshot metadata. The `handler` can obtain a reference to the initial message
   * sender with `sender()`.
   */
  final def save(snapshot: Any)(handler: Handler[SnapshotMetadata]): Unit = {
    implicit val timeout = Timeout(settings.saveTimeout)

    val payload = snapshot match {
      case tree: ConcurrentVersionsTree[_, _] => tree.copy()
      case other                              => other
    }

    val prototype = Snapshot(payload, id, lastHandledEvent, currentVersion, _lastReceivedSequenceNr)
    val metadata = prototype.metadata
    val iid = instanceId

    if (saveRequests.contains(metadata)) {
      handler(Failure(new IllegalStateException(s"snapshot with metadata $metadata is currently being saved")))
    } else {
      saveRequests += (metadata -> handler)
      val snapshot = snapshotCaptured(prototype)
      eventLog.ask(SaveSnapshot(snapshot, sender(), iid)).recover {
        case t => SaveSnapshotFailure(metadata, t, iid)
      }.pipeTo(self)(sender())
    }
  }

  /**
   * Override to provide an application-defined log sequence number from which event replay will start.
   *
   * If `Some(snr)` is returned snapshot loading will be skipped and replay will start from
   * the given sequence number `snr`.
   *
   * If `None` is returned the actor proceeds with the regular snapshot loading procedure.
   */
  def replayFromSequenceNr: Option[Long] =
    None

  /**
   * Internal API.
   */
  private[eventuate] def snapshotCaptured(snapshot: Snapshot): Snapshot =
    snapshot

  /**
   * Internal API.
   */
  private[eventuate] def snapshotLoaded(snapshot: Snapshot): Unit =
    _lastHandledEvent = snapshot.lastEvent

  /**
   * Internal API.
   */
  private[eventuate] def unhandledMessage(msg: Any): Unit = {
    val behavior = _commandContext.current
    if (behavior.isDefinedAt(msg)) behavior(msg) else unhandled(msg)
  }

  /**
   * Internal API.
   */
  private[eventuate] def init(): Unit =
    replayFromSequenceNr match {
      case Some(snr) => replay(snr, subscribe = true)
      case None      => load()
    }

  /**
   * Internal API.
   */
  private[eventuate] def load(): Unit = {
    implicit val timeout = Timeout(settings.loadTimeout)
    val iid = instanceId

    eventLog ? LoadSnapshot(id, iid) recover {
      case t => LoadSnapshotFailure(t, iid)
    } pipeTo self
  }

  /**
   * Internal API.
   */
  private[eventuate] def replay(fromSequenceNr: Long = 1L, subscribe: Boolean = false): Unit = {
    implicit val timeout = Timeout(settings.readTimeout)
    val sub = if (subscribe) Some(self) else None
    val iid = instanceId

    eventLog ? Replay(fromSequenceNr, replayBatchSize, sub, aggregateId, instanceId) recover {
      case t => ReplayFailure(t, fromSequenceNr, iid)
    } pipeTo self
  }

  /**
   * Internal API.
   */
  private[eventuate] def initiating(replayAttempts: Int): Receive = {
    case LoadSnapshotSuccess(Some(snapshot), iid) => if (iid == instanceId) {
      val behavior = _snapshotContext.current
      if (behavior.isDefinedAt(snapshot.payload)) {
        snapshotLoaded(snapshot)
        behavior(snapshot.payload)
        replay(snapshot.metadata.sequenceNr + 1L, subscribe = true)
      } else {
        logger.warning(s"snapshot loaded (metadata = ${snapshot.metadata}) but onSnapshot doesn't handle it, replaying from scratch")
        replay(subscribe = true)
      }
    }
    case LoadSnapshotSuccess(None, iid) => if (iid == instanceId) {
      replay(subscribe = true)
    }
    case LoadSnapshotFailure(cause, iid) => if (iid == instanceId) {
      replay(subscribe = true)
    }
    case ReplaySuccess(Seq(), progress, iid) => if (iid == instanceId) {
      context.become(initiated)
      versionChanged(currentVersion)
      recovered()
      unstashAll()
    }
    case ReplaySuccess(events, progress, iid) => if (iid == instanceId) {
      events.foreach(receiveEvent)
      // reset retry attempts
      context.become(initiating(settings.replayRetryMax))
      replay(progress + 1L)
    }
    case ReplayFailure(cause, progress, iid) => if (iid == instanceId) {
      if (replayAttempts < 1) {
        logger.error(cause, "replay failed (maximum number of {} replay attempts reached), stopping self", settings.replayRetryMax)
        Try(onRecovery(Failure(cause)))
        context.stop(self)
      } else {
        // retry replay request while decreasing the remaining attempts
        val attemptsRemaining = replayAttempts - 1
        logger.warning(
          "replay failed [{}] ({} replay attempts remaining), scheduling retry in {}ms",
          cause.getMessage, attemptsRemaining, settings.replayRetryDelay.toMillis)
        context.become(initiating(attemptsRemaining))
        context.system.scheduler.scheduleOnce(settings.replayRetryDelay, self, ReplayRetry(progress))
      }
    }
    case ReplayRetry(progress) =>
      replay(progress)
    case Terminated(ref) if ref == eventLog =>
      context.stop(self)
    case other =>
      stash()
  }

  /**
   * Internal API.
   */
  private[eventuate] def initiated: Receive = {
    case Written(event) => if (event.localSequenceNr > lastSequenceNr) {
      receiveEvent(event)
    }
    case ConditionalRequest(condition, cmd) =>
      conditionalSend(condition, cmd)
    case SaveSnapshotSuccess(metadata, iid) => if (iid == instanceId) {
      saveRequests.get(metadata).foreach(handler => handler(Success(metadata)))
      saveRequests = saveRequests - metadata
    }
    case SaveSnapshotFailure(metadata, cause, iid) => if (iid == instanceId) {
      saveRequests.get(metadata).foreach(handler => handler(Failure(cause)))
      saveRequests = saveRequests - metadata
    }
    case Terminated(ref) if ref == eventLog =>
      context.stop(self)
    case msg =>
      unhandledMessage(msg)
  }

  /**
   * Initialization behavior.
   */
  final def receive = initiating(settings.replayRetryMax)

  /**
   * Adds the current command to the user's command stash. Must not be used in the event handler.
   */
  override def stash(): Unit =
    if (eventHandling) throw new StashError("stash() must not be used in event handler") else super.stash()

  /**
   * Prepends all stashed commands to the actor's mailbox and then clears the command stash.
   * Has no effect if the actor is recovering i.e. if `recovering` returns `true`.
   */
  override def unstashAll(): Unit =
    if (!recovering) super.unstashAll()

  /**
   * Sets `recovering` to `false` before calling `super.preRestart`.
   */
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    _recovering = false
    super.preRestart(reason, message)
  }

  /**
   * Initiates recovery.
   */
  override def preStart(): Unit = {
    _lastHandledEvent = DurableEvent(null, id)
    context.watch(eventLog)
    init()
  }

  /**
   * Sets `recovering` to `false` before calling `super.postStop`.
   */
  override def postStop(): Unit = {
    _recovering = false
    super.postStop()
  }
}

