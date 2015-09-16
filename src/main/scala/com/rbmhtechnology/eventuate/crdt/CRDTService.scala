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

package com.rbmhtechnology.eventuate.crdt

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import com.rbmhtechnology.eventuate._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.higherKinds
import scala.util._

/**
 * Typeclass to be implemented by CRDTs if they shall be managed by [[CRDTService]]
 *
 * @tparam A CRDT type
 * @tparam B CRDT value type
 */
trait CRDTServiceOps[A, B] {

  /**
   * Default CRDT instance.
   */
  def zero: A

  /**
   * Returns the CRDT value (for example, the entries of an OR-Set)
   */
  def value(crdt: A): B

  /**
   * Must return `true` if CRDT checks preconditions. Should be overridden to return
   * `false` if CRDT does not check preconditions, as this will significantly increase
   * write throughput.
   */
  def precondition: Boolean = true

  /**
   * Update phase 1 ("atSource"). Prepares an operation for phase 2.
   */
  def prepare(crdt: A, operation: Any): Option[Any] = Some(operation)

  /**
   * Update phase 2 ("downstream").
   */
  def update(crdt: A, operation: Any, event: DurableEvent): A
}

object CRDTService {
  /**
   * Persistent event with update operation.
   *
   * @param id id of CRDT instance.
   * @param operation update operation.
   */
  case class ValueUpdated(id: String, operation: Any)
}

/**
 * A generic, replicated CRDT service that manages a map of CRDTs identified by name.
 * Replication is based on the replicated event `log` that preserves causal ordering
 * of events.
 *
 * @tparam A CRDT type
 * @tparam B CRDT value type
 */
trait CRDTService[A, B] {
  import CRDTService._

  private implicit val timeout = Timeout(10.seconds)

  private var system: Option[ActorSystem] = None
  private var worker: Option[ActorRef] = None

  /**
   * CRDT service id.
   */
  def serviceId: String

  /**
   * Event log.
   */
  def log: ActorRef

  /**
   * CRDT service operations.
   */
  def ops: CRDTServiceOps[A, B]

  /**
   * Starts the CRDT service.
   */
  def start()(implicit system: ActorSystem): Unit = if (worker.isEmpty) {
    // all CRDTs of same type managed within a single CRDTActor
    val aggregateId: String = ops.zero.getClass.getSimpleName

    this.system = Some(system)
    this.worker = Some(system.actorOf(Props(new CRDTActor(serviceId, Some(aggregateId), log))))
  }

  /**
   * Stops the CRDT service.
   */
  def stop(): Unit = for {
    s <- system
    w <- worker
  } {
    s.stop(w)
    system = None
    worker = None
  }

  /**
   * Returns the current value of the CRDT identified by `id`.
   */
  def value(id: String): Future[B] = withWorkerAndDispatcher { (w, d) =>
    w.ask(GetValue(id)).mapTo[GetValueReply].map(_.value)(d)
  }

  /**
   * Updates the CRDT identified by `id` with given `operation`.
   * Returns the updated value of the CRDT.
   */
  protected def op(id: String, operation: Any): Future[B] = withWorkerAndDispatcher { (w, d) =>
    w.ask(UpdateValue(id, operation)).mapTo[UpdateValueReply].map(_.value)(d)
  }

  private def withWorkerAndDispatcher(async: (ActorRef, ExecutionContext) => Future[B]): Future[B] = worker match {
    case None    => Future.failed(new Exception("Service not started"))
    case Some(w) => async(w, system.get.dispatcher)
  }

  private case class GetValue(id: String)
  private case class GetValueReply(id: String, value: B)
  private case class UpdateValue(id: String, operation: Any)
  private case class UpdateValueReply(id: String, value: B)

  private class CRDTActor(
      override val id: String,
      override val aggregateId: Option[String],
      override val eventLog: ActorRef) extends EventsourcedActor {

    var crdts: Map[String, A] = Map.empty.withDefault(_ => ops.zero)

    override def stateSync: Boolean =
      ops.precondition

    override val onCommand: Receive = {
      case GetValue(id) =>
        sender() ! GetValueReply(id, ops.value(crdts(id)))
      case UpdateValue(id, operation) =>
        ops.prepare(crdts(id), operation) match {
          case None =>
            sender() ! UpdateValueReply(id, ops.value(crdts(id)))
          case Some(op) =>
            persist(ValueUpdated(id, op)) {
              case Success(evt) =>
                onEvent(evt)
                sender() ! UpdateValueReply(id, ops.value(crdts(id)))
              case Failure(err) =>
                sender() ! Status.Failure(err)
            }
        }
    }

    override val onEvent: Receive = {
      case ValueUpdated(id, operation) =>
        val crdt = crdts.get(id) match {
          case Some(crdt) => crdt
          case None       => ops.zero
        }
        crdts = crdts + (id -> ops.update(crdt, operation, lastHandledEvent))
        onChange(crdts(id), operation) // TODO: make onChange callbacks configurable (needed in tests only)
    }
  }

  /** For testing purposes only */
  private[crdt] def onChange(crdt: A, operation: Any): Unit = ()
}
