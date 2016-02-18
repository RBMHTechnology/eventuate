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

package com.rbmhtechnology.eventuate

import java.util.{ Optional => JOption }

import akka.actor.{ Actor, ActorRef }

import scala.compat.java8.OptionConverters._

/**
 * Java API for actors that implement [[EventsourcedView]].
 *
 * Actor handlers may be initialized once in the constructor with various set-methods (e.g. setOnCommand, setOnEvent)
 * or by overriding the respective handler (e.g. onCommand(...), onEvent(...)).
 * If a handler is overridden, the behaviour of it's respective set-method will be ignored.
 *
 * Example:
 *          {{{
 *          public class HelloActor extends AbstractEventsourcedView {
 *             public HelloActor(final String id, final ActorRef eventLog) {
 *               super(id, eventLog);
 *
 *               onCommand(ReceiveBuilder
 *                 .match(String.class, str -> str.equals("Hello"), value -> sender().tell("World", self())
 *                 .matchAny(ev -> value -> sender().tell("Please try again", self())
 *                 .build());
 *             }
 *
 *             public PartialFunction<Object, BoxedUnit> onEvent() {
 *               return ReceiveBuilder
 *                 .matchAny(HelloEvent.class, ev -> handleEvent(ev))
 *                 .build();
 *             }
 *          }
 *          }}}
 *
 * @see [[EventsourcedView]]
 * @define akkaReceiveBuilder http://doc.akka.io/japi/akka/2.3.9/akka/japi/pf/ReceiveBuilder.html
 */
abstract class AbstractEventsourcedView(val id: String, val eventLog: ActorRef) extends EventsourcedView with ConditionalRequests {
  private var commandBehaviour: Option[Receive] = None
  private var eventBehaviour: Option[Receive] = None
  private var snapshotBehaviour: Option[Receive] = None
  private var recoveryHandler: Option[ResultHandler[Unit]] = None

  override final def onRecovery: Handler[Unit] = onRecover.asScala

  override final def aggregateId: Option[String] = getAggregateId.asScala

  /**
   * Java API of [[EventsourcedView.aggregateId aggregateId]].
   *
   * @see [[EventsourcedView]]
   */
  def getAggregateId: JOption[String] = JOption.empty()

  /**
   * Java API of [[EventsourcedView.lastEmitterAggregateId lastEmitterAggregateId]].
   *
   * @see [[EventsourcedView]]
   */
  final def getLastEmitterAggregateId: JOption[String] = lastHandledEvent.emitterAggregateId.asJava

  /**
   * Java API of [[EventsourcedView.save save]].
   *
   * Must be supplied with a [[ResultHandler]] to process successful or failed results.
   *
   * @see [[com.rbmhtechnology.eventuate.EventsourcedView EventsourcedView]]
   */
  def save(snapshot: Any, handler: ResultHandler[SnapshotMetadata]): Unit =
    save(snapshot)(handler.asScala)

  /**
   * Java API of the [[EventsourcedView.onCommand command]] handler.
   *
   * Returns a partial function that defines the actor's command handling behaviour.
   * Use [[$akkaReceiveBuilder ReceiveBuilder]] to define the behaviour.
   *
   * Takes precedence over [[setOnCommand]].
   *
   * @see [[EventsourcedView]]
   */
  override def onCommand = commandBehaviour.getOrElse(Actor.emptyBehavior)

  /**
   * Java API of the [[EventsourcedView.onEvent event]] handler.
   *
   * Returns a partial function that defines the actor's event handling behaviour.
   * Use [[$akkaReceiveBuilder ReceiveBuilder]] to define the behaviour.
   *
   * Takes precedence over [[setOnEvent]].
   *
   * @see [[EventsourcedView]]
   */
  override def onEvent = eventBehaviour.getOrElse(Actor.emptyBehavior)

  /**
   * Java API of the [[EventsourcedView.onSnapshot snapshot]] handler.
   *
   * Returns a partial function that defines the actor's snapshot handling behaviour.
   * Use [[$akkaReceiveBuilder ReceiveBuilder]] to define the behaviour.
   *
   * Takes precedence over [[setOnSnapshot]].
   *
   * @see [[EventsourcedView]]
   */
  override def onSnapshot = snapshotBehaviour.getOrElse(Actor.emptyBehavior)

  /**
   * Java API of the [[EventsourcedView.onRecovery recovery]] handler.
   *
   * Returns a result handler that defines the actor's recover handling behaviour.
   * Use [[ResultHandler]] to define the behaviour.
   *
   * Takes precedence over [[setOnRecover]].
   *
   * @see [[EventsourcedView]]
   */
  def onRecover: ResultHandler[Unit] = recoveryHandler.getOrElse(ResultHandler.none[Unit])

  /**
   * Java API that sets this actor's [[EventsourcedView.onCommand command]] handler.
   *
   * Supplied with a partial function that defines the actor's command handling behaviour.
   * Use [[$akkaReceiveBuilder ReceiveBuilder]] to define the behaviour.
   *
   * If [[onCommand]] is implemented, the supplied behaviour is ignored.
   *
   * @param handler a function that defines this actor's command handling behaviour.
   * @see [[EventsourcedView]]
   */
  final protected def setOnCommand(handler: Receive): Unit =
    if (commandBehaviour.isEmpty) commandBehaviour = Some(handler)
    else throw new IllegalStateException("Actor command behaviour has already been set with setOnCommand(...).  " +
      "Use commandContext.become(...) to change it.")

  /**
   * Java API that sets this actor's [[EventsourcedView.onEvent event]] handler.
   *
   * Supplied with a partial function that defines the actor's event handling behaviour.
   * Use [[$akkaReceiveBuilder ReceiveBuilder]] to define the behaviour.
   *
   * If [[onEvent]] is implemented, the supplied behaviour is ignored.
   *
   * @param handler a function that defines this actor's event handling behaviour.
   * @see [[EventsourcedView]]
   */
  final protected def setOnEvent(handler: Receive): Unit =
    if (eventBehaviour.isEmpty) eventBehaviour = Some(handler)
    else throw new IllegalStateException("Actor event behaviour has already been set with setOnEvent(...).  " +
      "Use eventContext.become(...) to change it.")

  /**
   * Java API that sets this actor's [[EventsourcedView.onSnapshot snapshot]] handler.
   *
   * Supplied with a partial function that defines the actor's snapshot handling behaviour.
   * Use [[$akkaReceiveBuilder ReceiveBuilder]] to define the behaviour.
   *
   * If [[onSnapshot]] is implemented, the supplied behaviour is ignored.
   *
   * @param handler a function that defines this actor's snapshot handling behaviour.
   * @see [[EventsourcedView]]
   */
  final protected def setOnSnapshot(handler: Receive): Unit =
    if (snapshotBehaviour.isEmpty) snapshotBehaviour = Some(handler)
    else throw new IllegalStateException("Actor snapshot behaviour has already been set with setOnSnapshot(...).  " +
      "Use snapshotContext.become(...) to change it.")

  /**
   * Java API that sets this actor's [[EventsourcedView.onRecovery recovery]] handler.
   *
   * Supplied with a result handler that defines the actor's recover handling behaviour.
   * Use [[ResultHandler]] to define the behaviour.
   *
   * If [[onRecover]] is implemented, the supplied behaviour is ignored.
   *
   * @param handler a [[ResultHandler]] that defines this actor's recover handling behaviour.
   * @see [[EventsourcedView]]
   */
  final protected def setOnRecover(handler: ResultHandler[Unit]): Unit =
    if (recoveryHandler.isEmpty) recoveryHandler = Some(handler)
    else throw new IllegalStateException("Actor recover behaviour has already been set with setOnRecover(...).  " +
      "The behaviour can only be set once.")
}
