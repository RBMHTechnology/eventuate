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

import java.util.{ Optional => JOption }

import akka.actor.AbstractActor
import akka.actor.Actor.Receive
import akka.actor.ActorRef
import akka.japi.pf.ReceiveBuilder

import scala.compat.java8.OptionConverters._

import scala.runtime.BoxedUnit

private[eventuate] object ReceiveConverters {

  implicit class ScalaReceiveConverter(receive: Receive) {
    def asJava: AbstractActor.Receive =
      new AbstractActor.Receive(receive.asInstanceOf[PartialFunction[Any, BoxedUnit]])
  }

  implicit class JavaReceiveConverter(receive: AbstractActor.Receive) {
    def asScala: Receive =
      receive.onMessage.asInstanceOf[Receive]
  }
}

private[eventuate] object AbstractEventsourcedComponent {

  import ReceiveConverters._

  /**
   * Java API of the actors [[com.rbmhtechnology.eventuate.BehaviorContext]].
   *
   * Provides a context for managing behaviors.
   */
  final class BehaviorContext private[eventuate] (context: com.rbmhtechnology.eventuate.BehaviorContext) {

    /**
     * The initial behavior.
     */
    def initial: AbstractActor.Receive =
      context.initial.asJava

    /**
     * The current behavior.
     */
    def current: AbstractActor.Receive =
      context.current.asJava

    /**
     * Sets the given `behavior` as `current` behavior. Replaces the `current` behavior on the behavior stack.
     *
     * @param behavior the behavior to set.
     */
    def become(behavior: AbstractActor.Receive): Unit =
      become(behavior, replace = true)

    /**
     * Sets the given `behavior` as `current` behavior.
     *
     * @param behavior the behavior to set.
     * @param replace if `true` (default) replaces the `current` behavior on the behavior stack, if `false`
     *                pushes `behavior` on the behavior stack.
     */
    def become(behavior: AbstractActor.Receive, replace: Boolean): Unit =
      context.become(behavior.asScala, replace)

    /**
     * Pops the current `behavior` from the behavior stack, making the previous behavior the `current` behavior.
     * If the behavior stack contains only a single element, the `current` behavior is reverted to the `initial`
     * behavior.
     */
    def unbecome(): Unit =
      context.unbecome()
  }
}

/**
 * Java API for actors that implement any event-sourced component.
 *
 * @define akkaReceiveBuilder http://doc.akka.io/japi/akka/2.5.2/akka/japi/pf/ReceiveBuilder.html
 */
private[eventuate] abstract class AbstractEventsourcedComponent extends EventsourcedView with ConditionalRequests {

  import ReceiveConverters._

  /**
   * Java API of [[EventsourcedView.aggregateId aggregateId]].
   *
   * @see [[EventsourcedView]]
   */
  def getAggregateId: JOption[String] = JOption.empty()

  override final def aggregateId: Option[String] = getAggregateId.asScala

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
   * Returns a receive object that defines the actor's command handling behaviour.
   * Use [[$akkaReceiveBuilder ReceiveBuilder]] to define the behaviour.
   *
   * @see [[EventsourcedView]]
   */
  def createOnCommand(): AbstractActor.Receive = AbstractActor.emptyBehavior

  final override def onCommand: Receive = createOnCommand().asScala

  /**
   * Java API of the [[EventsourcedView.onSnapshot snapshot]] handler.
   *
   * Returns a receive object that defines the actor's snapshot handling behaviour.
   * Use [[$akkaReceiveBuilder ReceiveBuilder]] to define the behaviour.
   *
   * @see [[EventsourcedView]]
   */
  def createOnSnapshot(): AbstractActor.Receive = AbstractActor.emptyBehavior

  final override def onSnapshot: Receive = createOnSnapshot().asScala

  /**
   * Java API of the [[EventsourcedView.onRecovery recovery]] handler.
   *
   * Returns a result handler that defines the actor's recover handling behaviour.
   * Use [[ResultHandler]] to define the behaviour.
   *
   * @see [[EventsourcedView]]
   */
  def createOnRecovery(): ResultHandler[Unit] = ResultHandler.none[Unit]

  override final def onRecovery: Handler[Unit] = createOnRecovery().asScala

  /**
   * Creates a new empty `ReceiveBuilder`. Mimics [[AbstractActor.receiveBuilder]].
   */
  final def receiveBuilder(): ReceiveBuilder = ReceiveBuilder.create()

  /**
   * Returns this AbstractActor's ActorContext. Mimics [[AbstractActor.getContext]].
   */
  def getContext: AbstractActor.ActorContext = context.asInstanceOf[AbstractActor.ActorContext]

  /**
   * Returns the ActorRef for this actor. Mimics [[AbstractActor.getSelf]].
   */
  def getSelf: ActorRef = self

  /**
   * The reference sender Actor of the currently processed message. Mimics [[AbstractActor.getSender]].
   */
  def getSender: ActorRef = sender()

  /**
   * Java API of [[EventsourcedView.recovering lastEmirecoveringterId]].
   *
   * @see [[EventsourcedView]]
   */
  final def isRecovering: Boolean = recovering

  /**
   * Java API of [[EventsourcedView.lastVectorTimestamp lastVectorTimestamp]].
   *
   * @see [[EventsourcedView]]
   */
  final def getLastVectorTimestamp: VectorTime = lastVectorTimestamp

  /**
   * Java API of [[EventsourcedView.lastSequenceNr lastSequenceNr]].
   *
   * @see [[EventsourcedView]]
   */
  final def getLastSequenceNr: Long = lastSequenceNr

  /**
   * Java API of [[EventsourcedView.lastSystemTimestamp lastSystemTimestamp]].
   *
   * @see [[EventsourcedView]]
   */
  final def getLastSystemTimestamp: Long = lastSystemTimestamp

  /**
   * Java API of [[EventsourcedView.lastEmitterId lastEmitterId]].
   *
   * @see [[EventsourcedView]]
   */
  final def getLastEmitterId: String = lastEmitterId

  /**
   * Java API of [[EventsourcedView.lastEmitterAggregateId lastEmitterAggregateId]].
   *
   * @see [[EventsourcedView]]
   */
  final def getLastEmitterAggregateId: JOption[String] = lastEmitterAggregateId.asJava

  /**
   * Java API of [[EventsourcedView.commandContext commandContext]].
   *
   * Returns the command [[BehaviorContext]].
   *
   * @see [[EventsourcedView]]
   */
  final def getCommandContext: AbstractEventsourcedComponent.BehaviorContext =
    new AbstractEventsourcedComponent.BehaviorContext(commandContext)

  /**
   * Java API of [[EventsourcedView.eventContext eventContext]].
   *
   * Returns the event [[BehaviorContext]].
   *
   * @see [[EventsourcedView]]
   */
  final def getEventContext: AbstractEventsourcedComponent.BehaviorContext =
    new AbstractEventsourcedComponent.BehaviorContext(eventContext)

  /**
   * Java API of [[EventsourcedView.snapshotContext snapshotContext]].
   *
   * Returns the snapshot [[BehaviorContext]].
   *
   * @see [[EventsourcedView]]
   */
  final def getSnapshotContext: AbstractEventsourcedComponent.BehaviorContext =
    new AbstractEventsourcedComponent.BehaviorContext(snapshotContext)
}

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
 * @define akkaReceiveBuilder http://doc.akka.io/japi/akka/2.5.2/akka/japi/pf/ReceiveBuilder.html
 */
abstract class AbstractEventsourcedView(val id: String, val eventLog: ActorRef) extends AbstractEventsourcedComponent {

  import ReceiveConverters._

  /**
   * Java API of the [[EventsourcedView.onEvent event]] handler.
   *
   * Returns a receive object that defines the actor's event handling behaviour.
   * Use [[$akkaReceiveBuilder ReceiveBuilder]] to define the behaviour.
   *
   * @see [[EventsourcedView]]
   */
  def createOnEvent(): AbstractActor.Receive = AbstractActor.emptyBehavior

  override final def onEvent: Receive = createOnEvent().asScala
}
