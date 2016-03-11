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

import java.lang.{ Iterable => JIterable, Long => JLong }
import java.util.{ Optional => JOption }

import akka.actor.ActorRef

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

private[eventuate] trait EventsourcedProcessorHandlers {
  import EventsourcedProcessor._
  def processEvent: Process = throw new IllegalStateException("Method 'processEvent' must be implemented.")
}

private[eventuate] trait EventSourcedProcessorAdapter extends EventsourcedProcessorHandlers with EventsourcedWriterFailureHandlerAdapter {
  this: EventsourcedProcessor =>

  type JProcess = PartialFunction[Any, JIterable[Any]]

  object JProcess {
    object emptyBehavior extends JProcess {
      def isDefinedAt(x: Any) = false
      def apply(x: Any) = throw new UnsupportedOperationException("Empty process behavior apply()")
    }
  }

  private var processBehaviour: Option[JProcess] = None

  abstract override final def processEvent: Process =
    onProcessEvent.andThen(seq => seq.asScala.to[collection.immutable.Seq])

  /**
   * Java API of [[EventsourcedProcessor.processEvent event-processing]] handler.
   *
   * Returns a partial function that defines the actor's event processing behaviour.
   * Use [[ProcessBuilder]] to define the behaviour.
   *
   * Takes precedence over [[setOnProcessEvent]].
   *
   * @see [[EventsourcedProcessor]]
   */
  def onProcessEvent: JProcess = processBehaviour.getOrElse(JProcess.emptyBehavior)

  /**
   * Java API that sets this actor's [[EventsourcedProcessor.processEvent event-processing]] handler.
   *
   * Supplied with a partial function that defines the actor's event processing behaviour.
   * Use [[ProcessBuilder]] to define the behaviour.
   *
   * If [[onProcessEvent]] is implemented, the supplied behaviour is ignored.
   *
   * @param handler This actor's event processing handler.
   * @see [[EventsourcedProcessor]]
   */
  def setOnProcessEvent(handler: JProcess): Unit =
    if (processBehaviour.isEmpty) processBehaviour = Some(handler)
    else throw new IllegalStateException("Actor process behaviour has already been set with setOnProcessEvent(...).  " +
      "The behaviour can only be set once.")
}

private[eventuate] trait EventsourcedProcessorWriteSuccessHandlerAdapter extends EventsourcedWriterSuccessHandlers[Long, Long] {
  this: EventsourcedProcessor =>

  abstract override final def writeSuccess(result: Long): Unit =
    onWriteSuccess(result)

  /**
   * Java API of [[EventsourcedProcessor.writeSuccess writeSuccess]].
   *
   * @see [[EventsourcedProcessor]]
   */
  def onWriteSuccess(result: Long): Unit =
    super.writeSuccess(result)
}

/**
 * Java API for actors that implement [[EventsourcedProcessor]].
 *
 * @see [[AbstractEventsourcedView]] for a detailed usage of the Java API
 * @see [[EventsourcedProcessor]]
 */
class AbstractEventsourcedProcessor(id: String, eventLog: ActorRef, val targetEventLog: ActorRef) extends AbstractEventsourcedView(id, eventLog)
  with EventsourcedProcessor with EventSourcedProcessorAdapter with EventsourcedProcessorWriteSuccessHandlerAdapter {

  override final def readSuccess(result: Long): Option[Long] =
    onReadSuccess(result).asScala.map(_.asInstanceOf[Long])

  /**
   * Java API of [[EventsourcedProcessor.readSuccess readSuccess]].
   *
   * @see [[EventsourcedProcessor]]
   */
  def onReadSuccess(result: Long): JOption[JLong] =
    super.readSuccess(result).map(_.asInstanceOf[JLong]).asJava
}
