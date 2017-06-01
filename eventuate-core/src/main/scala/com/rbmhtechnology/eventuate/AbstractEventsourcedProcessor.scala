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

import scala.collection.immutable._
import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

private[eventuate] trait EventsourcedProcessorHandlers {
  import EventsourcedProcessor._
  def processEvent: Process = throw new IllegalStateException("Method 'processEvent' must be implemented.")
}

private[eventuate] trait EventSourcedProcessorAdapter extends EventsourcedProcessorHandlers with EventsourcedWriterFailureHandlerAdapter {
  this: EventsourcedProcessor =>

  /**
   * Java API of [[EventsourcedProcessor.processEvent event-processing]] handler.
   *
   * Returns a [[AbstractEventsourcedProcessor.Process]] that defines the actor's event processing behaviour.
   * Use [[ProcessBuilder]] to define the behaviour.
   *
   * @see [[EventsourcedProcessor]]
   */
  def createOnProcessEvent(): AbstractEventsourcedProcessor.Process =
    AbstractEventsourcedProcessor.emptyProcessBehavior

  abstract override final def processEvent: Process =
    createOnProcessEvent().asScala

  /**
   * creates a new empty [[ProcessBuilder]]
   */
  final def processBuilder(): ProcessBuilder =
    ProcessBuilder.create()
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
 * Java API
 */
object AbstractEventsourcedProcessor {

  final class Process(val behaviour: PartialFunction[Any, JIterable[Any]])

  final val emptyProcessBehavior: Process = new Process(PartialFunction.empty)

  implicit class ProcessConverter(p: Process) {
    def asScala: EventsourcedProcessor.Process =
      p.behaviour.andThen(seq => seq.asScala.to[Seq])
  }
}

/**
 * Java API for actors that implement [[EventsourcedProcessor]].
 *
 * @see [[AbstractEventsourcedView]] for a detailed usage of the Java API
 * @see [[EventsourcedProcessor]]
 */
class AbstractEventsourcedProcessor(val id: String, val eventLog: ActorRef, val targetEventLog: ActorRef) extends AbstractEventsourcedComponent
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
