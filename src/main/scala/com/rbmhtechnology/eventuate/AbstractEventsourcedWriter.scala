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

import java.lang.{ Long => JLong }
import java.util.concurrent.CompletionStage
import java.util.{ Optional => JOption }

import akka.actor.ActorRef

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.Future

private[eventuate] trait EventsourcedWriterSuccessHandlers[R, W] {
  def readSuccess(result: R): Option[Long]
  def writeSuccess(result: W): Unit
}

private[eventuate] trait EventsourcedWriterFailureHandlers {
  def readFailure(cause: Throwable): Unit
  def writeFailure(cause: Throwable): Unit
}

private[eventuate] trait EventsourcedWriterFailureHandlerAdapter extends EventsourcedWriterFailureHandlers {
  this: EventsourcedWriter[_, _] =>

  abstract override final def readFailure(cause: Throwable): Unit =
    onReadFailure(cause)

  abstract override final def writeFailure(cause: Throwable): Unit =
    onWriteFailure(cause)

  /**
   * Java API of [[EventsourcedWriter.readFailure readFailure]].
   *
   * @see [[EventsourcedWriter]]
   */
  def onReadFailure(cause: Throwable): Unit =
    super.readFailure(cause)

  /**
   * Java API of [[EventsourcedWriter.writeFailure writeFailure]].
   *
   * @see [[EventsourcedWriter]]
   */
  def onWriteFailure(cause: Throwable): Unit =
    super.writeFailure(cause)
}

private[eventuate] trait EventsourcedWriterSuccessHandlerAdapter[R, W] extends EventsourcedWriterSuccessHandlers[R, W] {
  this: EventsourcedWriter[R, W] =>

  abstract override final def readSuccess(result: R): Option[Long] =
    onReadSuccess(result).asScala.map(_.asInstanceOf[Long])

  abstract override final def writeSuccess(result: W): Unit =
    onWriteSuccess(result)

  /**
   * Java API of [[EventsourcedWriter.readSuccess readSuccess]].
   *
   * @see [[EventsourcedWriter]]
   */
  def onReadSuccess(result: R): JOption[JLong] =
    super.readSuccess(result).map(_.asInstanceOf[JLong]).asJava

  /**
   * Java API of [[EventsourcedWriter.writeSuccess writeSuccess]].
   *
   * @see [[EventsourcedWriter]]
   */
  def onWriteSuccess(result: W): Unit =
    super.writeSuccess(result)
}

/**
 * Java API for actors that implement [[EventsourcedWriter]].
 *
 * @see [[AbstractEventsourcedView]] for a detailed usage of the Java API
 * @see [[EventsourcedWriter]]
 */
abstract class AbstractEventsourcedWriter[R, W](id: String, eventLog: ActorRef) extends AbstractEventsourcedView(id, eventLog)
  with EventsourcedWriter[R, W] with EventsourcedWriterFailureHandlerAdapter with EventsourcedWriterSuccessHandlerAdapter[R, W] {

  override def read(): Future[R] =
    onRead().toScala

  override def write(): Future[W] =
    onWrite().toScala

  /**
   * Java API of [[EventsourcedWriter.read read]].
   *
   * @see [[EventsourcedWriter]]
   */
  def onRead(): CompletionStage[R]

  /**
   * Java API of [[EventsourcedWriter.write write]].
   *
   * @see [[EventsourcedWriter]]
   */
  def onWrite(): CompletionStage[W]
}
