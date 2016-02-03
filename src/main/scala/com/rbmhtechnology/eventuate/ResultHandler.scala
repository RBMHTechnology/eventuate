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

import java.util.function.Consumer

import scala.compat.java8.FunctionConverters._
import scala.util.{ Failure, Success, Try }

object ResultHandler {

  /**
   * Creates an empty result handler. No actions are performed for this handler.
   *
   * @tparam A type of the result object
   * @return the result handler
   */
  def none[A]: ResultHandler[A] =
    new ResultHandler[A](asJavaConsumer((_: A) => ()), asJavaConsumer((_: Throwable) => ()))

  /**
   * Creates a result handler that performs the given action of either the success or the failure case, depending on the result.
   *
   * @param success action to perform in case of success
   * @param failure action to perform in case of failure
   * @tparam A type of the result object
   * @return the result handler
   */
  def on[A](success: Consumer[A], failure: Consumer[Throwable]): ResultHandler[A] =
    new ResultHandler[A](success, failure)

  /**
   * Creates a result handler that performs the given action in case of a successful result.
   *
   * @param success action to perform in case of success
   * @tparam A type of the result object
   * @return the result handler
   */
  def onSuccess[A](success: Consumer[A]): ResultHandler[A] =
    new ResultHandler[A](success, asJavaConsumer((_: Throwable) => ()))

  /**
   * Creates a result handler that performs the given action in case of a failed result.
   *
   * @param failure action to perform in case of failure
   * @tparam A type of the result object
   * @return the result handler
   */
  def onFailure[A](failure: Consumer[Throwable]): ResultHandler[A] =
    new ResultHandler[A](asJavaConsumer((_: A) => ()), failure)
}

/**
 * Java API handler that accepts a success and a failure function. Used to asynchronously execute the appropriate function based on the result
 * of an action or calculation.
 *
 * @param success the function to call in case of success. The result value is supplied.
 * @param failure the function to call in case of failure. The error is supplied.
 * @tparam A type of the result object
 */
class ResultHandler[A] private (val success: Consumer[A], val failure: Consumer[Throwable]) {

  /**
   * Resolves the handler successfully.
   *
   * @param v the result object.
   */
  def acceptSuccess(v: A): Unit = {
    success.accept(v)
  }

  /**
   * Resolves the handler with a failure.
   * @param t the error cause.
   */
  def acceptFailure(t: Throwable): Unit = {
    failure.accept(t)
  }

  /**
   * Returns a partial function that resolves this handler with a Try.
   */
  def accept: PartialFunction[Try[A], Unit] = {
    case Success(a) => acceptSuccess(a)
    case Failure(e) => acceptFailure(e)
  }

  /**
   * Converts the result handler into an [[EventsourcedView.Handler]].
   */
  def asScala: EventsourcedView.Handler[A] = {
    case Success(a) => acceptSuccess(a)
    case Failure(e) => acceptFailure(e)
  }
}
