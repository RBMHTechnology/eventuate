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

package com.rbmhtechnology.eventuate.log

import akka.actor._
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.log.EventLog.EventLogAvailable
import com.rbmhtechnology.eventuate.log.EventLog.EventLogUnavailable
import com.typesafe.config.Config

private class CircuitBreakerSettings(config: Config) {
  val openAfterRetries =
    config.getInt("eventuate.log.circuit-breaker.open-after-retries")
}

/**
 * A wrapper that can protect [[EventLog]] implementations from being overloaded while they are retrying to
 * serve a write request. If the circuit breaker is closed, it forwards all requests to the underlying event
 * log. If it is open, it replies with a failure message to the requestor. The circuit breaker can be opened
 * by sending it `ServiceFailure` messages with a `retry` value greater than or equal to the configuration
 * parameter `eventuate.log.circuit-breaker.open-after-retries`. It can be closed again by sending it a
 * `ServiceNormal` or `ServiceInitialized` message. These messages are usually sent by [[EventLog]]
 * implementations and not by applications.
 *
 * @see [[EventLogSPI.write]]
 */
class CircuitBreaker(logProps: Props, batching: Boolean, logId: String) extends Actor {
  import CircuitBreaker._

  private val settings =
    new CircuitBreakerSettings(context.system.settings.config)

  private val eventLog: ActorRef =
    context.watch(createLog)

  private val closed: Receive = {
    case ServiceInitialized | ServiceNormal =>
    // normal operation
    case ServiceFailed(retry, cause) =>
      publishUnavailable(cause)
      if (retry >= settings.openAfterRetries) context.become(open)
    case Terminated(_) =>
      context.stop(self)
    case msg =>
      eventLog forward msg
  }

  private val open: Receive = {
    case ServiceInitialized | ServiceNormal =>
      publishAvailable()
      context.become(closed)
    case ServiceFailed(retry, cause) =>
    // failure persists
    case Terminated(_) =>
      context.stop(self)
    case Write(events, initiator, replyTo, cid, iid) =>
      // Write requests are not made via ask
      replyTo.tell(WriteFailure(events, Exception, cid, iid), initiator)
    case msg =>
      // All other requests are made via ask
      sender() ! Status.Failure(Exception)
  }

  def receive =
    closed

  override def preStart(): Unit = {
    super.preStart()
    publishAvailable()
  }

  private def createLog(): ActorRef =
    if (batching) context.actorOf(Props(new BatchingLayer(logProps))) else context.actorOf(logProps)

  private def publishUnavailable(cause: Throwable): Unit =
    context.system.eventStream.publish(EventLogUnavailable(logId, cause))

  private def publishAvailable(): Unit =
    context.system.eventStream.publish(EventLogAvailable(logId))
}

object CircuitBreaker {
  /**
   * Default [[EventLogUnavailableException]] instance.
   */
  val Exception = new EventLogUnavailableException

  /**
   * An event that controls [[CircuitBreaker]] state.
   */
  sealed trait ServiceEvent

  /**
   * Sent by an event log to indicate that it has been successfully initialized.
   */
  case object ServiceInitialized extends ServiceEvent

  /**
   * Sent by an event log to indicate that it has successfully written an event batch.
   */
  case object ServiceNormal extends ServiceEvent

  /**
   * Sent by an event log to indicate that it failed to write an event batch. The current
   * retry count is given by the `retry` parameter.
   */
  case class ServiceFailed(retry: Int, cause: Throwable) extends ServiceEvent

}
