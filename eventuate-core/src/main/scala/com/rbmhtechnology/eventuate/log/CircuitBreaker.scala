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
class CircuitBreaker(logProps: Props, batching: Boolean) extends Actor {
  import CircuitBreaker._

  private val settings =
    new CircuitBreakerSettings(context.system.settings.config)

  private val eventLog: ActorRef =
    context.watch(createLog)

  private val closed: Receive = {
    case serviceFailed: ServiceFailed =>
      if (serviceFailed.retry >= settings.openAfterRetries) {
        publish(serviceFailed)
        context.become(open)
      }
    case _: ServiceEvent =>
    // normal operation
    case Terminated(_) =>
      context.stop(self)
    case msg =>
      eventLog forward msg
  }

  private val open: Receive = {
    case _: ServiceFailed =>
    // failure persists
    case serviceEvent: ServiceEvent =>
      publish(serviceEvent)
      context.become(closed)
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

  private def createLog(): ActorRef =
    if (batching) context.actorOf(Props(new BatchingLayer(logProps))) else context.actorOf(logProps)

  private def publish(serviceEvent: ServiceEvent): Unit =
    context.system.eventStream.publish(serviceEvent)
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
  case class ServiceInitialized(logId: String) extends ServiceEvent

  /**
   * Sent by an event log to indicate that it has successfully written an event batch.
   *
   * This is also published on the event-stream when it closes the [[CircuitBreaker]]
   * (after previous failures that opened the [[CircuitBreaker]]).
   */
  case class ServiceNormal(logId: String) extends ServiceEvent

  /**
   * Sent by an event log to indicate that it failed to write an event batch. The current
   * retry count is given by the `retry` parameter.
   *
   * This is also published on the event-stream when it opens the [[CircuitBreaker]],
   * i.e. when `retry` exceeds a configured limit.
   */
  case class ServiceFailed(logId: String, retry: Int, cause: Throwable) extends ServiceEvent

}
