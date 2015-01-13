/*
 * Copyright (C) 2015 Red Bull Media House GmbH - all rights reserved.
 */

package com.rbmhtechnology.eventuate

import akka.actor._

import scala.collection.immutable.Seq

object EventLogProtocol {
  case class Read(from: Long, max: Int, exclusion: String)
  case class ReadSuccess(events: Seq[DurableEvent])
  case class ReadFailure(cause: Throwable)

  case class Delay(commands: Seq[Any], requestor: ActorRef, instanceId: Int)
  case class DelaySuccess(command: Any, instanceId: Int)

  case class Write(events: Seq[DurableEvent], requestor: ActorRef, instanceId: Int)
  case class WriteSuccess(event: DurableEvent, instanceId: Int)
  case class WriteFailure(event: DurableEvent, cause: Throwable, instanceId: Int)
  case class Written(event: DurableEvent)

  case class Replay(from: Long, requestor: ActorRef, instanceId: Int)
  case class Replaying(event: DurableEvent, instanceId: Int)
  case class ReplaySuccess(instanceId: Int)
  case class ReplayFailure(cause: Throwable, instanceId: Int)
}
