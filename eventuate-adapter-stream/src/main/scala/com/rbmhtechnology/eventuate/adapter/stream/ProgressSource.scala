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

package com.rbmhtechnology.eventuate.adapter.stream

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor._
import akka.pattern.ask
import akka.stream._
import akka.stream.scaladsl.Source
import akka.util.Timeout

import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.typesafe.config.Config

import scala.concurrent.Future
import scala.concurrent.duration._

private class ProgressSourceSettings(config: Config) {
  val readTimeout =
    config.getDuration("eventuate.log.read-timeout", TimeUnit.MILLISECONDS).millis
}

object ProgressSource {
  /**
   * Creates a source stage that reads the processing progress for `sourceLogId` from `targetLog`. The source completes
   * after emission of the processing progress value or fails if the progress value could not be read. Behavior of the
   * progress source can be configured with:
   *
   *  - `eventuate.log.read-timeout`. Timeout for reading a processing progress value from the event log. A read timeout
   *  or another read failure causes this stage to fail.
   */
  def apply(sourceLogId: String, targetLog: ActorRef)(implicit system: ActorSystem): Graph[SourceShape[Long], NotUsed] = {
    implicit val timeout = Timeout(new ProgressSourceSettings(system.settings.config).readTimeout)
    import system.dispatcher

    Source.fromFuture(targetLog.ask(GetReplicationProgress(sourceLogId)).flatMap {
      case GetReplicationProgressSuccess(_, progress, _) => Future.successful(progress)
      case GetReplicationProgressFailure(cause)          => Future.failed(cause)
    })
  }
}
