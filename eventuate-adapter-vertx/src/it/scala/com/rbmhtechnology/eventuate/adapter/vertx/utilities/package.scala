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

package com.rbmhtechnology.eventuate.adapter.vertx

import akka.testkit.TestProbe

import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.Failure

package object utilities {

  implicit class VertxTestProbeExtension(probe: TestProbe) {
    def expectVertxMsg[T](body: T, max: Duration = Duration.Undefined)(implicit t: ClassTag[T]): EventBusMessage[T] = {
      probe.expectMsgPF[EventBusMessage[T]](max, hint = s"EventBusMessage($body, _, _)") {
        case m: EventBusMessage[T] if m.body == body => m
      }
    }

    def receiveNVertxMsg[T](n: Int): Seq[EventBusMessage[T]] =
      probe.receiveN(n).asInstanceOf[Seq[EventBusMessage[T]]]

    def expectFailure[T](max: Duration = Duration.Undefined)(implicit t: ClassTag[T]): T = {
      probe.expectMsgPF[T](max, hint = s"Failure($t)") {
        case f @ Failure(err: T) => err
      }
    }
  }
}
