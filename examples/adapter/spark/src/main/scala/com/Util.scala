/*
 * Copyright 2015 - 2017 Red Bull Media House GmbH <http://www.redbullmediahouse.com> and Mike Slinn - all rights reserved.
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

package sapi

import akka.actor.ActorSystem

object Util {
  // Pause for messages to be displayed before shutting down Akka
  def pauseThenStop(seconds: Int = 1)(implicit system: ActorSystem): Unit = {
    import scala.concurrent.duration._
    import system.dispatcher
    import scala.language.postfixOps
    system.scheduler.scheduleOnce(seconds seconds) {
      system.terminate()
      ()
    }
    ()
  }
}
