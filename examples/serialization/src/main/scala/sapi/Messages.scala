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

//#event-sourced-actor

// Commands
sealed trait SCommand
case object PrintCommand extends SCommand
case class AppendCommand(entry: String) extends SCommand

// Command replies
sealed trait SCommandReply
case class AppendFailureCommandReply(cause: Throwable) extends SCommandReply
case class AppendSuccessCommandReply(entry: String) extends SCommandReply

// Events
sealed trait SEvent
case class AppendedEvent(entry: String) extends SEvent
//#

import com.rbmhtechnology.eventuate.VectorTime
//#interactive-conflict-resolution
case class ResolvedEvent(selectedTimestamp: VectorTime) extends SEvent
//#
