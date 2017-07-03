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

object ConditionalExample extends App {
  import akka.actor._
  //#conditional-requests
  import akka.pattern.ask
  import akka.util.Timeout
  import com.rbmhtechnology.eventuate._
  import EventsourcedViews._
  import scala.concurrent.duration._
  import scala.util._

  //#
  import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog

  implicit val system: ActorSystem = ActorSystem(ReplicationConnection.DefaultRemoteSystemName)

  // This will create a directory called `target/log-qt-2/` to contain the log files
  val eventLog: ActorRef = system.actorOf(LeveldbEventLog.props("qt-2"))

  //#conditional-requests
  class ExampleActor(
    override val id: String,
    override val eventLog: ActorRef
  ) extends EventsourcedActor {
    private var currentState: Vector[String] = Vector.empty
    override val aggregateId = Some(id)

    override def onCommand: PartialFunction[Any, Unit] = {
      case AppendCommand(entry) => persist(AppendedEvent(entry)) {
        case Success(_) =>
          sender() ! AppendSuccessWithTimestampCommandReply(entry, lastVectorTimestamp)

        case Failure(_) =>
          // ...
      }
      // ...
    }

    override def onEvent: PartialFunction[Any, Unit] = {
      case AppendedEvent(entry) => currentState = currentState :+ entry
    }
  }

  class ExampleView(override val id: String, override val eventLog: ActorRef)
    extends EventsourcedView with ConditionalRequests {
    // ...
  //#
    private var appendCount: Long = 0L
    private var resolveCount: Long = 0L

    override def onCommand: PartialFunction[Any, Unit] = {
      case GetAppendCount => sender() ! GetAppendCountReply(appendCount)
      case GetResolveCount => sender() ! GetResolveCountReply(resolveCount)
    }

    override def onEvent: PartialFunction[Any, Unit] = {
      case _: AppendedEvent => appendCount += 1L
      case _: Resolved => resolveCount += 1L
    }
  //#conditional-requests
  }

  val ea: ActorRef = system.actorOf(Props(new ExampleActor("ea", eventLog)))
  val ev: ActorRef = system.actorOf(Props(new ExampleView("ev", eventLog)))

  import system.dispatcher
  implicit val timeout = Timeout(5.seconds)

  for {
    AppendSuccessWithTimestampCommandReply(_, timestamp) <- ea ? AppendCommand("a")
    GetAppendCountReply(count)  <- ev ? ConditionalRequest(timestamp, GetAppendCount)
  } println(s"append count = $count")
  //#

  Util.pauseThenStop()
}
