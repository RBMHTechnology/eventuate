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

object ActorExample extends App {
  //#event-sourced-actor
  import akka.actor._
  import com.rbmhtechnology.eventuate.EventsourcedActor
  import scala.util._

  class ExampleActor(
    override val id: String,
    override val aggregateId: Option[String],
    override val eventLog: ActorRef
  ) extends EventsourcedActor {
    import scala.collection.mutable
    private val currentState: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty

    override def onCommand: PartialFunction[Any, Unit] = {
      case PrintCommand =>
        println(s"[id = $id, aggregate id = ${ aggregateId.getOrElse("<undefined>")}] ${ currentState.mkString(",") }")

      case AppendCommand(entry) => persist(AppendedEvent(entry)) {
        case Success(_)   => sender() ! AppendSuccessCommandReply(entry)
        case Failure(err) => sender() ! AppendFailureCommandReply(err)
      }
    }

    override def onEvent: PartialFunction[Any, Unit] = {
      case AppendedEvent(entry) => currentState :+ entry
    }
  }
  //#

  //#create-one-instance
  import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog
  import com.rbmhtechnology.eventuate.ReplicationConnection._

  // `DefaultRemoteSystemName` is defined as "location" in the
  // [[ReplicationConnection]] object, so the ActorSystem name
  // is "location"
  implicit val system: ActorSystem = ActorSystem(DefaultRemoteSystemName)
  //#

  //#create-one-instance

  // Wrap a new instance of a `LeveldbEventLog` configuration object with
  // log id "qt-1" into an Actor.
  // This creates a log directory called `target/log-qt-1/`
  val eventLog: ActorRef = system.actorOf(LeveldbEventLog.props("qt-1"))
  //#

  //#create-one-instance

  // Create a new instance of ExampleActor with id=="1" and aggregateId==Some("a");
  // also provide the eventLog [[ActorRef]] to the actorOf [[Actor]] factory
  val ea1 = system.actorOf(Props(new ExampleActor("1", Some("a"), eventLog)))

  ea1 ! AppendCommand("a")
  ea1 ! AppendCommand("b")
  //#

  //#print-one-instance
  ea1 ! PrintCommand
  //#

  //#create-two-instances
  val b2: ActorRef =
    system.actorOf(Props(new ExampleActor("2", Some("b"), eventLog)))
  val c3: ActorRef =
    system.actorOf(Props(new ExampleActor("3", Some("c"), eventLog)))

  b2 ! AppendCommand("a")
  b2 ! AppendCommand("b")

  c3 ! AppendCommand("x")
  c3 ! AppendCommand("y")
  //#

  //#print-two-instances
  b2 ! PrintCommand
  c3 ! PrintCommand
  //#

  //#create-replica-instances
  // created at location 1
  val d4 = system.actorOf(Props(new ExampleActor("4", Some("d"), eventLog)))

  // created at location 2
  val d5 = system.actorOf(Props(new ExampleActor("5", Some("d"), eventLog)))

  d4 ! AppendCommand("a")
  //#

  Thread.sleep(1000)

  d4 ! PrintCommand // fixme why is this not referenced in user-guide.rst?
  d5 ! PrintCommand

  //#send-another-append
  d5 ! AppendCommand("b")
  //#

  Thread.sleep(1000)

  d4 ! PrintCommand
  d5 ! PrintCommand

  Util.pauseThenStop()
}
