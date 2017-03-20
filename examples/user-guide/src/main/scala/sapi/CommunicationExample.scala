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

//#ping-pong-events
sealed trait SPEvent
case class PingEvent(num: Int) extends SPEvent
case class PongEvent(num: Int) extends SPEvent
//#

object CommunicationExample extends App {
  //#event-driven-communication1
  import akka.actor._
  import com.rbmhtechnology.eventuate.{EventsourcedActor, PersistOnEvent}
  import com.rbmhtechnology.eventuate.EventsourcedView.Handler
  import com.rbmhtechnology.eventuate.ReplicationConnection.DefaultRemoteSystemName
  import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog

  val system: ActorSystem = ActorSystem(DefaultRemoteSystemName)
  val eventLog: ActorRef = system.actorOf(LeveldbEventLog.props("qt-1"))
  //#
  //#ping-actor

  class PingActor(
    val id: String,
    val eventLog: ActorRef,
    completion: ActorRef
  ) extends EventsourcedActor with PersistOnEvent {
    override def onCommand: PartialFunction[Any, Unit] = {
      case "serve" =>
        persist(PingEvent(1))(Handler.empty)

      case x =>
        Console.err.println(s"Error: PingActor.onCommand did not expect a $x message")
    }

    override def onEvent: PartialFunction[Any, Unit] = {
      case PongEvent(10) if !recovering =>
        completion ! "done"

      case PongEvent(i)  =>
        persistOnEvent(PingEvent(i + 1))

      case x =>
        // Error: PongActor.onEvent did not expect a Append(a) message
        // Error: PingActor.onEvent did not expect a PingEvent(6222) message
        // Error: PongActor.onEvent did not expect a PongEvent(6222) message
        Console.err.println(s"Error: PingActor.onEvent did not expect a $x message")
    }
  }
  //#
  //#pong-actor

  class PongActor(
    val id: String,
    val eventLog: ActorRef
  ) extends EventsourcedActor with PersistOnEvent {
    override def onCommand: PartialFunction[Any, Unit] = {
      case _ =>
    }

    override def onEvent: PartialFunction[Any, Unit] = {
      case PingEvent(i) =>
        persistOnEvent(PongEvent(i))

      case x =>
        // leftovers from running ActorExample:
        // Error: PingActor.onEvent did not expect a Append(x) message
        Console.err.println(s"Error: PongActor.onEvent did not expect a $x message")
    }
  }
  //#
  //#event-driven-communication2

  val pingActor: ActorRef = system.actorOf(
    Props(new PingActor("ping", eventLog, system.deadLetters))
  )
  val pongActor: ActorRef = system.actorOf(
    Props(new PongActor("pong", eventLog))
  )

  pingActor ! "serve"
  //#
}
