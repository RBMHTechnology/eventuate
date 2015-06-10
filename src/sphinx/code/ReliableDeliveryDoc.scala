/*
 * Copyright (C) 2015 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package doc

object ReliableDelivery extends App {
  //#reliable-delivery
  import scala.concurrent.duration._
  import scala.util._
  import akka.actor._
  import com.rbmhtechnology.eventuate.EventsourcedActor
  import com.rbmhtechnology.eventuate.ConfirmedDelivery

  case class DeliverCommand(message: String)
  case class DeliverEvent(message: String)

  case class Confirmation(deliveryId: String)
  case class ConfirmationEvent(deliveryId: String)

  case class ReliableMessage(deliveryId: String, message: String)
  case object Redeliver

  class ExampleActor(destination: ActorPath,
                     override val id: String,
                     override val eventLog: ActorRef)
    extends EventsourcedActor with ConfirmedDelivery {

    import context.dispatcher

    context.system.scheduler.schedule(
      initialDelay = 10.seconds,
      interval = 5.seconds,
      receiver = self,
      message = Redeliver)

    override val onCommand: Receive = {
      case DeliverCommand(message) =>
        persist(DeliverEvent(message)) {
          case Success(evt) => onEvent(evt)
          case Failure(err) => // ...
        }
      case Confirmation(deliveryId) =>
        persist(ConfirmationEvent(deliveryId)) {
          case Success(evt) => onEvent(evt)
          case Failure(err) => // ...
        }
      case Redeliver =>
        redeliverUnconfirmed()
    }

    override val onEvent: Receive = {
        case DeliverEvent(message) =>
          val deliveryId = lastSequenceNr.toString
          deliver(deliveryId, ReliableMessage(deliveryId, message), destination)
        case ConfirmationEvent(deliveryId) =>
          confirm(deliveryId)
    }
  }
  //#

  class Destination extends Actor {
    import context.dispatcher

    def receive = {
      case r @ ReliableMessage(deliveryId, message) =>
        context.system.scheduler.scheduleOnce(3.seconds, sender(), Confirmation(deliveryId))
    }
  }

  import com.rbmhtechnology.eventuate.ReplicationConnection._
  import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog

  val system: ActorSystem = ActorSystem(DefaultRemoteSystemName)
  val eventLog: ActorRef = system.actorOf(LeveldbEventLog.props("rd"))

  val dest = system.actorOf(Props(new Destination))
  val actor = system.actorOf(Props(new ExampleActor(dest.path, "r1", eventLog)))

  actor ! DeliverCommand("test")
}
