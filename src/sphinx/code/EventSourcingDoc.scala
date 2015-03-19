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

object EventSourcing {
  //#command-handler
  import scala.util._
  import akka.actor._
  import com.rbmhtechnology.eventuate.EventsourcedActor

  case class ExampleEvent(data: String)
  case class ExampleCommand(data: String)
  case class ExampleCommandSuccess(data: String)
  case class ExampleCommandFailure(cause: Throwable)

  class ExampleActor(override val replicaId: String,
                     override val eventLog: ActorRef) extends EventsourcedActor {

    /** Command handler */
    override val onCommand: Receive = {
      case ExampleCommand(data) =>
        // validate command
        // ...

        // derive event
        val event = ExampleEvent(data)
        // persist event
        persist(event) {
          case Success(evt) =>
            // handle event
            onEvent(evt)
            // success reply
            sender() ! ExampleCommandSuccess(data)
          case Failure(cause) =>
            // failure reply
            sender() ! ExampleCommandFailure(cause)
        }
    }

    override val onEvent: Receive = {
      case ExampleEvent(data) => // ...
    }
  }
  //#

  trait EventsourcedActorAPI {
    //#persist-signature
    def persist[A](event: A, customRoutingDestinations: Set[String] = Set())(handler: Try[A] => Unit): Unit
    //#

    //#delay-signature
    def delay[A](command: A)(handler: A => Unit): Unit
    //#
  }

  class EventsourcedActorAPIImpl extends EventsourcedActorAPI with EventsourcedActor {
    override def eventLog: ActorRef = ???
    override def replicaId: String = ???
    override def onCommand: Receive = ???
    //#event-handler
    /** Event handler */
    override val onEvent: Receive = {
      case ExampleEvent(details) =>
        val eventSequenceNr = lastSequenceNr
        val eventVectorTimestamp = lastVectorTimestamp
        // ...

        // update actor state
        // ...
    }
    //#
  }
}
