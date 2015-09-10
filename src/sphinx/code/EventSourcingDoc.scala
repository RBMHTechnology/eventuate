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

  class ExampleActor(override val id: String,
                     override val eventLog: ActorRef) extends EventsourcedActor {

    /** Command handler */
    override val onCommand: Receive = {
      case ExampleCommand(data) =>
        // validate command
        // ...

        // derive event
        val event = ExampleEvent(data)
        // persist event (asynchronously)
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
    def persist[A](event: A, customDestinationAggregateIds: Set[String] = Set())(handler: Try[A] => Unit): Unit
    //#
  }

  class EventsourcedActorAPIImpl extends EventsourcedActorAPI with EventsourcedActor {
    override def eventLog: ActorRef = ???
    override def id: String = ???
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

object SaveSnapshot {
  //#snapshot-save
  import scala.util._
  import akka.actor._
  import com.rbmhtechnology.eventuate.EventsourcedActor
  import com.rbmhtechnology.eventuate.SnapshotMetadata

  case object Save
  case class SaveSuccess(metadata: SnapshotMetadata)
  case class SaveFailure(cause: Throwable)
  case class ExampleState(components: Vector[String] = Vector.empty)

  class ExampleActor(override val id: String,
                     override val eventLog: ActorRef) extends EventsourcedActor {

    var state: ExampleState = ExampleState()

    override val onCommand: Receive = {
      case Save =>
        // save snapshot of internal state (asynchronously)
        save(state) {
          case Success(metadata) =>
            // success reply
            sender() ! SaveSuccess(metadata)
          case Failure(cause) =>
            // failure reply
            sender() ! SaveFailure(cause)
        }
      case cmd => // ...
    }

    override val onEvent: Receive = {
      case evt => // update state ...
    }
  }
  //#
}

object LoadSnapshot {
  import akka.actor._
  import com.rbmhtechnology.eventuate.EventsourcedActor

  case object Save
  case class ExampleState(components: Vector[String] = Vector.empty)

  //#snapshot-load
  class ExampleActor(override val id: String,
                     override val eventLog: ActorRef) extends EventsourcedActor {

    var state: ExampleState = ExampleState()

    override val onCommand: Receive = {
      case Save => // ...
      case cmd => // ...
    }

    override val onEvent: Receive = {
      case evt => // update state ...
    }

    /** Snapshot handler */
    override val onSnapshot: Receive = {
      case s: ExampleState =>
        // initialize internal state from loaded snapshot
        state = s
    }
  }
  //#
}

object ClockEntryClass {
  import akka.actor._
  import com.rbmhtechnology.eventuate.EventsourcedActor

  //#clock-entry-class
  class ExampleActor(override val id: String,
                     override val eventLog: ActorRef) extends EventsourcedActor {

    override def sharedClockEntry: Boolean = false

    // ...
  //#
    override val onCommand: Receive = {
      case cmd => // ...
    }

    override val onEvent: Receive = {
      case evt => // ...
    }
  //#clock-entry-class
  }
  //#
}

object ClockEntryInstance {
  import akka.actor._
  import com.rbmhtechnology.eventuate.EventsourcedActor

  //#clock-entry-instance
  class ExampleActor(override val id: String,
                     override val eventLog: ActorRef,
                     override val sharedClockEntry: Boolean) extends EventsourcedActor {

    // ..
  //#
    override val onCommand: Receive = {
      case cmd => // ...
    }

    override val onEvent: Receive = {
      case evt => // ...
    }
  //#clock-entry-instance
  }
  //#
}
