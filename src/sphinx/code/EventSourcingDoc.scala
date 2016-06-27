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
    override def onCommand = {
      case ExampleCommand(data) =>
        // validate command
        // ...

        // derive event
        val event = ExampleEvent(data)
        // persist event (asynchronously)
        persist(event) {
          case Success(evt) =>
            // success reply
            sender() ! ExampleCommandSuccess(data)
          case Failure(cause) =>
            // failure reply
            sender() ! ExampleCommandFailure(cause)
        }
    }

    override def onEvent = {
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
    override def onCommand = ???
    //#event-handler
    /** Event handler */
    override def onEvent = {
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

    override def onCommand = {
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

    override def onEvent = {
      case evt => // update state ...
    }
  }
  //#
}

object Recovery {
  import akka.actor._
  import com.rbmhtechnology.eventuate.EventsourcedActor
  import scala.util._

  //#recovery-handler
  class ExampleActor(override val id: String,
                     override val eventLog: ActorRef) extends EventsourcedActor {
  //#
    override def onCommand: Receive = ???
    override def onEvent: Receive = ???
  //#recovery-handler
  // ...

    override def onRecovery = {
      case Success(_) => // ...
      case Failure(_) => // ...
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

    override def onCommand = {
      case Save => // ...
      case cmd => // ...
    }

    override def onEvent = {
      case evt => // update state ...
    }

    /** Snapshot handler */
    override def onSnapshot = {
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

    //override def sharedClockEntry: Boolean = false

    // ...
  //#
    override def onCommand = {
      case cmd => // ...
    }

    override def onEvent = {
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
                     override val eventLog: ActorRef
                     /*override val sharedClockEntry: Boolean*/) extends EventsourcedActor {

    // ..
  //#
    override def onCommand = {
      case cmd => // ...
    }

    override def onEvent = {
      case evt => // ...
    }
  //#clock-entry-instance
  }
  //#
}

object BatchReplay {
  import akka.actor._
  import com.rbmhtechnology.eventuate.EventsourcedActor

  //#replay-batch-size
  class ExampleActor(override val id: String,
                     override val eventLog: ActorRef) extends EventsourcedActor {

    override def replayBatchSize: Int = 64

    // ...
  //#
    override def onCommand = {
      case cmd => // ...
    }

    override def onEvent = {
      case evt => // ...
    }
  //#replay-batch-size
  }
  //#
}

object Processor {
  import akka.actor._
  import com.rbmhtechnology.eventuate.EventsourcedProcessor
  import scala.collection.immutable.Seq

  //#processor
  class Processor(
      val id: String,
      val eventLog: ActorRef,
      val targetEventLog: ActorRef)
    extends EventsourcedProcessor {
    // ...

  //#
    override def onCommand = {
      case cmd => // ...
    }

  //#processor
    override val processEvent: Process = {
      // exclude event
      case "my-event-1" => Seq()
      // transform event
      case "my-event-2" => Seq("my-event-2a")
      // transform and split event
      case "my-event-3" => Seq("my-event-3a", "my-event-3b")
    }
  }
  //#
}

object CommandStash {
  import akka.actor._
  import com.rbmhtechnology.eventuate.EventsourcedActor
  import scala.util._

  //#command-stash
  case class CreateUser(userId: String, name: String)
  case class UpdateUser(userId: String, name: String)

  sealed trait UserEvent

  case class UserCreated(userId: String, name: String) extends UserEvent
  case class UserUpdated(userId: String, name: String) extends UserEvent

  case class User(userId: String, name: String)

  class UserManager(val eventLog: ActorRef) extends EventsourcedActor {
    private var users: Map[String, User] = Map.empty

    override val id = "example"

    override def onCommand = {
      case CreateUser(userId, name) =>
        persistUserEvent(UserCreated(userId, name), unstashAll())
      case UpdateUser(userId, name) if users.contains(userId) =>
        // UpdateUser received after CreateUser
        persistUserEvent(UserUpdated(userId, name))
      case UpdateUser(userId, name) =>
        // UpdateUser received before CreateUser
        stash()
      // ...
    }

    override def onEvent = {
      case UserCreated(userId, name) =>
        users = users.updated(userId, User(userId, name))
      case UserUpdated(userId, name) =>
        users = users.updated(userId, User(userId, name))
    }

    private def persistUserEvent(event: UserEvent, onSuccess: => Unit = ()) =
      persist(event) {
        case Success(evt) =>
          sender() ! evt
          onSuccess
        case Failure(err) =>
          sender() ! err
      }
  }
  //#
}

object BehaviorChanges {
  import com.rbmhtechnology.eventuate.EventsourcedActor

  //#behavior-changes
  trait ExampleActor extends EventsourcedActor {
    // default command handler
    override def onCommand: Receive = {
      case "a" => commandContext.become(newCommandHandler)
    }

    // default event handler
    override def onEvent: Receive = {
      case "x" => eventContext.become(newEventHandler)
    }

    def newCommandHandler: Receive = {
      case "b" =>
        // restores default command handler
        commandContext.unbecome()
    }

    def newEventHandler: Receive = {
      case "y" =>
        // restores default event handler
        eventContext.unbecome()
    }
    //#
  }
}
