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

object EventsourcedActors extends App {
  //#event-sourced-actor
  import scala.util._
  import akka.actor._
  import com.rbmhtechnology.eventuate.EventsourcedActor

  // Commands
  case object Print
  case class Append(entry: String)

  // Command replies
  case class AppendSuccess(entry: String)
  case class AppendFailure(cause: Throwable)

  // Event
  case class Appended(entry: String)

  class ExampleActor(override val id: String,
                     override val aggregateId: Option[String],
                     override val eventLog: ActorRef) extends EventsourcedActor {

    private var currentState: Vector[String] = Vector.empty

    override def onCommand = {
      case Print =>
        println(s"[id = $id, aggregate id = ${aggregateId.getOrElse("<undefined>")}] ${currentState.mkString(",")}")
      case Append(entry) => persist(Appended(entry)) {
        case Success(evt) => sender() ! AppendSuccess(entry)
        case Failure(err) => sender() ! AppendFailure(err)
      }
    }

    override def onEvent = {
      case Appended(entry) => currentState = currentState :+ entry
    }
  }
  //#

  import com.rbmhtechnology.eventuate.ReplicationConnection._
  import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog

  //#create-one-instance
  val system: ActorSystem = // ...
  //#
    ActorSystem(DefaultRemoteSystemName)

  //#create-one-instance
  val eventLog: ActorRef = // ...
  //#
    system.actorOf(LeveldbEventLog.props("qt-1"))

  //#create-one-instance

  val ea1 = system.actorOf(Props(new ExampleActor("1", Some("a"), eventLog)))

  ea1 ! Append("a")
  ea1 ! Append("b")
  //#

  //#print-one-instance
  ea1 ! Print
  //#

  //#create-two-instances
  val b2 = system.actorOf(Props(new ExampleActor("2", Some("b"), eventLog)))
  val c3 = system.actorOf(Props(new ExampleActor("3", Some("c"), eventLog)))

  b2 ! Append("a")
  b2 ! Append("b")

  c3 ! Append("x")
  c3 ! Append("y")
  //#

  //#print-two-instances
  b2 ! Print
  c3 ! Print
  //#

  //#create-replica-instances
  // created at location 1
  val d4 = system.actorOf(Props(new ExampleActor("4", Some("d"), eventLog)))

  // created at location 2
  val d5 = system.actorOf(Props(new ExampleActor("5", Some("d"), eventLog)))

  d4 ! Append("a")
  //#

  Thread.sleep(1000)

  d4 ! Print
  d5 ! Print

  //#send-another-append
  d5 ! Append("b")
  //#

  Thread.sleep(1000)

  d4 ! Print
  d5 ! Print
}

object EventsourcedActorsUpdated {
  import scala.util._
  import akka.actor._

  case object Print
  case class Append(entry: String)
  case class AppendSuccess(entry: String)
  case class AppendFailure(cause: Throwable)
  case class Appended(entry: String)

  {
    //#detecting-concurrent-update
    import com.rbmhtechnology.eventuate.EventsourcedActor
    import com.rbmhtechnology.eventuate.VectorTime

    class ExampleActor(override val id: String,
                       override val aggregateId: Option[String],
                       override val eventLog: ActorRef) extends EventsourcedActor {

      private var currentState: Vector[String] = Vector.empty
      private var updateTimestamp: VectorTime = VectorTime()

      override def onCommand = {
        // ...
    //#
        case _ =>
    //#detecting-concurrent-update
      }

      override def onEvent = {
        case Appended(entry) =>
          if (updateTimestamp < lastVectorTimestamp) {
            // regular update
            currentState = currentState :+ entry
            updateTimestamp = lastVectorTimestamp
          } else if (updateTimestamp conc lastVectorTimestamp) {
            // concurrent update
            // TODO: track conflicting versions
          }
      }
    }
    //#
  }

  {
    //#tracking-conflicting-versions
    import scala.collection.immutable.Seq
    import com.rbmhtechnology.eventuate.{ConcurrentVersions, Versioned}
    import com.rbmhtechnology.eventuate.EventsourcedActor

    class ExampleActor(override val id: String,
                       override val aggregateId: Option[String],
                       override val eventLog: ActorRef) extends EventsourcedActor {

      private var versionedState: ConcurrentVersions[Vector[String], String] =
        ConcurrentVersions(Vector.empty, (s, a) => s :+ a)

      override def onCommand = {
        // ...
    //#
        case _ =>
    //#tracking-conflicting-versions
      }

      override def onEvent = {
        case Appended(entry) =>
          versionedState = versionedState.update(entry, lastVectorTimestamp)
          if (versionedState.conflict) {
            val conflictingVersions: Seq[Versioned[Vector[String]]] = versionedState.all
            // TODO: resolve conflicting versions
          } else {
            val currentState: Vector[String] = versionedState.all.head.value
            // ...
          }
      }
    }
    //#
  }

  {
    import com.rbmhtechnology.eventuate._

    //#automated-conflict-resolution
    class ExampleActor(override val id: String,
                       override val aggregateId: Option[String],
                       override val eventLog: ActorRef) extends EventsourcedActor {

      private var versionedState: ConcurrentVersions[Vector[String], String] =
        ConcurrentVersions(Vector.empty, (s, a) => s :+ a)

      override def onCommand = {
        // ...
    //#
        case _ =>
    //#automated-conflict-resolution
      }

      override def onEvent = {
        case Appended(entry) =>
          versionedState = versionedState
            .update(entry, lastVectorTimestamp, lastSystemTimestamp, lastEmitterId)
          if (versionedState.conflict) {
            val conflictingVersions = versionedState.all.sortWith { (v1, v2) =>
              if (v1.systemTimestamp == v2.systemTimestamp) v1.creator < v2.creator
              else v1.systemTimestamp > v2.systemTimestamp
            }
            val winnerTimestamp: VectorTime = conflictingVersions.head.vectorTimestamp
            versionedState = versionedState.resolve(winnerTimestamp)
          }
      }
    }
    //#
  }

  {
    import scala.collection.immutable.Seq
    import com.rbmhtechnology.eventuate._

    //#interactive-conflict-resolution
    case class Append(entry: String)
    case class AppendRejected(entry: String, conflictingVersions: Seq[Versioned[Vector[String]]])

    case class Resolve(selectedTimestamp: VectorTime)
    case class Resolved(selectedTimestamp: VectorTime)

    class ExampleActor(override val id: String,
                       override val aggregateId: Option[String],
                       override val eventLog: ActorRef) extends EventsourcedActor {

      private var versionedState: ConcurrentVersions[Vector[String], String] =
        ConcurrentVersions(Vector.empty, (s, a) => s :+ a)

      override def onCommand = {
        case Append(entry) if versionedState.conflict =>
          sender() ! AppendRejected(entry, versionedState.all)
        case Append(entry) =>
          // ...
        case Resolve(selectedTimestamp) => persist(Resolved(selectedTimestamp)) {
          case Success(evt) => // reply to sender omitted ...
          case Failure(err) => // reply to sender omitted ...
        }
      }

      override def onEvent = {
        case Appended(entry) =>
          versionedState = versionedState
            .update(entry, lastVectorTimestamp, lastSystemTimestamp, lastEmitterId)
        case Resolved(selectedTimestamp) =>
          versionedState = versionedState.resolve(selectedTimestamp, lastVectorTimestamp)
      }
    }
    //#
  }
}

object EventsourcedViews {
  //#event-sourced-view
  import akka.actor.ActorRef
  import com.rbmhtechnology.eventuate.EventsourcedView
  import com.rbmhtechnology.eventuate.VectorTime

  case class Appended(entry: String)
  case class Resolved(selectedTimestamp: VectorTime)

  case object GetAppendCount
  case class GetAppendCountReply(count: Long)

  case object GetResolveCount
  case class GetResolveCountReply(count: Long)

  class ExampleView(override val id: String, override val eventLog: ActorRef) extends EventsourcedView {
    private var appendCount: Long = 0L
    private var resolveCount: Long = 0L

    override def onCommand = {
      case GetAppendCount => sender() ! GetAppendCountReply(appendCount)
      case GetResolveCount => sender() ! GetResolveCountReply(resolveCount)
    }

    override def onEvent = {
      case Appended(_) => appendCount += 1L
      case Resolved(_) => resolveCount += 1L
    }
  }
  //#
}

object ConditionalRequests extends App {
  import akka.actor._
  import com.rbmhtechnology.eventuate.ReplicationConnection
  import com.rbmhtechnology.eventuate.log.leveldb.LeveldbEventLog
  import EventsourcedViews._

  val system: ActorSystem = ActorSystem(ReplicationConnection.DefaultRemoteSystemName)
  val eventLog = system.actorOf(LeveldbEventLog.props("qt-2"))

  //#conditional-requests
  import scala.concurrent.duration._
  import scala.util._
  import akka.actor._
  import akka.pattern.ask
  import akka.util.Timeout
  import com.rbmhtechnology.eventuate._

  case class Append(entry: String)
  case class AppendSuccess(entry: String, updateTimestamp: VectorTime)

  class ExampleActor(override val id: String,
                     override val eventLog: ActorRef) extends EventsourcedActor {

    private var currentState: Vector[String] = Vector.empty
    override val aggregateId = Some(id)

    override def onCommand = {
      case Append(entry) => persist(Appended(entry)) {
        case Success(evt) =>
          sender() ! AppendSuccess(entry, lastVectorTimestamp)
        // ...
      }
      // ...
    }

    override def onEvent = {
      case Appended(entry) => currentState = currentState :+ entry
    }
  }

  class ExampleView(override val id: String, override val eventLog: ActorRef)
    extends EventsourcedView with ConditionalRequests {
    // ...
  //#
    private var appendCount: Long = 0L
    private var resolveCount: Long = 0L

    override def onCommand = {
      case GetAppendCount => sender() ! GetAppendCountReply(appendCount)
      case GetResolveCount => sender() ! GetResolveCountReply(resolveCount)
    }

    override def onEvent = {
      case Appended(_) => appendCount += 1L
      case Resolved(_) => resolveCount += 1L
    }
  //#conditional-requests
  }

  val ea = system.actorOf(Props(new ExampleActor("ea", eventLog)))
  val ev = system.actorOf(Props(new ExampleView("ev", eventLog)))

  import system.dispatcher
  implicit val timeout = Timeout(5.seconds)

  for {
    AppendSuccess(_, timestamp) <- ea ? Append("a")
    GetAppendCountReply(count)  <- ev ? ConditionalRequest(timestamp, GetAppendCount)
  } println(s"append count = $count")
  //#
}

object EventCommunication {
  import akka.actor._

  var system: ActorSystem = _
  var eventLog: ActorRef = _

  //#event-driven-communication
  // some imports omitted ...
  import com.rbmhtechnology.eventuate.EventsourcedView.Handler
  import com.rbmhtechnology.eventuate.EventsourcedActor
  import com.rbmhtechnology.eventuate.PersistOnEvent

  case class Ping(num: Int)
  case class Pong(num: Int)

  class PingActor(val id: String, val eventLog: ActorRef, completion: ActorRef)
    extends EventsourcedActor with PersistOnEvent {

    override def onCommand = {
      case "serve" => persist(Ping(1))(Handler.empty)
    }

    override def onEvent = {
      case Pong(10) if !recovering => completion ! "done"
      case Pong(i)  => persistOnEvent(Ping(i + 1))
    }
  }

  class PongActor(val id: String, val eventLog: ActorRef)
    extends EventsourcedActor with PersistOnEvent {

    override def onCommand = {
      case _ =>
    }
    override def onEvent = {
      case Ping(i) => persistOnEvent(Pong(i))
    }
  }

  val pingActor = system.actorOf(Props(new PingActor("ping", eventLog, system.deadLetters)))
  val pongActor = system.actorOf(Props(new PongActor("pong", eventLog)))

  pingActor ! "serve"
  //#
}
