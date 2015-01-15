[![Build Status](https://travis-ci.org/RBMHTechnology/eventuate.svg?branch=master)](https://travis-ci.org/RBMHTechnology/eventuate)

Eventuate
=========

Eventuate is an implementation of the concepts and system model described in [Event Sourcing at Global Scale](http://krasserm.github.io/2015/01/13/event-sourcing-at-global-scale). The project has proof-of-concept status and will gradually evolve into a production-ready toolkit for event sourcing at global scale. API and implementation may significantly change. The re-usable parts of the prototype are contained in package `com.rbmhtechnology.eventuate`, the example application in package `com.rbmhtechnology.example` (Scala version) and package `com.rbmhtechnology.example.japi` (Java version). The following sections give a (still incomplete) overview of the prototype features and instructions for running the example application. 

Dependencies
------------

### Sbt

    resolvers += "OJO Snapshots" at "https://oss.jfrog.org/oss-snapshot-local"

    libraryDependencies += "com.rbmhtechnology" %% "eventuate" % "0.1-SNAPSHOT"

### Maven

...

### Gradle 

...

Sites
-----

The [example application](#example-application) has 6 sites (A - F), each running in a separate process on localhost. By changing the configuration, sites can also be distributed on multiple hosts. Sites A - F are connected via bi-directional, asynchronous event replication connections as shown in the following figure.  
  
    A        E
     \      /    
      C -- D
     /      \
    B        F

Each site must configure

- a hostname and port for Akka Remoting (`akka.remote.netty.tcp.hostname` and `akka.remote.netty.tcp.port`)
- an id for the local event log copy (`log.id` which is equal to the site id)
- a list of replication connections to other sites (`log.connections`)

For example, this is the configuration of site C:
  
    akka.remote.netty.tcp.hostname = "127.0.0.1"
    akka.remote.netty.tcp.port=2554
  
    log.id = "C"
    log.connections = ["127.0.0.1:2552", "127.0.0.1:2553", "127.0.0.1:2555"]

Later versions will support the automated setup of replication connections when a site joins or leaves a replication network.

Event log
---------

The replicated event log implementation of the prototype is backed by a LevelDB instance on each site. A site-local event log exposes a replication endpoint that is used by the inter-site replication protocol for replicating events. A site-local event log and its replication endpoint can be instantiated as follows:
  
```scala
import akka.actor._
  
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.log.LeveldbEventLog

val system = ActorSystem("site")
val endpoint = new ReplicationEndpoint(system, logId => LeveldbEventLog.props(logId))
```

A replication endpoint and the `log.connections` configuration on each site implement the replicated event log. Please note that the current implementation doesn't allow cycles in the replication network to preserve the happens-before relationship (= potential causal relationship) of events. This is a limitation that will be removed in later versions (which will be able re-order events based on their vector timestamps). 

Event-sourced actors
--------------------

An event-sourced actor (EA) can be implemented by extending the `EventsourcedActor` trait.

```scala
class MyEA(override val log: ActorRef, override val processId: String) extends EventsourcedActor {
  override def onCommand: Receive = {
    case "some-cmd" => persist("some-event") {
      case Success(evt) => onEvent(evt)
      case Failure(err) => // persistence failed ...
    }
  }

  override def onEvent: Receive = {
    case "some-event" => // ...
  }
}
```

Similar to akka-persistence, commands are processed by a command handler (`onCommand`), events are processed by an event handler (`onEvent`) and events are written to the event log with the `persist` method whose signature is `persist[A](event: A)(handler: Try[A] => Unit)`. An EA must also implement `log` which is the event log that the EA shares with other EAs. The event log `ActorRef` of a site can be obtained from the replication endpoint with `endpoint.log`. 

The `processId` of an EA must be globally unique as it contributes to vector clocks. In the current implementation, each EA instance maintains its own vector clock, as it represents a light-weight process that exchanges events with other EA instances via a shared distributed `log`. Therefore, vector clock sizes linearly grow with the number of EA instances. Future versions will provide means to limit vector clock sizes, for example, by partitioning EAs in the event space or by grouping several EAs into the same "process", so that the system can scale to a large number of EAs.  

An EA does not only receive events persisted by itself but also those persisted by other EAs (located on the same site or on other sites) as long as they share a replicated `log` and can process those in its event handler. With the following definition of `MyEA`

```scala
class MyEA(val log: ActorRef, val processId: String) extends EventsourcedActor {
  def onCommand: Receive = {
    case cmd: String => persist(s"${cmd}-${processId}") {
      case Success(evt) => onEvent(evt)
      case Failure(err) => // ...
    }
  }

  def onEvent: Receive = {
    case evt: String => println(s"[${processId}] event = ${evt} (event timestamp = ${lastTimestamp})")
  }
}
```

and two instances of `MyEA`, `ea1` and `ea2` (with `processId`s `ea1` and `ea2`)

```scala
val ea1 = system.actorOf(Props(new MyEA(endpoint.log, "ea1")))
val ea2 = system.actorOf(Props(new MyEA(endpoint.log, "ea2")))
```

we send `ea1` and `ea2` a `test` command.

```scala
ea1 ! "test"
ea2 ! "test"
```

If `ea1` and `ea2` receive the `test` command before having received an event from the other actor then the generated events are concurrent and the output should look like

    [ea1] event = test-ea1 (event timestamp = VectorTime(ea1 -> 1))
    [ea2] event = test-ea1 (event timestamp = VectorTime(ea1 -> 1))
    [ea1] event = test-ea2 (event timestamp = VectorTime(ea2 -> 1))
    [ea2] event = test-ea2 (event timestamp = VectorTime(ea2 -> 1))

Please note that the timestamps in the output are event timestamps and not the current time of the vector clocks of `ea1` and `ea2`. If, on the other hand, `ea2` receives the `test` command after having having received an event from `ea1` then the event written by `ea2` causally depends on that from `ea1` and the output should look like

    [ea1] event = test-ea1 (event timestamp = VectorTime(ea1 -> 1))
    [ea2] event = test-ea1 (event timestamp = VectorTime(ea1 -> 1))
    [ea2] event = test-ea2 (event timestamp = VectorTime(ea1 -> 1,ea2 -> 2))
    [ea1] event = test-ea2 (event timestamp = VectorTime(ea1 -> 1,ea2 -> 2))
    
Maybe you'll see a different ordering of lines as `ea1` and `ea2` are running concurrently.

Event-sourced views
-------------------

Classes that extend the `EventsourcedView` (EV) trait (instead of `EventsourcedActor`) only implement the event consumer part of an EA but not the event producer part (i.e. they do not have a `persist` method). EVs also don't have a `processId` (as they do not contribute to vector clocks) but still need to implement `log` from which they consume events.  

```scala
class MyEV(val log: ActorRef) extends EventsourcedView {
  def onCommand: Receive = {
    case cmd => // ...
  }

  def onEvent: Receive = {
    case evt => // ...
  }
}
```

From a CQRS perspective, 

- EVs should be used to implement the query side (Q) of CQRS (and maintain a read model) whereas
- EAs should be used to implement the command side (C) of CQRS (and maintain a write model)


Vector timestamps
-----------------

The vector timestamp of an event can be obtained with `lastTimestamp` during event processing. Vector timestamps, for example, can be attached as version vectors to current state and compared to timestamps of new events in order to determine whether previous state updates happened-before or are concurrent to the current event.

```scala
class MyEA(val log: ActorRef, val processId: String) extends EventsourcedActor {
  var updateTimestamp: VectorTime = VectorTime()

  def updateState(event: String): Unit = {
    // ...
  }

  def onCommand: Receive = {
    // ...
  }

  def onEvent: Receive = {
    case evt: String if updateTimestamp < lastTimestamp =>
      // all previous state updates happened-before current event
      updateState(evt)
      updateTimestamp = lastTimestamp
    case evt: String if updateTimestamp conc lastTimestamp =>
      // one or more previous state updates are concurrent to current event
      // TODO: track concurrent versions ... 
  }
}
```

Vector timestamps can be compared with operators `<`, `<=`, `>`, `>=`, `conc` and `equiv` where
 
- `t1 < t2` returns `true` if `t1` happened-before `t2`
- `t1 conc t2` returns `true` if `t1` is concurrent to `t2`
- `t1 equiv t2` returns `true` if `t1` is equivalent to `t2`

Causal ordering
---------------

The replicated event log preserves the happens-before relationship (= potential causal ordering) of events: if `e1 -> e2`, where `->` is the happens-before relationship, then all EAs that consume `e1` and `e2` will consume `e1` before `e2`. The main benefit of causal ordering is that non-commutative state update operations (such as appending to a list) can be used.   

Concurrent versions
-------------------

The ordering of concurrent events, however, is not defined: if `e1` is concurrent to `e2` then some EA instances may consume `e1` before `e2`, others `e2` before `e1`. With commutative state update operations (see CmRDTs, for example), this is not an issue. With non-commutative state update operations, applications may want to track state updates from concurrent events as concurrent (or conflicting) versions of application state. This can be done with `ConcurrentVersions[S, A]` where `S` is the type of application state for which concurrent versions shall be tracked in case of concurrent events of type `A`.

```scala
class MyEA(val log: ActorRef, val processId: String) extends EventsourcedActor {
  var versionedState: ConcurrentVersions[Vector[String], String] = ConcurrentVersions(Vector(), (s, a) => s :+ a)

  def updateState(event: String): Unit = {
    versionedState = versionedState.update(event, lastTimestamp, lastProcessId)
    if (versionedState.conflict) {
      // TODO: either automated conflict resolution immediately or interactive conflict resolution later ...
    }
  }

  def onCommand: Receive = {
    // ...
  }

  def onEvent: Receive = {
    case evt: String => updateState(evt)
  }
}
```

In the example above, application state is of type `Vector[String]` and update events are of type `String`. State updates (appending to a `Vector`) are not commutative and we track concurrent versions of application state as `ConcurrentVersions[Vector[String], String]`. The state update function (= event projection function) `(s, a) => s :+ a` is called whenever `versionedState.update` is called (with event, event timestamp and the `processId` of the event emitter as arguments). In case of a concurrent update, `versionedState.conflict` returns `true` and the application should [resolve](#conflict-resolution) the conflict.

Please note that concurrent events are not necessarily conflicting events. Concurrent updates to different domain objects may be acceptable to an application whereas concurrent updates to the same domain object may be considered as conflict. In this case, concurrent versions for individual domain objects should be tracked. For example, an order management application could declare application state as follows:

```scala
var versionedState: Map[String, ConcurrentVersions[Order, OrderEvent]] 
```

The current implementation of `ConcurrentVersions` still needs to be optimized to de-allocate versions that are older than a certain threshold (as they can be re-created any time from the event log, if needed). This will be supported in future project releases.      

Conflict resolution
-------------------

Concurrent versions of application state represent update conflicts from concurrent events. Conflict resolution selects one of the concurrent versions as the winner which can be done in an automated or interactive way. The following is an example of automated conflict resolution: 

```scala
class MyEA(val log: ActorRef, val processId: String) extends EventsourcedActor {
  var versionedState: ConcurrentVersions[Vector[String], String] = ConcurrentVersions(Vector(), (s, a) => s :+ a)

  def updateState(event: String): Unit = {
    versionedState = versionedState.update(event, lastTimestamp, lastProcessId)
    if (versionedState.conflict) {
      // conflicting versions, sorted by processIds of EAs that emitted the concurrent update events
      val conflictingVersions: Seq[Versioned[Vector[String]]] = versionedState.all.sortBy(_.processId)
      // example conflict resolution function: lower process id wins
      val winnerVersion: VectorTime = conflictingVersions.head.version
      // resolve conflict by specifying the winner version
      versionedState = versionedState.resolve(winnerVersion)
    }
  }

  def onCommand: Receive = {
    // ...
  }

  def onEvent: Receive = {
    case evt: String => updateState(evt)
  }
}
```

In the example above, the version created from the event with the lower emitter `processId` is selected to be the winner. The selected version is identified by its version vector (`winnerVersion`) and passed as argument to `versionedState.update` during conflict resolution. Using emitter `processId`s for choosing a winner is just one example how conflicts can be resolved. Others could compare conflicting versions directly to make a decision. It is only important that the conflict resolution result does not depend on the order of conflicting events so that all application state replicas converge to the same value. 

Interactive conflict resolution does not resolve conflicts immediately but requests the user to select a winner version. In this case, the selected version must be stored as `Resolved` event in the event log (see example application code for details). Furthermore, interactive conflict resolution requires global agreement to avoid concurrent conflict resolutions. This can be achieved by static rules (for example, only the initial creator of a domain object may resolve conflicts for that object) or by global coordination, if needed. Global coordination can work well if conflicts do not occur frequently.

Sending messages
----------------

For sending messages to other non-event-sourced actors (representing external services, for example), EAs have several options:

- during command processing with at-most-once delivery semantics

    ```scala
    trait MyEA extends EventsourcedActor {
      def onCommand: Receive = {
        case cmd => persist("my-event") {
          case Success(evt) => sender() ! "command accepted"
          case Failure(err) => sender() ! "command failed"
        }
      }
    
      def onEvent: Receive = {
        case evt => // ...
      }
    }
    ```

- during event processing with at-least-once delivery semantics (see also [at-least-once-delivery](http://doc.akka.io/docs/akka/2.3.8/scala/persistence.html#at-least-once-delivery) in akka-persistence)

    ```scala
    trait MyEA extends EventsourcedActor with Delivery {
      val destination: ActorPath
    
      def onCommand: Receive = {
        case cmd => // ...
      }
    
      def onEvent: Receive = {
        case "my-deliver-event" => deliver("my-delivery-id", "my-message", destination)
        case "my-confirm-event" => confirm("my-delivery-id")
      }
    }
    ```

- during event processing with at-most-once delivery semantics 

    ```scala
    trait MyEA extends EventsourcedActor {
      val destination: ActorRef

      def onCommand: Receive = {
        case cmd => // ...
      }

      def onEvent: Receive = {
        case "my-event" if !recovering => destination ! "my-message"
      }
    }
    ```
    
Conditional commands
--------------------

Conditional commands are commands that have a vector timestamp attached as condition and can be processed by EAs and EVs.

```scala
case class ConditionalCommand(condition: VectorTime, cmd: Any)
```

Their processing is delayed until an EA's or EV's `lastTimestamp` is greater than or equal (`>=`) the specified `condition`. Hence, conditional commands can help to achieve read-your-write consistency across EAs and EVs (and even across sites), for example.   

Example application
-------------------

The example application is an over-simplified order management application that allows users to add and remove items from orders via a command-line interface. 

### Domain

The `Order` domain object is defined as follows:
 
```scala
 case class Order(id: String, items: List[String] = Nil, cancelled: Boolean = false) {
   def addItem(item: String): Order = ...
   def removeItem(item: String): Order = ...
   def cancel: Order = ...
 }
```

Order creation and updates are tracked as events in the replicated event log and replicated order state is maintained by `OrderManager` actors on all sites.

### Replication

As already mentioned, order events are replicated across sites A - F (all [running](#running) on `localhost`).

    A        E
     \      /    
      C -- D
     /      \
    B        F

Every site can create and update update orders, even under presence of network partitions. In the example application, network partitions can be created by shutting down sites. For example, when shutting down site `C`, partitions `A`, `B` and `E-D-F` are created. 

Concurrent updates to orders with different `id`s do not conflict, concurrent updates to the same order are considered as conflict and must be resolved by the user before further updates to that order are possible.    

### Interface

The example application has a simple command-line interface to create and update orders:

- `create <order-id>` creates an order with specified `order-id`.
- `add <order-id> <item>` adds an item to an order's `items` list.
- `remove <order-id> <item>` removes an item from an order's `items` list.
- `cancel <order-id>` cancels an order.

If a conflict from a concurrent update occurred, the conflict must be resolved by selecting one of the conflicting versions:

- `resolve <order-id> <index>` resolves a conflict by selecting a version `index`. Only the site that initially created the order object can resolve the conflict (static rule for distributed agreement).  

Other commands are:

- `count <order-id>` prints the number of updates to an order (incl. creation). Update counts are maintained by a separate view that subscribes to order events. 
- `state` prints the current state (= all orders) on a site (which may differ across sites during network partitions).
- `exit` stops a site and the replication links to and from that site.

Update results from user commands and replicated events are written to `stdout`. Update results from replayed events are not shown.

### Running

Before you continue, install [sbt](http://www.scala-sbt.org/) and run `sbt test` from the project's root (needs to be done only once). Then, open 6 terminal windows (representing sites A - F) and run

- `example-site A` for starting site A
- `example-site B` for starting site B
- `example-site C` for starting site C
- `example-site D` for starting site D
- `example-site E` for starting site E
- `example-site F` for starting site F

Alternatively, run `example` which opens these terminal windows automatically (works only on Mac OS X at the moment). For running the Java version of the example application, run `example-site` or `example` with `java` as additional argument. 

Create and update some orders and see how changes are replicated across sites. To make concurrent updates to an order, for example, enter `exit` on site `C`, and add different items to that order on sites `A` and `F`. When starting site `C` again, both updates propagate to all other sites which are then displayed as conflict. Resolve the conflict with the `resolve` command.  
