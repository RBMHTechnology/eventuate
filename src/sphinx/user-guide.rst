.. |br| raw:: html

   <br />

.. _user-guide:

----------
User guide
----------

Welcome to the `Eventuate`_ user guide.

.. _Eventuate: http://rbmhtechnology.github.io/eventuate/

What You Will Learn
-------------------
This user guide presents simple examples that demonstrate how to:

- Implement an event-sourced actor.
- Replicate actor state with event sourcing.
- Detect concurrent updates to replicated state.
- Track conflicts from concurrent updates.
- Resolve conflicts automatically and interactively.
- Make concurrent updates conflict-free with operation-based CRDTs.
- Implement an event-sourced view over many event-sourced actors.
- Achieve causal read consistency across event-sourced actors and views.
- Implement event-driven communication between event-sourced actors.

You can find further details in the :ref:`reference` documentation.

Prerequisites
-------------
Before working through this user guide you should:

* Be familiar with the Eventuate :ref:`overview` and :ref:`architecture`.
* Have `Java 8 <http://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html>`_ installed.
* Have SBT installed (`Mac <http://www.scala-sbt.org/0.13/docs/Installing-sbt-on-Mac.html>`_,
  `Windows <http://www.scala-sbt.org/0.13/docs/Installing-sbt-on-Windows.html>`_,
  `Linux <http://www.scala-sbt.org/0.13/docs/Installing-sbt-on-Linux.html>`_).
* Understand how the Akka ActorSystem and Actors work.
  The `Akka documentation <http://akka.io/docs/>`_ is excellent.
* Understand how `Akka Persistence <http://doc.akka.io/docs/akka/current/scala/persistence.html>`_ works,
  because Eventuate is a functional superset of Akka Persistence.
* To work through the Java examples, you should be familiar with `programming Java 8 <https://docs.oracle.com/javase/tutorial/>`_.
* To work through the Scala examples, you should be familiar with programming Scala.
  The ultimate reference for Scala is `Programming in Scala <https://www.artima.com/shop/programming_in_scala>`_;
  this book takes most people weeks or months of dedicated reading to complete, and years to assimilate.
  A faster and better way to learn is via `ScalaCourses.com <https://www.GetScala.com>`_.
  Their Introduction to Scala course starts you off with Scala, SBT and various editors,
  then moves on to object-oriented programming and introduces functional programming.
  The Intermediate Scala course teaches the more advanced Scala necessary and enough Akka so you can work through and
  understand the material presented in this user guide.

.. _guide-event-sourced-actors:

About the Code Examples
-----------------------
Code examples in this document are provided for Scala and Java, in the SBT project provided in the ``examples/user-guide``
directory of the `Eventuate GitHub project <https://github.com/RBMHTechnology/eventuate/tree/master/examples/user-guide>`_.

The `Scaladoc`_ is currently the API reference for both Scala and Java.
No separate Javadoc exists.
Java programmers may find the Scaladoc somewhat confusing because it requires rudimentary knowledge of Scala in order to interpret it.
Studying the provided working Java programs should help.

The Scala code examples use Scala 2.12, because the Spark adapter is not used.
Most of Eventuate is compatible with Scala 2.12, with the exception of the
`Spark Adapter <http://rbmhtechnology.github.io/eventuate/adapters/spark.html>`_.
Spark does not yet support Scala 2.12, and Scala 2.12 support will be slow in coming to Spark.

The Java code examples use `Javaslang`_ and Java 8.
Javaslang core is a functional library for Java 8+.
It helps to reduce the amount of code and to increase the robustness of the code itself.

The database used in the code examples is `LevelDB <https://github.com/google/leveldb>`_, an open source project by Google, written in C++.
LevelDB is a fast key-value storage the provides ordered mapping from ``Array[Byte]`` keys to ``Array[Byte]`` values.
The Eventuate project uses a `JNI wrapper for LevelDB <https://github.com/fusesource/leveldbjni>`_ to implement a
`Scala wrapper <http://rbmhtechnology.github.io/eventuate/latest/api/index.html#com.rbmhtechnology.eventuate.log.leveldb.package>`_.

Akka configuration values are provided in ``src/main/resources/application.conf``, as per the
`Akka configuration documentation <http://doc.akka.io/docs/akka/current/general/configuration.html>`_:

.. literalinclude:: ../../examples/user-guide/src/main/resources/application.conf
   :language: none

.. _Javaslang: http://www.javaslang.io/
.. _Scaladoc: http://rbmhtechnology.github.io/eventuate/latest/api/index.html

Event-Sourced Actors
--------------------
An event-sourced actor is an actor that captures changes to its internal state as a sequence of events.
It *persists* these events to an event log and *replays* them to recover internal state after a crash or an orderly restart.
This is the basic idea behind `event sourcing`_: instead of storing current application state,
the full history of changes is stored as *immutable facts* and current state is derived from these facts.

Event-sourced actors respond to *commands* and *events*.
One or more events are created when an event-sourced actor processes a valid command.
Many organization adopt the conventions that commands are named as imperatives (e.g. ``CreateProduct``), in contrast to events,
which are named as verbs in the past tense (e.g. ``ProductCreated``).

* **During command processing**, event-sourced actors validate external commands against internal state and, if validation succeeds,
  they persist (write) one or more events to their named event log.
* **During event processing**, event-sourced actors replay (read) previously-written events from their named log,
  then they update their internal state. Event-sourced actors can also create and write additional events during event processing.
  This topic is discussed in greater detail in the :ref:`guide-event-driven-communication` section of the Eventuate :ref:`architecture` document.

*When writing in Scala:* concrete event-sourced actors mix in the `EventsourcedActor`_ trait and define two methods:
the `onCommand`_ method, which persists valid commands as events, and the `onEvent`_ method, which reads events,
takes any necessary action and modifies internal state.
In the following code, ``ExampleActor`` encapsulates state ``currentState`` of type ``Vector[String]``, to which entries can be appended.

*When writing in Java:* concrete event-sourced actors extend the `AbstractEventsourcedActor`_ abstract class and define the ``onCommand`` and ``onEvent`` methods,
which have the same responsibilities as the Scala version of the same methods.
In the following code, ``ExampleActor`` encapsulates state ``currentState`` of type ``Collection<String>``, to which entries can be appended.
Note that the Java code is a lot longer than the Scala version.
This is one of the differences between the two languages; not only Scala is much more succinct, it is also more expressive and flexible.

ActorExample
^^^^^^^^^^^^
This code example for this section and the next is provided in the accompanying source code for this User Guide,
in the ``ActorExample.scala`` and ``ActorExample.java`` programs.

First we'll define the messages:

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/Messages.scala
      :snippet: event-sourced-actor
   .. includecode:: ../../examples/user-guide/src/main/java/japi/Messages.java
      :snippet: event-sourced-actor

Now the code:

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/ActorExample.scala
      :snippet: event-sourced-actor
   .. includecode:: ../../examples/user-guide/src/main/java/japi/ActorExample.java
      :snippet: event-sourced-actor

As shown above, ``EventsourcedActor`` implementations must define a global unique ``id`` and an ``eventLog`` actor reference for writing and replaying events.
An event-sourced actor may also define an optional ``aggregateId``, which affects how events are routed between event-sourced actors.
Section :ref:`event-log` explains how to create ``eventLog`` actor references.

As already mentioned, When an event-sourced actor receives a command, first the actor persists the command as an event, then it modifies its own internal state.
Referring to the above code, here is an example sequence:

1. An ``Append`` command is received by ``ExampleActor``'s ``onCommand`` command handler.
2. The ``onCommand`` command handler derives an ``Appended`` event and ``persist``\ s it to the ``eventLog`` pointed to
   by the ``ActorRef`` that it was passed when it was created.
3. If persistence succeeds, the sender of the command is informed about successful processing.
   If persistence fails, the command sender is informed about the failure so it can retry, if appropriate.
4. The ``ExampleActor``'s ``onEvent`` handler is automatically called after a successful ``persist``.
5. The ``onEvent`` handler updates ``currentState``.
6. `EventsourcedActor`_ subclasses that need to persist new events within the `onEvent`_ handler should mix in the
   `PersistOnEvent`_ trait and invoke the `persistOn`_ method.

During normal application startup, or if the actor is restarted, persisted events are replayed via the `onEvent`_ handler,
which recovers internal state. Only then may new commands be processed.

You can run the above Scala example from IntelliJ IDEA by launching ``sapi.ActorExample``.
Output is:

.. code-block:: none

    Connected to the target VM, address: '127.0.0.1:55525', transport: 'socket'
    [id = 5, aggregate id = d] a,b,a,b
    [WARN] [SECURITY][03/19/2017 14:36:21.262] [location-eventuate.log.dispatchers.write-dispatcher-6]
    [akka.serialization.Serialization(akka://location)] Using the default Java serializer for class [sapi.ActorExample$Appended]
    which is not recommended because of performance implications. Use another serializer or disable this warning using the
    setting 'akka.actor.warn-about-java-serializer-usage'
    [id = 4, aggregate id = d] a,b,a,b,a
    [id = 2, aggregate id = b] a,b,a,b,a,b
    [id = 1, aggregate id = a] a,b,a,b,a,b
    [id = 3, aggregate id = c] x,y,x,y,x,y
    [id = 5, aggregate id = d] a,b,a,b,a,b
    [id = 4, aggregate id = d] a,b,a,b,a,b
    Disconnected from the target VM, address: '127.0.0.1:55525', transport: 'socket'

    Process finished with exit code 0

The above serialization warning is addressed in the `Custom Serialization`_ document.

To run the Scala version from the command line, move to the ``eventuate-user-guide`` directory and type:

.. code-block:: none

    sbt "runMain sapi.ActorExample"

FIXME: Uncaught error from thread [location-eventuate.log.dispatchers.write-dispatcher-7] shutting down JVM since
'akka.jvm-exit-on-fatal-error' is enabled for ActorSystem[location]
java.lang.UnsatisfiedLinkError: org.fusesource.leveldbjni.internal.NativeOptions.init()V |br|
|br|
See http://stackoverflow.com/questions/19425613/unsatisfiedlinkerror-with-native-library-under-sbt |br|
|br|
I tried setting ``fork in Runtime := true`` but no joy

You can run the above Java example from IntelliJ IDEA by launching ``japi.ActorExample``.

To run the Java version from the command line, move to the ``eventuate-user-guide`` directory and type:

.. code-block:: none

    sbt "runMain japi.ActorExample"

FIXME: It is clear that none of the Java examples have ever worked.
I addressed the problems that I understood, but more problems remain.

.. _AbstractEventsourcedActor: http://rbmhtechnology.github.io/eventuate/latest/api/index.html#com.rbmhtechnology.eventuate.AbstractEventsourcedActor
.. _Custom Serialization: http://rbmhtechnology.github.io/eventuate/reference/event-sourcing.html#custom-serialization
.. _EventsourcedActor: http://rbmhtechnology.github.io/eventuate/latest/api/index.html#com.rbmhtechnology.eventuate.EventsourcedActor
.. _onCommand: http://rbmhtechnology.github.io/eventuate/latest/api/index.html#com.rbmhtechnology.eventuate.EventsourcedActor@onCommand:EventsourcedView.this.Receive
.. _onEvent: http://rbmhtechnology.github.io/eventuate/latest/api/index.html#com.rbmhtechnology.eventuate.EventsourcedActor@onEvent:EventsourcedView.this.Receive
.. _persistOn: http://rbmhtechnology.github.io/eventuate/latest/api/index.html#com.rbmhtechnology.eventuate.PersistOnEvent@persistOnEvent[A](event:A,customDestinationAggregateIds:Set[String]):Unit
.. _persistOnEvent: http://rbmhtechnology.github.io/eventuate/latest/api/com/rbmhtechnology/eventuate/PersistOnEvent.html

Working With a Single Instance of an EventsourcedActor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
In the following, a single instance of ``ExampleActor`` is created and two ``Append`` commands are sent to it:

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/ActorExample.scala
      :snippet: create-one-instance
   .. includecode:: ../../examples/user-guide/src/main/java/japi/ActorExample.java
      :snippet: create-one-instance

Send a ``Print`` command to the ``ExampleActor`` instance like this:

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/ActorExample.scala
      :snippet: print-one-instance
   .. includecode:: ../../examples/user-guide/src/main/java/japi/ActorExample.java
      :snippet: print-one-instance

The output should be:

.. code-block:: none

    [id = 1, aggregate id = a] a,b

When the application is restarted, persisted events are replayed via the ``onEvent`` handler, which recovers ``currentState``.
Sending another ``Print`` command should again print:

.. code-block:: none

    [id = 1, aggregate id = a] a,b

Shared Event Logs
^^^^^^^^^^^^^^^^^
In the following sections, several instances of ``ExampleActor`` are created.
They share a :ref:`replicated-event-log`, event though they are running at different *locations*.

A shared event log is a prerequisite for event-sourced actors to be able to read each other’s events.
However, sharing an event log does not necessarily mean all events are consumed by every actor that accesses the same log.
The ``aggreagteId`` determines which events actors consume from other actors;
``aggreagteId`` acts as a filtering mechanism, because actors that define a ``aggreagteId`` property only receive events
from other actors with the same ``aggreagteId`` value.

Isolated EventsourcedActor Instances
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
``EventsourcedActor`` instances with different ``aggregateId`` values are isolated from each other,
which means they do not consume each other’s events:

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/ActorExample.scala
      :snippet: create-two-instances
   .. includecode:: ../../examples/user-guide/src/main/java/japi/ActorExample.java
      :snippet: create-two-instances

Here is an example of sending ``Print`` commands to isolated actors:

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/ActorExample.scala
      :snippet: print-two-instances
   .. includecode:: ../../examples/user-guide/src/main/java/japi/ActorExample.java
      :snippet: print-two-instances

should display:

.. code-block:: none

    [id = 2, aggregate id = b] a,b
    [id = 3, aggregate id = c] x,y

Replicated EventsourcedActor Instances
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
``EventsourcedActor`` instances with the same ``aggregateId`` are replicants, which means they consume each other’s events [#]_.

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/ActorExample.scala
      :snippet: create-replica-instances
   .. includecode:: ../../examples/user-guide/src/main/java/japi/ActorExample.java
      :snippet: create-replica-instances

In the code above, there are two ``EventsourcedActor`` subclass instances: ``d4`` and ``d5``.
``d4`` processes an ``Append`` command and persists an ``Appended`` event.
Next, both ``d4`` and ``d5`` consume that event and update their internal state.
After a short time, the values in the actors distributed throughout the network converge to a single value.
Once this has happened, sending a ``Print`` command to both actors should display the same value:

.. code-block:: none

    [id = 4, aggregate id = d] a
    [id = 5, aggregate id = d] a

FIXME The ActorExample.scala code does not match the above description. See "fixme why is this not referenced in user-guide.rst?"

After both replicas have converged, another ``Append`` is sent to ``d5``:

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/ActorExample.scala
      :snippet: send-another-append
   .. includecode:: ../../examples/user-guide/src/main/java/japi/ActorExample.java
      :snippet: send-another-append

Again both actors consume the event and after a waiting a short time for the values to converge,
sending another ``Print`` command should display:

.. code-block:: none

    [id = 4, aggregate id = d] a,b
    [id = 5, aggregate id = d] a,b

.. warning::
   As you have probably recognized, replica convergence in this example can only be achieved if the second ``Append``
   command is sent after both actors have processed the ``Appended`` event from the first ``Append`` command.

   In other words, the first ``Appended`` event must *happen before* the second one.
   Only in this case can these two events can have a causal relationship.
   Since the network required for Eventuate guarantees that events will be delivered in potential causal order,
   the ``EventsourcedActor`` replicas are able to converge to the same state.

   When concurrent updates are made to both replicas, the corresponding ``Appended`` events are not causally related and
   may be delivered in any order to both replicas.
   This may cause replicas to diverge because *append* operations do not commute.

   The following sections give examples how to detect and handle concurrent updates.

Detecting Concurrent Updates
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Eventuate tracks *happened-before* relationships of events (events that are potentially causally related) with :ref:`vector-clocks`.
Lets consider for a moment why this is important. Assume that an event-sourced actor emits an event ``e1`` for changing internal state
and later receives an event ``e2`` from a replica instance. If the replica instance emits ``e2`` after having processed ``e1``,
the actor can apply ``e2`` as regular update. However, if the replica instance emits ``e2`` before having received ``e1``,
the actor has received a concurrent, potentially conflicting event.

The actor determines if ``e2`` is a regular (causally related) or a concurrent update by comparing the vector timestamps.
Let ``t1`` be the vector timestamp of ``e1`` and ``t2`` be the vector timestamp of ``e2``.
If events ``e1`` and ``e2`` are concurrent then ``t1 conc t2`` evaluates ``true``.
Otherwise, they are causally related and ``t1 < t2`` evaluates ``true`` because ``e1`` *happened-before* ``e2``.

The vector timestamp of an event can be obtained from the ``lastVectorTimestamp`` property during event processing.
Vector timestamps can be attached as *update timestamp* to current state and compared with the vector timestamp of a
new event in order to determine whether the new event is causally related to the previous state update or not\ [#]_:

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/ConcurrentExample.scala
      :snippet: detecting-concurrent-update
   .. includecode:: ../../examples/user-guide/src/main/java/japi/ConcurrentExample.java
      :snippet: detecting-concurrent-update

The Java code
(`japi.ConcurrentExample <https://github.com/RBMHTechnology/eventuate/tree/master/examples/user-guide/src/main/java/japi/ConcurrentExample>`_)
is not executable at this time; hopefully we will address that soon.
You can run the Scala code from the command line by typing:

.. code-block:: none

    sbt "runMain sapi.ConcurrentExample"

.. _tracking-conflicting-versions:

Identifying Conflicting Versions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
This section demonstrates that update timestamps can be stored in an actor's current state and compared with
the vector timestamps of new events.
We will also see that this process can be abstracted, so that applications do not need to deal with these low level details.

Since state update operations from concurrent events do not commute, conflicting versions of actor state must be identified and resolved.
This can be done with Eventuate’s
`ConcurrentVersions <http://rbmhtechnology.github.io/eventuate/latest/api/index.html#com.rbmhtechnology.eventuate.ConcurrentVersions>`_
trait and an application-defined *update function* of type ``(S, A) => S``, where ``S`` is the type of actor state and ``A`` is the update type.
In the following example, the ``ConcurrentVersions`` type is ``ConcurrentVersions[Vector[String], String]`` and the update function is ``(s, a) => s :+ a``:

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/TrackingExample.scala
      :snippet: tracking-conflicting-versions
   .. includecode:: ../../examples/user-guide/src/main/java/japi/TrackingExample.java
      :snippet: tracking-conflicting-versions

Internally, ``ConcurrentVersions`` maintains versions of actor state in a tree structure where each concurrent ``versionedState.update`` creates a new branch.
The shape of the tree is determined solely by the vector timestamps of the corresponding ``versionedState.update`` events.

An event’s vector timestamp is passed as ``lastVectorTimestamp`` argument to ``update``.
The ``update`` method internally creates a new version by applying the update function ``(s, a) => s :+ a`` to the
closest predecessor version and the actual update value (``entry``).
The ``lastVectorTimestamp`` is attached as update timestamp to the newly created version.

In this example, concurrent versions of actor state and their update timestamp can be obtained with ``versionedState.all`` which is a sequence of type
``Seq[Versioned[Vector[String]]]``.
The Versioned_ data type represents a particular version of actor state and its update timestamp (= ``vectorTimestamp`` field).

If ``all`` contains only a single element, there is no conflict and the element represents the current, conflict-free actor state.
If the sequence contains two or more elements, there is a conflict where the elements represent conflicting versions of actor states.
They can be resolved either automatically or interactively.

The Java code
(`japi.TrackingExample <https://github.com/RBMHTechnology/eventuate/tree/master/examples/user-guide/src/main/java/japi/TrackingExample>`_)
is not executable at this time; hopefully we will address that soon.
You can run the Scala code from the command line by typing:

.. code-block:: none

    sbt "runMain sapi.TrackingExample"

Resolving Conflicting Versions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Only concurrent updates to replicas with the same ``aggregateId`` may conflict.
Concurrent updates to actors with different ``aggregateId`` do not conflict, unless an application performs custom :ref:`event-routing`.

Also, if the data type of actor state is designed in a way that update operations commute, concurrent updates can be made conflict-free.
This is discussed in the :ref:`commutative-replicated-data-types` section of this User Guide.

.. _automated-conflict-resolution:

Automated Conflict Resolution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The following is a simple example of automated conflict resolution:
if a conflict has been detected, the version with the higher wall clock timestamp is selected to be the winner.
In case of equal wall clock timestamps, the version with the lower emitter id is selected.
The emitter id is the ``id`` of the ``EventsourcedActor`` that emitted the event.
The wall clock timestamp can be obtained from the actor's ``lastSystemTimestamp`` property,
and the emitter id can be obtained from the actor's ``lastEmitterId`` property.

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/ResolveExample.scala
      :snippet: automated-conflict-resolution
   .. includecode:: ../../examples/user-guide/src/main/java/japi/ResolveExample.java
      :snippet: automated-conflict-resolution

Here, conflicting versions are sorted by descending wall clock timestamp and ascending emitter id where the latter is tracked as ``creator`` of the version.
The first version is selected to be the winner.
Its vector timestamp is passed as argument to ``resolve``, which selects this version and discards all other versions.

More advanced conflict resolution could select a winner depending on the actual value of concurrent versions.
After selection, an application could even update the winner with the *merged* value of all conflicting versions\ [#]_.

The Java code
(`japi.ResolveExample <https://github.com/RBMHTechnology/eventuate/tree/master/examples/user-guide/src/main/java/japi/japi/ResolveExample>`_)
is not executable at this time; hopefully we will address that soon.
You can run the Scala code the command line by typing:

.. code-block:: none

    sbt "runMain sapi.ResolveExample"

.. note::
   For replicas to converge, it is important that winner selection does not depend on the order of conflicting events.
   In our example, this is the case because wall clock timestamp and emitter id comparison is transitive.

Interactive conflict resolution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Interactive conflict resolution does not resolve conflicts immediately but requests the user to inspect and resolve a conflict.
The following is a very simple example of interactive conflict resolution: a user selects a winner version if conflicting versions of application state exist.
First we need an additional ``Event`` definition:

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/Messages.scala
      :snippet: conditional-requests
   .. includecode:: ../../examples/user-guide/src/main/java/japi/Messages.java
      :snippet: conditional-requests

Now the code:

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/InteractiveResolveExample.scala
      :snippet: interactive-conflict-resolution
   .. includecode:: ../../examples/user-guide/src/main/java/japi/ResolveExample.java
      :snippet: interactive-conflict-resolution

When a user tries to ``Append`` in presence of a conflict, the ``ExampleActor`` rejects the update and requests the user
to select a winner version from a sequence of conflicting versions.
The user then sends the update timestamp of the winner version as ``selectedTimestamp`` with a ``Resolve`` command from
which a ``Resolved`` event is derived and persisted. Handling of ``Resolved`` at all replicas finally resolves the conflict.

In addition to just selecting a winner, an application could also update the winner version in a second step, for example,
with a value derived from the merge result of conflicting versions.
Support for *atomic*, interactive conflict resolution with an application-defined merge function is planned for later Eventuate releases.

The Java code
(`japi.InteractiveResolveExample <https://github.com/RBMHTechnology/eventuate/tree/master/examples/user-guide/src/main/java/japi/japi/InteractiveResolveExample>`_)
is not executable at this time; hopefully we will address that soon.
You can run the Scala code the command line by typing:

.. code-block:: none

    sbt "runMain sapi.InteractiveResolveExample"

.. note::
   Interactive conflict resolution requires agreement among replicas that are affected by a given conflict: only one of
   them may emit the ``Resolved`` event. This does not necessarily mean distributed lock acquisition or leader (= resolver)
   election but can also rely on static rules such as *only the initial creator location of an aggregate is allowed to
   resolve the conflict*\ [#]_. This rule is implemented in the :ref:`example-application`.

.. _commutative-replicated-data-types:

Operation-Based CRDTs
---------------------
If state update operations commute there is no need to use Eventuate’s ``ConcurrentVersions`` utility.
To illustrate this, let's examine a simple example: a replicated counter,
which converges because its increment and decrement operations commute.

A formal approach to commutative replicated data types
(also known as CmRDTs, or `operation-based CRDTs <https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type#Operation-based_CRDTs>`_)
is given in the paper
`A comprehensive study of Convergent and Commutative Replicated Data Types`_ by Marc Shapiro et al.
Eventuate provides a strong foundation for implementing operation-based CRDTs because:

- Update operations can be modeled as events and reliably broadcasted to all replicas by a :ref:`replicated-event-log`.
- The command and event handler of an event-sourced actor can be used to implement the two update phases mentioned in
  the aforementioned paper: *atSource* (first mentioned on page 7) and *downstream* (first mentioned on page 5).
- All *downstream* preconditions mentioned in the paper are satisfied in case of causal delivery of update operations
  which is guaranteed for actors consuming from a replicated event log.

Eventuate currently implements 5 out of 12 operation-based CRDTs specified in the cited paper.
These are *Counter*, *MV-Register*, *LWW-Register*, *OR-Set* and *OR-Cart* (a shopping cart CRDT).
They can be instantiated and used via their corresponding *CRDT services*.
CRDT operations are asynchronous methods on the service interfaces.
CRDT services free applications from dealing with low-level details like event-sourced actors or command messages directly.
This is the definition of ORSetService_:

.. tabbed-code::
    .. includecode:: ../../eventuate-crdt/src/main/scala/com/rbmhtechnology/eventuate/crdt/ORSet.scala
       :snippet: or-set-service
    .. includecode:: ../../examples/user-guide/src/main/java/japi/CrdtExample.java
       :snippet: or-set-service

The ORSetService_ is a CRDT service that manages ORSet_ instances.
It implements the asynchronous ``add`` and ``remove`` methods and inherits the ``value(id: String): Future[Set[A]]``
method from ``CRDTService[ORSet[A], Set[A]]`` for reading the current value.
Their ``id`` parameter identifies an ``ORSet`` instance.
Instances are automatically created by the service on demand.
A usage example is the ReplicatedOrSetSpec_ that is based on Akka’s `multi node testkit`_.

A CRDT service also implements a ``save(id: String): Future[SnapshotMetadata]`` method for saving CRDT snapshots.
:ref:`snapshots` may reduce recovery times of CRDTs with a long update history but are not required for CRDT persistence.

New operation-based CRDTs and their corresponding services can be developed with the CRDT development framework,
by defining an instance of the CRDTServiceOps_ type class and implementing the CRDTService_ trait.
Take a look at the `CRDT sources`_ for examples.

Eventuate’s CRDT approach is described in `A service framework for operation-based CRDTs`_, by Martin Krasser (2016).

.. _A service framework for operation-based CRDTs: http://krasserm.github.io/2016/10/19/operation-based-crdt-framework/

.. _guide-event-sourced-views:

Event-Sourced Views
-------------------
Event-sourced views are a functional subset of event-sourced actors.
They can only consume events from an event log but cannot produce new events.
Concrete event-sourced views must implement the ``EventsourcedView`` trait.
In the following example, the view counts all ``Appended`` and ``Resolved`` events emitted by all event-sourced actors
to the same ``eventLog``:

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/EventsourcedViews.scala
      :snippet: event-sourced-view
   .. includecode:: ../../examples/user-guide/src/main/java/japi/ViewExample.java
      :snippet: event-sourced-view

The Java code
(`japi.ViewExample <https://github.com/RBMHTechnology/eventuate/tree/master/examples/user-guide/src/main/java/japi/ViewExample>`_)
is not executable at this time; hopefully we will address that soon.
You can run the Scala code the command line by typing:

.. code-block:: none

    sbt "runMain sapi.ViewExample"

Event-sourced views handle events in the same way as event-sourced actors by implementing an ``onEvent`` handler.
The ``onCommand`` handler in the example processes the queries ``GetAppendCount`` and ``GetResolveCount``.

``ExampleView`` implements the mandatory global unique ``id`` but does not define an ``aggregateId``.
A view that does not define an ``aggregateId`` can consume events from all event-sourced actors on the same event log.
If instead it does define an ``aggregateId``, it can only consume events from event-sourced actors with the same ``aggregateId``,
providing that it follows the default :ref:`event-routing` rules.

.. hint::
   While event-sourced views maintain view state in-memory, :ref:`ref-event-sourced-writers` can be used to persist
   view state to external databases.
   A specialization of event-sourced writers are :ref:`ref-event-sourced-processors` whose external database is an event log.
   TODO say why this is significant.

.. _conditional-requests:

Conditional Requests
--------------------
Causal read consistency is the default when reading state from a single event-sourced actor or view.
The event stream received by that actor is always causally ordered, hence, it will never see an *effect* before having seen its *cause*.

The situation is different when a client reads from multiple actors.
Imagine two event-sourced actor replicas where a client updates one replica and observes the updated state with the reply.
A subsequent (TODO what?) from the other replica, made by the same client, may return the old state which violates causal consistency.

Similar considerations can be made for reading from an event-sourced view after having made an update to an event-sourced actor.
For example, an application that successfully appended an entry to ``ExampleActor`` may not immediately see that update in
the ``appendCount`` of ``ExampleView``.
To achieve causal read consistency, the view should delay command processing until the emitted event has been consumed by the view.
This can be achieved with a ``ConditionalRequest``.
First we'll define a new message:

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/Messages.scala
      :snippet: conditional-requests
   .. includecode:: ../../examples/user-guide/src/main/java/japi/Messages.java
      :snippet: conditional-requests

Now the code:

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/ConditionalExample.scala
      :snippet: conditional-requests
   .. includecode:: ../../examples/user-guide/src/main/java/japi/ConditionalExample.java
      :snippet: conditional-requests

.. raw:: html

    <p style='text-align: left'>
    Here, the <code>ExampleActor</code> includes the event’s vector timestamp in its <code>AppendSuccess(entry, lastVectorTimestamp)</code> reply.
    </p>

Together with the actual ``GetAppendCount`` command, the timestamp is included as condition in a ``ConditionalRequest``
and sent to the view.
For ``ConditionalRequest`` processing, an event-sourced view must extend the ``ConditionalRequests`` trait.
``ConditionalRequests`` internally delays the command, if needed, and only dispatches ``GetAppendCount`` to the
view’s ``onCommand`` handler if the condition timestamp is in the *causal past* of the view (which is earliest the case
when the view consumed the update event).

The Java code
(`japi.ConditionalExample <https://github.com/RBMHTechnology/eventuate/tree/master/examples/user-guide/src/main/java/japi/ConditionalExample>`_)
is not executable at this time; hopefully we will address that soon.
You can run the Scala code the command line by typing:

.. code-block:: none

    sbt "runMain sapi.ConditionalExample"

When running the example with an empty event log, it should display:

.. code-block:: none

    append count = 1

.. note::
   Event-sourced actors, stateful
   `event-sourced writers <http://rbmhtechnology.github.io/eventuate/latest/api/index.html#com.rbmhtechnology.eventuate.EventsourcedWriter>`_
   and `processors <http://rbmhtechnology.github.io/eventuate/latest/api/index.html#com.rbmhtechnology.eventuate.EventsourcedProcessor>`_
   can also extend ``ConditionalRequests``, just as event-sourced views can.

.. note::
   Delaying conditional requests may cause them to be re-ordered relative to other conditional and non-conditional requests.
   TODO Describe what this looks like

.. _guide-event-driven-communication:

Event-Driven Communication
--------------------------
Earlier sections have already shown one form of event collaboration: *state replication*.
For that purpose, event-sourced actors of the same type exchange their events to re-construct actor state at different locations.

In more general cases, event-sourced actors of different type exchange events to achieve a common goal.
They react on received events by updating internal state and producing new events.
This form of event collaboration is called *event-driven communication*.

In the following example, two event-sourced actors collaborate in a ping-pong game.

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/CommunicationExample.scala
      :snippet: event-driven-communication1
   .. includecode:: ../../examples/user-guide/src/main/java/japi/CommunicationExample.java
      :snippet: event-driven-communication1


A ``PingActor`` emits a ``PingEvent`` on receiving a ``PongEvent``

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/CommunicationExample.scala
      :snippet: ping-actor
   .. includecode:: ../../examples/user-guide/src/main/java/japi/CommunicationExample.java
      :snippet: ping-actor


A ``PongActor`` emits a ``PongEvent`` on receiving a ``PingEvent``.

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/CommunicationExample.scala
      :snippet: pong-actor
   .. includecode:: ../../examples/user-guide/src/main/java/japi/CommunicationExample.java
      :snippet: pong-actor

Now we can run the Ping-Pong game:

.. tabbed-code::
   .. includecode:: ../../examples/user-guide/src/main/scala/sapi/CommunicationExample.scala
      :snippet: event-driven-communication2
   .. includecode:: ../../examples/user-guide/src/main/java/japi/CommunicationExample.java
      :snippet: event-driven-communication2

The ping-pong game is started by sending the ``PingActor`` a ``”serve”`` command which ``persist``\ s the first ``PingEvent``.
This event however is not consumed by the emitter but rather by the ``PongActor``.
The ``PongActor`` reacts on the ``PingEvent`` by emitting a ``PongEvent``. Other than in previous examples,
the event is not emitted in the actor’s ``onCommand`` handler but in the ``onEvent`` handler.
For that purpose, the actor has to mixin the ``PersistOnEvent`` trait and use the ``persistOnEventMethod`` method.
The emitted ``PongEvent`` too isn’t consumed by its emitter but rather by the ``PingActor``, emitting another ``PingEvent``, and so on.
The game ends when the ``PingActor`` received the 10th ``PongEvent``.

The Java code
(`japi.CommunicationExample <https://github.com/RBMHTechnology/eventuate/tree/master/examples/user-guide/src/main/java/japi/CommunicationExample>`_)
is not executable at this time; hopefully we will address that soon.
You can run the Scala code the command line by typing:

.. code-block:: none

    sbt "runMain sapi.CommunicationExample"

.. note::
   The ping-pong game is **reliable**.
   When an actor crashes and is restarted, the game is reliably resumed from where it was interrupted.
   The ``persistOnEvent`` method is idempotent i.e. no duplicates are written under failure conditions and later event replay.
   When deployed at different location, the ping-pong actors are also **partition-tolerant**.
   When their game is interrupted by a network partition, it is automatically resumed when the partition heals.

   Furthermore, the actors don’t need to care about idempotency in their business logic
   i.e. they can assume to receive a **de-duplicated** and **causally-ordered** event stream in their ``onEvent`` handler.
   This is a significant advantage over at-least-once delivery based communication with ConfirmedDelivery_, for example,
   which can lead to duplicates and message re-ordering.

In a more real-world example, there would be several actors of different type collaborating to achieve a common goal,
for example, in a distributed business process.
These actors can be considered as event-driven and event-sourced *microservices*,
collaborating on a causally ordered event stream in a reliable and partition-tolerant way.
Furthermore, when partitioned, they remain available for local writes and automatically catch up with their collaborators when the partition heals.

Further ``persistOnEvent`` details are described in the PersistOnEvent_ API docs.

.. _ZooKeeper: http://zookeeper.apache.org/
.. _event sourcing: http://martinfowler.com/eaaDev/EventSourcing.html
.. _vector clock update rules: http://en.wikipedia.org/wiki/Vector_clock
.. _version vector update rules: http://en.wikipedia.org/wiki/Version_vector
.. _Lamport timestamps: http://en.wikipedia.org/wiki/Lamport_timestamps
.. _multi node testkit: http://doc.akka.io/docs/akka/2.4/dev/multi-node-testing.html
.. _ReplicatedOrSetSpec: https://github.com/RBMHTechnology/eventuate/blob/master/src/multi-jvm/scala/com/rbmhtechnology/eventuate/crdt/ReplicatedORSetSpec.scala
.. _CRDT sources: https://github.com/RBMHTechnology/eventuate/tree/master/eventuate-crdt/main/scala/com/rbmhtechnology/eventuate/crdt
.. _A comprehensive study of Convergent and Commutative Replicated Data Types: http://hal.upmc.fr/file/index/docid/555588/filename/techreport.pdf

.. _Versioned: latest/api/index.html#com.rbmhtechnology.eventuate.Versioned
.. _ORSet: latest/api/index.html#com.rbmhtechnology.eventuate.crdt.ORSet
.. _ORSetService: latest/api/index.html#com.rbmhtechnology.eventuate.crdt.ORSetService
.. _CRDTService: latest/api/index.html#com.rbmhtechnology.eventuate.crdt.CRDTService
.. _CRDTServiceOps: latest/api/index.html#com.rbmhtechnology.eventuate.crdt.CRDTServiceOps
.. _ConfirmedDelivery: latest/api/index.html#com.rbmhtechnology.eventuate.ConfirmedDelivery

.. [#] ``EventsourcedActor``\ s and ``EventsourcedView``\ s that have an undefined ``aggregateId`` can consume events from all other actors on the same event log.
.. [#] Attached update timestamps are not version vectors because Eventuate uses `vector clock update rules`_ instead of `version vector update rules`_.
   Consequently, update timestamp equivalence cannot be used as criterion for replica convergence.
.. [#] A formal approach to automatically *merge* concurrent versions of application state are convergent replicated data types (CvRDTs) or state-based CRDTs.
.. [#] Distributed lock acquisition or leader election require an external coordination service like ZooKeeper_, for example, whereas static rules do not.
