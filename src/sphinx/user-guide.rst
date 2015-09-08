.. _user-guide:

----------
User guide
----------

This is a brief user guide to Eventuate. It is recommended to read sections :ref:`introduction` and :ref:`architecture` first. Based on simple examples, you’ll see how to

- implement an event-sourced actor
- replicate actor state with event sourcing
- detect concurrent updates to replicated state
- track conflicts from concurrent updates
- resolve conflicts automatically and interactively
- make concurrent updates conflict-free with operation-based CRDTs
- implement an event-sourced view over many event-sourced actors and
- achieve read-your-write consistency across event-sourced actors and views.

The user guide only scratches the surface of Eventuate. You can find further details in the :ref:`reference`.

Event-sourced actors
--------------------

An event-sourced actor is an actor that captures changes to its internal state as a sequence of events. It *persists* these events to an event log and *replays* them to recover internal state after a crash or a planned re-start. This is the basic idea behind `event sourcing`_: instead of storing current application state, the full history of changes is stored as *immutable facts* and current state is derived from these facts.

Event-sourced actors distinguish between *commands* and *events*. During command processing they usually validate external commands against internal state and, if validation succeeds, write one or more events to their event log. During event processing they consume events they have written and update internal state by handling these events.

Concrete event-sourced actors must implement the ``EventsourcedActor`` trait. The following ``ExampleActor`` maintains state of type ``Vector[String]`` to which entries can be appended:

.. includecode:: code/UserGuideDoc.scala
   :snippet: event-sourced-actor

For modifying ``currentState``, applications send ``Append`` commands which are handled by the ``onCommand`` handler. From an ``Append`` command, the handler derives an ``Appended`` event and ``persist``\ s it to the given ``eventLog``. If persistence succeeds, the ``onEvent`` handler is called with the persisted event and the command sender is informed about successful processing. If persistence fails, the command sender is informed about the failure so it can retry, if needed. 

The ``onEvent`` handler updates ``currentState`` from successfully persisted events. If the actor is re-started, either after a crash or during normal application start, persisted events are replayed to ``onEvent`` which recovers internal state before new commands are processed.

``EventsourcedActor`` implementations must define a global unique ``id`` and require an ``eventLog`` actor reference for writing and replaying events. An event-sourced actor may also define an optional ``aggregateId`` which has an impact how events are routed between event-sourced actors.

.. hint::
   Section :ref:`event-log` explains how to create ``eventLog`` actor references. 

Creating a single instance
~~~~~~~~~~~~~~~~~~~~~~~~~~

In the following, a single instance of ``ExampleActor`` is created and two ``Append`` commands are sent to it: 

.. includecode:: code/UserGuideDoc.scala
   :snippet: create-one-instance

Sending a ``Print`` command 

.. includecode:: code/UserGuideDoc.scala
   :snippet: print-one-instance

should print::

    [id = 1, aggregate id = a] a,b

When the application is re-started, persisted events are replayed to ``onEvent`` which recovers ``currentState``. Sending another ``Print`` command should print again::

    [id = 1, aggregate id = a] a,b

.. note::
   In the following sections, several instances of ``ExampleActor`` are created. It is assumed that they share a :ref:`replicated-event-log` and are running at different *locations*. 

   A shared event log is a pre-requisite for event-sourced actors to consume each other’s events. However, sharing an event log doesn’t necessarily mean broadcast communication between all actors on the same log. It is the ``aggreagteId`` that determines which actors consume each other’s events.

Creating two isolated instances
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When creating two instances of ``ExampleActor`` with different ``aggregateId``\ s, they are isolated from each other, by default, and do not consume each other’s events:

.. includecode:: code/UserGuideDoc.scala
   :snippet: create-two-instances

Sending two ``Print`` commands

.. includecode:: code/UserGuideDoc.scala
   :snippet: print-two-instances

should print::

    [id = 2, aggregate id = b] a,b
    [id = 3, aggregate id = c] x,y

Creating two replica instances
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When creating two ``ExampleActor`` instances with the same ``aggregateId``, they consume each other’s events [#]_.

.. includecode:: code/UserGuideDoc.scala
   :snippet: create-replica-instances

Here, ``d4`` processes an ``Append`` command and persists an ``Appended`` event. Both, ``d4`` and ``d5``, consume that event and update their internal state. After waiting a bit for convergence, sending a ``Print`` command to both actors should print::

    [id = 4, aggregate id = d] a
    [id = 5, aggregate id = d] a

After both replicas have converged, another ``Append`` is sent to ``d5``. 

.. includecode:: code/UserGuideDoc.scala
   :snippet: send-another-append

Again both actors consume the event and sending another ``Print`` command should print::

    [id = 4, aggregate id = d] a,b
    [id = 5, aggregate id = d] a,b

.. warning::
   As you have probably recognized, replica convergence in this example can only be achieved if the second ``Append`` command is sent after both actors have processed the ``Appended`` event from the first ``Append`` command. 

   In other words, the first ``Appended`` event must *happen before* the second one. Only in this case, these two events can have a causal relationship. Since events are guaranteed to be delivered in potential causal order to all replicas, they can converge to the same state.

   When concurrent updates are made to both replicas, the corresponding ``Appended`` events are not causally related and can be delivered in any order to both replicas. This may cause replicas to diverge because *append* operations do not commute. The following sections give examples how to detect and handle concurrent updates.

Detecting concurrent updates
----------------------------

Eventuate tracks *happened-before* relationships (= potential causality) of events with :ref:`vector-clocks`. Why is that needed at all? Let’s assume that an event-sourced actor emits an event ``e1`` for changing internal state and later receives an event ``e2`` from a replica instance. If the replica instance emits ``e2`` after having processed ``e1``, the actor can apply ``e2`` as regular update. If the replica instance emits ``e2`` before having received ``e1``, the actor receives a concurrent, potentially conflicting event. 

How can the actor determine if ``e2`` is a regular i.e. causally related or concurrent update? It can do so by comparing the vector timestamps of ``e1`` and ``e2``, where ``t1`` is the vector timestamp of ``e1`` and ``t2`` the vector timestamp of ``e2``. If events ``e1`` and ``e2`` are concurrent then ``t1 conc t2`` evaluates to ``true``. Otherwise, they are causally related and ``t1 < t2`` evaluates to ``true`` (because ``e1`` *happened-before* ``e2``).

The vector timestamp of an event can be obtained with ``lastVectorTimestamp`` during event processing. Vector timestamps can be attached as *update timestamp* to current state and compared with the vector timestamp of a new event in order to determine whether the new event is causally related to the previous state update or not\ [#]_:

.. includecode:: code/UserGuideDoc.scala
   :snippet: detecting-concurrent-update

Attaching update timestamps to current state and comparing them with vector timestamps of new events can be easily abstracted over so that applications don’t have to deal with these low level details, as shown in the next section. 

.. _tracking-conflicting-versions:

Tracking conflicting versions
-----------------------------

If state update operations from concurrent events do not commute, conflicting versions of actor state arise that must be tracked and resolved. This can be done with Eventuate’s ``ConcurrentVersions[S, A]`` abstraction and an application-defined *update function* of type ``(S, A) => S`` where ``S`` is the type of actor state and ``A`` the update type. In our example, the ``ConcurrentVersions`` type is ``ConcurrentVersions[Vector[String], String]`` and the update function ``(s, a) => s :+ a``:

.. includecode:: code/UserGuideDoc.scala
   :snippet: tracking-conflicting-versions

Internally, ``ConcurrentVersions`` maintains versions of actor state in a tree structure where each concurrent ``update`` creates a new branch. The shape of the tree is determined solely by the vector timestamps of the corresponding update events. 

An event’s vector timestamp is passed as ``lastVectorTimestamp`` argument to ``update``. The ``update`` method internally creates a new version by applying the update function ``(s, a) => s :+ a`` to the closest predecessor version and the actual update value (``entry``). The ``lastVectorTimestamp`` is attached as update timestamp to the newly created version.

Concurrent versions of actor state and their update timestamp can be obtained with ``all`` which is a sequence of type ``Seq[Versioned[Vector[String]]]`` in our example. The Versioned_ data type represents a particular version of actor state and its update timestamp. 

If ``all`` contains only a single element, there is no conflict and the element represents the current, conflict-free actor state. If the sequence contains two or more elements, there is a conflict where the elements represent conflicting versions of actor states. They can be resolved either automatically or interactively.

.. note::
   Only concurrent updates to replicas with the same ``aggregateId`` may conflict. Concurrent updates to actors with different ``aggregateId`` do not conflict (unless an application does custom :ref:`event-routing`).

   Also, if the data type of actor state is designed in a way that update operations commute, concurrent updates can be made conflict-free. This is discussed in section :ref:`commutative-replicated-data-types`.

Resolving conflicting versions
------------------------------

.. _automated-conflict-resolution:

Automated conflict resolution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following is a very simple example of automated conflict resolution: if a conflict has been detected, the version with the lower emitter id is selected to be the winner. The emitter id of an event can be obtained with ``lastEmitterId`` during event handling. It is the ``id`` of the ``EventsourcedActor`` that emitted the event.

.. includecode:: code/UserGuideDoc.scala
   :snippet: automated-conflict-resolution

Here, conflicting versions are sorted by ascending emitter id (tracked internally as ``creator`` of the version) and the first version is selected as the winner. Its update timestamp is passed as argument to ``resolve`` which selects this version and discards all other versions.

Alternatively, we could also have used POSIX timestamps to let the *last* writer win. In case of equal timestamps, the lower emitter id wins. This requires synchronized system clocks to give reasonable result, however, convergence does not depend on proper synchronization. If system clock synchronization is not an option, `Lamport timestamps`_ can also be used to consistently resolve the conflict.

More advanced conflict resolution could select a winner depending on the actual value of concurrent versions. After selection, an application could even update the winner with the *merged* value of all conflicting versions\ [#]_.

.. note::
   For replicas to converge, it is important that winner selection does not depend on the order of conflicting events. In our example, this is the case because emitter id comparison is transitive.

Interactive conflict resolution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Interactive conflict resolution does not resolve conflicts immediately but requests the user to inspect and resolve a conflict. The following is a very simple example of interactive conflict resolution: a user selects a winner version if conflicting versions of application state exist.

.. includecode:: code/UserGuideDoc.scala
   :snippet: interactive-conflict-resolution

When a user tries to ``Append`` in presence of a conflict, the ``ExampleActor`` rejects the update and requests the user to select a winner version from a sequence of conflicting versions. The user then sends the update timestamp of the winner version as ``selectedTimestamp`` with a ``Resolve`` command from which a ``Resolved`` event is derived and persisted. Handling of ``Resolved`` at all replicas finally resolves the conflict.

In addition to just selecting a winner, an application could also update the winner version in a second step, for example, with a value derived from the merge result of conflicting versions. Support for *atomic*, interactive conflict resolution with an application-defined merge function is planned for later Eventuate releases.

.. note::
   Interactive conflict resolution requires agreement among replicas that are affected by a given conflict: only one of them may emit the ``Resolved`` event. This does not necessarily mean distributed lock acquisition or leader (= resolver) election but can also rely on static rules such as *only the initial creator location of an aggregate is allowed to resolve the conflict*\ [#]_. This rule is implemented in the :ref:`example-application`.

.. _commutative-replicated-data-types:

Operation-based CRDTs
---------------------

If state update operations commute, there’s no need to use Eventuate’s ``ConcurrentVersions`` utility. A simple example is a replicated counter, which converges because its increment and decrement operations commute. 

A formal to approach to commutative replicated data types (CmRDTs) or operation-based CRDTs is given in the paper `A comprehensive study of Convergent and Commutative Replicated Data Types`_ by Marc Shapiro et al. Eventuate is a good basis for implementing operation-based CRDTs:

- Update operations can be modeled as events and reliably broadcasted to all replicas by a :ref:`replicated-event-log`.
- The command and event handler of an event-sourced actor can be used to implement the two update phases mentioned in the paper: *atSource* and *downstream*, respectively.
- All *downstream* preconditions mentioned in the paper are satisfied in case of causal delivery of update operations which is guaranteed for actors consuming from a replicated event log.

Eventuate currently implements 3 out of 12 operation-based CRDTs specified in the paper. These are *Counter*, *MV-Register* and *OR-Set*. They can be instantiated and used via their corresponding *CRDT services*. CRDT operations are asynchronous methods on the service interfaces. CRDT services free applications from dealing with low-level details like event-sourced actors or command messages directly. The following is the definition of ORSetService_:

.. includecode:: ../main/scala/com/rbmhtechnology/eventuate/crdt/ORSet.scala
   :snippet: or-set-service

The ORSetService_ is a CRDT service that manages ORSet_ instances. It implements the asynchronous ``add`` and ``remove`` methods and inherits the ``value(id: String): Set[A]`` method from ``CRDTService[ORSet[A], Set[A]]`` for reading the current value. Their ``id`` parameter identifies an ``ORSet`` instance. Instances are automatically created by the service on demand. A usage example is the ReplicatedOrSetSpec_ that is based on Akka’s `multi node testkit`_.

New operation-based CRDTs and their corresponding services can be developed with the CRDT development framework, by defining an instance of the CRDTServiceOps_ type class and implementing the CRDTService_ trait. Take a look at the `CRDT sources`_ for examples. 

.. hint::
   Eventuate’s CRDT approach is also described in `this article`_.

.. _this article: https://krasserm.github.io/2015/02/17/Implementing-operation-based-CRDTs/

Event-sourced views
-------------------

Event-sourced views are a functional subset of event-sourced actors. They can only consume events from an event log but cannot produce new events. Concrete event-sourced views must implement the ``EventsourcedView`` trait. In the following example, the view counts all ``Appended`` and ``Resolved`` events emitted by all event-sourced actors to the same ``eventLog``:

.. includecode:: code/UserGuideDoc.scala
   :snippet: event-sourced-view

Event-sourced views handle events in the same way as event-sourced actors by implementing an ``onEvent`` handler. The ``onCommand`` handler in the example processes the read commands ``GetAppendCount`` and ``GetResolveCount``.

``ExampleView`` implements the mandatory global unique ``id`` but doesn’t define an ``aggregateId``. A view that doesn’t define an ``aggregateId`` can consume events from all event-sourced actors on the same event log. If it defines an ``aggregateId`` it can only consume events from event-sourced actors with the same ``aggregateId`` (assuming the default :ref:`event-routing` rules). 

.. _conditional-commands:

Conditional commands
--------------------

Events emitted by one event-sourced actor are asynchronously consumed by other event sourced-actors or views. For example, an application that successfully appended an entry to an ``ExampleActor`` may not immediately see that update in the ``appendCount`` of ``ExampleView``. To achieve read-your-write consistency between an event-sourced actor and a view, the view should delay command processing until the emitted event has been consumed by the view. This can be achieved with a ``ConditionalCommand``.

.. includecode:: code/UserGuideDoc.scala
   :snippet: conditional-commands

Here, the ``ExampleActor`` includes the event’s vector timestamp in its ``AppendSuccess`` reply. Together with the actual ``GetAppendCount`` command, the timestamp is included as condition in a ``ConditionalCommand`` and sent to the view. ``EventsourcedView`` internally delays the command, if needed, and only dispatches ``GetAppendCount`` to ``onCommand`` after having received an event whose timestamp is ``>=`` the condition timestamp in the conditional command\ [#]_. When running the example with an empty event log, it should print::

    append count = 1

.. note::
   Not only event-sourced views but also event-sourced actors can receive and delay conditional commands. Delaying conditional commands may re-order them relative to other conditional and non-conditional commands.

.. _ZooKeeper: http://zookeeper.apache.org/
.. _event sourcing: http://martinfowler.com/eaaDev/EventSourcing.html
.. _vector clock update rules: http://en.wikipedia.org/wiki/Vector_clock
.. _version vector update rules: http://en.wikipedia.org/wiki/Version_vector
.. _Lamport timestamps: http://en.wikipedia.org/wiki/Lamport_timestamps
.. _multi node testkit: http://doc.akka.io/docs/akka/2.3.9/dev/multi-node-testing.html
.. _ReplicatedOrSetSpec: https://github.com/RBMHTechnology/eventuate/blob/master/src/multi-jvm/scala/com/rbmhtechnology/eventuate/crdt/ReplicatedORSetSpec.scala
.. _CRDT sources: https://github.com/RBMHTechnology/eventuate/tree/master/src/main/scala/com/rbmhtechnology/eventuate/crdt
.. _A comprehensive study of Convergent and Commutative Replicated Data Types: http://hal.upmc.fr/file/index/docid/555588/filename/techreport.pdf

.. _Versioned: latest/api/index.html#com.rbmhtechnology.eventuate.Versioned
.. _ORSet: latest/api/index.html#com.rbmhtechnology.eventuate.crdt.ORSet
.. _ORSetService: latest/api/index.html#com.rbmhtechnology.eventuate.crdt.ORSetService
.. _CRDTService: latest/api/index.html#com.rbmhtechnology.eventuate.crdt.CRDTService
.. _CRDTServiceOps: latest/api/index.html#com.rbmhtechnology.eventuate.crdt.CRDTServiceOps

.. [#] ``EventsourcedActor``\ s and ``EventsourcedView``\ s that have an undefined ``aggregateId`` can consume events from all other actors on the same event log. 

.. [#] Attached update timestamps are not version vectors because Eventuate uses `vector clock update rules`_ instead of `version vector update rules`_. Consequently, update timestamp equivalence cannot be used as criterion for replica convergence.

.. [#] A formal approach to automatically *merge* concurrent versions of application state are convergent replicated data types (CvRDTs) or state-based CRDTs.

.. [#] Distributed lock acquisition or leader election require an external coordination service like ZooKeeper_, for example, whereas static rules do not.

.. [#] More precisely, the command is dispatched if the current time of the view’s internal clock is ``>=`` the condition timestamp in the conditional command. The view’s internal clock is updated by *merging* its current time with the vector timestamps of received events. 
