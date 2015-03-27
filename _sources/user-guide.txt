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
- avoid conflicts with operation-based CRDTs
- implement an event-sourced view over all event-sourced actors and
- achieve read-your-write consistency across event-sourced actors and views.

The user guide only scratches the surface of Eventuate. You can find further details in the :ref:`reference`.

Event-sourced actors
--------------------

An event-sourced actor is an actor that captures changes to its internal state as a sequence of events. It *persists* these events to its event log and *replays* them to recover internal state after a crash or a planned re-start. This is the basic idea behind `event sourcing`_: instead of storing current application state, the full history of changes is stored as *immutable facts* and current state is derived from the history of changes.

Event-sourced actors distinguish between *commands* and *events*. During command processing they usually validate external commands against internal state and, if validation succeeds, write one or more events to their event log. During event processing they consume events they have written and update internal state by handling these events.

Concrete event-sourced actors must implement the ``EventsourcedActor`` trait. The following ``ExampleActor`` maintains state of type ``Vector[String]`` to which entries can be appended:

.. includecode:: code/UserGuideDoc.scala
   :snippet: event-sourced-actor

For modifying ``currentState``, applications can send ``Append`` commands which are handled by the ``onCommand`` handler. From an ``Append`` command, the handler derives an ``Appended`` event and ``persist``\ s it to the given ``eventLog``. If persistence succeeds, the ``onEvent`` handler is called with the persisted event and the command sender is informed about successful processing. If persistence fails, the command sender is informed about the failure so that it can retry, if needed. 

The ``onEvent`` handler modifies ``currentState`` from successfully persisted events. If the actor is re-started, either after a crash or during normal application start, persisted events are replayed to ``onEvent`` so that internal state is automatically recovered before new commands are processed.

The ``ExampleActor`` also implements an optional ``aggregateId`` and the mandatory ``replicaId`` to distinguish its instances (more on that later). The ``eventLog`` actor reference is used by an sourced actor to write events to and read events from an :ref:`event-log`. 

Creating a single instance
~~~~~~~~~~~~~~~~~~~~~~~~~~

In the following, a single instance of ``ExampleActor`` is created and two ``Append`` commands are sent to it: 

.. includecode:: code/UserGuideDoc.scala
   :snippet: create-one-instance

Section :ref:`event-log` explains how to create ``eventLog`` actor references. Sending a ``Print`` command 

.. includecode:: code/UserGuideDoc.scala
   :snippet: print-one-instance

should print::

    [id = ea1, replica id = r1] a,b

When the application is re-started, persisted events are replayed to ``onEvent`` so that ``currentState`` is recovered. Sending another ``Print`` command should print again::

    [id = ea1, replica id = r1] a,b

In the following sections, several instances of ``ExampleActor`` are created. They *share* an event log which can be either a :ref:`local-event-log` or a :ref:`replicated-event-log`, depending on their distribution across *locations*. 

A shared event log is a pre-requisite for event-sourced actors to consume each other’s events. However, sharing an event log doesn’t necessarily mean broadcast communication between all actors on the same log. It is their ``aggreagteId`` and ``replicaId`` that determine which actors consume each other’s events. The underlying :ref:`event-routing` rules can be customized by applications.

Creating two isolated instances
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When creating two instances of ``ExampleActor`` with different ``aggregateId``\ s, they are isolated from each other and do not consume each other’s events:

.. includecode:: code/UserGuideDoc.scala
   :snippet: create-two-instances

Sending two ``Print`` commands

.. includecode:: code/UserGuideDoc.scala
   :snippet: print-two-instances

should print::

    [id = ea2, replica id = r1] a,b
    [id = ea3, replica id = r1] x,y

Creating two replica instances
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When creating two ``ExampleActor`` instances with the same ``aggregateId`` but different ``replicaId``\ s, they consume each other’s events [#]_. They are usually created at different locations (but can also be created at the same location, for example, for testing purposes).

.. includecode:: code/UserGuideDoc.scala
   :snippet: create-replica-instances

Here, ``ea4_r1`` processes an ``Append`` command and persists an ``Appended`` event. This event this then consumed by both, ``ea4_r1`` and ``ea4_r2``, so that they can update their internal state. After waiting a bit for convergence, sending a ``Print`` command to both actors should print::

    [id = ea4, replica id = r1] a
    [id = ea4, replica id = r2] a

After both replicas have converged, another ``Append`` is sent to ``ea4_r2``. 

.. includecode:: code/UserGuideDoc.scala
   :snippet: send-another-append

Again both actors consume the event and sending another ``Print`` command should print::

    [id = ea4, replica id = r1] a,b
    [id = ea4, replica id = r2] a,b

.. warning::
   As you have probably recognized, replica convergence in this example can only be achieved if the second ``Append`` command is sent after both actors have processed the ``Appended`` event from the first ``Append`` command. In this case, the second ``Appended`` event causally depends on the first one. Since events are guaranteed to be delivered in causal order to all replicas, they can converge to the same state.

   When concurrent updates are made to both replicas, the corresponding ``Appended`` events are not causally related and can be delivered in any order to both replicas. This may cause replicas to diverge because append operations do not commute. The following sections give examples how to detect and handle concurrent updates.

Detecting concurrent updates
----------------------------

Eventuate tracks causality of events with :ref:`vector-clocks` maintained by ``EventsourcedActor``\ s. Whenever an event-sourced actor writes an event, it advances its local time in the vector clock by 1 and attaches the current vector time as vector timestamp to the event. When handling an event, an event-sourced actor updates its vector clock according to the `vector clock update rules`_. But why are vector clocks and vector timestamps needed at all?

Let’s assume that an event-sourced actor emits an event ``e1`` for changing internal state and later receives an event ``e2`` from a replica instance. If the replica instance emits ``e2`` after having processed ``e1``, the actor can apply ``e2`` as regular update. If the replica instance emits ``e2`` before having received ``e1``, the actor has received a concurrent, potentially conflicting update. 

How can the actor determine if ``e2`` is a regular i.e. causally related or concurrent update? It can do so by comparing the vector timestamps of ``e1`` and ``e2``, where ``t1`` is the vector timestamp of ``e1`` and ``t2`` the vector timestamp of ``e2``. If events ``e1`` and ``e2`` are concurrent then ``t1 conc t2`` evaluates to ``true``. Otherwise, they are causally related and ``t1 < t2`` evaluates to ``true`` (because ``e1`` *happened-before* ``e2``).

The vector timestamp of an event can be obtained with ``lastVectorTimestamp`` during event processing. Vector timestamps can be attached as *update timestamp* to current state and compared to the vector timestamp of a new event in order to determine whether the new event is causally related to the previous state update or not\ [#]_:

.. includecode:: code/UserGuideDoc.scala
   :snippet: detecting-concurrent-update

Attaching update timestamps to current state and comparing them with vector timestamps of new events can be easily abstracted so that applications don’t have to deal with these low level details, as shown in the next section. 

Tracking conflicting versions
-----------------------------

If state update operations from concurrent events do not commute, conflicting versions of actor state arise that must be tracked and resolved. This can be done with Eventuate’s ``ConcurrentVersions[S, A]`` abstraction and an application-defined *update function* of type ``(S, A) => S`` where ``S`` is the type of actor state and ``A`` the update type. In our example the ``ConcurrentVersions`` type is ``ConcurrentVersions[Vector[String], String]`` and the update function ``(s, a) => s :+ a``:

.. includecode:: code/UserGuideDoc.scala
   :snippet: tracking-conflicting-versions

Internally, ``ConcurrentVersions`` maintains versions of actor state in a tree structure where each concurrent ``update`` creates a new branch. The shape of the tree is determined solely by the vector timestamps of the corresponding update events. 

An event’s vector timestamp is passed as ``lastVectorTimestamp`` argument to ``update``. A new version is internally created by applying the update function ``(s, a) => s :+ a`` to the closest predecessor version and the actual update value (``entry``). The ``lastVectorTimestamp`` is attached as update timestamp to the newly created version.

Concurrent versions of actor state and their update timestamp can be obtained with ``all`` which is a sequence of type ``Seq[Versioned[Vector[String]]]`` in our example. The Versioned_ data type represents a particular version of actor state and its update timestamp. 

If ``all`` contains only a single element, there is no conflict and the element represents the current, conflict-free actor state. If the sequence contains two or more elements, there is a conflict where the elements represent conflicting versions of actor states. They can be resolved either automatically or interactively.

.. note::
   Only concurrent updates to replicas with the same ``aggregateId`` may conflict. Concurrent updates to actors with different ``aggregateId`` do not conflict (unless an application does custom event routing).

   Also, if the data type of actor state is designed in a way that update operations commute, conflicts do not occur. This is discussed in section :ref:`commutative-replicated-data-types`.

Resolving conflicting versions
------------------------------

Automated conflict resolution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following is a very simple example of automated conflict resolution: if a conflict has been detected, the writer with the lower ``replicaId`` is selected to be the winner. The ``replicaId`` of an event writer can be obtained with ``lastEmitterReplicaId`` during event handling.

.. includecode:: code/UserGuideDoc.scala
   :snippet: automated-conflict-resolution

The conflicting versions are sorted by ascending ``emitterReplicaId`` and the first version is selected as the winner. Its update timestamp is passed as argument to ``resolve`` which selects this version and discards all other versions.

Alternatively, we could also have used the POSIX timestamps to let the *last* writer win. In case of equal timestamps, the lower ``emitterReplicaId`` wins. This requires synchronized system clocks to give reasonable result, however, convergence does not depend on proper synchronization.

More advanced conflict resolution examples could use logic that depends on the actual values of concurrent versions. After selecting a winner, an application could even update the winner version with *merged* content from all conflicting versions [#]_.

.. note::
   For replicas to converge, it is important that winner selection does not depend on the order of conflicting events. In our example, this is the case because ``replicaId`` comparison is transitive.

Interactive conflict resolution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Interactive conflict resolution does not resolve conflicts immediately but requests the user to select a winner version. In this case, the update timestamp of the selected winner must be explicitly stored within a conflict resolution event so that conflict resolution is repeatable at other replicas and during event replay.

.. includecode:: code/UserGuideDoc.scala
   :snippet: interactive-conflict-resolution

When a user tries to ``Append`` in presence of a conflict, the ``ExampleActor`` rejects the update and requests the user to select a winner version from a sequence of conflicting versions. The user then sends the update timestamp of the winner version as ``selectedTimestamp`` with a ``Resolve`` command from which a ``Resolved`` event is derived and persisted. Handling of ``Resolved`` at all replicas finally resolves the conflict.

.. note::
   Interactive conflict resolution requires agreement between replicas affected by a given conflict. Only one of them may emit the ``Resolved`` event. This does not necessarily mean distributed lock acquisition or leader (= resolver) election but can also rely on static rules such as *only the initial creator location of an aggregate is allowed to resolve the conflict*\ [#]_. This rule is implemented in the :ref:`example-application`.

.. _commutative-replicated-data-types:

Operation-based CRDTs
---------------------

If state update operations commute, there’s no need to use Eventuate’s ``ConcurrentVersions`` utility. A simple example is a replicated counter, which converges because the increment and decrement operations commute. A formal to approach to commutative replicated data types (CmRDTs) or operation-based CRDTs is given in the paper `A comprehensive study of Convergent and Commutative Replicated Data Types`_ by Marc Shapiro et al. Eventuate is a good basis for implementing operation-based CRDTs:

- Update operations can be modeled as events that are reliably broadcasted to all replicas by a :ref:`replicated-event-log`.
- The command and event handler of an event-sourced actor can be used to implement the two update phases mentioned in the paper: *atSource* and *downstream*, respectively.
- All *downstream* preconditions mentioned in the paper are satisfied by causal delivery of update operations which is guaranteed for actors consuming from a replicated event log.

Eventuate already provides implementations for some of the operation-based CRDTs in the paper. They can be accessed and used via *CRDT services*. CRDT services free applications from dealing with low-level details like event-sourced actors or command messages. CRDT operations are asynchronous methods on the service interface. The following is the definition of Eventuate’s ORSetService_:

.. includecode:: ../main/scala/com/rbmhtechnology/eventuate/crdt/ORSet.scala
   :snippet: or-set-service

The ORSetService_ is a CRDT service that manages ORSet_ instances which are specified in section 3.3.5 in the paper. It implements the asynchronous ``add`` and ``remove`` methods and inherits the ``value(id: String): Set[A]`` method from ``CRDTService`` for reading the current value. Their ``id`` parameter identifies an ``ORSet`` instance. Instances are automatically created by the service on demand. A usage example is the ReplicatedOrSetSpec_ that is based on Akka’s `multi node testkit`_.

New operation-based CRDTs and their service interfaces can be implemented with the CRDT development framework, by defining an instance of the CRDTServiceOps_ type class and implementing the CRDTService_ trait. Take a look at the `CRDT sources`_ for examples. 

.. hint::
   Eventuate’s CRDT approach is also described in `this article`_.

.. _this article: https://krasserm.github.io/2015/02/17/Implementing-operation-based-CRDTs/

Event-sourced views
-------------------

Event-sourced views are a functional subset of event-sourced actors. They can only consume events from an event log but cannot produce new events. A view that doesn’t define an ``aggregateId`` can consume events from all event-sourced actors on the same event log. If it defines an ``aggregateId`` it can only consume events from event-sourced actors with the same ``aggregateId`` (assuming the default event routing rules).

Concrete event-sourced views must implement the ``EventsourcedView`` trait. In the following example, the view counts all ``Appended`` and ``Resolved`` events emitted by all event-sourced actors on the same ``eventLog``. It doesn’t define an ``aggregateId``:

.. includecode:: code/UserGuideDoc.scala
   :snippet: event-sourced-view

Event-sourced views handle events in the same way as event-sourced actors by implementing an ``onEvent`` handler. The ``onCommand`` handler in the example processes the read commands ``GetAppendCount`` and ``GetResolveCount``.

.. _conditional-commands:

Conditional commands
--------------------

Events emitted by one event-sourced actor are asynchronously consumed by other event sourced-actors or views. For example, an application that successfully appended an entry to an ``ExampleActor`` may not immediately see that update in the ``appendCount`` of ``ExampleView``. To achieve read-your-write consistency between an event-sourced actor and a view, the view should delay command processing until the emitted event has been consumed by the view. This is possible with a ``ConditionalCommand``.

.. includecode:: code/UserGuideDoc.scala
   :snippet: conditional-commands

The ``ExampleActor`` includes the event’s vector timestamp in its ``AppendSuccess`` reply. Together with the actual ``GetAppendCount`` command, the timestamp is included as condition into a ``ConditionalCommand`` and sent to the view. ``EventsourcedView`` internally delays the command, if needed, and only dispatches ``GetAppendCount`` to ``onCommand`` after having received an event whose timestamp is ``>=`` the condition timestamp in the conditional command\ [#]_. When running the example with an empty event log, it should print::

    append count = 1

.. note::
   Not only event-sourced views but also event-sourced actors can receive and delay conditional commands. Also, delaying conditional commands may re-order them relative to other conditional and non-conditional commands.

.. _ZooKeeper: http://zookeeper.apache.org/
.. _event sourcing: http://martinfowler.com/eaaDev/EventSourcing.html
.. _vector clock update rules: http://en.wikipedia.org/wiki/Vector_clock
.. _version vector update rules: http://en.wikipedia.org/wiki/Version_vector
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

.. [#] Attached update timestamps are not version vectors because we use `vector clock update rules`_ instead of `version vector update rules`_. Consequently, update timestamp equivalence cannot be used as criterion for replica convergence.

.. [#] A formal approach to automatically *merge* concurrent versions of application state are convergent replicated data types (CvRDTs) or state-based CRDTs.

.. [#] Distributed lock acquisition or leader election require an external coordination service like ZooKeeper_, for example, whereas static rules do not.

.. [#] More precisely, the command is dispatched if the current time of the view’s internal clock is ``>=`` the condition timestamp in the conditional command. The view’s internal clock is updated by *merging* its current time with the vector timestamps of received events. 
