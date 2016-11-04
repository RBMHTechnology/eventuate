.. _ref-event-sourced-actors:

Event-sourced actors
--------------------

An introduction to event-sourced actors is already given in sections :ref:`overview`, :ref:`architecture` and the :ref:`user-guide`. Applications use event-sourced actors for writing events to an event log and for maintaining in-memory write models on the command side (C) of a CQRS_ application. Event-sourced actors distinguish command processing from event processing. They must extend the EventsourcedActor_ trait and implement a :ref:`command-handler` and an :ref:`event-handler`.

.. _command-handler:

Command handler
~~~~~~~~~~~~~~~

A command handler is partial function of type ``PartialFunction[Any, Unit]`` for which a type alias ``Receive`` exists. It can be defined by implementing ``onCommand``:

.. includecode:: ../code/EventSourcingDoc.scala
   :snippet: command-handler

Messages sent by an application to an event-sourced actor are received by its command handler. Usually, a command handler first validates a command, then derives one or more events from it, persists these events with ``persist`` and replies with the persistence result. The ``persist`` method has the following signature\ [#]_:

.. includecode:: ../code/EventSourcingDoc.scala
   :snippet: persist-signature

The ``persist`` method can be called one ore more times per received command. Calling ``persist`` does not immediately write events to the event log. Instead, events from ``persist`` calls are collected in memory and written to the event log when ``onCommand`` returns. 

Events are written asynchronously to the event-sourced actor’s ``eventLog``. After writing, the ``eventLog`` actor internally replies to the event-sourced actor with a success or failure message which is passed as argument to the persist ``handler``. Before calling the persist ``handler``, the event-sourced actor internally calls the ``onEvent`` handler with the written event if writing was successful and ``onEvent`` is defined at that event.

Both, event handler and persist handler are called on a dispatcher thread of the actor. They can therefore safely access internal actor state. The ``sender()`` reference of the original command sender is also preserved, so that a persist handler can reply to the initial command sender.

.. hint::
   The ``EventsourcedActor`` trait also defines a ``persistN`` method. Refer to the EventsourcedActor_ API documentation for details.

.. note::
   A command handler should not modify persistent actor state i.e. state that is derived from events. 

.. _state-sync:

State synchronization
~~~~~~~~~~~~~~~~~~~~~

As explained in section :ref:`command-handler`, events are persisted asynchronously. What happens if another command is sent to an event-sourced actor while persistence is in progress? This depends on the value of ``stateSync``, a member of ``EventsourcedActor`` that can be overridden.

.. includecode:: ../../../eventuate-core/src/main/scala/com/rbmhtechnology/eventuate/EventsourcedActor.scala
   :snippet: state-sync

If ``stateSync`` is ``true`` (default), new commands are stashed_ while persistence is in progress. Consequently, new commands see actor state that is *in sync* with the events in the event log. A consequence is limited write throughput, because :ref:`batching` of write requests is not possible in this case\ [#]_. This setting is recommended for event-sourced actors that must validate commands against current state.

If ``stateSync`` is ``false``, new commands are dispatched to ``onCommand`` immediately. Consequently, new commands may see stale actor state. The advantage is significantly higher write throughput as :ref:`batching` of write requests is possible. This setting is recommended for event-sourced actors that don’t need to validate commands against current state.

If a sender sends several (update) commands followed by a query to an event-sourced actor that has ``stateSync`` set to ``false``, the query will probably not see the state change from the preceding commands. To achieve read-your-write consistency, the command sender should wait for a reply from the last command before sending the query. The reply must of course be sent from within a ``persist`` handler.

.. note::
   State synchronization settings only apply to a single actor instance. Events that are emitted concurrently by other actors and handled by that instance can arrive at any time and modify actor state. Anyway, concurrent events are not relevant for achieving read-your-write consistency and should be handled as described in the :ref:`user-guide`.

.. _event-handler:

Event handler
~~~~~~~~~~~~~

An event handler is partial function of type ``PartialFunction[Any, Unit]`` for which a type alias ``Receive`` exists. It can be defined by implementing ``onEvent``. An event handler handles persisted events by updating actor state from event details. 

.. includecode:: ../code/EventSourcingDoc.scala
   :snippet: event-handler

Event metadata of the last handled event can be obtained with the ``last*`` methods defined by ``EventsourcedActor``. For example, ``lastSequenceNr`` returns the event’s local sequence number, ``lastVectorTimestamp`` returns the event’s vector timestamp. A complete reference is given by the EventsourcedActor_ API documentation.

.. note::
   An event handler should only update internal actor state without having further side-effects. An exception is :ref:`reliable-delivery` of messages and :ref:`guide-event-driven-communication` with PersistOnEvent_.

.. _ref-event-sourced-views:

Event-sourced views
-------------------

An introduction to event-sourced views is already given in sections :ref:`overview`, :ref:`architecture` and the :ref:`user-guide`. Applications use event-sourced views for for maintaining in-memory read models on the query side (Q) of a CQRS_ application.

Like event-sourced actors, event-sourced views distinguish command processing from event processing. They must implement the EventsourcedView_ trait. ``EventsourcedView`` is a functional subset of ``EventsourcedActor`` that cannot ``persist`` events.

.. _ref-event-sourced-writers:

Event-sourced writers
---------------------

An introduction to event-sourced writers is already given in sections :ref:`overview` and :ref:`architecture`. Applications use event-sourced writers for maintaining persistent read models on the query side (Q) of a CQRS_ application.

Like event-sourced views, event-sourced writers can only consume events from an event log but can make incremental batch updates to external, application-defined query databases. A query database can be a relational database, a graph database or whatever is needed by an application. Concrete writers must implement the EventsourcedWriter_ trait.

This section outlines how to update a persistent read model in Cassandra_ from events consumed by an event-sourced writer. The relevant events are:

.. includecode:: ../../../eventuate-examples/src/main/scala/com/rbmhtechnology/example/querydb/Emitter.scala
   :snippet: events

The persistent read model is a ``CUSTOMER`` table with the following structure::

     id | first  | last    | address
    ----+--------+---------+-------------
      1 | Martin | Krasser | Somewhere 1
      2 | Volker | Stampa  | Somewhere 3
      3 | ...    | ...     | ...

The read model update progress is written to a separate ``PROGRESS`` table with a single ``sequence_nr`` column::

     id | sequence_nr
    ----+-------------
      0 |           3

The stored sequence number is that of the last successfully processed event. An event is considered as successfully processed if its data have been written to the ``CUSTOMER`` table. Only a single row is needed in the ``PROGRESS`` table to track the update progress for the whole ``CUSTOMER`` table.

The event-sourced ``Writer`` in the following example implements ``EventsourcedWriter[Long, Unit]`` (where ``Long`` is the type of the initial read result and ``Unit`` the type of write results). It is initialized with an ``eventLog`` from which it consumes events and a Cassandra ``Session`` for writing event processing results.

.. includecode:: ../../../eventuate-examples/src/main/scala/com/rbmhtechnology/example/querydb/Writer.scala
   :snippet: writer

.. hint::
   The full example source code is available `here <https://github.com/RBMHTechnology/eventuate/tree/master/eventuate-examples/src/main/scala/com/rbmhtechnology/example/querydb>`_.

On a high level, the example ``Writer`` implements the following behavior:

- During initialization (after start or restart) it asynchronously ``read``\ s the stored update progress from the ``PROGRESS`` table. The read result is passed as argument to ``readSuccess`` and incremented by ``1`` before returning it to the caller. This causes the ``Writer`` to resume event processing from that position in the event log.
- Event are processed in ``onEvent`` by translating them to Cassandra update statements which are added to an in-memory ``batch`` of type ``Vector[BoundStatement]``. The batch is written to Cassandra when Eventuate calls the ``write`` method.
- The ``write`` method asynchronously updates the ``CUSTOMER`` table with the statements contained in ``batch`` and then updates the ``PROGRESS`` table with the sequence number of the last processed event. After having submitted the statements to Cassandra, the batch is cleared for further event processing. Event processing can run concurrently to write operations. 
- A ``batch`` that has been updated while a write operation is in progress is written directly after the current write operation successfully completes. If no write operation is in progress, a change to ``batch`` is written immediately. This keeps read model update delays at a minimum and increases batch sizes under increasing load. Batch sizes can be limited with ``replayBatchSize``.

If a ``write`` (or ``read``) operation fails, the writer is restarted, by default, and resumes event processing from the last stored sequence number + ``1``. This behavior can be changed by overriding ``writeFailure`` (or ``readFailure``) from ``EventsourcedWriter``.

.. note::
   The example does not use Cassandra ``BatchStatement``\ s for reasons explained in `this article <https://medium.com/@foundev/cassandra-batch-loading-without-the-batch-keyword-40f00e35e23e>`_. Atomic writes are not needed because database updates in this example are idempotent and can be re-tried in failure cases. Failure cases where idempotency is relevant are partial updates to the ``CUSTOMER`` table or a failed write to the ``PROGRESS`` table. ``BatchStatement``\ s should only be used when database updates are not idempotent and atomicity is required on database level.
   
.. _stateful-writers:

Stateful writers
~~~~~~~~~~~~~~~~

The above ``Writer`` implements a stateless writer. Although it accumulates batches while a write operation is in progress, it cannot recover permanent in-memory state from the event log, because event processing only starts from the last stored sequence number. If a writer needs to be stateful, it must return ``None`` from ``readSuccess``. In this case, event replay either starts from scratch or from a previously stored snapshot. A stateful writer should still write the update progress to the ``PROGRESS`` table but exclude events with a sequence number less than or equal to the stored sequence number from contributing to the update ``batch``.

.. _ref-event-sourced-processors:

Event-sourced processors
------------------------

An introduction to event-sourced processors is already given in sections :ref:`overview` and :ref:`architecture`. Applications use event-sourced processors to consume events form a source event log, process these events and write the processed events to a target event log. With processors, event logs can be connected to event stream processing pipelines and graphs.

Event-sourced processors are a specialization of :ref:`event-sourced-writers` where the *external database* is a target event log. Concrete stateless processors must implement the EventsourcedProcessor_ trait, stateful processors the StatefulProcessor_ trait (see also :ref:`stateful-writers`).

The following example ``Processor`` is an implementation of ``EventsourcedProcessor``. In addition to providing a source ``eventLog``, a concrete processor must also provide a ``targetEventLog``:

.. includecode:: ../code/EventSourcingDoc.scala
   :snippet: processor

The event handler implemented by a processor is ``processEvent``. The type of the handler is defined as:

.. includecode:: ../../../eventuate-core/src/main/scala/com/rbmhtechnology/eventuate/EventsourcedProcessor.scala
   :snippet: process

Processed events, to be written to the target event log, are returned by the handler as ``Seq[Any]``. With this handler signature, events from the source log can be 

- excluded from being written to the target log by returning an empty ``Seq`` 
- transformed one-to-one by returning a ``Seq`` of size 1 or even
- transformed and split by returning a ``Seq`` of size greater than ``1``

.. note::
   ``EventsourcedProcessor`` and ``StatefulProcessor`` internally ensure that writing to the target event log is idempotent. Applications don’t need to take extra care about idempotency.

.. _state-recovery:

State recovery
--------------

When an event-sourced actor or view is started or re-started, events are replayed to its ``onEvent`` handler so that internal state can be recovered\ [#]_. This is also the case for stateful event-sourced writers and processors. During event replay the ``recovering`` method returns ``true``. Applications can also define a recovery completion handler by overriding ``onRecovery``:

.. includecode:: ../code/EventSourcingDoc.scala
   :snippet: recovery-handler

If replay fails the completion handler is called with a ``Failure`` and the actor will be stopped, regardless of the action taken by the handler. The default recovery completion handler does nothing. Internally each replay request towards the event log is retried a couple of times in order to cope with a temporarily unresponsive event log or its underlying storage backend. The maximum number of retries for a replay request can be configured with:

.. includecode:: ../conf/common.conf
   :snippet: replay-retry-max

Moreover the configuration value ``replay-retry-delay`` is used to determine the delay between consecutive replay attempts:

.. includecode:: ../conf/common.conf
   :snippet: replay-retry-delay

At the beginning of event replay, the initiating actor is registered at its event log so that newly written events can be routed to that actor. During replay, the actor internally stashes these newly written events and dispatches them to ``onEvent`` after successful replay. In a similar way, the actor also stashes new commands and dispatches them to ``onCommand`` afterwards. This ensures that new commands never see partially recovered state. When the actor is stopped it is automatically de-registered from its event log.

Backpressure
~~~~~~~~~~~~

Events are replayed in batches. A given batch must have been handled by an event handler before the next batch is replayed. This allows slow event handlers to put backpressure on event replay. The default replay batch size can be configured with:

.. includecode:: ../conf/common.conf
   :snippet: replay-batch-size

Event-sourced components can override the configured default value by overriding ``replayBatchSize``:

.. includecode:: ../code/EventSourcingDoc.scala
   :snippet: replay-batch-size

Recovery using an application-defined log sequence number
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
In order to keep recovery times small it is almost always sensible to recover using snapshots. However, in some very rare cases an event-sourced actor or view can recover quickly using an application-defined log sequence number. If defined, only events with a sequence number equal to or larger than the given sequence number are replayed.

.. includecode:: ../code/EventSourcingDoc.scala
   :snippet: replay-from-sequence-nr

.. _snapshots:

Snapshots
---------

Recovery times increase with the number of events that are replayed to event-sourced components. They can be decreased by starting event replay from a previously saved snapshot of internal state rather than replaying events from scratch. Event-sourced components can save snapshots by calling ``save`` within their command handler:

.. includecode:: ../code/EventSourcingDoc.scala
   :snippet: snapshot-save

Snapshots are saved asynchronously. On completion, a user-defined handler of type ``Try[SnapshotMetadata] => Unit`` is called. Like a ``persist`` handler, a ``save`` handler may also close over actor state and can reply to the command sender using the ``sender()`` reference. 

An event-sourced actor that is :ref:`tracking-conflicting-versions` of application state can also save ``ConcurrentVersions[A, B]`` instances directly. One can even configure custom serializers for type parameter ``A`` as explained in section :ref:`snapshot-serialization`.

During recovery, the latest snapshot saved by an event-sourced component is loaded and can be handled with the ``onSnapshot`` handler. This handler should initialize internal actor state from the loaded snapshot: 

.. includecode:: ../code/EventSourcingDoc.scala
   :snippet: snapshot-load

If ``onSnapshot`` is not defined at the loaded snapshot or not overridden at all, event replay starts from scratch. If ``onSnapshot`` is defined at the loaded snapshot, only events that are not covered by that snapshot will be replayed. 

Event-sourced actors that implement ``ConfirmedDelivery`` for :ref:`reliable-delivery` automatically include unconfirmed messages into state snapshots. These are restored on recovery and re-delivered on recovery completion.

.. note::
   State objects passed as argument to ``save`` should be *immutable objects*. If this is not the case, the caller is responsible for creating a defensive copy before passing it as argument to ``save``.

Storage locations
~~~~~~~~~~~~~~~~~

Snapshots are currently stored in a directory that can be configured with

.. includecode:: ../conf/snapshot.conf
   :snippet: snapshot-dir

in ``application.conf``. The maximum number of stored snapshots per event-sourced component can be configured with

.. includecode:: ../conf/snapshot.conf
   :snippet: snapshot-num

If this number is exceeded, older snapshots are automatically deleted.

.. _event-routing:

Event routing
-------------

An event that is emitted by an event-sourced actor or processor can be routed to other event-sourced components if they share an :ref:`event-log`\ [#]_ . The default event routing rules are:

- If an event-sourced component has an undefined ``aggregateId``, all events are routed to it. It may choose to handle only a subset of them though.
- If an event-sourced component has a defined ``aggregateId``, only events emitted by event-sourced actors or processors with the same ``aggregateId`` are routed to it.

Routing destinations are defined during emission of an event and are persisted together with the event\ [#]_. This makes routing decisions repeatable during event replay and allows for routing rule changes without affecting past routing decisions. Applications can define additional routing destinations with the ``customDestinationAggregateIds`` parameter of ``persist``:

.. includecode:: ../code/EventRoutingDoc.scala
   :snippet: custom-routing

Here, ``ExampleEvent`` is routed to destinations with ``aggregateId``\ s ``Some(“a2”)`` and ``Some(“a3”)`` in addition to the default routing destinations with ``aggregateId``\s ``Some(“a1”)`` and ``None``.

.. _ref-event-driven-communication:

Event-driven communication
--------------------------

Event-driven communication is one form of :ref:`overview-event-collaboration` and covered in the :ref:`guide-event-driven-communication` section of the :ref:`user-guide`.

.. _reliable-delivery:

Reliable delivery
-----------------

Reliable, event-based remote communication between event-sourced actors should be done via a :ref:`replicated-event-log`. For reliable communication with other services that cannot connect to a replicated event log, event-sourced actors should use the ConfirmedDelivery_ trait:

.. includecode:: ../code/ReliableDeliveryDoc.scala
   :snippet: reliable-delivery

``ConfirmedDelivery`` supports the reliable delivery of messages to destinations by enabling applications to re-deliver messages until delivery is confirmed by destinations. In the example above, the reliable delivery of a message is initiated by sending a ``DeliverCommand`` to ``ExampleActor``. 

The handler of the generated ``DeliverEvent`` calls ``deliver`` to deliver a ``ReliableMessage`` to ``destination``. The ``deliveryId`` is an identifier to correlate ``ReliableMessage`` with a ``Confirmation`` message. The ``deliveryId`` can be any application-defined id. Here, the event’s sequence number is used which can be obtained with ``lastSequenceNumber``. 

The ``destination`` confirms the delivery of the message by sending a ``Confirmation`` reply to the event-sourced actor from which it generates a ``ConfirmationEvent``. The actor uses the ``persistConfirmation`` method to persist the confirmation event together with the delivery id. After successful persistence of the confirmation event, the corresponding reliable message is removed from the internal buffer of unconfirmed messages.

When the actor is re-started, unconfirmed reliable messages are automatically re-delivered to their ``destination``\ s. The example actor additionally schedules ``redeliverUnconfirmed`` calls to periodically re-deliver unconfirmed messages. This is done within the actor’s command handler.

.. note::
   In the above example a pattern guard is used for idempotent confirmation processing by ensuring that the ``deliveryId`` of the ``Confirmation`` message is still unconfirmed. This pattern may only be applied if the ``stateSync`` member of the ``EventsourcedActor`` is set to ``true``. For further details on ``stateSync`` see section :ref:`state-sync`.

.. note::
   If a snapshot is taken unconfirmed messages are stored in the snapshot along with the destination ``ActorPath``. That is why the actual ``ActorPath`` of the destination must not change between restarts of the actor, if, for example, the destination actor is within the same application and the application is restarted. That is why the destination actor must be named explicitly instead of having a name generated by the ``ActorSystem``.

.. _ref-conditional-requests:

Conditional requests
--------------------

Conditional requests are covered in the :ref:`conditional-requests` section of the :ref:`user-guide`.

.. _command-stashing:

Command stashing
----------------

``EventsourcedView`` and ``EventsourcedActor`` override ``stash()`` and ``unstashAll()`` of ``akka.actor.Stash`` so that application-specific subclasses can safely stash and unstash commands. Stashing of events is not allowed. Hence, ``stash()`` must only be used in a command handler, using it in an event handler will throw ``StashError``. On the other hand, ``unsatshAll()`` can be used anywhere i.e. in a command handler, persist handler or event handler. The following is a trivial usage example which calls ``stash()`` in the command handler and ``unstashAll()`` in the persist handler:

.. includecode:: ../code/EventSourcingDoc.scala
   :snippet: command-stash

The ``UserManager`` maintains a persistent ``users`` map. User can be added to the map by sending a ``CreateUser`` command and updated by sending and ``UpdateUser`` command. Should these commands arrive in wrong order i.e. ``UpdateUser`` before a corresponding ``CreateUser``, the ``UserManager`` stashes ``UpdateUser`` and unstashes it after having successfully processed another ``CreateUser`` command. 

In the above implementation, an ``UpdateUser`` command might be repeatedly stashed and unstashed if the corresponding ``CreateUser`` command is preceded by other unrelated ``CreateUser`` commands. Assuming that out-of-order user commands are rare, the performance impact is limited. Alternatively, one could record stashed user ids in transient actor state and conditionally call ``unstashAll()`` by checking that state.

Behavior changes
----------------

Event-sourced components distinguish command processing from event processing. Consequently, applications should be able to change the behavior of command handlers and event handlers independent of each other, at runtime. Command handling behavior can be changed with ``commandContext.become()`` and ``commandContext.unbecome()``, event handling behavior with ``eventContext.become()`` and ``eventContext.unbecome()`` (for details, see the BehaviorContext_ API docs):

.. includecode:: ../code/EventSourcingDoc.scala
   :snippet: behavior-changes

This works for all event-sourcing abstractions except for ``EventsourcedProcessor``. Its ``eventContext`` does not allow behavior changes as ``EventsourcedProcessor`` implements default ``onEvent`` behavior that should be changed by applications. An attempt to change that behavior will throw an ``UnsupportedOperationException``. Changing an ``EventsourcedProcessor``’s ``processEvent`` behavior is not supported yet.

.. note::
   Command and event handling behaviors are managed by internal behavior stacks. Eventuate does **not** include these behavior stacks into :ref:`snapshots` when applications ``save`` actor state. Although the state of an event handling behavior stack can be recovered by replaying events from scratch, that stack is not automatically recovered when a snapshot is loaded. Applications are therefore responsible to restore the required command and event handling behavior from application-specific snapshot details in the ``onSnapshot`` handler. Of course, this is only necessary if the required behavior differs from the default ``onEvent`` and ``onCommand`` behavior.

Failure handling
----------------

Event-sourced components register themselves at an EventLog_ actor in order to be notified about changes in the event log. Directly after registration, during recovery, they read from the event log in order to recover internal state from past events. After recovery has completed, the event log actor **pushes** newly written events to registered actors so that they can update application state with minimal latency. If a registered actor is restarted, it recovers again from the event log and continues to process push-updates after recovery has completed.

An EventLog_ actor processes write requests from :ref:`ref-event-sourced-actors`, :ref:`ref-event-sourced-processors` and :ref:`replication-endpoints`. If a write succeeds it pushes the written events to registered actors (under consideration of :ref:`event-routing` rules) and handles the next write request. Writing to a storage backend may also fail for several reasons. In the following, it is assumed that writes are made to a remote storage backend such as the :ref:`cassandra-storage-backend`.

A write failure reported from a storage backend driver does not necessarily mean that the events have not been written to the storage backend. For example, a write could have been actually applied to the remote storage backend but the ACK message got lost. This usually causes the driver to report a timeout. If an event log actor would simply continue with the next write request, after having informed the event emitter about the failure, the emitter and and other registered actors would erroneously assume that the emitted events do not exist in the event log. However, these events may become visible to newly registered actors that are about to recover or to replication endpoints that read events for replication. 

This would violate the event ordering and consistency guarantees made by Eventuate because some registered actors would see an event stream with missing events. The following describes two options to deal with that situation:

#. After a failed write, the event log actor notifies all registered actors to restart themselves so that another recovery phase would find out whether the events have been actually written or not. This is fine if the write failure was actually a lost ACK and the storage backend is immediately available for subsequent reads (neglecting a potentially high read load). If the write failure was because of a longer-lasting problem, such as a longer network partition that disconnects the application from the storage backend, registered actors would fail to recover and would be therefore be unavailable for in-memory reads.

#. The event log actor itself tries to find out whether the write was successful or not, either by reading from the storage backend or by retrying the write until it succeeds, before continuing with the next write request. In this case, the log actor would inform the event emitter either about a failed write if it can guarantee that the write has not been applied to the storage backend or about a successful write if retrying the write finally succeeded. Retrying writes can only be made to storage backends that support idempotent writes. With this strategy, registered actors don’t need be restarted and remain available for in-memory reads.

In Eventuate, the second approach is taken. Should there be a longer-lasting problem with the storage backend, it may take a longer time for an event log actor to make a decision about the success or failure of a write. During that time, it will reject further writes in order to avoid being overloaded with pending write requests. This is an application of the `circuit breaker`_ design pattern.

Consequently, a write failure reported by an event log actor means that the write was actually **not** applied to the storage backend. This additional guarantee comes at the cost of potentially long write reply delays but allows registered actors to remain available for in-memory reads during storage backend unavailability. It also provides clearer semantics of write failures. 

.. _circuit-breaker:

Circuit breaker
~~~~~~~~~~~~~~~

The strategy described above can be implemented by wrapping a CassandraEventLog_ in a CircuitBreaker_ actor. This is the default when creating the log actor for a :ref:`cassandra-storage-backend`. Should the event log actor need to retry a write ``eventuate.log.circuit-breaker.open-after-retries`` times or more, the circuit breaker opens. If open, it rejects all requests by replying with a failure message that contains an EventLogUnavailableException_. If retrying the write finally succeeds, the circuit breaker closes again. The maximum number of write retries can be configured with ``eventuate.log.cassandra.write-retry-max`` and the delay between write retries with ``eventuate.log.write-timeout``. If the maximum number of retries is reached, the event log actor gives up and stops itself which also stops all registered actors.

.. _persist-failure-handling:

``persist`` failure handling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Asynchronous ``persist`` operations send write requests to an EventLog_ actor. The write reply is passed as argument to the persist handler (see section :ref:`command-handler`). If the persist handler is called with a ``Failure`` one can safely assume that the events have not been written to the storage backend. As already explained above, a consequence of this additional guarantee is that persist handler callbacks may be delayed indefinitely.

For an ``EventsourcedActor`` with ``stateSync`` set to ``true``, this means that further commands sent to that actor will be stashed until the current write completes. In this case, it is the responsibility of the application not to overload that actor with further commands. For example, an application could use timeouts for command replies and prevent sending further commands to that actor if a timeout occurred. After an application-defined delay, command sending can be resumed. This is comparable to using an application-level circuit breaker. Alternatively, an application could restart an event-sourced actor on command timeout and continue sending new commands to that actor after recovery succeeded. This however may take a while depending on the unavailability duration of the storage backend. 

``EventsourcedActor``\ s with ``stateSync`` set to ``false`` do not stash commands but rather send write requests immediately to the event log actor. If the log actor is busy retrying a write and the :ref:`circuit-breaker` opens, later persist operations will be completed immediately with an ``EventLogUnavailableException`` failure, regardless whether the event-sourced actor has persist operations in progress or not. A persist operation of an ``EventsourcedActor`` with ``stateSync`` set to ``true`` will only be completed with an ``EventLogUnavailableException`` failure if that actor had no persist operation in progress at the time the circuit breaker opened.

.. _persist-on-event-failure-handling:

``persistOnEvent`` failure handling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``EventsourcedActor``\ s can also persist events in the :ref:`event-handler` if they additionally extend PersistOnEvent_. An asynchronous ``persistOnEvent`` operation may also fail for reasons explained in :ref:`persist-failure-handling`. If a ``persistOnEvent`` operation fails, the actor is automatically restarted by throwing a ``PersistOnEventException``.

Recovery failure handling
~~~~~~~~~~~~~~~~~~~~~~~~~

As explained in section :ref:`state-recovery`, event-sourced components are stopped if their recovery fails. Applications should either define a custom ``onRecovery`` completion handler to obtain information about recovery failure details or just watch these actors if recovery failure details are not relevant.

Batch write failure handling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Events are written in batches. When using the :ref:`cassandra-storage-backend`, there’s a *warn threshold* and *fail threshold* for batch sizes. The default settings in ``cassandra.yaml`` are::

    # Caution should be taken on increasing the size of this threshold as it can 
    # lead to node instability.
    batch_size_warn_threshold_in_kb: 5

    # Fail any batch exceeding this value. 50kb (10x warn threshold) by default.
    batch_size_fail_threshold_in_kb: 50

When the size of an event batch exceeds the *fail threshold*, the batch write fails with::

    com.datastax.driver.core.exceptions.InvalidQueryException: Batch too large

The corresponding entry in the Cassandra system log is::

    ERROR <timestamp> Batch of prepared statements for [eventuate.log_<id>] is of size 103800, exceeding specified threshold of 51200 by 52600. (see batch_size_fail_threshold_in_kb)

.. note::
   If Eventuate is the only writer to the Cassandra cluster then it is safe to increase these thresholds to higher values as Eventuate only makes single-partition batch writes (see also `CASSANDRA-8825`_). 

If other applications additionally make multi-partition batch writes to the same Cassandra cluster then is recommended to reduce

.. includecode:: ../conf/common.conf
   :snippet: write-batch-size

and

.. includecode:: ../conf/common.conf
   :snippet: index-update-limit

to a smaller value like ``32``, for example, or even smaller. Failed replication writes or index writes are re-tried automatically by Eventuate. Failed ``persist`` operations must be re-tried by the application. 

Batch replication failure handling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

During replication, events are batch-transferred over the network. The maximum number of events per batch can be configured with:

.. includecode:: ../conf/common.conf
   :snippet: write-batch-size

The maximum batch size in bytes the transport will accept is limited. If this limit is exceeded, batch transfer will fail. In this case, applications should either increase

.. includecode:: ../conf/common.conf
   :snippet: maximum-frame-size

or decrease the event batch size.

.. note::
   Batch sizes in Eventuate are currently defined in units of events whereas ``maximum-frame-size`` is defined in bytes. This mismatch will be removed in a later release (see also `ticket 166`_).

Custom serialization
--------------------

.. _event-serialization:

Custom event serialization
~~~~~~~~~~~~~~~~~~~~~~~~~~

Custom serializers for application-defined events can be configured with Akka's `serialization extension`_. For example, an application that wants to use a custom ``MyDomainEventSerializer`` for events of type ``MyDomainEvent`` (both defined in package ``com.example``) should add the following configuration to ``application.conf``:

.. includecode:: ../conf/serializer.conf
   :snippet: custom-event-serializer

``MyDomainEventSerializer`` must extend Akka’s Serializer_ trait. Please refer to Akka’s `serialization extension`_ documentation for further details.

Eventuate stores application-defined events as ``payload`` of DurableEvent_\ s. ``DurableEvent`` itself is serialized with DurableEventSerializer_, a `Protocol Buffers`_ based serializer that delegates ``payload`` serialization to a custom serializer. If no custom serializer is configured, Akka’s default serializer is used.

.. _replication-filter-serialization:

Custom replication filter serialization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the same way as for application-defined events, custom serializers for :ref:`replication-filters` can also be configured via Akka's `serialization extension`_. For example, an application that wants to use a custom ``MyReplicationFilterSerializer`` for replication filters of type ``MyReplicationFilter`` (both defined in package ``com.example``) should add the following configuration to ``application.conf``:

.. includecode:: ../conf/serializer.conf
   :snippet: custom-filter-serializer

Custom replication filter serialization also works if the custom filter is part of a composite filter that has been composed with ``and`` or ``or`` combinators (see ReplicationFilter_ API). If no custom filter serializer is configured, Akka’s default serializer is used.

.. _snapshot-serialization:

Custom snapshot serialization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Applications can also configure custom serializers for snapshots in the same way as for application-defined events and replication filters (see sections :ref:`event-serialization` and :ref:`replication-filter-serialization`). 

Custom snapshot serialization also works for state managed with ``ConcurrentVersions[A, B]``. A custom serializer configured for type parameter ``A`` is used whenever a snapshot of type ``ConcurrentVersions[A, B]`` is saved (see also :ref:`tracking-conflicting-versions`).

Event-sourced actors that extend ``ConfirmedDelivery`` for :ref:`reliable-delivery` of messages to destinations will also include unconfirmed messages as ``deliveryAttempts`` in a Snapshot_. The ``message`` field of a DeliveryAttempt_ can also be custom-serialized by configuring a serializer.

Custom CRDT serialization
~~~~~~~~~~~~~~~~~~~~~~~~~

Custom serializers can also be configured for the type parameter ``A`` of ``MVRegister[A]``, ``LWWRegister[A]`` and ``ORSet[A]`` :ref:`commutative-replicated-data-types`. These serializers are used for both persistent CRDT operations and CRDT snapshots.

Resolution of serializers when deserializing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When eventuate serializes application-defined events, :ref:`replication-filters` or snapshots it includes the ``identifier`` of the Akka serializer and the class or string based manifest_ when available. When deserializing these application-defined payloads a serializer is selected as follows:

- If a class-based manifest is included, the serializer that is configured in the Akka configuration for this class is selected
- In case of a string-based manifest or no manifest the serializer is selected by the included ``identifier``


.. [#] The ``customDestinationAggregateIds`` parameter is described in section :ref:`event-routing`.
.. [#] Writes from different event-sourced actors that have ``stateSync`` set to ``true`` are still batched, but not the writes from a single event-sourced actor.
.. [#] Event replay can optionally start from :ref:`snapshots` of actor state.
.. [#] Event-sourced processors can additionally route events between event logs.
.. [#] The routing destinations of a DurableEvent_ can be obtained with its ``destinationAggregateIds`` method.

.. _CQRS: http://martinfowler.com/bliki/CQRS.html
.. _stashed: http://doc.akka.io/docs/akka/2.4/scala/actors.html#stash
.. _watch: http://doc.akka.io/docs/akka/2.4/scala/actors.html#deathwatch-scala
.. _serialization extension: http://doc.akka.io/docs/akka/2.4/scala/serialization.html
.. _Serializer: http://doc.akka.io/api/akka/2.4/#akka.serialization.Serializer
.. _manifest: http://doc.akka.io/docs/akka/2.4/scala/serialization.html#Serializer_with_String_Manifest
.. _Protocol Buffers: https://developers.google.com/protocol-buffers/
.. _plausible clocks: https://github.com/RBMHTechnology/eventuate/issues/68
.. _Cassandra: http://cassandra.apache.org/
.. _circuit breaker: http://martinfowler.com/bliki/CircuitBreaker.html
.. _ticket 166: https://github.com/RBMHTechnology/eventuate/issues/166
.. _CASSANDRA-8825: https://issues.apache.org/jira/browse/CASSANDRA-8825

.. _ConfirmedDelivery: ../latest/api/index.html#com.rbmhtechnology.eventuate.ConfirmedDelivery
.. _DurableEvent: ../latest/api/index.html#com.rbmhtechnology.eventuate.DurableEvent
.. _DurableEventSerializer: ../latest/api/index.html#com.rbmhtechnology.eventuate.serializer.DurableEventSerializer
.. _EventsourcedActor: ../latest/api/index.html#com.rbmhtechnology.eventuate.EventsourcedActor
.. _EventsourcedView: ../latest/api/index.html#com.rbmhtechnology.eventuate.EventsourcedView
.. _EventsourcedWriter: ../latest/api/index.html#com.rbmhtechnology.eventuate.EventsourcedWriter
.. _EventsourcedProcessor: ../latest/api/index.html#com.rbmhtechnology.eventuate.EventsourcedProcessor
.. _StatefulProcessor: ../latest/api/index.html#com.rbmhtechnology.eventuate.StatefulProcessor
.. _ReplicationFilter: ../latest/api/index.html#com.rbmhtechnology.eventuate.ReplicationFilter
.. _Snapshot: ../latest/api/index.html#com.rbmhtechnology.eventuate.Snapshot
.. _DeliveryAttempt: ../latest/api/index.html#com.rbmhtechnology.eventuate.ConfirmedDelivery$$DeliveryAttempt
.. _PersistOnEvent: ../latest/api/index.html#com.rbmhtechnology.eventuate.PersistOnEvent
.. _BehaviorContext: ../latest/api/index.html#com.rbmhtechnology.eventuate.BehaviorContext
.. _EventLogUnavailableException: ../latest/api/index.html#com.rbmhtechnology.eventuate.log.EventLogUnavailableException
.. _CircuitBreaker: ../latest/api/index.html#com.rbmhtechnology.eventuate.log.CircuitBreaker
.. _EventLog: ../latest/api/index.html#com.rbmhtechnology.eventuate.log.EventLog
.. _CassandraEventLog: ../latest/api/index.html#com.rbmhtechnology.eventuate.log.cassandra.CassandraEventLog
