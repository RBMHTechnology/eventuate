Event-sourced actors
--------------------

An introduction to event-sourced actors is already given in section :ref:`architecture` and the :ref:`user-guide`. Applications use event-sourced actors for writing events to an event log and for maintaining state on the command side (C) of a CQRS_ architecture. Event-sourced actors distinguish command processing from event processing. They must extend the EventsourcedActor_ trait and implement a :ref:`command-handler` and an :ref:`event-handler`.

.. _command-handler:

Command handler
~~~~~~~~~~~~~~~

A command handler is partial function of type ``PartialFunction[Any, Unit]`` for which a type alias ``Receive`` exists. It can be defined by implementing ``onCommand``:

.. includecode:: ../code/EventSourcingDoc.scala
   :snippet: command-handler

Messages sent by an application to an event-sourced actor are received by its command handler. Usually, a command handler first validates a command, then derives one or more events from it, persists these events with ``persist`` and if persistence succeeds, calls the event handler\ [#]_ and replies to the command sender. The ``persist`` method has the following signature\ [#]_:

.. includecode:: ../code/EventSourcingDoc.scala
   :snippet: persist-signature

The ``persist`` method can be called one ore more times per received command. Calling ``persist`` does not immediately write events to the event log. Instead, events from ``persist`` calls are collected in memory and written to the event log when ``onCommand`` returns. 

Events are written asynchronously to the event-sourced actor’s ``eventLog``. After writing, the ``eventLog`` actor internally replies to the event-sourced actor with a success or failure message which is passed as argument to the persist ``handler``. 

The persist handler is called during a normal actor message dispatch. It can therefore safely access an actor’s internal state. The ``sender()`` reference of the original command sender is also preserved, so that a persist handler can reply to the initial command sender.

.. hint::
   The ``EventsourcedActor`` trait also defines a ``persistN`` method. Refer to the EventsourcedActor_ API documentation for details.

.. note::
   A command handler should not modify persistent actor state i.e. state that is derived from events. 

Handling persistence failures
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Persistence may fail for several reasons. For example, event serialization or writing to the storage backend may fail, to mention only two examples. In general, a persist handler that is called with a ``Failure`` argument should not make any assumptions whether the corresponding event has been written or not. For example, an event could have been actually written to the storage backend but the ACK was lost which causes ``persist`` to complete with a failure.

One way to deal with this situation is to restart the event-sourced actor and inspect the recovered state whether that event has been processed or not. If it has been processed, the application can continue with the next command, otherwise it should re-send the failed command. This strategy avoids duplicates in the event log.

Duplicates are not an issue if state update operations executed by an event handler are idempotent. In this case, an application may simply re-send a failed command without restarting the event-sourced actor. 

State synchronization
~~~~~~~~~~~~~~~~~~~~~

As explained in section :ref:`command-handler`, events are persisted asynchronously. What happens if another command is sent to an event-sourced actor while persistence is in progress? This depends on the value of ``stateSync``, a member of ``EventsourcedActor`` that can be overridden.

.. includecode:: ../../main/scala/com/rbmhtechnology/eventuate/EventsourcedActor.scala
   :snippet: state-sync

If ``stateSync`` is ``true`` (default), new commands are stashed_ while persistence is in progress. Consequently, new commands see actor state that is *in sync* with the events in the event log. A consequence is limited write throughput, because :ref:`batching` of write requests is not possible in this case\ [#]_. This setting is recommended for event-sourced actors that must validate commands against current state.

If ``stateSync`` is ``false``, new commands are dispatched to ``onCommand`` immediately. Consequently, new commands may see stale actor state. The advantage is significantly higher write throughput as :ref:`batching` of write requests is possible. This setting is recommended for event-sourced actors that don’t need to validate commands against current state.

If a sender sends several update commands followed by a read command to an event-sourced actor that has ``stateSync`` set to ``false``, the read command will probably not see the state change from the preceding update commands. To achieve read-your-write consistency, the command sender should wait for a reply from the last update command before sending the read command. The reply must of course be sent from within a ``persist`` handler.

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
   An event handler should only update internal actor state without having further side-effects. An exception is :ref:`reliable-delivery` of messages.

Causality tracking
~~~~~~~~~~~~~~~~~~

As described in section :ref:`vector-clocks`, Eventuate’s causality tracking default can be formalized in `plausible clocks`_. To achieve more fine-grained causality tracking, event-sourced actors can reserve their own entry in a vector clock. To reserve its own entry, a concrete ``EventsourcedActor`` must override the ``sharedClockEntry`` method to return ``false``.

.. includecode:: ../code/EventSourcingDoc.scala
   :snippet: clock-entry-class

The value of ``sharedClockEntry`` may also be instance-specific, if required.

.. includecode:: ../code/EventSourcingDoc.scala
   :snippet: clock-entry-instance

Event-sourced views
-------------------

An introduction to event-sourced views is already given in section :ref:`architecture` and the :ref:`user-guide`. Applications use event-sourced views for consuming events from an event log and for maintaining state on the query side (Q) of a CQRS_ architecture. 

Like event-sourced actors, event-sourced views distinguish command processing from event processing. They must implement the EventsourcedView_ trait. ``EventsourcedView`` is a functional subset of ``EventsourcedActor`` that cannot ``persist`` events.

State recovery
--------------

When an event-sourced actor or view is started or re-started, events are replayed to its ``onEvent`` handler so that internal state can be recovered\ [#]_. Event replay is initiated internally by sending a ``Replay`` message to the ``eventLog`` actor:

.. includecode:: ../../main/scala/com/rbmhtechnology/eventuate/EventsourcedView.scala
   :snippet: replay

The ``replay`` method is defined by EventsourcedView_ and automatically called when an ``EventsourcedView`` or ``EventsourcedActor`` is started or re-started.

Sending a ``Replay`` message automatically registers the sending actor at its event log, so that newly written events can be immediately routed to that actor. If the actor is stopped it is automatically de-registered.

While an event-sourced actor or view is recovering i.e. replaying messages, its ``recovering`` method returns ``true``. If recovery successfully completes, its empty ``onRecovered()`` method is called which can be overridden by applications.

During recovery, new commands are stashed_ and dispatched to ``onCommand`` after recovery successfully completed. This ensures that new commands never see partially recovered state.

.. _snapshots:

Snapshots
---------

Recovery times increase with the number of events that are replayed to event-sourced actors or views. They can be decreased by starting event replay from a previously saved snapshot of internal state rather than replaying events from scratch. Event-sourced actors and views can save snapshots by calling ``save`` within their command handler:

.. includecode:: ../code/EventSourcingDoc.scala
   :snippet: snapshot-save

Snapshots are saved asynchronously. On completion, a user-defined handler of type ``Try[SnapshotMetadata] => Unit`` is called. Like a ``persist`` handler, a ``save`` handler may also close over actor state and can reply to the command sender using the ``sender()`` reference. 

An event-sourced actor that is :ref:`tracking-conflicting-versions` of application state can also save ``ConcurrentVersions[A, B]`` instances directly. One can even configure custom serializers for type parameter ``A`` as explained in section :ref:`snapshot-serialization`.

During recovery, the latest snapshot saved by an event-sourced actor or view is loaded and can be handled with the ``onSnapshot`` handler. This handler should initialize internal actor state from the loaded snapshot: 

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

in ``application.conf``. The maximum number of stored snapshots per event-sourced actor or view can be configured with

.. includecode:: ../conf/snapshot.conf
   :snippet: snapshot-num

If this number is exceeded, older snapshots are automatically deleted.

.. _event-routing:


Event routing
-------------

An event that is emitted by an event-sourced actor can be routed to other event-sourced actors and views if they share an :ref:`event-log`\ [#]_ . The default event routing rules are:

- If an event-sourced actor or view has an undefined ``aggregateId``, all events are routed to it. It may choose to handle only a subset of them though.
- If an event-sourced actor or view has a defined ``aggregateId``, only events emitted by event-sourced actors with the same ``aggregateId`` are routed to it.

Routing destinations are defined during emission of an event and are persisted together with the event\ [#]_. This makes routing decisions repeatable during event replay and allows for routing rule changes without affecting past routing decisions. Applications can define additional routing destinations with the ``customDestinationAggregateIds`` parameter of ``persist``:

.. includecode:: ../code/EventRoutingDoc.scala
   :snippet: custom-routing

Here, ``ExampleEvent`` is routed to destinations with ``aggregateId``\ s ``Some(“a2”)`` and ``Some(“a3”)`` in addition to the default routing destinations with ``aggregateId``\s ``Some(“a1”)`` and ``None``.

.. _reliable-delivery:

Reliable delivery
-----------------

Reliable, event-based remote communication between event-sourced actors should be done via a :ref:`replicated-event-log`. For reliable communication with other services that cannot connect to a replicated event log, event-sourced actors should use the ConfirmedDelivery_ trait:

.. includecode:: ../code/ReliableDeliveryDoc.scala
   :snippet: reliable-delivery

``ConfirmedDelivery`` supports the reliable delivery of messages to destinations by enabling applications to re-deliver messages until delivery is confirmed by destinations. In the example above, the reliable delivery of a message is initiated by sending a ``DeliverCommand`` to ``ExampleActor``. 

The generated ``DeliverEvent`` calls ``deliver`` to deliver a ``ReliableMessage`` to ``destination``. The ``deliveryId`` is the correlation identifier for the delivery ``Confirmation``. The ``deliveryId`` can be any application-defined id. Here, the event’s sequence number is used which can be obtained with ``lastSequenceNumber``. 

The destination confirms the delivery of the message by sending a ``Confirmation`` reply to the event-sourced actor from which the actor generates a ``ConfirmationEvent``. When handling the event, message delivery can be confirmed by calling ``confirm`` with the ``deliveryId`` as argument.

When the actor is re-started, unconfirmed ``ReliableMessage``\ s are automatically re-delivered to their ``destination``\ s. The example actor additionally schedules ``redeliverUnconfirmed`` calls to periodically re-deliver unconfirmed messages. This is done within the actor’s command handler.

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

.. [#] An explicit ``onEvent`` call may become obsolete in future releases.
.. [#] The ``customDestinationAggregateIds`` parameter is described in section :ref:`event-routing`.
.. [#] Writes from different event-sourced actors that have ``stateSync`` set to ``true`` are still batched, but not the writes from a single event-sourced actor.
.. [#] Event replay can optionally start from :ref:`snapshots` of actor state.
.. [#] :ref:`processors` can additionally route events between event logs.
.. [#] The routing destinations of a DurableEvent_ can be obtained with its ``destinationAggregateIds`` method.

.. _CQRS: http://martinfowler.com/bliki/CQRS.html
.. _stashed: http://doc.akka.io/docs/akka/2.3.9/scala/actors.html#stash
.. _serialization extension: http://doc.akka.io/docs/akka/2.3.9/scala/serialization.html
.. _Serializer: http://doc.akka.io/api/akka/2.3.9/#akka.serialization.Serializer
.. _Protocol Buffers: https://developers.google.com/protocol-buffers/
.. _plausible clocks: https://github.com/RBMHTechnology/eventuate/issues/68

.. _ConfirmedDelivery: ../latest/api/index.html#com.rbmhtechnology.eventuate.ConfirmedDelivery
.. _DurableEvent: ../latest/api/index.html#com.rbmhtechnology.eventuate.DurableEvent
.. _DurableEventSerializer: ../latest/api/index.html#com.rbmhtechnology.eventuate.serializer.DurableEventSerializer
.. _EventsourcedActor: ../latest/api/index.html#com.rbmhtechnology.eventuate.EventsourcedActor
.. _EventsourcedView: ../latest/api/index.html#com.rbmhtechnology.eventuate.EventsourcedView
.. _ReplicationFilter: ../latest/api/index.html#com.rbmhtechnology.eventuate.ReplicationFilter
