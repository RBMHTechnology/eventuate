Event-sourced actors
--------------------

An introduction to event-sourced actors is already given in :ref:`architecture` and the :ref:`user-guide`. Applications use event-sourced actors for writing events to an event log and for maintaining state on the command side (C) of CQRS_ based applications. Event-sourced actors distinguish command processing from event processing. They must extend the EventsourcedActor_ trait and implement a command handler and an event handler.

.. _command-handler:

Command handler
~~~~~~~~~~~~~~~

A command handler is partial function of type ``PartialFunction[Any, Unit]`` for which a type alias ``Receive`` exists. It can be defined by implementing ``onCommand``:

.. includecode:: ../code/EventSourcingDoc.scala
   :snippet: command-handler

Messages sent by an application to an event-sourced actor are received by its command handler. Usually, a command handler first validates a command, then derives one or more events from it, persists these events with ``persist`` and if persistence succeeds, handles the events\ [#]_ and replies to the command sender. The ``persist`` method has the following signature\ [#]_:

.. includecode:: ../code/EventSourcingDoc.scala
   :snippet: persist-signature

The ``persist`` method can be called one ore more times per received command. Calling ``persist`` does not immediately write events to the event log. Instead, events from ``persist`` calls are collected in memory and written to the event log when ``onCommand`` returns. 

Events are written asynchronously to the event-sourced actor’s ``eventLog``. After writing, the ``eventLog`` actor internally replies to the event-sourced actor with a success or failure message which is passed as argument to the persist ``handler``. 

The persist handler is called during a normal actor message dispatch. It can therefore safely access an actor’s internal state. The ``sender()`` reference of the original command sender is also preserved, so that a persist handler can reply to the initial command sender.

.. hint::
   The ``EventsourcedActor`` trait also defines methods ``persistN`` and ``persistWithLocalTime``. Refer to the EventsourcedActor_ API documentation for details.

.. note::
   A command handler should not modify persistent actor state i.e. state that is derived from events. 

State synchronization
~~~~~~~~~~~~~~~~~~~~~

As explained in section :ref:`command-handler`, events are persisted asynchronously. What happens if a another command is sent to an event-sourced actor while persistence is in progress? This depends on the value of ``stateSync``, a member of ``EventsourcedActor`` that can be overridden.

.. includecode:: ../../main/scala/com/rbmhtechnology/eventuate/EventsourcedActor.scala
   :snippet: state-sync

If ``stateSync`` is ``true`` (default), new commands are stashed_ while persistence is in progress. Consequently, new commands see actor state that is *in sync* with the events in the event log. The consequence is limited write throughput, because :ref:`batching` of write requests is not possible in this case\ [#]_. This setting is recommended for event-sourced actors that must validate commands against current state.

If ``stateSync`` is ``false``, new commands are dispatched to ``onCommand`` immediately. Consequently, new commands may see stale actor state. The advantage is significantly higher write throughput as :ref:`batching` of write requests is possible. This setting is recommended for event-sourced actors that don’t need to validate commands against current state.

If a sender sends an update command followed by a read command to an event-sourced actor that has ``stateSync`` set to ``false``, the read command will probably not see the state change from the preceding update command. To achieve read-your-write consistency, there are two options:

#. The command sender waits for a reply to the update command before sending the read command. The reply must of course be sent from within a persist handler. From a throughput perspective, this is essentially the same as setting ``stateSync`` to ``true`` but allows for achieving read-your-write consistency for individual commands rather than all commands.

#. The command sender sends the read command immediately after the update command and the event-sourced actor calls ``delay`` with the read command as argument. The ``delay`` method has the following signature:

   .. includecode:: ../code/EventSourcingDoc.scala
      :snippet: delay-signature

   It delays processing of a command to that point in the future where all previously ``persist``\ ed events have been handled. The delayed command is passed as argument to the delay ``handler``\ [#]_. A delay handler must not call ``persist`` or related methods.

.. hint::
   For details about the ``delay`` method, refer to the EventsourcedActor_ API documentation. 

.. note::
   State synchronization settings only apply to a single actor instance. Events that are emitted concurrently by other actors and handled by that instance can arrive at any time and modify actor state. Anyway, concurrent events are not relevant for achieving read-your-write consistency and should be handled as described in the :ref:`user-guide`.

Event handler
~~~~~~~~~~~~~

An event handler is partial function of type ``PartialFunction[Any, Unit]`` for which a type alias ``Receive`` exists. It can be defined by implementing ``onEvent``. An event handler handles persisted events by updating actor state from event details. 

.. includecode:: ../code/EventSourcingDoc.scala
   :snippet: event-handler

Handled events also update an event-sourced actor’s vector clock. Events that are routed to an actor but not handled do not update its vector clock (see also section :ref:`event-routing`). Events emitted by an actor, *after* it handled an event, causally depend on that event.

Event metadata of the last handled event can be obtained with the ``last*`` methods defined by ``EventsourcedActor``. For example, ``lastSequenceNr`` returns the event’s local sequence number, ``lastVectorTimestamp`` returns the event’s vector timestamp. A complete reference is given by the EventsourcedActor_ API documentation.

.. note::
   An event handler should not have side-effects that are visible outside its actor. An exception is :ref:`reliable-delivery` of messages.

Event-sourced views
-------------------

An introduction to event-sourced views is already given in :ref:`architecture` and the :ref:`user-guide`. Applications use event-sourced views for consuming events from an event log and for maintaining state on the query side (Q) of CQRS_ based applications. 

Like event-sourced actors, event-sourced views distinguish command processing from event processing. They must implement the EventsourcedView_ trait. ``EventsourcedView`` is a functional subset of ``EventsourcedActor`` that can neither ``persist`` events nor ``delay`` commands. Furthermore, views don’t need to define a ``replicaId``.

State recovery
--------------

When an event-sourced actor or view is started or re-started, events are replayed to its ``onEvent`` handler so that internal state can be recovered\ [#]_. Event replay is initiated by sending a ``Replay`` message to the ``eventLog`` actor:

.. includecode:: ../../main/scala/com/rbmhtechnology/eventuate/Eventsourced.scala
   :snippet: replay

The ``replay`` method is defined by the Eventsourced_ trait and called internally by ``EventsourcedActor`` and ``EventsourcedView``. Both, ``EventsourcedActor`` and ``EventsourcedView``, extend ``Eventsourced``.

Sending a ``Replay`` message automatically registers the sending actor at its event log, so that newly written events can be immediately routed to that actor. If the actor is stopped it is automatically de-registered.

While an event-sourced actor or view is recovering i.e. replaying messages, its ``recovering`` method returns ``true``. If recovery successfully completes, its empty ``recovered()`` method is called which can be overridden by applications. 

During recovery, new commands are stashed_ and dispatched to ``onCommand`` after recovery successfully completed. This ensures that new commands never see partially recovered state.

.. _event-routing:

Event routing
-------------

An event that is emitted by an event-sourced actor can be routed to other event-sourced actors and views if they share an :ref:`event-log`\ [#]_ . The default event routing rules are:

- If an event-sourced actor or view has an undefined ``aggregateId``, all events are routed to it. It may choose to handle only a subset of them though.
- If an event-sourced actor or view has a defined ``aggregateId``, only events emitted by event-sourced actors with the same ``aggregateId`` are routed to it.

Routing destinations are defined during emission of an event and are persisted together with the event\ [#]_. This makes routing decisions repeatable during event replay and allows for routing rule changes without affecting past routing decisions. Applications can define additional routing destinations with the ``customRoutingDestinations`` parameter of ``persist``:

.. includecode:: ../code/EventRoutingDoc.scala
   :snippet: custom-routing

Here, ``ExampleEvent`` will routed to destinations with ``aggregateId``\ s ``Some(“a2”)`` and ``Some(“a3”)`` in addition to the default routing destination with ``aggregateId``\s ``Some(“a1”)`` and ``None``.

.. _reliable-delivery:

Reliable delivery
-----------------

Reliable, event-based remote communication between event-sourced actors and/or views should be done via a :ref:`replicated-event-log`. For reliable communication with other services that cannot connect to a replicated event log, event-sourced actors should use the ConfirmedDelivery_ trait:

.. includecode:: ../code/ReliableDeliveryDoc.scala
   :snippet: reliable-delivery

``ConfirmedDelivery`` supports the reliable delivery of messages to destinations by enabling applications to re-deliver messages until they are confirmed by their destinations. In this example, the reliable delivery of a message is initiated by sending a ``DeliverCommand`` to ``ExampleActor``. 

The generated ``DeliverEvent`` calls ``deliver`` to deliver a ``ReliableMessage`` to a ``destination``. The ``deliveryId`` is the correlation identifier for the delivery ``Confirmation``. The ``deliveryId`` can be any application-defined id. Here, the event’s sequence number is used which can be obtained with ``lastSequenceNumber``. 

The destination confirms the delivery of the message by sending a ``Confirmation`` reply to the event-sourced actor from which the actor generates a ``ConfirmationEvent``. When handling the event, message delivery can be confirmed by calling ``confirm`` with the ``deliveryId`` as argument.

When the actor is re-started, unconfirmed ``ReliableMessage``\ s are automatically re-delivered to their ``destination``\ s. The example actor additionally schedules ``redeliverUnconfirmed`` calls to periodically re-deliver unconfirmed messages. This is done within the actor’s command handler.

.. _snapshots:

Snapshots
---------

Snapshots of internal state can be taken from event-sourced actors and views. Snapshotting is an optimization to reduce recovery times. It is not implemented yet but coming soon.

Custom serialization
--------------------

Custom event serialization
~~~~~~~~~~~~~~~~~~~~~~~~~~

Custom serializers for application-defined events can be configured with Akka's `serialization extension`_ mechanism. For example, an application that wants to use a custom ``MyDomainEventSerializer`` for events of type ``MyDomainEvent`` (both defined in package ``com.example``) should add the following configuration to ``application.conf``:

.. includecode:: ../conf/serializer.conf
   :snippet: custom-event-serializer

``MyDomainEventSerializer`` must extend Akka’s Serializer_ trait. Please refer to Akka’s `serialization extension`_ documentation for further details.

Eventuate stores application-defined events as ``payload`` of DurableEvent_\ s. ``DurableEvent`` itself is serialized with DurableEventSerializer_, a `Protocol Buffers`_ serializer that delegates ``payload`` serialization to a custom serializer. If no custom serializer is configured, one of Akka’s default serializers is used.

.. _replication-filter-serialization:

Custom replication filter serialization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

in the same way as application-defined events, custom serializers for :ref:`replication-filters` can also be configured via Akka's `serialization extension`_ mechanism. For example, an application that wants to use a custom ``MyReplicationFilterSerializer`` for replication filters of type ``MyReplicationFilter`` (both defined in package ``com.example``) should add the following configuration to ``application.conf``:

.. includecode:: ../conf/serializer.conf
   :snippet: custom-filter-serializer

Custom replication filter serialization also works if the custom filter is part of a composite filter that has been created with ``and`` or ``or`` combinators (see ReplicationFilter_ API). If no custom filter serializer is configured, one of Akka’s default serializers is used.

.. [#] An explicit ``onEvent`` call may become obsolete in future releases.
.. [#] The ``customRoutingDestinations`` parameter is described in section :ref:`event-routing`.
.. [#] Writes from different event-sourced actors that have ``stateSync`` set to ``true`` are still batched, but not the writes from a single event-sourced actor.
.. [#] This mechanism of delaying commands might the replaced with something that is closer related to :ref:`conditional-commands` in future releases.
.. [#] Event replay can optionally start from :ref:`snapshots` of actor state.
.. [#] :ref:`processors` can additionally route events between event logs.
.. [#] The routing destinations of a DurableEvent_ can be obtained with method ``routingDestinations``.

.. _CQRS: http://martinfowler.com/bliki/CQRS.html
.. _stashed: http://doc.akka.io/docs/akka/2.3.9/scala/actors.html#stash
.. _serialization extension: http://doc.akka.io/docs/akka/2.3.9/scala/serialization.html
.. _Serializer: http://doc.akka.io/api/akka/2.3.9/#akka.serialization.Serializer
.. _Protocol Buffers: https://developers.google.com/protocol-buffers/

.. _ConfirmedDelivery: ../latest/api/index.html#com.rbmhtechnology.eventuate.ConfirmedDelivery
.. _DurableEvent: ../latest/api/index.html#com.rbmhtechnology.eventuate.DurableEvent
.. _DurableEventSerializer: ../latest/api/index.html#com.rbmhtechnology.eventuate.DurableEventSerializer
.. _Eventsourced: ../latest/api/index.html#com.rbmhtechnology.eventuate.Eventsourced
.. _EventsourcedActor: ../latest/api/index.html#com.rbmhtechnology.eventuate.EventsourcedActor
.. _EventsourcedView: ../latest/api/index.html#com.rbmhtechnology.eventuate.EventsourcedView
.. _ReplicationFilter: ../latest/api/index.html#com.rbmhtechnology.eventuate.ReplicationFilter
