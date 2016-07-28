.. _event-log:

Event log
---------

.. _local-event-log:

Local event log
~~~~~~~~~~~~~~~

A local event log belongs to a given *location*\ [#]_ and a location can have one or more local event logs. Depending on the storage backend, a local event log may optionally be replicated within that location for stronger durability guarantees but this is rather an implementation details of the local event log. From Eventuate’s perspective, event replication occurs between different locations which is further described in section :ref:`replicated-event-log`.

To an application, an event log is represented by an event log actor. Producers and consumers interact with that actor to write events to and read events from the event log. They also register at the event log actor to be notified about newly written events. The messages that can be exchanged with an event log actor are defined in EventsourcingProtocol_ and ReplicationProtocol_.

At the moment, two storage backends are directly supported by Eventuate, `LevelDB`_ and `Cassandra`_. Usage of the corresponding event log actors is explained in :ref:`leveldb-storage-backend` and :ref:`cassandra-storage-backend`, respectively. The integration of custom storage backends into Eventuate is explained in :ref:`custom-storage-backends`.

.. _leveldb-storage-backend:

LevelDB storage backend
^^^^^^^^^^^^^^^^^^^^^^^

A local event log actor with a LevelDB storage backend writes events to the local file system. It can be created with:

.. includecode:: ../code/EventLogDoc.scala
   :snippet: local-log-leveldb

Applications must provide a unique ``logId`` for that log, the ``prefix`` is optional and defaults to ``log``. This will create a directory ``log-L1`` in which the LevelDB files are stored. The root directory of all local LevelDB directories can be configured with the ``eventuate.log.leveldb.dir`` configuration key in ``application.conf``:

.. includecode:: ../conf/common.conf
   :snippet: leveldb-root-dir

With this configuration, the absolute path of the LevelDB directory in the above example is ``/var/eventuate/log-L1``. If not configured, ``eventuate.log.leveldb.dir`` defaults to ``target``. Further ``eventuate.log.leveldb`` configuration options are given in section :ref:`configuration`.

.. _cassandra-storage-backend:

Cassandra storage backend
^^^^^^^^^^^^^^^^^^^^^^^^^

Eventuate also provides an event log actor implementation that writes events to a Cassandra cluster. That actor can be created with:

.. includecode:: ../code/EventLogDoc.scala
   :snippet: local-log-cassandra

With default configuration settings the event log actor connects to a Cassandra node at ``127.0.0.1:9042``. This can be customized with

.. includecode:: ../conf/common.conf
   :snippet: cassandra-contact-points

Ports are optional and default to ``9042`` according to

.. includecode:: ../conf/common.conf
   :snippet: cassandra-default-port

If Cassandra requires authentication, the default username used by Eventuate is ``cassandra``, the default password is ``cassandra`` (which corresponds to Cassandra's default superuser). This can be changed with

.. includecode:: ../conf/common.conf
   :snippet: cassandra-authentication

Further details are described in the API docs of the `Cassandra extension`_ and the CassandraEventLog_ actor. A complete reference of ``eventuate.log.cassandra`` configuration options is given in section :ref:`configuration`.

.. note::
   Eventuate requires Cassandra version 2.1 or higher.

.. hint::
   For instructions how to run a local Cassandra cluster you may want to read the article `Chaos testing with Docker and Cassandra on Mac OS X`_.

.. _custom-storage-backends:

Custom storage backends
^^^^^^^^^^^^^^^^^^^^^^^

A custom storage backend can be integrated into Eventuate by extending the abstract EventLog_ actor and implementing the EventLogSPI_ trait. For implementation examples, please take a look at LeveldbEventLog.scala_ and CassandraEventLog.scala_.

.. _replicated-event-log:

Replicated event log
~~~~~~~~~~~~~~~~~~~~

Local event logs from different locations can be connected for event replication. For example, when connecting a local event log ``L1`` at location ``1`` with a local event log ``L2`` at location ``2``, then the events written to ``L1`` are asynchronously replicated to location ``2`` and merged into to ``L2``. Also, events written to ``L2`` are asynchronously replicated to location ``1`` and merged into ``L1``. Merging preserves the causal ordering of events which is tracked with :ref:`vector-clocks`. Setting up a bi-directional replication connection between local event logs ``L1`` and ``L2`` yields a *replicated event log* ``L``::

    L1 ---- L2

Since events can be written concurrently at different locations, the total order of events in the local event logs at different locations is likely to differ. The causal order of events, however, is consistent across locations: if event ``e1`` causes event ``e2`` (i.e. ``e1`` happened before ``e2``) then the offset of ``e1`` is less than the offset of ``e2`` at all locations. The offset of an event in a local event log is its local sequence number. On the other hand, if ``e1`` and ``e2`` are written concurrently, their relative order in a local event log is not defined: the offset of ``e1`` can be less than that of ``e2`` at one location but greater than that of ``e2`` at another location.

A replicated event log can also be set up for more than two locations. Here event log ``L`` is replicated across locations ``1`` - ``6``::

    L1           L5
      \         /
       L2 --- L4
      /         \
    L3           L6

A location may also have several local event logs that can be replicated independently of each other. The following example shows three replicated events logs ``L``, ``M`` and ``N`` that are replicated across locations ``1`` and ``2``::

    L1 ---- L2
    M1 ---- M2
    N1 ---- N2

The distribution of ``L``, ``M`` and ``N`` across locations may also differ::

    L1 ---- L2
    M1 ---- M2 --- M3
            N2 --- N3

.. note::
   Event replication is reliable and can recover from network failures. It can also recover from crashes of source and target locations i.e. event replication automatically resumes when a crashed location recovers. Replicated events are also guaranteed to be written *exactly-once* to a target log. This is possible because replication progress metadata are stored along with replicated events in the target log. This allows a replication target to reliably detect and ignore duplicates. Event-sourced components can therefore rely on receiving a de-duplicated event stream.

.. _replication-endpoints:

Replication endpoints
^^^^^^^^^^^^^^^^^^^^^

Events are replicated over *replication connections* that are established between *replication endpoints*. A location may have one or more replication endpoints and a replication endpoint can manage one or more event logs. The following examples assume two locations ``1`` and ``2`` and two replicated event logs ``L`` and ``M``::

    L1 ---- L2
    M1 ---- M2

Each location has a ``ReplicationEndpoint`` that manages the local event logs. Replication endpoints communicate with each other via `Akka Remoting`_ which must be enabled by all locations in their ``application.conf``:

.. includecode:: ../conf/location-1.conf
   :snippet: remoting-conf

The network address of the replication endpoint at location ``1`` is:

.. includecode:: ../conf/location-1.conf
   :snippet: endpoint-address

At location ``2`` it is:

.. includecode:: ../conf/location-2.conf
   :snippet: endpoint-address

The ``ReplicationEndpoint`` at location ``1`` can be created programmatically with:

.. includecode:: ../code/EventLogDoc.scala
   :snippet: replication-endpoint-1

A ``ReplicationEndpoint`` must have a global unique ``id``. Here, the location identifier ``1`` is used to identify the replication endpoint. Furthermore, the ``logNames``\ [#]_ of the replicated event logs (``L`` and ``M``) and a ``logFactory`` for creating the local event log actors are passed as constructor arguments. Input parameter of the ``logFactory`` is a unique ``logId`` that is generated by the replication endpoint from a combination of the given ``logNames`` and the endpoint ``id``.

The last ``ReplicationEndpoint`` constructor parameter is a set of ``ReplicationConnection``\ s. Here, it is a single replication connection that connects to the remote replication endpoint at location ``2``. With this replication connection, events are replicated from location ``2`` to location ``1``. For starting event replication, a replication endpoint must be activated by calling the ``activate()`` method. For replicating events in the other direction, a corresponding ``ReplicationEndpoint`` and ``ReplicationConnection`` must be set up at location ``2``:

.. includecode:: ../code/EventLogDoc.scala
   :snippet: replication-endpoint-2

The event log actors that are created by a ``ReplicationEndpoint`` can be obtained from its ``logs`` map. Map keys are the event log names, map values the event log ``ActorRef``\ s:

.. includecode:: ../code/EventLogDoc.scala
   :snippet: logs-map-1

Optionally, an application may also set an ``applicationName`` and an ``applicationVersion`` for a replication endpoint:

.. includecode:: ../code/EventLogDoc.scala
   :snippet: replication-endpoint-3

If the ``applicationName``\ s of two replication endpoints are equal, events are only replicated from the source endpoint to the target endpoint if the ``applicationVersion`` of the target endpoint is greater than or equal to that of the source endpoint. This is a simple mechanism to support incremental version upgrades of replicated applications where each replica can be upgraded individually without shutting down other replicas. This avoids permanent state divergence during upgrade which may occur if events are replicated from replicas with higher version to those with lower version. A replication endpoint whose replication attempts have been rejected due to an incompatible application version logs warning such as::

    Event replication rejected by remote endpoint 3. 
    Target ApplicationVersion(1,2) not compatible with 
    source ApplicationVersion(1,3).


If the ``applicationName``\ s of two replication endpoints are not equal, events are always replicated, regardless of their ``applicationVersion`` value.

.. hint::
   Further ``ReplicationEndpoint`` creation options are described in the API documentation of the ReplicationEndpoint_ and ReplicationConnection_ companion objects. A complete reference of configuration options is given in section :ref:`configuration`.

.. note::
   :ref:`failure-detection` reports endpoints with incompatible application versions as unavailable.

.. _replication-filters:

Replication filters
^^^^^^^^^^^^^^^^^^^

By default, all events are replicated. Applications may provide ``ReplicationFilter``\ s to limit replication to a subset of events. A custom replication filter can be defined, by extending ReplicationFilter_ and implementing a filter predicate (method ``apply``). For example, the following replication filter accepts DurableEvent_\ s with a matching ``emitterAggregateId``:

.. includecode:: ../code/EventLogDoc.scala
   :snippet: replication-filter-definition

Replication filters can also be composed. The following ``composedFilter`` accepts events with a defined ``emitterAggregateId`` of value ``order-17`` or ``order-19``:

.. includecode:: ../code/EventLogDoc.scala
   :snippet: replication-filter-composition

For the definition of filter logic based on application-defined events, replication filters should use the ``payload`` field of ``DurableEvent``.

.. _local-replication-filters:

Local replication filters
.........................

Local replication filters can be defined per ``ReplicationEndpoint`` and remote target event log id or local event log name. They are applied locally at the endpoint at which they are defined. The following example configures a local replication filter for the log with name ``L``:

.. includecode:: ../code/EventLogDoc.scala
   :snippet: local-filters

Only events that pass ``filter1`` are replicated to remote logs. By specifying a target log id (``ReplicationEndpointInfo.logId(remoteEndpointId, logName)``) as key a filter is only applied for this specific target log. If there is also a filter defined for the log name the one for the target log id takes precedence.

.. hint::
   Local replication filters are especially useful to prevent location-specific (or location-private) events from being replicated to other locations.

.. _remote-replication-filters:

Remote replication filters
..........................

Remote replication filters can be defined per ``ReplicationConnection`` and event log name. They are transferred to a remote replication endpoint and applied there while reading from a *source event log* during replication. The following example configures a remote replication filter for log ``L``:

.. includecode:: ../code/EventLogDoc.scala
   :snippet: remote-filters

.. hint::
   Serialization of replication filters can be customized as described in section :ref:`replication-filter-serialization`.

.. _update-notifications:

Update notifications
^^^^^^^^^^^^^^^^^^^^

After having replicated a non-empty event batch, a replication endpoint immediately makes another replication attempt. On the other hand, if the replicated event batch is empty, the next replication attempt is delayed by a duration that can be configured with:

.. includecode:: ../conf/common.conf
   :snippet: retry-delay

Consequently, event replication latency has an upper bound that is determined by this parameter. To minimize event replication latency, replication endpoints by default send event log update notifications to each other. The corresponding configuration parameter is:

.. includecode:: ../conf/common.conf
   :snippet: update-notifications

The impact of sending update notifications on average event replication latency, however, decreases with increasing event write load. Applications under high event write load may even experience increased event replication throughput if update notifications are turned ``off``.

.. _failure-detection:

Failure detection
^^^^^^^^^^^^^^^^^

Replication endpoints can notify applications about availability and unavailability of remote event logs. They can become unavailable during a network partition, during a downtime of their hosting application or because of incompatible ``applicationVersion``\ s of their replication endpoints, for example. A local replication endpoint publishes

- ``Available(endpointId: String, logName: String)`` messages to the local ``ActorSystem``\ s `event stream`_ if the remote replication endpoint is available, and
- ``Unavailable(endpointId: String, logName: String, causes: Seq[Throwable])`` messages to the local ``ActorSystem``\ s `event stream`_ if the remote replication endpoint is unavailable

Both messages are defined in ReplicationEndpoint_. Their ``endpointId`` parameter is the id of the remote replication endpoint, the ``logName`` parameter is the name of an event log that is managed by the remote endpoint. 

``Unavailable`` messages also contain all ``causes`` of an unavailability. Only those causes that occurred since last publication of a previous ``Available`` or ``Unavailable`` message with the same ``endpointId`` and ``logName`` are included. Causes are one or more instances of:

- ReplicationReadSourceException_ if reading from the storage backend of a remote endpoint failed,
- ReplicationReadTimeoutException_ if a remote endpoint doesn’t respond or
- IncompatibleApplicationVersionException_ if the application version of a remote endpoint is not compatible with that of a local endpoint (see :ref:`replication-endpoints`).

A failure detection limit can be configured with:

.. includecode:: ../conf/common.conf
   :snippet: failure-detection-limit

It instructs the failure detector to publish an ``Unavailable`` message if there is no successful reply from a remote replication endpoint within 60 seconds. ``Available`` and ``Unavailable`` messages are published periodically at intervals of ``eventuate.log.replication.failure-detection-limit``.

.. _disaster-recovery:

Disaster recovery
^^^^^^^^^^^^^^^^^

Total or partial event loss at a given location is classified as disaster. Event loss can be usually prevented by using a clustered :ref:`cassandra-storage-backend` (at each location) but a catastrophic failure may still lead to event loss. In this case, lost events can be recovered from other locations, optionally starting from an existing storage backup, a procedure called *disaster recovery*. 

If a storage backup exists, events can be partially recovered from that backup so that only events not covered by the backup must be copied from other locations. Recovery of events at a given location is only possible to the extend they have been previously replicated to other locations (or written to the backup). Events that have not been replicated to other locations or for which no storage backup exists cannot be recovered. 

Disaster recovery is executed per ``ReplicationEndpoint`` by calling its asynchronous ``recover()`` method. During recovery the endpoint is activated to replicate lost events. Only after recovery successfully completed the application may start their event-sourced components, otherwise recovery must be re-tried.

.. includecode:: ../code/EventLogDoc.scala
   :snippet: disaster-recovery-1

During execution of disaster recovery, directly connected endpoints must be available. These are endpoints for which replication connections have been configured at the endpoint to be recovered. Availability is needed because connected endpoints need to update internal metadata before they can resume event replication with the recovered endpoint.

.. hint::
   In theory a ``ReplicationEndpoint`` could always be activated at the beginning by calling its ``recover()`` method. However, as this requires that all directly connected endpoints must be available, it is not recommended.

Disaster recovery also deletes invalid snapshots, in case they survived the disaster. Invalid snapshots are those that cover lost events.

A complete reference of ``eventuate.log.recovery.*`` configuration options is given in section :ref:`configuration`. The example application also implements :ref:`example-disaster-recovery`.

.. note::
   Installing a storage backup is a separate administrative task that is not covered by running ``recover()``.

Deleting events
~~~~~~~~~~~~~~~

As outlined in the :ref:`overview`, an event log is a continuously growing store of immutable facts. Depending on the implementation of the application, not all events are necessarily needed to recover application state after an application or actor restart. For example, if the application saves snapshots, only those events that occurred after the snapshot need to be available. But even without snapshots there can be application-specific boundary conditions that allow an application to recover its state from a certain sequence number on. To keep a store from growing indefinitely in these cases a ``ReplicationEndpoint`` allows the deletion events up to a given sequence number from a local log. Deletion of events actually differentiates between:

- Logical deletion of events: Events that are logically deleted are not replayed in case of an actor restart. However they are still available for replication to event logs of connected ``ReplicationEndpoint``\ s. All storage backends support logical deletion of events.
- Physical deletion of events: Depending on the storage backend logically deleted events are eventually physically deleted. Physical deleted events are of course not available any more for local replay or replication. Physical deletion is currently only supported by the LevelDB backend.

While a location can very well decide if it needs certain events from a local event log to recover its state, it may be much less clear if these events might be needed in the future for replication to other locations.
Eventuate can defer physical deletion of events until they are replicated to known ``ReplicationEndpoint``\ s. In case a newly added location wants to catch up with the application's full event history, it has to connect to a location that actually has the full event history.

Considering this, deletion is invoked through the ``delete`` method of a ``ReplicationEndpoint``:

.. includecode:: ../code/EventLogDoc.scala
   :snippet: event-deletion-1

This method returns the sequence number up to which events are logically deleted. The returned sequence number can differ from the requested one (here ``100L``), if

- the log's current sequence number is smaller than the requested number. In this case the current sequence number is returned.
- there was a previous successful deletion request with a higher sequence number. In this case that sequence number is returned.

Depending on the storage backend, this call also triggers physical deletion of events in a reliable background process that survives event log restarts. To defer physical deletion of not yet replicated events, the third parameter takes a set of ``ReplicationEndpoint`` ids. Events are not physically deleted until they are replicated to these endpoints. If the set is empty, asynchronous deletion is triggered immediately.

.. _Cassandra: http://cassandra.apache.org/
.. _Getting Started: https://wiki.apache.org/cassandra/GettingStarted
.. _cassandra-unit: https://github.com/jsevellec/cassandra-unit/wiki
.. _LevelDB: https://github.com/google/leveldb
.. _Akka Remoting: http://doc.akka.io/docs/akka/2.4.4/scala/remoting.html
.. _event stream: http://doc.akka.io/docs/akka/2.4.4/scala/event-bus.html#event-stream
.. _Chaos testing with Docker and Cassandra on Mac OS X: http://rbmhtechnology.github.io/chaos-testing-with-docker-and-cassandra/

.. _EventsourcingProtocol: ../latest/api/index.html#com.rbmhtechnology.eventuate.EventsourcingProtocol$
.. _ReplicationProtocol: ../latest/api/index.html#com.rbmhtechnology.eventuate.ReplicationProtocol$
.. _ReplicationEndpoint: ../latest/api/index.html#com.rbmhtechnology.eventuate.ReplicationEndpoint$
.. _ReplicationConnection: ../latest/api/index.html#com.rbmhtechnology.eventuate.ReplicationConnection$
.. _ReplicationFilter: ../latest/api/index.html#com.rbmhtechnology.eventuate.ReplicationFilter
.. _DurableEvent: ../latest/api/index.html#com.rbmhtechnology.eventuate.DurableEvent
.. _Cassandra extension: ../latest/api/index.html#com.rbmhtechnology.eventuate.log.cassandra.Cassandra
.. _CassandraEventLog: ../latest/api/index.html#com.rbmhtechnology.eventuate.log.cassandra.CassandraEventLog
.. _EventLogSPI: ../latest/api/index.html#com.rbmhtechnology.eventuate.log.EventLogSPI
.. _EventLog: ../latest/api/index.html#com.rbmhtechnology.eventuate.log.EventLog
.. _ReplicationReadSourceException: ../latest/api/index.html#com.rbmhtechnology.eventuate.ReplicationProtocol$$ReplicationReadSourceException
.. _ReplicationReadTimeoutException: ../latest/api/index.html#com.rbmhtechnology.eventuate.ReplicationProtocol$$ReplicationReadTimeoutException
.. _IncompatibleApplicationVersionException: ../latest/api/index.html#com.rbmhtechnology.eventuate.ReplicationProtocol$$IncompatibleApplicationVersionException

.. _CassandraEventLog.scala: https://github.com/RBMHTechnology/eventuate/tree/master/src/main/scala/com/rbmhtechnology/eventuate/log/cassandra/CassandraEventLog.scala
.. _LeveldbEventLog.scala: https://github.com/RBMHTechnology/eventuate/tree/master/src/main/scala/com/rbmhtechnology/eventuate/log/leveldb/LeveldbEventLog.scala

.. [#] A location can be a whole data center, a node within a data center or even a process on a single node, for example.
.. [#] Log names must be unique per replication endpoint. Replication connections are only established between logs of the same name.
