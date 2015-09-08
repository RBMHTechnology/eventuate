.. _event-log:

Event log
---------

.. _local-event-log:

Local event log
~~~~~~~~~~~~~~~

A local event log belongs to a given *location*\ [#]_ and a location can have one or more local event logs. Depending on the storage backend, a local event log may optionally be replicated within that location for stronger durability guarantees but this is rather an implementation details of the local event log. From Eventuate’s perspective, event replication occurs between different locations which is further described in section :ref:`replicated-event-log`.

To an application, an event log is represented by an event log actor. Producers and consumers interact with that actor to write events to and read events from the event log. They also register at the event log actor to be notified about newly written events. The messages that can be exchanged with an event log actor are defined in EventsourcingProtocol_.

At the moment, two storage backends are supported by Eventuate, `LevelDB`_ and `Cassandra`_. The corresponding event log actors are provided by Eventuate and their usage is explained in the following sections.

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

.. _replicated-event-log:

Replicated event log
~~~~~~~~~~~~~~~~~~~~

Local event logs from different locations can be connected for event replication. For example, when connecting a local event log ``L1`` at location ``1`` with a local event log ``L2`` at location ``2``, then the events written to ``L1`` are asynchronously replicated to location ``2`` and merged into to ``L2``. Also, events written to ``L2`` are asynchronously replicated to location ``1`` and merged into ``L1``. Merging preserves the causal ordering of events which is tracked with :ref:`vector-clocks`. Setting up a bi-directional replication connection between local event logs ``L1`` and ``L2`` yields a *replicated event log* ``L``::

    L1 ---- L2

Since events can be written concurrently at different locations, the total order of events in the local event logs at different locations is likely to differ. The causal order of events, however, is consistent across locations: if event ``e1`` causes event ``e2`` (i.e. ``e1`` happened before ``e2``) then the offset of ``e1`` is less than the offset of ``e2`` at all locations. The offset of an event in a local event log is its local sequence number. On the other hand, if ``e1`` and ``e2`` are written concurrently, their relative order in a local event log is not defined: the offset of ``e1`` can be less than that of ``e2`` at one location but greater than that of ``e2`` at another location.

A replicated event log can also be set up for more than two locations (see also current :ref:`current-limitations`). Here event log ``L`` is replicated across locations ``1`` - ``6``::

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
   Event replication is reliable and can recover from network failures. It can also recover from crashes of source and target locations i.e. event replication automatically resumes when a crashed location recovers. Replicated events are also guaranteed to be written *exactly-once* to a target log. This is possible because replication progress metadata are stored along with replicated events in the target log. This allows a replication target to reliably detect and ignore duplicates. Event-sourced actors and views can therefore rely on receiving a de-duplicated event stream. 

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

The last ``ReplicationEndpoint`` constructor parameter is a set of ``ReplicationConnection``\ s. Here, it is a single replication connection that connects to the remote replication endpoint at location ``2``. With this replication connection, events are replicated from location ``2`` to location ``1``. For replicating events in the other direction, a corresponding ``ReplicationEndpoint`` and ``ReplicationConnection`` must be set up at location ``2``:

.. includecode:: ../code/EventLogDoc.scala
   :snippet: replication-endpoint-2

The event log actors that are created by a ``ReplicationEndpoint`` can be obtained from its ``logs`` map. Map keys are the event log names, map values the event log ``ActorRef``\ s:

.. includecode:: ../code/EventLogDoc.scala
   :snippet: logs-map-1

.. hint::
   Further ``ReplicationEndpoint`` creation options are described in the API documentation of the ReplicationEndpoint_ and ReplicationConnection_ companion objects. A complete reference of configuration options is given in section :ref:`configuration`.

.. _replication-filters:

Replication filters
^^^^^^^^^^^^^^^^^^^

By default, all events are replicated. Applications may provide ``ReplicationFilter``\ s to limit replication to a subset of events. A custom replication filter can be defined, by extending ReplicationFilter_ and implementing a filter predicate (method ``apply``). For example, the following replication filter selects DurableEvent_\ s with a matching ``emitterAggregateId``:

.. includecode:: ../code/EventLogDoc.scala
   :snippet: replication-filter-definition

Replication filters can be defined per ``ReplicationConnection`` and event log name. They are transferred to a remote replication endpoint and applied there while reading from a *source event log* during replication. The following example configures a replication filter for log ``L`` so that only events with a defined ``emitterAggregateId`` of value ``order-17`` are replicated from the remote source log:

.. includecode:: ../code/EventLogDoc.scala
   :snippet: replication-filter-application

Replication filters can also be composed. The following creates a composed filter so that events with a defined ``emitterAggregateId`` of value ``order-17`` or ``order-19`` are replicated:

.. includecode:: ../code/EventLogDoc.scala
   :snippet: replication-filter-composition

For the definition of filter logic based on application-defined events, replication filters should use the ``payload`` field of ``DurableEvent``.

.. hint::
   Serialization of replication filters can be customized as described in section :ref:`replication-filter-serialization`.

Batch replication
^^^^^^^^^^^^^^^^^

Events are replicated in batches. The maximum number of events per batch can be configured with

.. includecode:: ../conf/common.conf
   :snippet: replication-batch-size

Applications that increase the maximum batch size and/or store rather large events should also increase

.. includecode:: ../conf/common.conf
   :snippet: maximum-frame-size

otherwise, replication will fail.

Failure detection
^^^^^^^^^^^^^^^^^

Replication endpoints can notify applications about availability and un-availability of remote event logs. They can become unavailable during a network partition or during a downtime of their hosting application, for example. A local replication endpoint publishes

- ``Available(endpointId: String, logName: String)`` messages to the local ``ActorSystem``\ s `event stream`_ if the remote replication endpoint is available, and
- ``Unavailable(endpointId: String, logName: String)`` messages to the local ``ActorSystem``\ s `event stream`_ if the remote replication endpoint is unavailable

Both messages are defined in ReplicationEndpoint_. Their ``endpointId`` parameter is the id of the remote replication endpoint, the ``logName`` parameter is the name of an event log that is managed by the remote endpoint. The failure detection limit can be configured with:

.. includecode:: ../conf/common.conf
   :snippet: failure-detection-limit

It instructs the failure detector to publish an ``Unavailable`` message if there is no heartbeat from the remote replication endpoint within 60 seconds. ``Available`` and ``Unavailable`` messages are published periodically at intervals of ``eventuate.log.replication.failure-detection-limit``.

.. _Cassandra: http://cassandra.apache.org/
.. _Getting Started: https://wiki.apache.org/cassandra/GettingStarted
.. _cassandra-unit: https://github.com/jsevellec/cassandra-unit/wiki
.. _LevelDB: https://github.com/google/leveldb
.. _Akka Remoting: http://doc.akka.io/docs/akka/2.3.9/scala/remoting.html
.. _event stream: http://doc.akka.io/docs/akka/2.3.9/scala/event-bus.html#event-stream
.. _Chaos testing with Docker and Cassandra on Mac OS X: http://rbmhtechnology.github.io/chaos-testing-with-docker-and-cassandra/

.. _EventsourcingProtocol: ../latest/api/index.html#com.rbmhtechnology.eventuate.EventsourcingProtocol$
.. _ReplicationEndpoint: ../latest/api/index.html#com.rbmhtechnology.eventuate.ReplicationEndpoint$
.. _ReplicationConnection: ../latest/api/index.html#com.rbmhtechnology.eventuate.ReplicationConnection$
.. _ReplicationFilter: ../latest/api/index.html#com.rbmhtechnology.eventuate.ReplicationFilter
.. _DurableEvent: ../latest/api/index.html#com.rbmhtechnology.eventuate.DurableEvent
.. _Cassandra extension: ../latest/api/index.html#com.rbmhtechnology.eventuate.log.cassandra.Cassandra
.. _CassandraEventLog: ../latest/api/index.html#com.rbmhtechnology.eventuate.log.cassandra.CassandraEventLog

.. [#] A location can be a whole data center, a node within a data center or even a process on a single node, for example.
.. [#] Log names must be unique per replication endpoint. Replication connections are only established between logs of the same name.
