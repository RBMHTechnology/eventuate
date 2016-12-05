.. _overview:

--------
Overview
--------

Eventuate is a toolkit for building applications composed of event-driven and event-sourced services that communicate via causally ordered event streams. Services can either be co-located on a single node or distributed up to global scale. Services can also be replicated with causal consistency and remain available for writes during network partitions. Eventuate has a Java_ and Scala_ API, is written in Scala and built on top of `Akka`_, a toolkit for building highly concurrent, distributed, and resilient message-driven applications on the JVM.

.. _overview-event-sourcing:

Event sourcing
--------------

`Event sourcing`_ captures all changes to application state as a sequence of events. These events are persisted in an event log and can be replayed to recover application state. Events are immutable facts that are only ever appended to an event log which allows for very high transaction rates and efficient replication. Eventuate provides the following :ref:`event-sourcing abstractions <event-sourcing>` which can be extended by applications:

- :ref:`Event-sourced actor <event-sourced-actors>`. An Akka actor that consumes events from an event log and produces events to the same event log. Internal state derived from consumed events is an in-memory *write model* contributing to the command-side (C) of a CQRS_-based application. 
- :ref:`Event-sourced view <event-sourced-views>`. An Akka actor that only consumes events from an event log. Internal state derived from consumed events is an in-memory *read model* contributing to the query-side (Q) of CQRS.
- :ref:`Event-sourced writer <event-sourced-writers>`. An Akka actor that consumes events from an event log to update a persistent query database. State derived from consumed events is a persistent *read model* contributing to the query-side (Q) of CQRS.
- :ref:`Event-sourced processor <event-sourced-processors>`. An Akka actor that consumes events from one event log and produces processed events to another event log. Processors can be used to connect event logs to event processing pipelines or graphs.

Implementations of these abstractions are referred to as *event-sourced components*. Applications compose them to *event-sourced services* and make them accessible via application-level protocols\ [#]_.

.. _overview-event-collaboration:

Event collaboration
-------------------

Events produced by one event-sourced component can be consumed by other event-sourced components if they share a local or distributed event log. This allows them to communicate via events a.k.a. `event collaboration`_. Event collaboration use cases include:

- *Distributed business processes*. Event-sourced actors of different type communicate via events to achieve a common goal. They play different roles in a business process and react on received events by updating application state and producing new events. We refer to this form of event collaboration as :ref:`arch-event-driven-communication`.
- :ref:`arch-actor-state-replication`. Event-sourced actors of same type consume each other’s events to replicate internal state with causal consistency. Eventuate allows concurrent updates to replicated actor state and supports automated and interactive conflict resolution in case of conflicting updates.
- :ref:`arch-event-aggregation`. Event-sourced views and writers aggregate events from other event-sourced components to generate application-specific views.

Event collaboration is reliable. For example, a distributed business process that fails due to a network partition automatically resumes when the partition heals.

.. _overview-event-log:

Event log
---------

An Eventuate :ref:`Event log <event-logs>` can be operated at a single location or replicated across multiple locations. A *location* is an availability zone that accepts writes to a :ref:`local-event-log` even if it is partitioned from other locations. Local event logs from multiple locations can be connected to a :ref:`replicated-event-log` that preserves causal event ordering. 

Locations can be geographically distinct locations, nodes within a cluster or even processes on the same node, depending on the granularity of availability zones needed by an application. Event-sourced actors and processors always write to their local event log. Event-sourced components can either collaborate over a local event log at the same location or over a replicated event log at different locations.

Local event logs have pluggable storage backends. At the moment, Eventuate provides plugins for a :ref:`cassandra-storage-backend` and a :ref:`leveldb-storage-backend`. The Cassandra plugin writes events to a Cassandra_ cluster and should be used if stronger durability guarantees are needed. The LevelDB storage plugin writes events to a LevelDB_ instance on the local filesystem and should be used if weaker durability guarantees are acceptable or a lightweight storage backend is needed. 

Storage backends from different locations do not directly communicate with each other. Asynchronous event replication across locations is Eventuate-specific and also works between locations with different storage backends. Synchronous event replication within a storage backend at a given location is optional and only used to achieve stronger durability.

.. _overview-event-bus:

Event bus
---------

Event-sourced components have a subscription at their event log. Newly written events are pushed to subscribers which allows them to update application state with minimal latency. An event written at one location is reliably pushed to subscribers at that location and to subscribers at remote locations. Consequently, event-sourced components that exchange events via a replicated event log communicate over a federated, durable and partition-tolerant event bus that preserves causal event ordering. During inter-location network partitions services can continue to write events locally. Delivery of events to remote locations automatically resumes when the partition heals.

.. _overview-event-ordering:

Event ordering and consistency
------------------------------

The delivery order of events during push updates (see :ref:`overview-event-bus`) is identical to that during later event replays because the delivery order of events to event-sourced components is determined by local event storage order. Within a location, all event-sourced components see the same order of events. The delivery and storage order of replicated events at distinct locations is consistent with causal order: causally related events have the same order at all locations whereas concurrent events may have different order. This is important to achieve `causal consistency`_ which is the strongest possible consistency for applications that choose AP of CAP_ i.e. applications that should remain available for writes during network partitions\ [#]_. In Eventuate, causality is tracked as *potential causality* with :ref:`vector-clocks`.

Applications that favor strong consistency (CP of CAP) of actor state on the command-side should consider single-location deployments with event-sourced actor singletons. From a high-level perspective, single-location Eventuate applications share many similarities with `Akka Persistence`_ applications\ [#]_\ [#]_. In this context, Eventuate can be regarded as functional superset of Akka Persistence with additional support for 

- actor state replication by relaxing strong consistency to causal consistency
- event aggregation from multiple producers that preserves causal event ordering and
- event collaboration with stronger ordering guarantees than provided by plain reliable messaging\ [#]_. 

.. _overview-operation-based-crdts:

Operation-based CRDTs
---------------------

Eventuate comes with an implementation of :ref:`operation-based-crdts` (commutative replicated data types or CmRDTs) as specified in `A comprehensive study of Convergent and Commutative Replicated Data Types`_. In contrast to state-based CRDTs (convergent replicated data types or CvRDTs), operation-based CRDTs require a reliable broadcast channel with causal delivery order for communicating update operations among replicas. Exactly these properties are provided by an Eventuate event bus so it was straightforward to implement operation-based CRDTs on top of it. Operations are persisted as events and delivered to replicas over the event bus. The state of operation-based CRDTs can be recovered by replaying these events, optionally starting from a state snapshot.

.. _overview-stream-processing-adapters:

Stream processing adapters
--------------------------

Although Eventuate can be used to build distributed stream processing applications, it doesn’t aim to compete with existing, more elaborate stream processing frameworks such as `Spark Streaming`_ or `Akka Streams`_, for example. Eventuate rather provides :ref:`adapters` to these frameworks so that events produced by Eventuate applications can be further analyzed there and results written back to Eventuate event logs. 

Related projects
----------------

Two other Red Bull Media House Technology projects are related to Eventuate:

- The `Eventuate Chaos`_ project provides utilities for chaos-testing Eventuate applications. They can randomly introduce node crashes, network partitions and packet loss into distributed Eventuate applications and Apache Cassandra clusters. Among other tests, they have been used to test the convergence of :ref:`overview-operation-based-crdts` under chaotic conditions.
- The `Eventuate Tools`_ project provides tools that support the operation of Eventuate-based applications. The current version provides `log viewer`_, a command-line tool for viewing the contents of an event log. Tools for monitoring Eventuate applications and collecting metrics are planned.

.. [#] Eventuate is not a complete (micro-)service development framework. It focuses more on event-sourcing building blocks and the reliable and causally consistent communication infrastructure between event-sourced services. It leaves service deployment and accessibility an application-level concern. Later versions of Eventuate may extend into these directions though.
.. [#] Wyatt Lloyd et al, `Don’t settle for Eventual`_: Scalable Causal Consistency for Wide-Area Storage with COPS.
.. [#] `A comparison of Akka Persistence with Eventuate`_
.. [#] `Akka Persistence and Eventuate - A CQRS/ES tool comparison`_
.. [#] See also `reliable messaging in Akka Persistence`_ and :ref:`reliable messaging in Eventuate <reliable-delivery>`.

.. _Java: http://www.oracle.com/technetwork/java/javase/overview/index.html
.. _Scala: http://www.scala-lang.org/
.. _Akka: http://akka.io
.. _Akka Persistence: http://doc.akka.io/docs/akka/2.4/scala/persistence.html
.. _Akka Streams: http://doc.akka.io/docs/akka/2.4/scala/stream/index.html
.. _Spark Streaming: http://spark.apache.org/streaming/
.. _Cassandra: http://cassandra.apache.org/
.. _LevelDB: https://github.com/google/leveldb
.. _Event sourcing: http://martinfowler.com/eaaDev/EventSourcing.html
.. _event collaboration: http://martinfowler.com/eaaDev/EventCollaboration.html
.. _CAP: http://en.wikipedia.org/wiki/CAP_theorem
.. _CRDT: http://en.wikipedia.org/wiki/Conflict-free_replicated_data_type 
.. _CQRS: http://martinfowler.com/bliki/CQRS.html
.. _causal consistency: http://en.wikipedia.org/wiki/Causal_consistency
.. _reliable messaging in Akka Persistence: http://doc.akka.io/docs/akka/2.4/scala/persistence.html#At-Least-Once_Delivery
.. _Eventuate Chaos: https://github.com/RBMHTechnology/eventuate-chaos
.. _Eventuate Tools: https://github.com/RBMHTechnology/eventuate-tools
.. _log viewer: https://github.com/RBMHTechnology/eventuate-tools/blob/master/log-viewer/README.md

.. _Don’t settle for Eventual: https://www.cs.cmu.edu/~dga/papers/cops-sosp2011.pdf
.. _A comparison of Akka Persistence with Eventuate: https://krasserm.github.io/2015/05/25/akka-persistence-eventuate-comparison/
.. _Akka Persistence and Eventuate - A CQRS/ES tool comparison: http://www.slideshare.net/mrt1nz/akka-persistence-and-eventuate
.. _A comprehensive study of Convergent and Commutative Replicated Data Types: http://hal.upmc.fr/file/index/docid/555588/filename/techreport.pdf
