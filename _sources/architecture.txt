.. _architecture:

------------
Architecture
------------

Event logs
----------

Eventuate applications store events in event logs. An event log can be replicated across *locations* where each location has its own copy of events in a local event log. Replication occurs asynchronously over *replication connections* between *replication endpoints*.

.. figure:: images/architecture-1.png
   :figwidth: 70%

   Fig. 1

   An event log, replicated across locations A, B and C.

Events within a local log are totally ordered. This total order however is likely to differ among locations, as events can be written concurrently. The strongest ordering guarantee that can be given across locations is `causal ordering`_ [#]_ which is tracked with `vector clocks`_. Causal ordering is guaranteed to be consistent with total ordering in local event logs. 

A replication endpoint can also manage more than one local event log. Event logs are indexed by name and replication occurs only between logs of the same name. Logs with different names are isolated from each other [#]_ and their distribution across locations may differ, as shown in the following figure.

.. figure:: images/architecture-2.png
   :figwidth: 70%

   Fig. 2

   Three replicated event logs. Log X (blue) is replicated across locations A, B and C. Log Y (red) is replicated across locations A and B and log Z (green) is replicated across locations A and C.

Eventuate event logs are comparable to `Apache Kafka`_ topics. The main difference is that Kafka chooses consistency over write-availability for replicated topics whereas Eventuate chooses write-availability over consistency for replicated event logs, giving up total ordering for causal ordering.

Event storage backends at individual locations are pluggable. A location that requires strong durability guarantees should use a storage backend that is (synchronously) replicated within that location (`Apache Kafka`_ is an good fit here), others may use a more lightweight, non-replicated storage backend in case of weaker durability requirements.

Event replication across locations is reliable. Should a location crash or a network partition occur, replication automatically resumes when crashed location recovers and/or the partition heals. Built-in failure detectors inform applications about (un)availability of other locations. Replication endpoints also ensure that no duplicates are ever written to target event logs.

Replication connections can be configured with replication filters, so that only events matching one or more filter criteria are replicated. This is especially useful for smaller locations (for example, a mobile device) that only needs to exchange a subset of events with other locations.

Event-sourced actors
--------------------

Event-sourced actors produce events to and consume events from an event log. During *command processing* they usually validate external commands against internal state and, if validation succeeds, write one or more events to their event log. During *event processing* they consume events they have written and update internal state by handling these events. This is the basic idea behind `event sourcing`_. When used in context of a `CQRS`_ architecture, event-sourced actors usually implement the command-side (C).

.. figure:: images/architecture-3.png
   :figwidth: 70%

   Fig. 3

   An event-sourced actor, producing events to and consuming events from an event log.

When an event-sourced actor is re-started, internal state is recovered by replaying events from its local event log. Since events in a local event log are totally ordered, event replay at a given location is deterministic. Event replay can also be started from a snapshot of internal state which is an optimization to reduce recovery times.

In addition to consuming their own events, event-sourced actors can also consume events produced by other event-sourced actors to the same event log. This enables `event collaboration`_ between actors (:ref:`arch-fig4`). The underlying event routing can be customized with application-defined routing logic. 

A special form of event collaboration is state replication where actors of the same type consume the same events at different locations to re-construct state (see also :ref:`crdt-services`). Another example of event collaboration is a distributed business process where actors of different type collaborate by exchanging events to achieve a common goal.

.. _arch-fig4:

.. figure:: images/architecture-4.png
   :figwidth: 70%

   Fig. 4 

   Two event-sourced actors exchanging events over a distributed event log.

Event-sourced actors may also interact with external services by sending commands and processing replies. Commands can be sent with at-most-once or at-least-once delivery semantics, depending on the reliability requirements of an application. Replies from external services are usually processed like external commands which may result in further events to be written. This way, external services can be included into reliable, event-driven business processes controlled by event-sourced actors.

.. figure:: images/architecture-5.png
   :figwidth: 70%

   Fig. 5

   External service integration.

From a functional perspective, there’s no difference whether event-sourced actors exchange events over a local event log or over a distributed event log. This is also useful for testing purposes as it doesn’t require to setup a distributed log.

Event-sourced views
-------------------

Event-sourced views are a functional subset of event-sourced actors. They can only consume events from an event log but cannot produce new events. Views do not only maintain state in-memory but often persist it to a database. By additionally storing the sequence number of the last processed event in the database, writing can be made idempotent. When used in context of a `CQRS`_ architecture, views implement the query-side (Q).

.. _processors:

Event-sourced processors
------------------------

An event-sourced processor consumes events from one or more event logs, processes them (stateless or stateful) and produces the processed events to another event log. Event-sourced processors are gateways between otherwise partitioned event logs. They are not implemented yet but coming soon.

Snapshots
---------

Snapshots of internal state can be taken from event-sourced actors, views and processors. Snapshotting is an optimization to reduce recovery times of event-sourced components. Snapshotting is not implemented yet but coming soon.

Vector clocks
-------------

In the current system model, an event-sourced actor represents a lightweight “process” with its own consistency boundary. After having consumed an event e :sub:`i`, events e :sub:`i+1`, e :sub:`i+2`, ..., e :sub:`i+n`, generated by that actor, causally depend on e :sub:`i`. To track causality, each event-sourced actor maintains a vector clock which is used to timestamp written events. For any two events, applications can determine if they are causally related or if they are concurrent by comparing their vector timestamps. 

Only events that are actually handled by an event-sourced actor contribute to its vector clock. This allows to keep vector clock sizes small, even if a large number of event-sourced actors is used. For example, if an application follows a one-\ aggregate_-per-actor design, vector clock sizes scale only with the (small) number of locations rather than the (potentially large) number of aggregates.

.. _crdt-services:

CRDT services
-------------

Eventuate provides implementations of operation-based CRDT_\ s (commutative replicated data types or CmRDTs) that rely on a replicated event log to reliably broadcast update operations. CmRDTs are managed by `CRDT services`_ that provide applications convenient access to CmRDTs. New CmRDT types can be integrated into the CRDT service infrastructure with the CRDT development framework.

Batching
--------

Eventuate internally uses batching to optimize read and write throughput. It is used for

- producing new events to the event log: Whenever a write operation to a an event log is in progress, new write requests are batched and served when the previous write operation completed. This strategy leads to dynamically increasing write-batch sizes (up to a configurable maximum) under increasing write loads. If there is no current write operation in progress, a new write request is served immediately, keeping latency at a minimum.

- consuming events from the event log: Events can be read from the event log in batches which allows for efficient integration of external consumers.

- replicating events: Events are replicated in batches of configurable size. They are batch-read from a source log, batch-transferred over a replication connection and batch-written to a target log.

Adapters
--------

Eventuate aims to integrate with stream processing solutions such as Spark Streaming, Storm or Samza. The ability to exchange events with these solutions enables support for many analytics use cases. We plan to provide adapters for

- `Spark Streaming`_
- Samza_
- Storm_
- akka-streams_
- scalaz-stream_

We haven’t started yet working on this. Should you have any preferences or proposals for further integrations, please `let us know`_. Of course, we love contributions :)

.. _CQRS: http://martinfowler.com/bliki/CQRS.html
.. _CRDT: http://en.wikipedia.org/wiki/Conflict-free_replicated_data_type
.. _CRDT services: https://krasserm.github.io/2015/02/17/Implementing-operation-based-CRDTs/

.. _akka-streams: http://doc.akka.io/docs/akka-stream-and-http-experimental/current/scala.html
.. _scalaz-stream: https://github.com/scalaz/scalaz-stream
.. _Spark Streaming: https://spark.apache.org/streaming/
.. _Samza: http://samza.apache.org/
.. _Storm: https://storm.apache.org/
.. _Apache Kafka: https://kafka.apache.org/

.. _vector clocks: http://en.wikipedia.org/wiki/Vector_clock
.. _causal ordering: http://krasserm.github.io/2015/01/13/event-sourcing-at-global-scale/#event-log
.. _event sourcing: http://martinfowler.com/eaaDev/EventSourcing.html
.. _event collaboration: http://martinfowler.com/eaaDev/EventCollaboration.html
.. _aggregate: http://martinfowler.com/bliki/DDD_Aggregate.html

.. _let us know: https://groups.google.com/forum/#!forum/eventuate

.. [#] In the linked article, the term *site* is synonymous with *location*.
.. [#] :ref:`processors` can be used to connect partitioned event logs.  


