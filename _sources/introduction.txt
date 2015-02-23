.. _introduction:

------------
Introduction
------------

Eventuate is a toolkit for building distributed, highly-available and partition-tolerant event-sourced applications. It is written in Scala and built on top of `Akka`_, a toolkit for building highly concurrent, distributed, and resilient message-driven applications on the JVM.

`Event sourcing`_ captures all changes to application state as a sequence of events. These events are persisted in an event log and can be replayed to recover application state. Events are immutable facts that are only ever appended to a log which allows for very high transaction rates and efficient replication.

Eventuate supports replication of application state through asynchronous event replication between multiple _locations_. These can be geographically distinct locations, nodes within a data center or even processes on the same node, for example. Locations consume replicated events to re-construct application state locally. Eventuate allows multiple locations to concurrently update replicated state (master-master setup) and supports pre-defined or custom conflict resolution strategies in case of conflicting updates. This is known as operation-based `optimistic replication`_ where operations are represented by application-defined events.

Individual locations remain available for local writes even in the presence of network partitions. Events that have been captured locally during a network partition are replicated later when the partition heals. Storing events locally and replicating them later can also be useful for distributed applications deployed on temporarily connected devices, for example.

At the core of Eventuate is a replicated event log. Events captured at a location are stored locally and replicated asynchronously to other locations based on a replication protocol that preserves the `causal ordering`_ of events where causality is tracked with `vector clocks`_. For any two events, applications can determine if they have a causal relationship or if they are concurrent by comparing their vector timestamps. This is important to achieve eventual consistency of replicated application state. An event log can also be partitioned for scaling writes.

Storage technologies at individual locations are pluggable (SPI not public yet). A location deployed on a mobile device, for example, will probably choose to write events to the local filesystem whereas a location deployed in a data center could choose to write events to an `Apache Kafka`_ cluster. Event replication across locations is independent of the storage technologies used at individual locations, so that distributed applications with hybrid event stores are possible.

To model application state, any custom data types can be used. Applications just need to ensure that projecting events from a causally ordered event stream onto these data types yield a consistent result. This may involve detection of conflicts from concurrent events, selecting one of the conflicting versions as the winner or even merging them. Conflict resolution can be automated or interactive. Eventuate also provides implementations of `operation-based CRDTs`_, specially-designed data structures used to achieve `strong eventual consistency`_.

Eventuate’s approach to optimistic replication and eventual consistency is nothing new. It is implemented in many distributed database systems that choose AP from `CAP`_. These database systems internally maintain a transaction log that is replicated to re-construct state at different replicas. Besides offering a programming model for storing state changes instead of current state, Eventuate also differs from these database systems in the following ways:

- The transaction log is exposed to applications as event log. Although many distributed database systems provide change feeds, these deliver rather technical events (DocumentCreated, DocumentUpdated, ... for example) compared to domain-specific events in an event log with clear semantics in the application domain (CustomerRelocated, PaymentDelayed, ...). Change feeds with domain-specific events can have significant advantages for building custom read models in `CQRS`_ or for application integration where semantic integration is often a major challenge.

- Application state can be any application-defined data type together with an event projection function to update and recover state from an event stream. To recover application state at a particular location, the event stream can be deterministically replayed from a local event log. For application state that can be updated at multiple locations, Eventuate provides utilities to track concurrent versions which can be used as input for automated or interactive conflict resolution.

.. _Akka: http://akka.io
.. _Apache Kafka: http://kafka.apache.org/
.. _Event sourcing: http://martinfowler.com/eaaDev/EventSourcing.html
.. _CQRS: http://martinfowler.com/bliki/CQRS.html
.. _CAP: http://en.wikipedia.org/wiki/CAP_theorem

.. _optimistic replication: http://en.wikipedia.org/wiki/Optimistic_replication
.. _causal ordering: http://krasserm.github.io/2015/01/13/event-sourcing-at-global-scale/#event-log
.. _vector clocks: http://en.wikipedia.org/wiki/Vector_clock
.. _operation-based CRDTs: https://krasserm.github.io/2015/02/17/Implementing-operation-based-CRDTs/
.. _strong eventual consistency: http://en.wikipedia.org/wiki/Eventual_consistency#Strong_eventual_consistency
