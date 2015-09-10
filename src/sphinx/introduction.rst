.. _introduction:

------------
Introduction
------------

Eventuate is a toolkit for building distributed, highly-available and partition-tolerant event-sourced applications. It is written in Scala_ and built on top of `Akka`_, a toolkit for building highly concurrent, distributed, and resilient message-driven applications on the JVM.

`Event sourcing`_ captures all changes to application state as a sequence of events. These events are persisted in an event log and can be replayed to recover application state. Events are immutable facts that are only ever appended to a log which allows for very high transaction rates and efficient replication.

Eventuate supports replication of application state through asynchronous event replication across *locations*. These can be geographically distinct locations\ [#]_, nodes within a data center or even processes on the same node, for example. Locations consume replicated events to re-construct application state locally. Eventuate allows multiple locations to concurrently update replicated application state (multi-master) and supports automated and interactive conflict resolution strategies in case of conflicting updates. This is referred to as operation-based `optimistic replication`_ where operations are represented by application-defined events.

Events captured at a location are stored in a local event log and asynchronously replicated to other locations based on a replication protocol that preserves the *happened-before* relationship (= potential causality) of events. Causality is tracked with :ref:`vector-clocks`. For any two events, applications can determine if they have a potential causal relationship or if they are concurrent by comparing their vector timestamps. This is important to achieve `causal consistency`_ which is the strongest possible consistency for *always-on* applications i.e. applications that should remain available for writes during network partitions\ [#]_.

Individual locations even remain available for writes during inter-location network partitions. Events that have been captured locally during a network partition are replicated later when the partition heals. Storing events locally and replicating them later can also be useful for applications distributed across temporarily connected devices.

Storage backends at individual locations are pluggable (see also :ref:`current-limitations`). A location running on a mobile device, for example, could choose to write events to the local filesystem whereas a location running in a data center may want to write events to a Cassandra_ cluster. Asynchronous event replication across locations is independent of the storage technologies used at individual locations. A distributed Eventuate application may use different storage backends at different location.

To model application state, any custom data type can be used. Applications need to ensure that projecting events from a causally ordered event stream onto these data types gives a consistent result. This may involve detection of conflicts from concurrent events, selecting one of the conflicting versions as the winner or even merging them, for which Eventuate provides utilities. Eventuate also provides implementations_ of operation-based CRDT_\ s, specially-designed data structures used to achieve `strong eventual consistency`_.

.. [#] See also `Event sourcing at global scale`_. In this article, the term *site* is synonymous with *location*.
.. [#] Wyatt Lloyd et al, `Don’t settle for Eventual`_: Scalable Causal Consistency for Wide-Area Storage with COPS.

.. _Scala: http://www.scala-lang.org/
.. _Akka: http://akka.io
.. _Cassandra: http://cassandra.apache.org/
.. _LevelDB: https://github.com/google/leveldb
.. _Event sourcing: http://martinfowler.com/eaaDev/EventSourcing.html
.. _CAP: http://en.wikipedia.org/wiki/CAP_theorem
.. _CRDT: http://en.wikipedia.org/wiki/Conflict-free_replicated_data_type 

.. _optimistic replication: http://en.wikipedia.org/wiki/Optimistic_replication
.. _causal consistency: http://en.wikipedia.org/wiki/Causal_consistency
.. _implementations: https://krasserm.github.io/2015/02/17/Implementing-operation-based-CRDTs/
.. _strong eventual consistency: http://en.wikipedia.org/wiki/Eventual_consistency#Strong_eventual_consistency

.. _Event sourcing at global scale: http://krasserm.github.io/2015/01/13/event-sourcing-at-global-scale/
.. _Don’t settle for Eventual: https://www.cs.cmu.edu/~dga/papers/cops-sosp2011.pdf