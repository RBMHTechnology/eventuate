=========
Eventuate
=========

Eventuate is a toolkit for building applications composed of event-driven and event-sourced services that collaborate by exchanging events over shared event logs. Services can either be co-located on a single node or distributed up to global scale. Services can also be replicated with causal consistency and remain available for writes during network partitions. Eventuate has a Java_ and Scala_ API, is written in Scala and built on top of `Akka`_, a toolkit for building highly concurrent, distributed, and resilient message-driven applications on the JVM. Eventuate

- provides event-sourcing abstractions for building stateful services on the command-side and query-side of CQRS_-based applications
- offers services a reliable and partition-tolerant event storage and event-based communication infrastructure that preserves causal ordering
- supports the development of *always-on* applications by allowing services to be distributed across multiple availability zones (locations)
- supports stateful service replication with causal consistency and concurrent state updates with automated and interactive conflict resolution options
- supports the implementation of reliable business processes from collaborating services that are tolerant to inter-service network partitions
- supports the aggregation of events from distributed services for updating persistent and in-memory query databases
- provides implementations of operation-based CRDTs as specified in `A comprehensive study of Convergent and Commutative Replicated Data Types`_
- provides adapters to 3rd-party stream processing frameworks for analyzing generated events (planned)

.. _Java: http://www.oracle.com/technetwork/java/javase/overview/index.html
.. _Scala: http://www.scala-lang.org/
.. _Akka: http://akka.io
.. _CQRS: http://martinfowler.com/bliki/CQRS.html
.. _A comprehensive study of Convergent and Commutative Replicated Data Types: http://hal.upmc.fr/file/index/docid/555588/filename/techreport.pdf

-----------------
Table of contents
-----------------

.. toctree::
   :maxdepth: 1

   getting-started
   overview
   architecture
   user-guide
   reference
   resources
   download
   developers
   project
   faq
   example-application
   current-limitations
