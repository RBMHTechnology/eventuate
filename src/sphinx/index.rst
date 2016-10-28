=========
Eventuate
=========

Eventuate is a toolkit for building applications composed of event-driven and event-sourced services that communicate via causally ordered event streams. Services can either be co-located on a single node or distributed up to global scale. Services can also be replicated with causal consistency and remain available for writes during network partitions. Eventuate has a Java_ and Scala_ API, is written in Scala and built on top of `Akka`_, a toolkit for building highly concurrent, distributed, and resilient message-driven applications on the JVM. Eventuate

- provides abstractions for building stateful event-sourced services, persistent and in-memory query databases and event processing pipelines
- enables services to communicate over a reliable and partition-tolerant event bus with causal event ordering and distribution up to global scale
- supports stateful service replication with causal consistency and concurrent state updates with automated and interactive conflict resolution
- provides implementations of operation-based CRDTs as specified in `A comprehensive study of Convergent and Commutative Replicated Data Types`_
- supports the development of *always-on* applications by allowing services to be distributed across multiple availability zones (locations)
- supports the implementation of reliable business processes from event-driven and command-driven service interactions
- supports the aggregation of events from distributed services for updating query databases
- provides adapters to 3rd-party stream processing frameworks for analyzing event streams

.. _Java: http://www.oracle.com/technetwork/java/javase/overview/index.html
.. _Scala: http://www.scala-lang.org/
.. _Akka: http://akka.io
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
   adapters
   api-docs
   resources
   download
   developers
   project
   faq
   example-application
