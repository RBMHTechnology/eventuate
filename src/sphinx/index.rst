=========
Eventuate
=========

Eventuate is a toolkit for building distributed, highly-available and partition-tolerant event-sourced applications. It is written in Scala_ and built on top of Akka_, a toolkit for building highly concurrent, distributed, and resilient message-driven applications on the JVM. Eventuate

- derives current application state from application-defined events (`event sourcing`_)
- replicates application state by replicating event streams across multiple *locations*
- allows updates to replicated state at multiple locations concurrently (multi-master)
- allows individual locations to continue writing even if they are partitioned from other locations
- provides means to detect, track and resolve conflicting updates (automated and interactive)
- enables applications to implement a causal consistency model
- preserves causal ordering of replicated event streams
- provides implementations of operation-based CRDTs
- supports replication at any scale e.g. from single node to multi-datacenter

.. note::
   This project has **early access** status (see also :ref:`current-limitations`).

.. _Scala: http://www.scala-lang.org/
.. _Akka: http://akka.io
.. _event sourcing: http://martinfowler.com/eaaDev/EventSourcing.html

.. toctree::
   :maxdepth: 1

   getting-started
   introduction
   architecture
   download
   user-guide
   reference
   developers
   project
   articles
   example-application
   current-limitations
