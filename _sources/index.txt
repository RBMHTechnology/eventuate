=========
Eventuate
=========

Eventuate is a toolkit for building distributed, highly-available and partition-tolerant event-sourced applications. It is written in Scala_ and built on top of Akka_, a toolkit for building highly concurrent, distributed, and resilient message-driven applications on the JVM. Eventuate

- derives current application state from logged events (`event sourcing`_)
- replicates application state by replicating events across multiple locations
- allows updates to replicated state at multiple locations concurrently (multi-master)
- allows individual locations to continue writing even if they are partitioned from other locations
- provides means to detect, track and resolve conflicting updates (interactive and automated)
- enables reliable event collaboration between event-sourced microservices
- enables applications to implement a causal consistency model
- preserves causal ordering of replicated events
- provides implementations of operation-based CRDTs
- supports distribution up to global scale

.. _Scala: http://www.scala-lang.org/
.. _Akka: http://akka.io
.. _event sourcing: http://martinfowler.com/eaaDev/EventSourcing.html

-----------------
Table of contents
-----------------

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
   faq
   example-application
   current-limitations
