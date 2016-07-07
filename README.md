[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/RBMHTechnology/eventuate?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![Build Status](https://travis-ci.org/RBMHTechnology/eventuate.svg?branch=master)](https://travis-ci.org/RBMHTechnology/eventuate)
[![Stories in Ready](https://badge.waffle.io/rbmhtechnology/eventuate.svg?label=ready&title=Ready)](http://waffle.io/rbmhtechnology/eventuate)

Eventuate
=========

Eventuate is a toolkit for building applications composed of event-driven and event-sourced services that collaborate by exchanging events over shared event logs. Services can either be co-located on a single node or distributed up to global scale. Services can also be replicated with causal consistency and remain available for writes during network partitions. Eventuate has a [Java](http://www.oracle.com/technetwork/java/javase/overview/index.html) and [Scala](http://www.scala-lang.org/) API, is written in Scala and built on top of [Akka](http://akka.io), a toolkit for building highly concurrent, distributed, and resilient message-driven applications on the JVM. Eventuate

- provides event-sourcing abstractions for building stateful services on the command-side and query-side of CQRS-based applications
- offers services a reliable and partition-tolerant event storage and event-based communication infrastructure that preserves causal ordering
- supports the development of *always-on* applications by allowing services to be distributed across multiple availability zones (locations)
- supports stateful service replication with causal consistency and concurrent state updates with automated and interactive conflict resolution options
- supports the implementation of reliable business processes from collaborating services that are tolerant to inter-service network partitions
- supports the aggregation of events from distributed services for updating persistent and in-memory query databases
- provides implementations of operation-based CRDTs as specified in [A comprehensive study of Convergent and Commutative Replicated Data Types](http://hal.upmc.fr/file/index/docid/555588/filename/techreport.pdf)
- provides adapters to 3rd-party stream processing frameworks for analyzing generated events

Documentation
-------------

- [Home](http://rbmhtechnology.github.io/eventuate/)
- [Overview](http://rbmhtechnology.github.io/eventuate/overview.html)
- [Architecture](http://rbmhtechnology.github.io/eventuate/architecture.html)
- [User guide](http://rbmhtechnology.github.io/eventuate/user-guide.html)
- [Reference](http://rbmhtechnology.github.io/eventuate/reference.html)
- [API docs](http://rbmhtechnology.github.io/eventuate/latest/api/index.html)
- [Articles](http://rbmhtechnology.github.io/eventuate/resources.html)
- [FAQs](http://rbmhtechnology.github.io/eventuate/faq.html)

Project
-------

- [Downloads](http://rbmhtechnology.github.io/eventuate/download.html)
- [Contributing](http://rbmhtechnology.github.io/eventuate/developers.html)

Community
---------

- [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/RBMHTechnology/eventuate?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
- [Mailing list](https://groups.google.com/forum/#!forum/eventuate)
