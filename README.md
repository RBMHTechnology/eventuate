[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/RBMHTechnology/eventuate?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![Build Status](https://travis-ci.org/RBMHTechnology/eventuate.svg?branch=master)](https://travis-ci.org/RBMHTechnology/eventuate)
[![Stories in Ready](https://badge.waffle.io/rbmhtechnology/eventuate.svg?label=ready&title=Ready)](http://waffle.io/rbmhtechnology/eventuate)

Eventuate
=========

Please note: This project is in **maintenance mode**. Only critical bugs will be fixed, but there is no more feature development!

Eventuate is a toolkit for building applications composed of event-driven and event-sourced services that communicate via causally ordered event streams. Services can either be co-located on a single node or distributed up to global scale. Services can also be replicated with causal consistency and remain available for writes during network partitions. Eventuate has a [Java](http://www.oracle.com/technetwork/java/javase/overview/index.html) and [Scala](http://www.scala-lang.org/) API, is written in Scala and built on top of [Akka](http://akka.io), a toolkit for building highly concurrent, distributed, and resilient message-driven applications on the JVM. Eventuate

- provides abstractions for building stateful event-sourced services, persistent and in-memory query databases and event processing pipelines
- enables services to communicate over a reliable and partition-tolerant event bus with causal event ordering and distribution up to global scale
- supports stateful service replication with causal consistency and concurrent state updates with automated and interactive conflict resolution
- provides implementations of operation-based CRDTs as specified in [A comprehensive study of Convergent and Commutative Replicated Data Types](http://hal.upmc.fr/file/index/docid/555588/filename/techreport.pdf)
- supports the development of *always-on* applications by allowing services to be distributed across multiple availability zones (locations)
- supports the implementation of reliable business processes from event-driven and command-driven service interactions
- supports the aggregation of events from distributed services for updating query databases
- provides adapters to 3rd-party stream processing frameworks for analyzing event streams

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
