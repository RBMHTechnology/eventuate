[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/RBMHTechnology/eventuate?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![Build Status](https://travis-ci.org/RBMHTechnology/eventuate.svg?branch=master)](https://travis-ci.org/RBMHTechnology/eventuate)
[![Stories in Ready](https://badge.waffle.io/rbmhtechnology/eventuate.svg?label=ready&title=Ready)](http://waffle.io/rbmhtechnology/eventuate)

Eventuate
=========

Eventuate is a toolkit for building distributed, highly-available and partition-tolerant event-sourced applications. It is written in [Scala](http://www.scala-lang.org/) and built on top of [Akka](http://akka.io), a toolkit for building highly concurrent, distributed, and resilient message-driven applications on the JVM. Eventuate

- allows actors to interact over a distributed, reliable and persistent event bus that preserves causal event ordering
- derives current application state from logged events (event sourcing)
- replicates application state by replicating events across multiple locations
- allows updates to replicated state at multiple locations concurrently (multi-master)
- allows individual locations to continue writing even if they are partitioned from other locations
- provides means to detect, track and resolve conflicting updates (interactive and automated)
- enables applications to implement a causal consistency model
- provides implementations of operation-based CRDTs
- supports distribution up to global scale

Documentation
-------------

- [Home](http://rbmhtechnology.github.io/eventuate/)
- [Introduction](http://rbmhtechnology.github.io/eventuate/introduction.html)
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
