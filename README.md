[![Build Status](https://travis-ci.org/RBMHTechnology/eventuate.svg?branch=master)](https://travis-ci.org/RBMHTechnology/eventuate)

Eventuate
=========

Eventuate is a toolkit for building distributed, highly-available and partition-tolerant event-sourced applications. It is written in [Scala](http://www.scala-lang.org/) and built on top of [Akka](http://akka.io), a toolkit for building highly concurrent, distributed, and resilient message-driven applications on the JVM. Eventuate

- derives current application state from logged events (event sourcing)
- replicates application state by replicating events across multiple locations
- allows updates to replicated state at multiple locations concurrently (multi-master)
- allows individual locations to continue writing even if they are partitioned from other locations
- provides means to detect, track and resolve conflicting updates (automated and interactive)
- enables applications to implement a causal consistency model
- preserves causal ordering of replicated events
- provides implementations of operation-based CRDTs
- supports distribution up to global scale.

Find out more in the [introduction](http://rbmhtechnology.github.io/eventuate/introduction.html) and the [project documentation](http://rbmhtechnology.github.io/eventuate/). This project has **early access** status (see also [current limitations](http://rbmhtechnology.github.io/eventuate/current-limitations.html)).
