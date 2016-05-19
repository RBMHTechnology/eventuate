.. _faq:

---
FAQ
---

.. contents::
   :local:

How does Akka Persistence compare to Eventuate?
-----------------------------------------------

A brief comparison is given in section :ref:`overview-event-ordering` in the Eventuate :ref:`overview`. A more detailed comparison is given in

- `A comparison of Akka Persistence with Eventuate`_ and
- `Akka Persistence and Eventuate - A CQRS/ES tool comparison`_

How can Apache Kafka be integrated with Eventuate?
--------------------------------------------------

At the moment, there is no `Apache Kafka`_ integration with Eventuate. We can imagine the following integration options and plan to provide them in future releases:

- *Event log storage backend*: Apache Kafka would serve best as :ref:`overview-event-log` storage backend. A storage plugin would map a :ref:`local-event-log` to a Kafka topic **partition** to preserve total event order. Replication of that topic is Kafka-specific and used to provide stronger durability guarantees within a location. Asynchronous replication across locations is Eventuate-specific and preserves causal event order. Kafka storage backends from different locations do not directly communicate with each other.
- *Stream processing adapter*: Another option is to treat Kafka as an external integration hub to other stream processing applications and to implement a :ref:`stream processing adapter <overview-stream-processing-adapters>` for exchanging events with Kafka. It needs to be investigated if implementation efforts for this and the previous option can be shared.

.. _A comparison of Akka Persistence with Eventuate: https://krasserm.github.io/2015/05/25/akka-persistence-eventuate-comparison/
.. _Akka Persistence and Eventuate - A CQRS/ES tool comparison: http://www.slideshare.net/mrt1nz/akka-persistence-and-eventuate
.. _Apache Kafka: http://kafka.apache.org/