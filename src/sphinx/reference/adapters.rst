.. _adapters:

--------
Adapters
--------

.. _spark-adapter:

Spark adapter
-------------

The Eventuate Spark adapter allows applications to consume events from event logs and to process them in `Apache Spark`_. Writing processed events back to event logs is not possible yet but will be supported in future versions.

.. warning::
   The Spark adapter is experimental. Its feature set and API is likely to change based on user feedback.

Batch processing
~~~~~~~~~~~~~~~~

`SparkBatchAdapter`_ supports event batch processing from event logs with a :ref:`cassandra-storage-backend`. The batch adapter internally uses the `Spark Cassandra Connector`_ for exposing an event log as `Spark RDD`_ of `DurableEvent`_\ s:

.. includecode:: ../../../eventuate-example-spark/src/main/scala/com/rbmhtechnology/example/spark/SparkBatchAdapterExample.scala
   :snippet: spark-batch-adapter

A `SparkBatchAdapter`_ is instantiated with a ``SparkContext``, configured for connecting to a Cassandra storage backend, and a :ref:`event-serialization` configuration (if any). The ``eventBatch`` method exposes an event log with given ``logId`` as ``RDD[DurableEvent]``, optionally starting from a custom sequence number.

Event logs can span several partitions in a Cassandra cluster and the batch adapter reads from these partitions concurrently. Hence, events in the resulting RDD are ordered per partition. Applications that require a total order by ``localSequenceNr`` can sort the resulting RDD:

.. includecode:: ../../../eventuate-example-spark/src/main/scala/com/rbmhtechnology/example/spark/SparkBatchAdapterExample.scala
   :snippet: spark-batch-sorting

Exposing `Spark DataFrames`_ directly is not possible yet but will be supported in future versions. In the meantime, applications should convert RDDs to DataFrames or Datasets as shown in the following example:

.. includecode:: ../../../eventuate-example-spark/src/main/scala/com/rbmhtechnology/example/spark/SparkBatchAdapterExample.scala
   :snippet: spark-batch-dataframe

.. hint::
   The full example source code is in `SparkBatchAdapterExample.scala`_

Stream processing
~~~~~~~~~~~~~~~~~

`SparkStreamAdapter`_ supports event stream processing from event logs with any storage backend. The stream adapter connects to the `ReplicationEndpoint`_\ [#replicationEP]_ of an event log for exposing it as `Spark DStream`_ of `DurableEvent`_\ s:

.. includecode:: ../../../eventuate-example-spark/src/main/scala/com/rbmhtechnology/example/spark/SparkStreamAdapterExample.scala
   :snippet: spark-stream-adapter

A `SparkStreamAdapter`_ is instantiated with a Spark ``StreamingContext`` and a :ref:`event-serialization` configuration (if any). The ``eventStream`` method exposes an event log with given ``logName`` as ``DStream[DurableEvent]``. The stream is updated by interacting with the event log's replication endpoint at given ``host`` and ``port``.

The stream starts from the given ``fromSequenceNr`` and is updated with both, replayed events and newly written events. The storage level of events in Spark can be set with the ``storageLevel`` parameter. Applications that want to enforce event processing in strict event log storage order should repartition the stream with ``.repartition(1)``, as shown in the example.

For persisting the stream processing progress, an application should store the last processed sequence number at a custom place. When the application is restarted, the stored sequence number should be used as argument to the ``eventStream`` call. Later versions will additionally support internal storage of event processing progresses.

.. hint::
   The full example source code is in `SparkStreamAdapterExample.scala`_

.. _vertx-adapter:

Vert.x adapter
--------------

The Eventuate Vert.x adapter allows applications using a `Vert.x`_ instance to interact with event logs. Events can both be consumed from and produced to an event log by a Vert.x instance.

Event exchange is performed over the Vert.x `event bus`_. Events delivered to a Vert.x instance are either published to all subscribers or sent to a single subscriber on the event bus. Events received from a Vert.x instance are persisted to an event log by consuming events from a particular endpoint on the event bus.

Event Producers
~~~~~~~~~~~~~~~

The Vert.x adapter exchanges events with a Vert.x instance by using so called *event producers*. An event producer consumes events from a given source and produces the same events to a specified destination. Both sources and destinations can either be an event bus endpoint or an even log.

The Vert.x adapter supports two kinds of event producers:

- **Vert.x event producers** consume events from an event log and publish or send the events to a configurable event bus endpoint.
- **Log event producers** consume events from a given event bus endpoint and persist the events in an event log.

An event producer establishes an unidirectional connection between exactly one event log and one or multiple event bus endpoints. Event producers are instantiated by using the ``EventProducer`` API. The configuration of a producer consists of:

- an event source,
- an event destination and
- a unique id.

*Vert.x producers* are created by using the ``EventProducer.fromLog`` method. Applications define the source log, the delivery method and an arbitrary amount of event bus endpoints, the events will be delivered to.

.. tabbed-code::
   .. includecode:: ../../../eventuate-example-vertx/src/main/scala/com/rbmhtechnology/docs/vertx/Documentation.scala
      :snippet: vertx-event-producer
   .. includecode:: ../../../eventuate-example-vertx/src/main/java/com/rbmhtechnology/docs/vertx/japi/Documentation.java
      :snippet: vertx-event-producer

*Log producers* are created by using the ``EventProducer.fromEndpoints`` method. Multiple event bus endpoints can be defined, which are used to consume events from the event bus and persist the same events to the given event log.

.. tabbed-code::
   .. includecode:: ../../../eventuate-example-vertx/src/main/scala/com/rbmhtechnology/docs/vertx/Documentation.scala
      :snippet: log-event-producer
   .. includecode:: ../../../eventuate-example-vertx/src/main/java/com/rbmhtechnology/docs/vertx/japi/Documentation.java
      :snippet: log-event-producer

An event log must be supplied as an ``ActorRef`` which is usually obtained from a `ReplicationEndpoint`_\ [#replicationEP]_. Event producers are implementation-agnostic in respect to event logs - any event log implementation may be used in combination with a producer.

.. note::
   The id of a producer must be unique and should be stable over time. It is used as the primary key to store meta information about the producer.

.. hint::
   Event producers are covered in more detail in the sections `Vert.x Publish Event Producer`_ and `Vert.x Point-to-Point Event Producer`_. Log producers are covered in the section `Log Event Producer`_.

Event processing
~~~~~~~~~~~~~~~~

Applications process events sent from Vert.x producers by registering event bus handlers at the configured endpoints on the event bus. An event bus endpoint is a simple address represented as a ``String``, which can follow any addressing scheme. Vert.x producers deliver events on the event bus as instances of an event bus `message`_. Event bus handlers access the underlying event by obtaining the body of a message.

.. tabbed-code::
   .. includecode:: ../../../eventuate-example-vertx/src/main/scala/com/rbmhtechnology/docs/vertx/Documentation.scala
      :snippet: event-processing-vertx-producer
   .. includecode:: ../../../eventuate-example-vertx/src/main/java/com/rbmhtechnology/docs/vertx/japi/Documentation.java
      :snippet: event-processing-vertx-producer

For events to be written to an event log, applications send events to the specified endpoints configured for a log producer. The log producer consumes all events from these endpoints and persists the events to the configured event log.

.. tabbed-code::
   .. includecode:: ../../../eventuate-example-vertx/src/main/scala/com/rbmhtechnology/docs/vertx/Documentation.scala
      :snippet: event-processing-log-producer
   .. includecode:: ../../../eventuate-example-vertx/src/main/java/com/rbmhtechnology/docs/vertx/japi/Documentation.java
      :snippet: event-processing-log-producer

.. note::
   Event processing in event handlers should be performed idempotent because a Vert.x producer may deliver the same event multiple times under certain conditions. Events may be redelivered after a restart of a producer if it was not able to successfully persist its read progress on shutdown (or crash).

Adapter Usage
~~~~~~~~~~~~~

Event producers are managed by a ``VertxAdapter``. Applications can connect to multiple event logs by instantiating event producers and supplying them to the ``VertxAdapterConfig``.

The ``VertxAdatperConfig`` is passed to the ``VertxAdapter`` together with an ``ActorSystem`` and the ``Vert.x`` instance the adapter will connect to. The adapter is also supplied with a ``StorageProvider`` which is used to persist the read progress of the individual event producers.

Applications invoke the ``start`` method of the system to initialize the registered event producers and start event exchange with the ``Vert.x`` instance.

.. tabbed-code::
   .. includecode:: ../../../eventuate-example-vertx/src/main/scala/com/rbmhtechnology/docs/vertx/Documentation.scala
      :snippet: adapter-example
   .. includecode:: ../../../eventuate-example-vertx/src/main/java/com/rbmhtechnology/docs/vertx/japi/Documentation.java
      :snippet: adapter-example

.. warning::
   The ``start`` method should only be called after all handlers on the event bus have been registered. Failing to do so may lead to loss of events because a producer might try to deliver events to an event bus endpoint which has not yet an event handler assigned to it.

The following sections contain a detailed description of the different kinds of event producers.

Vert.x Publish Event Producer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A *Publish Event Producer* publishes events from an event log to *multiple* subscribers on the event bus. Events are delivered to specific endpoints defined in the configuration of the producer. A producer can route events to different event bus endpoints based on the content of the event. Event routing is enabled by supplying a partial function which maps events to event bus endpoints. If the partial function is not defined at the event, the event will not be processed.

.. tabbed-code::
   .. includecode:: ../../../eventuate-example-vertx/src/main/scala/com/rbmhtechnology/docs/vertx/Documentation.scala
      :snippet: vertx-publish-producer
   .. includecode:: ../../../eventuate-example-vertx/src/main/java/com/rbmhtechnology/docs/vertx/japi/Documentation.java
      :snippet: vertx-publish-producer

Event publishing is performed with *At-Most-Once* delivery semantics, so no guarantees about the successful delivery of events can be made.

Applications consume events by registering an event handler at the configured endpoints on the event bus.

.. tabbed-code::
   .. includecode:: ../../../eventuate-example-vertx/src/main/scala/com/rbmhtechnology/docs/vertx/Documentation.scala
      :snippet: event-processing-vertx-producer
   .. includecode:: ../../../eventuate-example-vertx/src/main/java/com/rbmhtechnology/docs/vertx/japi/Documentation.java
      :snippet: event-processing-vertx-producer

Read progress from the source event log is tracked by persisting the ``localSequenceNr`` of the latest sent event to the ``StorageProvider`` supplied to the ``VertxAdapter``. After publishing one or multiple events the read progress is persisted. The producer continues publishing events from the latest known ``localSequenceNr`` once the it is started.

Vert.x Point-to-Point Event Producer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A *Point-to-Point Event Producer* sends an event to a *single* subscriber on the event bus. If a single subscriber is registered for an endpoint all events are delivered to this subscriber. If multiple subscribers are registered for the same endpoint, events are delivered alternately to only one of those subscribers using a non-strict round-robin algorithm. Event routing can be enabled by supplying a partial function.

.. tabbed-code::
   .. includecode:: ../../../eventuate-example-vertx/src/main/scala/com/rbmhtechnology/docs/vertx/Documentation.scala
      :snippet: vertx-ptp-producer-at-most-once
   .. includecode:: ../../../eventuate-example-vertx/src/main/java/com/rbmhtechnology/docs/vertx/japi/Documentation.java
      :snippet: vertx-ptp-producer-at-most-once

Point-to-point event producers support both *At-Most-Once* and *At-Least-Once* delivery semantics. If not specified otherwise *At-Most-Once* delivery is chosen. *At-Least-Once* delivery is enabled by configuring the adapter accordingly.

.. tabbed-code::
   .. includecode:: ../../../eventuate-example-vertx/src/main/scala/com/rbmhtechnology/docs/vertx/Documentation.scala
      :snippet: vertx-ptp-producer-at-least-once
   .. includecode:: ../../../eventuate-example-vertx/src/main/java/com/rbmhtechnology/docs/vertx/japi/Documentation.java
      :snippet: vertx-ptp-producer-at-least-once

Events sent by a point-to-point event producer are received by registering an event handler on the event bus.

Using *At-Least-Once* delivery semantics, every event must be confirmed by the receiver. Unconfirmed events are redelivered until a confirmation was received by the adapter. Event handlers confirm event delivery by replying to the event bus message with a ``Confirmation``.

.. tabbed-code::
   .. includecode:: ../../../eventuate-example-vertx/src/main/scala/com/rbmhtechnology/docs/vertx/Documentation.scala
      :snippet: vertx-ptp-producer-handler
   .. includecode:: ../../../eventuate-example-vertx/src/main/java/com/rbmhtechnology/docs/vertx/japi/Documentation.java
      :snippet: vertx-ptp-producer-handler

Event confirmations are persisted on a per-event basis or in batches of configurable size.

- **Per-event confirmations**:
  Using per-event confirmations, every confirmation received by the adapter is persisted to the source event log. Confirmation events are not delivered to any event bus handlers but will increase the size of the source event log. With this confirmation mode events will not be redelivered once an event confirmation has been received.

- **Batch event confirmations**:
  Using batch confirmations, events are delivered in batches where the next batch is only delivered once all events of the previous batch have been confirmed. Batches containing events which have not been confirmed are redelivered as a whole, resulting in redelivery of all events of the same batch. This approach leads to modest storage requirements as no individual per-event confirmation information has to be tracked. Using this confirmation mode, events may be redelivered multiple times even though a confirmation has already been received.

Log Event Producer
~~~~~~~~~~~~~~~~~~

A *Log Event Producer* consumes events from multiple event bus endpoints and persists these events to a single event log. Every persisted event creates a write confirmation which is returned to the sender of the event, containing the result of the write operation.

Log event producers can be configured with an optional event filter. This filter is applied to events sent to the producer.

.. tabbed-code::
   .. includecode:: ../../../eventuate-example-vertx/src/main/scala/com/rbmhtechnology/docs/vertx/Documentation.scala
      :snippet: log-event-multiple-producer
   .. includecode:: ../../../eventuate-example-vertx/src/main/java/com/rbmhtechnology/docs/vertx/japi/Documentation.java
      :snippet: log-event-multiple-producer

Events accepted by the filter are written to the configured destination log. Events rejected by the filter are dropped and a corresponding write result is returned to the sender.

Applications persist events by sending them to the endpoint configured for the producer. The result of the write operation is returned as a response message.

.. tabbed-code::
   .. includecode:: ../../../eventuate-example-vertx/src/main/scala/com/rbmhtechnology/docs/vertx/Documentation.scala
      :snippet: log-producer-handler
   .. includecode:: ../../../eventuate-example-vertx/src/main/java/com/rbmhtechnology/docs/vertx/japi/Documentation.java
      :snippet: log-producer-handler

.. note::
   A single endpoint can only be configured once as the source for an log event producer. This ensures that write confirmations can reliably be returned to the source endpoint. Configuring the same source endpoint for multiple producers will lead to a configuration error.

Message Codecs
~~~~~~~~~~~~~~

All messages transmitted over the event bus must provide a Vert.x `message codec`_. The event bus uses this message codec to serialize and deserialize the body of an event bus message.

Events sent or received by the Vert.x adapter may not have an instance of a ``MessageCodec`` defined, since they usually originate from an external system. To ease the integration of external events into a Vert.x application, the adapter offers a generic message codec for types serializable by the ``ActorSystem`` provided to the ``VertxAdapter``. All events persisted to an event log are serializable by the ``ActorSystem``, hence the generic message codec can be used for those objects.

The generic ``MessageCodec`` is applied for an object type by registering the type with the ``VertxAdapterConfig``.

.. tabbed-code::
   .. includecode:: ../../../eventuate-example-vertx/src/main/scala/com/rbmhtechnology/docs/vertx/Documentation.scala
      :snippet: message-codec
   .. includecode:: ../../../eventuate-example-vertx/src/main/java/com/rbmhtechnology/docs/vertx/japi/Documentation.java
      :snippet: message-codec

A message codec for the type is created which uses the ``Serializer`` assigned to the type at the ``ActorSystem``. This codec is registered as the default message codec for the type and will subsequently be used to encode and decode all messages of this type on the event bus.

.. note::
   The generic ``MessageCodec`` can also be used for events not stored in an event log if a ``Serializer`` for the event type is configured at the ``ActorSystem``. If no ``Serializer`` for a type is configured the generated ``MessageCodec`` will fail to process instances of the type.

.. hint::
   A detailed example can be found in `VertxAdapterExample.scala`_ or `VertxAdapterExample.java`_.

.. _Apache Spark: http://spark.apache.org/
.. _Spark Cassandra Connector: https://github.com/datastax/spark-cassandra-connector
.. _Spark RDD: http://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds
.. _Spark DStream: http://spark.apache.org/docs/latest/streaming-programming-guide.html#discretized-streams-dstreams
.. _Spark DataFrames: http://spark.apache.org/docs/latest/sql-programming-guide.html#dataframes
.. _DurableEvent: ../latest/api/index.html#com.rbmhtechnology.eventuate.DurableEvent
.. _ReplicationEndpoint: ../latest/api/index.html#com.rbmhtechnology.eventuate.ReplicationEndpoint
.. _SparkBatchAdapter: ../latest/api/index.html#com.rbmhtechnology.eventuate.adapter.spark.SparkBatchAdapter
.. _SparkStreamAdapter: ../latest/api/index.html#com.rbmhtechnology.eventuate.adapter.spark.SparkStreamAdapter
.. _SparkBatchAdapterExample.scala: https://github.com/RBMHTechnology/eventuate/blob/master/eventuate-example-spark/src/main/scala/com/rbmhtechnology/example/spark/SparkBatchAdapterExample.scala
.. _SparkStreamAdapterExample.scala: https://github.com/RBMHTechnology/eventuate/blob/master/eventuate-example-spark/src/main/scala/com/rbmhtechnology/example/spark/SparkStreamAdapterExample.scala

.. _Vert.x: http://vertx.io/
.. _event bus: http://vertx.io/docs/vertx-core/java/#event_bus
.. _message codec: http://vertx.io/docs/apidocs/io/vertx/core/eventbus/MessageCodec.html
.. _message: http://vertx.io/docs/apidocs/io/vertx/core/eventbus/Message.html
.. _VertxAdapterExample.scala: https://github.com/RBMHTechnology/eventuate/blob/master/eventuate-example-vertx/src/main/scala/com/rbmhtechnology/example/vertx/VertxAdapterExample.scala
.. _VertxAdapterExample.java: https://github.com/RBMHTechnology/eventuate/blob/master/eventuate-example-vertx/src/main/java/com/rbmhtechnology/example/vertx/japi/VertxAdapterExample.java

.. [#replicationEP] See also :ref:`replication-endpoints` in the reference documentation.