.. _akka-streams-adapter:

Akka Streams adapter
--------------------

This adapter provides an `Akka Streams`_ interface for Eventuate :ref:`event-logs`. It allows applications to consume event streams from event logs, write event streams to event logs, build idempotent event stream processing networks and exchange events with other systems that provide a `Reactive Streams`_ API. 

The examples in the following subsections depend on the event log references ``logA``, ``logB`` and ``logC``. Here, they reference isolated :ref:`local-event-log`\ s with a :ref:`leveldb-storage-backend`: 

.. includecode:: ../../../eventuate-example-stream/src/main/scala/com/rbmhtechnology/example/stream/DurableEventLogs.scala
   :snippet: durable-event-logs

We could also have used the local references of :ref:`replicated-event-log`\ s but this had no influence on the examples. Obtaining the local references of replicated event logs is explained in section :ref:`replication-endpoints`.

.. _event-source:

Event source
~~~~~~~~~~~~

An event source can be created with DurableEventSource_ from an event log reference. The result is a ``Graph[SourceShape[DurableEvent], ActorRef]`` which can be used with the Akka Streams `Scala DSL`_ or `Java DSL`_. Here, the Scala DSL is used to create a ``Source[DurableEvent, ActorRef]`` from ``logA``: 

.. includecode:: ../../../eventuate-example-stream/src/main/scala/com/rbmhtechnology/example/stream/DurableEventSourceExample.scala
   :snippet: durable-event-source-1

A ``DurableEventSource`` does not only emit events that already exist in an event log but also those events that have been written to the event log after the source has been materialized. A DurableEvent_ contains the application-specific event (``payload`` field) and its metadata (all other fields). 

To create a source that emits elements from a given sequence number and/or for a given aggregate id only, the parameters ``fromSequenceNr`` and ``aggregateId`` should be used, respectively. If ``aggregateId`` is ``None`` events with any aggregate id, defined or not, are emitted (see also :ref:`event-routing`).

.. includecode:: ../../../eventuate-example-stream/src/main/scala/com/rbmhtechnology/example/stream/DurableEventSourceExample.scala
   :snippet: durable-event-source-2

.. note::
   ``DurableEventSource`` emits events in local storage order. Local storage order is consistent with the *potential causality* of events which is tracked with :ref:`vector-clocks`. Find more details in section :ref:`event-logs`. Emission in local storage order also means that the order is repeatable across source materializations.

.. _event-writer:

Event writer
~~~~~~~~~~~~

An event writer is a stream stage that writes input events to an event log in stream order. It can be created with DurableEventWriter_ from a unique writer id and an event log reference. The result is a ``Graph[FlowShape[DurableEvent, DurableEvent], NotUsed]`` that emits the written events with updated metadata.

The following example converts a stream of ``String``\ s to a stream of ``DurableEvent``\ s and writes that stream to ``logA``. It then extracts ``payload`` and ``localSequenceNr`` from the written events and prints the results to ``stdout``:

.. includecode:: ../../../eventuate-example-stream/src/main/scala/com/rbmhtechnology/example/stream/DurableEventWriterExample.scala
   :snippet: durable-event-writer

The writer sets the ``emitterId`` of the input events to ``writerId``. The ``processId``, ``localLogId``, ``localSequenceNr`` and ``systemTimestamp`` are set by the event log. The event log also updates the local time of ``vectorTimestamp``. All other ``DurableEvent`` fields are written to the event log without modification. 

Input events are batched if they are produced faster than they can be written. The maximum batch size can be configured with ``eventuate.log.write-batch-size``. On write failure, the writer fails the stream.

.. _event-processor:

Event processor
~~~~~~~~~~~~~~~

An event processor is a stream stage that expects input events from one or more source event logs, processes these events with application-defined *processing logic* and writes the processed events to a target event log. An event processor can be a *stateful processor* or a *stateless processor* and can be created with DurableEventProcessor_.

Stateful processors apply processing logic of type ``(S, DurableEvent) => (S, Seq[O])`` to input events where ``S`` is the type of the processing state and ``O`` is the type of the processed **payload**. Stateless processors apply processing logic of type ``DurableEvent => Seq[O]`` to input events.

Processing logic can filter, transform and/or split input events. To filter an input event from the event stream, an empty sequence should be returned. To transform an input event into one output event a sequence of length 1 should be returned. To split an input event into multiple output events a sequence of corresponding length should be returned.

.. note::
   Application-defined processing logic can read the payload and metadata from the input event but can only return updated payloads. This makes metadata update a processor-internal concern, ensuring that event processing to the target log works correctly and is idempotent.

The following example is a stateless processor that consumes events from ``logA`` and writes the processing results to ``logB``. The processing logic filters an input event if the payload equals ``a``, it appends the source sequence number if the payload equals ``b`` and duplicates the input event if the payload equals ``c``. Events with other payloads remain unchanged:

.. includecode:: ../../../eventuate-example-stream/src/main/scala/com/rbmhtechnology/example/stream/DurableEventProcessorExample.scala
   :snippet: durable-event-processor-stateless

The example assumes that ``logA`` still contains the events that have been written by the event writer in the previous section. The next example uses a stateful processor that counts the number of events with a ``b`` payload and appends that number to all events. The processor consumes events from ``logA`` and writes the processing results to ``logC``:

.. includecode:: ../../../eventuate-example-stream/src/main/scala/com/rbmhtechnology/example/stream/DurableEventProcessorExample.scala
   :snippet: durable-event-processor-stateful

.. note::
   When running the examples a second time or more often, no events will be written to ``logB`` and ``logC`` because the processors will detect previously processed events as duplicates and discard them. This makes event processing **idempotent** i.e. it can be re-started after failures without generating duplicates in the target event logs.

Input events are batched if they are produced faster than they can be processed. The maximum batch size can be configured with ``eventuate.log.write-batch-size``. On write failure, a processor fails the stream.

Consuming from a shared source
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In the above example, both processors use their own ``DurableEventSource`` to read from ``logA``. A better alternative is to use a single source and broadcast the events to both processors which reduces the read load on ``logA``:

.. includecode:: ../../../eventuate-example-stream/src/main/scala/com/rbmhtechnology/example/stream/DurableEventProcessorExample.scala
   :snippet: durable-event-processor-shared-source

.. _consume-multiple-sources:

Consuming from multiple sources
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An event processor may also consume events from multiple sources. In the following example, the processor consumes the merged stream from ``logA`` and ``logB`` and writes the processing results to ``logC``:

.. includecode:: ../../../eventuate-example-stream/src/main/scala/com/rbmhtechnology/example/stream/DurableEventProcessorExample.scala
   :snippet: durable-event-processor-multiple-sources

.. note::
   The example assumes that ``logA`` and ``logB`` are independent i.e. have no causal relationship. A plain stream ``merge`` is sufficient in this case. If these two logs had a causal relationship (e.g. after having processed events from ``logA`` into ``logB``) a plain stream ``merge`` may generate a stream that is not consistent with *potential causality*. 

   Processing such a stream may generate ``vectorTimestamps`` that indicate concurrency of otherwise causally related events. This is acceptable for some applications but many others require stream merges that preserve causality. We will therefore soon provide a `causal stream merge stage`_.

Event processing progress tracking
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An event processor does not only write processed events to a target event log but also writes the latest source log sequence number to that log for tracking processing progress. When composing an event processing stream, an application should first read processing progresses from target logs in order to initialize ``DurableEventSource``\ s with appropriate ``fromSequenceNr``\ s. Eventuate provides ProgressSource_ for reading the processing progress for a given source from a target log. 

In :ref:`consume-multiple-sources`, for example, the processing progress for ``logA`` and ``logB`` is stored at ``logC``. The following example creates two sources, ``sourceA`` and ``sourceB``, that first read the progress for values for ``logA`` and ``logB`` from ``logC``, respectively, and then create the actual ``DurableEventSource``\ s with an appropriate ``fromSequenceNr``:

.. includecode:: ../../../eventuate-example-stream/src/main/scala/com/rbmhtechnology/example/stream/ProgressSourceExample.scala
   :snippet: progress-source

.. _Akka Streams: http://doc.akka.io/docs/akka/2.4/scala/stream/index.html
.. _Reactive Streams: http://www.reactive-streams.org/

.. _Scala DSL: http://doc.akka.io/api/akka/2.4/#akka.stream.scaladsl.package
.. _Java DSL: http://doc.akka.io/api/akka/2.4/#akka.stream.javadsl.package

.. _DurableEvent: ../latest/api/index.html#com.rbmhtechnology.eventuate.DurableEvent
.. _DurableEventSource: ../latest/api/index.html#com.rbmhtechnology.eventuate.adapter.stream.DurableEventSource$
.. _DurableEventWriter: ../latest/api/index.html#com.rbmhtechnology.eventuate.adapter.stream.DurableEventWriter$
.. _DurableEventProcessor: ../latest/api/index.html#com.rbmhtechnology.eventuate.adapter.stream.DurableEventProcessor$
.. _ProgressSource: ../latest/api/index.html#com.rbmhtechnology.eventuate.adapter.stream.ProgressSource$

.. _causal stream merge stage: https://github.com/RBMHTechnology/eventuate/issues/342
