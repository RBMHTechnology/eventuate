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

`SparkStreamAdapter`_ supports event stream processing from event logs with any storage backend. The stream adapter connects to the `ReplicationEndpoint`_\ [#]_ of an event log for exposing it as `Spark DStream`_ of `DurableEvent`_\ s:

.. includecode:: ../../../eventuate-example-spark/src/main/scala/com/rbmhtechnology/example/spark/SparkStreamAdapterExample.scala
   :snippet: spark-stream-adapter

A `SparkStreamAdapter`_ is instantiated with a Spark ``StreamingContext`` and a :ref:`event-serialization` configuration (if any). The ``eventStream`` method exposes an event log with given ``logName`` as ``DStream[DurableEvent]``. The stream is updated by interacting with the event log's replication endpoint at given ``host`` and ``port``.

The stream starts from the given ``fromSequenceNr`` and is updated with both, replayed events and newly written events. The storage level of events in Spark can be set with the ``storageLevel`` parameter. Applications that want to enforce event processing in strict event log storage order should repartition the stream with ``.repartition(1)``, as shown in the example.

For persisting the stream processing progress, an application should store the last processed sequence number at a custom place. When the application is restarted, the stored sequence number should be used as argument to the ``eventStream`` call. Later versions will additionally support internal storage of event processing progresses.

.. hint::
   The full example source code is in `SparkStreamAdapterExample.scala`_

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

.. [#] See also :ref:`replication-endpoints` in the reference documentation.
