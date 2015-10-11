.. _current-limitations:

-------------------
Current limitations
-------------------

The following is a list of known limitations that are going to be addressed in future versions. If some of these limitations are particularly painful to you, please `let us know`_ so that we can target them accordingly.

- Replication network topologies must be statically defined at the moment. Dynamic changes, such as adding new locations or changing the replication network topology at runtime, will be supported in future versions. We also think about an integration with `Akka Cluster`_.

- There is no storage plugin API at the moment. Applications can currently choose between a LevelDB_ and Cassandra_ storage backend. The drivers for these backends are packaged with Eventuate and section :ref:`event-log` describes their usage.

- :ref:`snapshots` are written to the local filesystem at the moment. Applications that need to share snapshots across nodes should consider storing them on NFS or replicate them with ``rsync``, for example.

- No performance optimizations have been made yet.

.. _Akka Cluster: http://doc.akka.io/docs/akka/2.3.9/scala/cluster-usage.html
.. _Akka Remoting: http://doc.akka.io/docs/akka/2.3.9/scala/remoting.html
.. _Cassandra: http://cassandra.apache.org/
.. _LevelDB: https://github.com/google/leveldb
.. _NAT: http://de.wikipedia.org/wiki/Network_Address_Translation
.. _let us know: https://groups.google.com/forum/#!forum/eventuate

.. _direct event routing: https://github.com/RBMHTechnology/eventuate/issues/45
.. _advanced event routing: https://github.com/RBMHTechnology/eventuate/issues/46
