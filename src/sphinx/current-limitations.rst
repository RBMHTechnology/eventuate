.. _current-limitations:

-------------------
Current limitations
-------------------

The following is a list of known limitations that are going to be addressed in future versions. If some of these limitations are particularly painful to you, please `let us know`_ so that we can target them accordingly.

- The current version of Eventuate does not support cycles in the replication network\ [#]_. An example of a valid replication network topology is::

    A        E
     \      /
      C -- D
     /      \
    B        F

  whereas the following topology is not valid::

    A ------ E
     \      /
      C -- D
     /      \
    B        F

- Replication network topologies must be statically defined at the moment. Dynamic changes, such add adding new locations at runtime, will be supported in future versions. We also think about an integration with `Akka Cluster`_.

- The storage plugin API is not public yet. Applications can currently choose between a LevelDB_ and Cassandra_ storage backend. The drivers for these backends are packaged with Eventuate and section :ref:`event-log` describes their usage.

- :ref:`snapshots` are written to the local filesystem at the moment. Applications that need to share snapshots across nodes should consider storing them on NFS or replicate them with ``rsync``, for example.

- No performance optimizations have been made yet.

.. [#] In the example figures, a line between two locations represents a bi-directional replication link. Such a link is not considered to be a cycle.

.. _Akka Cluster: http://doc.akka.io/docs/akka/2.3.9/scala/cluster-usage.html
.. _Akka Remoting: http://doc.akka.io/docs/akka/2.3.9/scala/remoting.html
.. _Cassandra: http://cassandra.apache.org/
.. _LevelDB: https://github.com/google/leveldb
.. _NAT: http://de.wikipedia.org/wiki/Network_Address_Translation
.. _let us know: https://groups.google.com/forum/#!forum/eventuate

.. _direct event routing: https://github.com/RBMHTechnology/eventuate/issues/45
.. _advanced event routing: https://github.com/RBMHTechnology/eventuate/issues/46
