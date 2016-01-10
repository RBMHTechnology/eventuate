.. _current-limitations:

-------------------
Current limitations
-------------------

The following is a list of known limitations that are going to be addressed in future versions. If some of these limitations are particularly painful to you, please `let us know`_ so that we can target them accordingly.

- Replication network topologies must be statically defined at the moment. Dynamic changes, such as adding new locations or changing the replication network topology at runtime, will be supported in future versions. We also think about an integration with `Akka Cluster`_.

- :ref:`snapshots` are written to the local filesystem at the moment. Applications that need to share snapshots across nodes should consider storing them on NFS or replicate them with ``rsync``, for example.

- No performance optimizations have been made yet.

.. _Akka Cluster: http://doc.akka.io/docs/akka/2.4.1/scala/cluster-usage.html
.. _let us know: https://groups.google.com/forum/#!forum/eventuate
