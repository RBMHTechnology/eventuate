.. _current-limitations:

-------------------
Current limitations
-------------------

The following is a list of known limitations that are going to be addressed in future versions. If any of these limitations are particularly painful to you, please `let us know`_ so that we can target them accordingly.

- The current version of Eventuate does not support cycles in the replication network. An example of a valid replication network topology is::

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

- Event replication is currently based on `Akka Remoting`_ which doesn't support NAT_, for example. We are investigating alternatives.

- The only supported event log storage backend at the moment is LevelDB_. There will be a plugin API in the future together with a default implementation of a replicated storage backend.

- Event routing is limited at the moment. In addition to event broadcast and ``aggregateId``-based collaboration groups, there is `direct event routing`_ supported but `advanced event routing`_ will come in a later release.

- No performance optimizations have been made yet.

- :ref:`snapshots` are not supported yet.

.. _Akka Cluster: http://doc.akka.io/docs/akka/2.3.9/scala/cluster-usage.html
.. _Akka Remoting: http://doc.akka.io/docs/akka/2.3.9/scala/remoting.html
.. _LevelDB: https://github.com/google/leveldb
.. _NAT: http://de.wikipedia.org/wiki/Network_Address_Translation
.. _let us know: https://groups.google.com/forum/#!forum/eventuate

.. _direct event routing: https://github.com/RBMHTechnology/eventuate/issues/45
.. _advanced event routing: https://github.com/RBMHTechnology/eventuate/issues/46
