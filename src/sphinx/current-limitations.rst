.. _current-limitations:

-------------------
Current Limitations
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

- The only supported event log storage backend at the moment is LevelDB_. There will be a plugin API in the future together with a default implementation of a replicated storage backend.

- Event routing is limited at the moment. In addition to event broadcast and ``aggregateId``-based collaboration groups, there is `direct event routing`_ supported but `advanced event routing`_ will come in a later release.

.. _LevelDB: https://github.com/google/leveldb
.. _let us know: https://groups.google.com/forum/#!forum/eventuate

.. _direct event routing: https://github.com/RBMHTechnology/eventuate/issues/45
.. _advanced event routing: https://github.com/RBMHTechnology/eventuate/issues/46
