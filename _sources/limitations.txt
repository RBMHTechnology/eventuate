-----------
Limitations
-----------

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

- The only event log storage backend supported at the moment is LevelDB_. There will be a plugin API in the future together with a default implementation of a replicated storage backend.

- ...

.. _LevelDB: https://github.com/google/leveldb
.. _let us know: https://groups.google.com/forum/#!forum/eventuate

