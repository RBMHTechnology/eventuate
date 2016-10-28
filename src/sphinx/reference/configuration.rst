.. _configuration:

-------------
Configuration
-------------

This is the reference configuration of Eventuate. It is processed by Typesafe's config_ library and can be overridden by applications:

eventuate-core
--------------

.. literalinclude:: ../../../eventuate-core/src/main/resources/reference.conf

eventuate-crdt
--------------

.. literalinclude:: ../../../eventuate-crdt/src/main/resources/reference.conf

eventuate-log-cassandra
-----------------------

.. literalinclude:: ../../../eventuate-log-cassandra/src/main/resources/reference.conf

eventuate-log-leveldb
---------------------

.. literalinclude:: ../../../eventuate-log-leveldb/src/main/resources/reference.conf

eventuate-adapter-stream
------------------------

.. literalinclude:: ../../../eventuate-adapter-stream/src/main/resources/reference.conf

.. _config: https://github.com/typesafehub/config
