.. _example-application:

-------------------
Example application
-------------------

.. hint::
   The example application is rather outdated and we plan to design and implement a better one to demonstrate the latest Eventuate features and best practices. If you have any preferences what you'd like to see in a new example application, please add a comment to ticket `#337`_ or contact us in the forum_.

The example application is an over-simplified order management application that allows users to add and remove items from orders via a command-line interface. Order instances are replicated across locations and can be updated at any location. Orders remain writeable during partitions and conflicting updates can be resolved interactively when the partition heals. The example application source code is available as `Scala version`_ and as `Java 8 version`_.

.. note::
   This example application was created to demonstrate *interactive conflict resolution*. A more realistic order management application would probably use automated conflict resolution using a `shopping cart CRDT`_, for example.

.. hint::
   There is a tutorial-style `activator`_ template for the scala-version that guides you through the code: `akka-eventuate-scala`_.

Domain
------

The ``Order`` domain object is defined as follows:

.. includecode:: ../../eventuate-examples/src/main/scala/com/rbmhtechnology/example/ordermgnt/Order.scala
   :snippet: order-definition

Order creation and updates are tracked as events in a replicated event log. At each location, there is one event-sourced ``OrderActor`` instance per created ``Order`` instance and one event-sourced ``OrderView`` instance that counts the updates made to all orders.

Replication
-----------

``OrderActor``\ s are replicated across locations A - F, each running as separate process on ``localhost`` (see section :ref:`running`)::

    A ------ E
     \      /    
      C -- D
     /      \
    B        F

Each location can create and update orders, even under presence of partitions. In the example application, partitions can be created by shutting down locations. For example, when shutting down location ``C``, partitions ``B`` and ``A-E-D-F`` are created. 

Concurrent updates to orders with different order id do not conflict. Concurrent updates to replicas of the same order are considered as conflict and must be resolved by the user, otherwise, further updates to that order are rejected. Updates are only rejected if a conflict is already *visible* to a location.

Interface
---------

Each location of the example application has a simple command-line interface to create and update orders:

- ``create <order-id>`` creates an order with given ``order-id``.
- ``add <order-id> <item>`` adds ``item`` to an order's items list.
- ``remove <order-id> <item>`` removes ``item`` from an order's items list.
- ``cancel <order-id>`` cancels an order.
- ``save <order-id>`` saves a snapshot of an order.

If there’s a conflict from a concurrent update, the conflict must be resolved by selecting one of the conflicting versions\ [#]_:

- ``resolve <order-id> <index>`` resolves a conflict by selecting a version ``index``. Only the location that initially created the order can resolve the conflict\ [#]_.

Other commands are:

- ``count <order-id>`` prints the number of updates to an order, including creation. Update counts are maintained by a separate view that consumes order events from all ``OrderActor``\ s on the replicated event log. 
- ``state`` prints the current state i.e. all orders at a location. Location state may differ during partitions.
- ``exit`` stops a location and the replication connections to and from that location.

Update results from user commands and replicated events are written to `stdout`. A location’s current state is also printed after successful recovery.

.. _running:

Running
-------

Before you can run the example application, install sbt_ and run::

    sbt compile

from the project's root directory (needs to be done only once). Then, run::

    ./eventuate-example/bin/ordermgnt

This should open six terminal windows, representing locations A - F. For running the Java version of the example application use the ``-j`` or ``--java`` option::

    ./eventuate-example/bin/ordermgnt --java

Create and update some orders and see how changes are propagated to other locations. To make concurrent updates to an order, for example, enter ``exit`` at location ``C``, and add different items to that order at locations ``B`` and ``F``. When starting location ``C`` again with:: 

    ./eventuate-example/bin/ordermgnt-location A

or the Java version with::

    ./eventuate-example/bin/ordermgnt-location --java A

both updates propagate to all other locations which are then displayed as conflict. Resolve the conflict with the ``resolve`` command. Conflict resolution writes a conflict resolution event to the replicated event log so that the conflict is automatically resolved at all locations.

.. _example-disaster-recovery:

Disaster recovery
-----------------

:ref:`disaster-recovery` in the example application can be tested by removing the event log of a location and starting the location again with disaster recovery enabled. For example, to remove the event log at location ``C``, stop the location with ``exit`` and delete its LevelDB directory::

    rm -r eventuate-example/target/example-logs/s-C_default/

To delete the event log written by the Java version of the example application run::

    rm -r eventuate-example/target/example-logs/j-C_default/

To start location ``C`` again with disaster recovery enabled, use the ``-r`` or ``--recover`` option::

    ./example/ordermgnt-location --recover C

or the Java version with::

    ./eventuate-example/bin/ordermgnt-location --recover --java C

Recovery may take up to 20 seconds when using the default :ref:`configuration` settings for event replication and disaster recovery. To speed up the process you may want to the use following configuration settings::

    eventuate.log.replication.retry-delay = 1s
    eventuate.log.replication.remote-read-timeout = 2s
    eventuate.log.recovery.remote-operation-retry-max = 10
    eventuate.log.recovery.remote-operation-retry-delay = 1s
    eventuate.log.recovery.remote-operation-timeout = 1s

Disaster recovery can also start from a previous, older backup of the LevelDB directory. After having removed the current LevelDB directory, install the backup and try running disaster recovery again.

.. [#] Merging the content of conflicting versions is another option which will be supported in a later release (see `#101`_). 
.. [#] This is a static rule for distributed agreement which doesn’t require coordination among locations.

.. _sbt: http://www.scala-sbt.org/

.. _Scala version: https://github.com/RBMHTechnology/eventuate/tree/master/src/test/scala/com/rbmhtechnology/example/ordermgnt
.. _Java 8 version: https://github.com/RBMHTechnology/eventuate/tree/master/src/test/java/com/rbmhtechnology/example/japi/ordermgnt
.. _activator: https://www.typesafe.com/community/core-tools/activator-and-sbt
.. _akka-eventuate-scala: https://www.typesafe.com/activator/template/akka-eventuate-scala


.. _shopping cart CRDT: latest/api/index.html#com.rbmhtechnology.eventuate.crdt.ORCartService
.. _#337: https://github.com/RBMHTechnology/eventuate/issues/337
.. _#101: https://github.com/RBMHTechnology/eventuate/issues/101
.. _forum: https://gitter.im/RBMHTechnology/eventuate
