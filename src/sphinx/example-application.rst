.. _example-application:

-------------------
Example application
-------------------

The example application is an over-simplified order management application that allows users to add and remove items from orders via a command-line interface. Order instances are replicated across locations and can be updated at any location. Orders remain writeable during partitions and conflicting updates can be resolved interactively when the partition heals. The example application source code is available as `Scala version`_ and as `Java 8 version`_.

.. hint::
   There is a tutorial-style `activator`_ template for the scala-version that guides you through the code: `akka-eventuate-scala`_.

Domain
------

The ``Order`` domain object is defined as follows:

.. includecode:: ../test/scala/com/rbmhtechnology/example/Order.scala
   :snippet: order-definition

Order creation and updates are tracked as events in a replicated event log. At each location, there is one event-sourced ``OrderActor`` instance per created ``Order`` instance and one event-sourced ``OrderView`` instance that counts the updates made to all orders.

Replication
-----------

``OrderActor``\ s are replicated across locations A - F, each running as separate process on ``localhost`` (see section :ref:`running`)::

    A        E
     \      /    
      C -- D
     /      \
    B        F

Each location can create and update orders, even under presence of partitions. In the example application, partitions can be created by shutting down locations. For example, when shutting down location ``C``, partitions ``A``, ``B`` and ``E-D-F`` are created. 

Concurrent updates to orders with different order id do not conflict. Concurrent updates to replicas of the same order are considered as conflict and must be resolved by the user, otherwise, further updates to that order are rejected. Updates are only rejected if a conflict is already *visible* to a location.

Interface
---------

Each location of the example application has a simple command-line interface to create and update orders:

- ``create <order-id>`` creates an order with given ``order-id``.
- ``add <order-id> <item>`` adds ``item`` to an order's items list.
- ``remove <order-id> <item>`` removes ``item`` from an order's items list.
- ``cancel <order-id>`` cancels an order.
- ``save <order-id>`` saves a snapshot of an order.

If there’s a conflict from a concurrent update, the conflict must be resolved by selecting one of the conflicting versions:

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

    sbt test:compile

from the project's root directory (needs to be done only once). Then, run::

    ./example

This should open six terminal windows, representing locations A - F. For running the Java version of the example application run::

    ./example java

Create and update some orders and see how changes are propagated to other locations. To make concurrent updates to an order, for example, enter ``exit`` at location ``C``, and add different items to that order at locations ``A`` and ``F``. When starting location ``C`` again with:: 

    ./example-location A

or the Java version with::

    ./example-location A java

both updates propagate to all other locations which are then displayed as conflict. Resolve the conflict with the ``resolve`` command. Conflict resolution writes a conflict resolution event to the replicated event log so that the conflict is automatically resolved at all locations.

.. [#] This is a static rule for distributed agreement which doesn’t require coordination among locations.

.. _sbt: http://www.scala-sbt.org/

.. _Scala version: https://github.com/RBMHTechnology/eventuate/tree/master/src/test/scala/com/rbmhtechnology/example
.. _Java 8 version: https://github.com/RBMHTechnology/eventuate/tree/master/src/test/java/com/rbmhtechnology/example/japi
.. _activator: https://www.typesafe.com/community/core-tools/activator-and-sbt
.. _akka-eventuate-scala: https://www.typesafe.com/activator/template/akka-eventuate-scala
