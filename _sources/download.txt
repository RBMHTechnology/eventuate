--------
Download
--------

Binaries
--------

Eventuate snapshots are published to the `OJO snapshot repository`_. There are no releases yet.

Maven
~~~~~

To include the latest development snapshot into a Maven project, add the following to your ``pom.xml``::

    <repository>
        <id>ojo-snapshots</id>
        <name>OJO Snapshots</name>
        <url>https://oss.jfrog.org/oss-snapshot-local</url>
    </repository>

    <dependency>
        <groupId>com.rbmhtechnology</groupId>
        <artifactId>eventuate_2.11</artifactId>
        <version>0.1-SNAPSHOT</version>
    </dependency>

SBT
~~~

To include the latest development snapshot into an SBT_ project, add the following to your ``build.sbt``::

    resolvers += "OJO Snapshots" at "https://oss.jfrog.org/oss-snapshot-local"

    libraryDependencies += "com.rbmhtechnology" %% "eventuate" % "0.1-SNAPSHOT"

Sources
-------

To download the Eventuate sources, clone the `Github repository`_.

.. _OJO snapshot repository: https://oss.jfrog.org/oss-snapshot-local/com/rbmhtechnology/eventuate_2.11/
.. _Github repository: https://github.com/RBMHTechnology/eventuate

.. _SBT: http://www.scala-sbt.org/
