--------
Download
--------

Maven repository
----------------

Eventuate snapshot JARs are currently published to the `OJO snapshot repository`_.

Maven dependency
----------------

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

SBT dependency
--------------

To include the latest development snapshot into an SBT project, add the following to your ``build.sbt``::

    resolvers += "OJO Snapshots" at "https://oss.jfrog.org/oss-snapshot-local"

    libraryDependencies += "com.rbmhtechnology" %% "eventuate" % "0.1-SNAPSHOT"

.. _OJO snapshot repository: https://oss.jfrog.org/oss-snapshot-local/com/rbmhtechnology/eventuate_2.11/
