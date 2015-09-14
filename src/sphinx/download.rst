--------
Download
--------

Binaries
--------

Release binaries are published to Bintray_, snapshot binaries to OJO_ (oss.jfrog.org)

Maven
~~~~~

To include the latest release into a Maven project, add the following to your ``pom.xml``::

    <repository>
        <id>eventuate-releases</id>
        <name>Eventuate Releases</name>
        <url>https://dl.bintray.com/rbmhtechnology/maven</url>
    </repository>

    <dependency>
        <groupId>com.rbmhtechnology</groupId>
        <artifactId>eventuate_2.11</artifactId>
        <version>0.3</version>
    </dependency>

To include the latest development snapshot::

    <repository>
        <id>ojo-snapshots</id>
        <name>OJO Snapshots</name>
        <url>https://oss.jfrog.org/oss-snapshot-local</url>
    </repository>

    <dependency>
        <groupId>com.rbmhtechnology</groupId>
        <artifactId>eventuate_2.11</artifactId>
        <version>0.4-SNAPSHOT</version>
    </dependency>

SBT
~~~

To include the latest release into an sbt_ project, add the following to your ``build.sbt``::

    resolvers += "Eventuate Releases" at "https://dl.bintray.com/rbmhtechnology/maven"

    libraryDependencies += "com.rbmhtechnology" %% "eventuate" % "0.3"

To include the latest development snapshot::

    resolvers += "OJO Snapshots" at "https://oss.jfrog.org/oss-snapshot-local"

    libraryDependencies += "com.rbmhtechnology" %% "eventuate" % "0.4-SNAPSHOT"

Sources
-------

To download the Eventuate sources, clone the `Github repository`_. Source jar files are also published to Bintray_ and OJO_.

.. _OJO: http://oss.jfrog.org/artifactory/simple/oss-snapshot-local/
.. _Bintray: https://bintray.com/rbmhtechnology/maven/eventuate
.. _Github repository: https://github.com/RBMHTechnology/eventuate

.. _sbt: http://www.scala-sbt.org/
