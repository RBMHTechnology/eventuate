.. _transport-security:

Transport Security
------------------

In a setup with several locations Eventuate uses `Akka Remoting`_ for communication between locations. Akka Remoting supports using TLS as transport protocol. In combination with mutual authentication (client and server) this allows to prevent that untrusted nodes can connect to a replication network. The configuration snippet listed here acts as an example for how to set this up. For more details see `Configuring SSL/TLS for Akka Remoting`_.

.. literalinclude:: ../conf/tls.conf

This example uses a single file for key- and trust-store. Having a single self-signed certificate in this store suffices to establish mutually authenticated connections via TLS between locations. However a self-signed key makes replacing the certificate in a rolling upgrade tedious. The following script (derived from the scripts of the documentation of `Lightbend's SSL-Config library`_) creates a root certificate that is used to sign the certificate for client and server authentication. The root certificate is kept in the private keystore ``rootca.jks``, which does not have to be deployed with the application. It is imported as ``trustedCertEntry`` into the keystore ``keystore.jks``, which is the one referenced by the akka configuration.

.. literalinclude:: ../code/create-keystore.sh

If the certificate for client and server authentication shall be replaced, corresponding key-stores can be deployed location by location without the need to stop the entire replication network at once.

For debugging TLS connections please read `Debugging SSL/TLS Connections`_ from the java documentation.

.. _configuration:

Configuration
-------------

This is the reference configuration of Eventuate. It is processed by Typesafe's config_ library and can be overridden by applications:

eventuate-core
~~~~~~~~~~~~~~

.. literalinclude:: ../../../eventuate-core/src/main/resources/reference.conf

eventuate-crdt
~~~~~~~~~~~~~~

.. literalinclude:: ../../../eventuate-crdt/src/main/resources/reference.conf

eventuate-log-cassandra
~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../../eventuate-log-cassandra/src/main/resources/reference.conf

eventuate-log-leveldb
~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../../eventuate-log-leveldb/src/main/resources/reference.conf

.. _config: https://github.com/typesafehub/config
.. _Akka Remoting: http://doc.akka.io/docs/akka/2.4/scala/remoting.html
.. _Configuring SSL/TLS for Akka Remoting: http://doc.akka.io/docs/akka/2.4/scala/remoting.html#Configuring_SSL_TLS_for_Akka_Remoting
.. _Lightbend's SSL-Config library: http://typesafehub.github.io/ssl-config/CertificateGeneration.html#using-keytool
.. _Debugging SSL/TLS Connections: http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/ReadDebug.html