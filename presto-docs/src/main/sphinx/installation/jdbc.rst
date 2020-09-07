===========
JDBC Driver
===========

Presto can be accessed from Java using the JDBC driver.
Download :maven_download:`jdbc` and add it to the classpath of your Java application.

The driver is also available from Maven Central:

.. parsed-literal::

    <dependency>
        <groupId>io.prestosql</groupId>
        <artifactId>presto-jdbc</artifactId>
        <version>\ |version|\ </version>
    </dependency>

Requirements
------------

The JDBC driver is compatible with Java 8, and can be used with applications
running on Java virtual machines version 8 and higher.

Driver Name
-----------

The driver class name is ``io.prestosql.jdbc.PrestoDriver``.
Most users do not need this information as drivers are loaded automatically.

Connecting
----------

The following JDBC URL formats are supported:

.. code-block:: none

    jdbc:presto://host:port
    jdbc:presto://host:port/catalog
    jdbc:presto://host:port/catalog/schema

For example, use the following URL to connect to Presto
running on ``example.net`` port ``8080`` with the catalog ``hive``
and the schema ``sales``:

.. code-block:: none

    jdbc:presto://example.net:8080/hive/sales

The above URL can be used as follows to create a connection:

.. code-block:: java

    String url = "jdbc:presto://example.net:8080/hive/sales";
    Connection connection = DriverManager.getConnection(url, "test", null);

Connection Parameters
---------------------

The driver supports various parameters that may be set as URL parameters,
or as properties passed to ``DriverManager``. Both of the following
examples are equivalent:

.. code-block:: java

    // URL parameters
    String url = "jdbc:presto://example.net:8080/hive/sales";
    Properties properties = new Properties();
    properties.setProperty("user", "test");
    properties.setProperty("password", "secret");
    properties.setProperty("SSL", "true");
    Connection connection = DriverManager.getConnection(url, properties);

    // properties
    String url = "jdbc:presto://example.net:8080/hive/sales?user=test&password=secret&SSL=true";
    Connection connection = DriverManager.getConnection(url);

These methods may be mixed; some parameters may be specified in the URL,
while others are specified using properties. However, the same parameter
may not be specified using both methods.

Parameter Reference
-------------------

====================================== =======================================================================
Name                                   Description
====================================== =======================================================================
``user``                               Username to use for authentication and authorization.
``password``                           Password to use for LDAP authentication.
``socksProxy``                         SOCKS proxy host and port. Example: ``localhost:1080``
``httpProxy``                          HTTP proxy host and port. Example: ``localhost:8888``
``clientInfo``                         Extra information about the client.
``clientTags``                         Client tags for selecting resource groups. Example: ``abc,xyz``
``traceToken``                         Trace token for correlating requests across systems.
``applicationNamePrefix``              Prefix to append to any specified ``ApplicationName`` client info
                                       property, which is used to set the source name for the Presto query.
                                       If neither this property nor ``ApplicationName`` are set, the source
                                       for the query is ``presto-jdbc``.
``accessToken``                        Access token for token based authentication.
``SSL``                                Use HTTPS for connections
``SSLKeyStorePath``                    The location of the Java KeyStore file that contains the certificate
                                       and private key to use for authentication.
``SSLKeyStorePassword``                The password for the KeyStore.
``SSLKeyStoreType``                    The type of the KeyStore. The default type is provided by the Java
                                       ``keystore.type`` security property or ``jks`` if none exists.
``SSLTrustStorePath``                  The location of the Java TrustStore file to use.
                                       to validate HTTPS server certificates.
``SSLTrustStorePassword``              The password for the TrustStore.
``SSLTrustStoreType``                  The type of the TrustStore. The default type is provided by the Java
                                       ``keystore.type`` security property or ``jks`` if none exists.
``KerberosRemoteServiceName``          Presto coordinator Kerberos service name. This parameter is
                                       required for Kerberos authentication.
``KerberosPrincipal``                  The principal to use when authenticating to the Presto coordinator.
``KerberosUseCanonicalHostname``       Use the canonical hostname of the Presto coordinator for the Kerberos
                                       service principal by first resolving the hostname to an IP address
                                       and then doing a reverse DNS lookup for that IP address.
                                       This is enabled by default.
``KerberosServicePrincipalPattern``    Presto coordinator Kerberos service principal pattern. The default is
                                       ``${SERVICE}@${HOST}``. ``${SERVICE}`` is replaced with the value of
                                       ``KerberosRemoteServiceName`` and ``${HOST}`` is replaced with the
                                       hostname of the coordinator (after canonicalization if enabled).
``KerberosConfigPath``                 Kerberos configuration file.
``KerberosKeytabPath``                 Kerberos keytab file.
``KerberosCredentialCachePath``        Kerberos credential cache.
``useSessionTimeZone``                 Should dates and timestamps use the session time zone (default: false).
                                       Note that this property only exists for backward compatibility with the
                                       previous behavior and will be removed in the future.
``extraCredentials``                   Extra credentials for connecting to external services,
                                       specified as a list of key-value pairs. For example,
                                       ``foo:bar;abc:xyz`` creates the credential named ``abc``
                                       with value ``xyz`` and the credential named ``foo`` with value ``bar``.
``roles``                              Authorization roles to use for catalogs, specified as a list of
                                       key-value pairs for the catalog and role. For example,
                                       ``catalog1:roleA;catalog2:roleB`` sets ``roleA``
                                       for ``catalog1`` and ``roleB`` for ``catalog2``.
``sessionProperties``                  Session properties to set for the system and for catalogs,
                                       specified as a list of key-value pairs.
                                       For example, ``abc:xyz;example.foo:bar`` sets the system property
                                       ``abc`` to the value ``xyz`` and the ``foo`` property for
                                       catalog ``example`` to the value ``bar``.
====================================== =======================================================================
