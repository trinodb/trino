# JDBC driver

The Trino [JDBC driver](https://wikipedia.org/wiki/JDBC_driver) allows
users to access Trino using Java-based applications, and other non-Java
applications running in a JVM. Both desktop and server-side applications, such
as those used for reporting and database development, use the JDBC driver.

The JDBC driver uses the [](/client/client-protocol) over HTTP/HTTPS to
communicate with the coordinator on the cluster.

## Requirements

The Trino JDBC driver has the following requirements:

- Java version 8 or higher. Java 22 or higher is recommended for improved
  decompression performance.
- All users that connect to Trino with the JDBC driver must be granted access to
  query tables in the `system.jdbc` schema.
- Network access over HTTP/HTTPS to the coordinator of the Trino cluster.
- Network access to the configured object storage, if the
  [](jdbc-spooling-protocol) is enabled.

The JDBC driver version should be identical to the version of the Trino cluster,
or newer. Older versions typically work, but only a subset is regularly tested.
Versions before 350 are not supported.

(jdbc-installation)=
## Installation

Download {maven_download}`jdbc` and add it to the classpath of your Java application.

The driver is also available from Maven Central:

```xml
<dependency>
    <groupId>io.trino</groupId>
    <artifactId>trino-jdbc</artifactId>
    <version>|trino_version|</version>
</dependency>
```

We recommend using the latest version of the JDBC driver. A list of all
available versions can be found in the [Maven Central Repository](https://repo1.maven.org/maven2/io/trino/trino-jdbc/). Navigate to the
directory for the desired version, and select the `trino-jdbc-xxx.jar` file
to download, where `xxx` is the version number.

Once downloaded, you must add the JAR file to a directory in the classpath
of users on systems where they will access Trino.

After you have downloaded the JDBC driver and added it to your
classpath, you'll typically need to restart your application in order to
recognize the new driver. Then, depending on your application, you
may need to manually register and configure the driver.

## Registering and configuring the driver

Drivers are commonly loaded automatically by applications once they are added to
its classpath. If your application does not, such as is the case for some
GUI-based SQL editors, read this section. The steps to register the JDBC driver
in a UI or on the command line depend upon the specific application you are
using. Please check your application's documentation.

Once registered, you must also configure the connection information as described
in the following section.

## Connecting

When your driver is loaded, registered and configured, you are ready to connect
to Trino from your application. The following JDBC URL formats are supported:

```text
jdbc:trino://host:port
jdbc:trino://host:port/catalog
jdbc:trino://host:port/catalog/schema
```

The value for `port` is optional if Trino is available at the default HTTP port
`80` or with `SSL=true` and the default HTTPS port `443`.

The following is an example of a JDBC URL used to create a connection:

```text
jdbc:trino://example.net:8080/hive/sales
```

This example JDBC URL locates a Trino instance running on port `8080` on
`example.net`, with the catalog `hive` and the schema `sales` defined.

:::{note}
Typically, the JDBC driver classname is configured automatically by your
client. If it is not, use `io.trino.jdbc.TrinoDriver` wherever a driver
classname is required.
:::

(jdbc-java-connection)=
## Connection parameters

The driver supports various parameters that may be set as URL parameters,
or as properties passed to `DriverManager`. Both of the following
examples are equivalent:

```java
// properties
String url = "jdbc:trino://example.net:8080/hive/sales";
Properties properties = new Properties();
properties.setProperty("user", "test");
properties.setProperty("password", "secret");
properties.setProperty("SSL", "true");
Connection connection = DriverManager.getConnection(url, properties);

// URL parameters
String url = "jdbc:trino://example.net:8443/hive/sales?user=test&password=secret&SSL=true";
Connection connection = DriverManager.getConnection(url);
```

These methods may be mixed; some parameters may be specified in the URL,
while others are specified using properties. However, the same parameter
may not be specified using both methods.

(jdbc-parameter-reference)=
## Parameter reference

:::{list-table}
:widths: 35, 65
:header-rows: 1

* - Name
  - Description
* - `user`
  - Username to use for authentication and authorization.
* - `password`
  - Password to use for LDAP authentication.
* - `sessionUser`
  - Session username override, used for impersonation.
* - `socksProxy`
  - SOCKS proxy host and port. Example: `localhost:1080`
* - `httpProxy`
  - HTTP proxy host and port. Example: `localhost:8888`
* - `clientInfo`
  - Extra information about the client.
* - `clientTags`
  - Client tags for selecting resource groups. Example: `abc,xyz`
* - `path`
  - Set the default [SQL path](/sql/set-path) for the session. Useful for
    setting a catalog and schema location for [](udf-catalog).
* - `traceToken`
  - Trace token for correlating requests across systems.
* - `source`
  - Source name for the Trino query. This parameter should be used in preference
    to `ApplicationName`. Thus, it takes precedence over `ApplicationName`
    and/or `applicationNamePrefix`.
* - `applicationNamePrefix`
  - Prefix to append to any specified `ApplicationName` client info property,
    which is used to set the source name for the Trino query if the `source`
    parameter has not been set. If neither this property nor `ApplicationName`
    or `source` are set, the source name for the query is `trino-jdbc`.
* - `accessToken`
  - [JWT](/security/jwt) access token for token based authentication.
* - `SSL`
  - Set `true` to specify using TLS/HTTPS for connections.
* - `SSLVerification`
  - The method of TLS verification. There are three modes: `FULL`
    (default), `CA` and `NONE`. For `FULL`, the normal TLS verification
    is performed. For `CA`, only the CA is verified but hostname mismatch
    is allowed. For `NONE`, there is no verification.
* - `SSLKeyStorePath`
  - Use only when connecting to a Trino cluster that has [certificate
    authentication](/security/certificate) enabled. Specifies the path to a
    [PEM](/security/inspect-pem) or [JKS](/security/inspect-jks) file, which must
    contain a certificate that is trusted by the Trino cluster you connect to.
* - `SSLKeyStorePassword`
  - The password for the KeyStore, if any.
* - `SSLKeyStoreType`
  - The type of the KeyStore. The default type is provided by the Java
    `keystore.type` security property or `jks` if none exists.
* - `SSLUseSystemKeyStore`
  - Set `true` to automatically use the system KeyStore based on the operating
    system. The supported OSes are Windows and macOS. For Windows, the
    `Windows-MY` KeyStore is selected. For macOS, the `KeychainStore`
    KeyStore is selected. For other OSes, the default Java KeyStore is loaded.
    The KeyStore specification can be overridden using `SSLKeyStoreType`.
* - `SSLTrustStorePath`
  - The location of the Java TrustStore file to use to validate HTTPS server
    certificates.
* - `SSLTrustStorePassword`
  - The password for the TrustStore.
* - `SSLTrustStoreType`
  - The type of the TrustStore. The default type is provided by the Java
    `keystore.type` security property or `jks` if none exists.
* - `SSLUseSystemTrustStore`
  - Set `true` to automatically use the system TrustStore based on the operating
    system. The supported OSes are Windows and macOS. For Windows, the
    `Windows-ROOT` TrustStore is selected. For macOS, the `KeychainStore`
    TrustStore is selected. For other OSes, the default Java TrustStore is
    loaded. The TrustStore specification can be overridden using
    `SSLTrustStoreType`.
* - `hostnameInCertificate`
  - Expected hostname in the certificate presented by the Trino server. Only
    applicable with full SSL verification enabled.
* - `KerberosRemoteServiceName`
  - Trino coordinator Kerberos service name. This parameter is required for
    Kerberos authentication.
* - `KerberosPrincipal`
  - The principal to use when authenticating to the Trino coordinator.
* - `KerberosUseCanonicalHostname`
  - Use the canonical hostname of the Trino coordinator for the Kerberos service
    principal by first resolving the hostname to an IP address and then doing a
    reverse DNS lookup for that IP address. This is enabled by default.
* - `KerberosServicePrincipalPattern`
  - Trino coordinator Kerberos service principal pattern. The default is
    `${SERVICE}@${HOST}`. `${SERVICE}` is replaced with the value of
    `KerberosRemoteServiceName` and `${HOST}` is replaced with the hostname of
    the coordinator (after canonicalization if enabled).
* - `KerberosConfigPath`
  - Kerberos configuration file.
* - `KerberosKeytabPath`
  - Kerberos keytab file.
* - `KerberosCredentialCachePath`
  - Kerberos credential cache.
* - `KerberosDelegation`
  - Set to `true` to use the token from an existing Kerberos context. This
    allows client to use Kerberos authentication without passing the Keytab or
    credential cache. Defaults to `false`.
* - `extraCredentials`
  - Extra credentials for connecting to external services, specified as a list
    of key-value pairs. For example, `foo:bar;abc:xyz` creates the credential
    named `abc` with value `xyz` and the credential named `foo` with value
    `bar`.
* - `roles`
  - Authorization roles to use for catalogs, specified as a list of key-value
    pairs for the catalog and role. For example, `catalog1:roleA;catalog2:roleB`
    sets `roleA` for `catalog1` and `roleB` for `catalog2`.
* - `sessionProperties`
  - Session properties to set for the system and for catalogs, specified as a
    list of key-value pairs. For example, `abc:xyz;example.foo:bar` sets the
    system property `abc` to the value `xyz` and the `foo` property for catalog
    `example` to the value `bar`.
* - `externalAuthentication`
  - Set to true if you want to use external authentication via
    [](/security/oauth2). Use a local web browser to authenticate with an
    identity provider (IdP) that has been configured for the Trino coordinator.
* - `externalAuthenticationTokenCache`
  - Allows the sharing of external authentication tokens between different
    connections for the same authenticated user until the cache is invalidated,
    such as when a client is restarted or when the classloader reloads the JDBC
    driver. This is disabled by default, with a value of `NONE`. To enable, set
    the value to `MEMORY`. If the JDBC driver is used in a shared mode by
    different users, the first registered token is stored and authenticates all
    users.
* - `disableCompression`
  -  Whether compression should be enabled.
* - `assumeLiteralUnderscoreInMetadataCallsForNonConformingClients`
  - When enabled, the name patterns passed to `DatabaseMetaData` methods are
    treated as underscores. You can use this as a workaround for applications
    that do not escape schema or table names when passing them to
    `DatabaseMetaData` methods as schema or table name patterns.
* - `timezone`
  - Sets the time zone for the session using the [time zone
    passed](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/ZoneId.html#of(java.lang.String)).
    Defaults to the timezone of the JVM running the JDBC driver.
* - `explicitPrepare`
  - Defaults to `true`. When set to `false`, prepared statements are executed
    calling a single `EXECUTE IMMEDIATE` query instead of the standard
    `PREPARE <statement>` followed by `EXECUTE <statement>`. This reduces
    network overhead and uses smaller HTTP headers and requires Trino 431 or
    greater.
* - `encoding`
  - Set the encoding when using the [spooling protocol](jdbc-spooling-protocol).
    Valid values are JSON with Zstandard compression, `json+zstd` (recommended),
    JSON with LZ4 compression `json+lz4`, and uncompressed JSON `json`. By
    default, the default encoding configured on the cluster is used.
* - `validateConnection`
  - Defaults to `false`. If set to `true`, connectivity and credentials are validated 
    when the connection is created, and when `java.sql.Connection.isValid(int)` is called.
:::

(jdbc-spooling-protocol)=
## Spooling protocol

The Trino JDBC driver automatically uses of the spooling protocol to improve
throughput for client interactions with higher data transfer demands, if the
[](protocol-spooling) is configured on the cluster.

Optionally use the `encoding` parameter to configure a different desired
encoding, compared to the default on the cluster.

The JVM process using the JDBC driver must have network access to the spooling
object storage.
