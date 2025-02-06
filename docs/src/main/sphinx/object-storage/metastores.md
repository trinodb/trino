# Metastores

Object storage access is mediated through a *metastore*. Metastores provide
information on directory structure, file format, and metadata about the stored
data. Object storage connectors support the use of one or more metastores. A
supported metastore is required to use any object storage connector.

Additional configuration is required in order to access tables with Athena
partition projection metadata or implement first class support for Avro tables.
These requirements are discussed later in this topic.

(general-metastore-properties)=
## General metastore configuration properties

The following table describes general metastore configuration properties, most
of which are used with either metastore.

At a minimum, each Delta Lake, Hive or Hudi object storage catalog file must set
the `hive.metastore` configuration property to define the type of metastore to
use. Iceberg catalogs instead use the `iceberg.catalog.type` configuration
property to define the type of metastore to use.

Additional configuration properties specific to the Thrift and Glue Metastores
are also available. They are discussed later in this topic.

:::{list-table} General metastore configuration properties
:widths: 35, 50, 15
:header-rows: 1

* - Property Name
  - Description
  - Default
* - `hive.metastore`
  - The type of Hive metastore to use. Trino currently supports the default Hive
    Thrift metastore (`thrift`), and the AWS Glue Catalog (`glue`) as metadata
    sources. You must use this for all object storage catalogs except Iceberg.
  - `thrift`
* - `iceberg.catalog.type`
  - The Iceberg table format manages most metadata in metadata files in the
    object storage itself. A small amount of metadata, however, still requires
    the use of a metastore. In the Iceberg ecosystem, these smaller metastores
    are called Iceberg metadata catalogs, or just catalogs. The examples in each
    subsection depict the contents of a Trino catalog file that uses the
    Iceberg connector to configures different Iceberg metadata catalogs.

    You must set this property in all Iceberg catalog property files. Valid
    values are `hive_metastore`, `glue`, `jdbc`, `rest`, `nessie`, and
    `snowflake`.
  - `hive_metastore`
* - `hive.metastore-cache.cache-partitions`
  - Enable caching for partition metadata. You can disable caching to avoid
    inconsistent behavior that results from it.
  - `true`
* - `hive.metastore-cache.cache-missing`
  - Enable caching the fact that a table is missing to prevent future metastore
    calls for that table.
  - `true`
* - `hive.metastore-cache.cache-missing-partitions`
  - Enable caching the fact that a partition is missing to prevent future
    metastore calls for that partition.
  - `false`
* - `hive.metastore-cache.cache-missing-stats`
  - Enable caching the fact that table statistics for a specific table are 
    missing to prevent future metastore calls.
  - `false`
* - `hive.metastore-cache-ttl`
  - [Duration](prop-type-duration) of how long cached metastore data is considered valid.
  - `0s`
* - `hive.metastore-stats-cache-ttl`
  - [Duration](prop-type-duration) of how long cached metastore statistics are considered valid.
  - `5m`
* - `hive.metastore-cache-maximum-size`
  - Maximum number of metastore data objects in the Hive metastore cache.
  - `20000`
* - `hive.metastore-refresh-interval`
  - Asynchronously refresh cached metastore data after access if it is older
    than this but is not yet expired, allowing subsequent accesses to see fresh
    data.
  -
* - `hive.metastore-refresh-max-threads`
  - Maximum threads used to refresh cached metastore data.
  - `10`
* - `hive.user-metastore-cache-ttl`
  - [Duration](prop-type-duration) of how long cached metastore statistics, which are user specific
    in user impersonation scenarios, are considered valid.
  - `10s`
* - `hive.user-metastore-cache-maximum-size`
  - Maximum number of metastore data objects in the Hive metastore cache,
    which are user specific in user impersonation scenarios.
  - `1000`
* - `hive.hide-delta-lake-tables`
  - Controls whether to hide Delta Lake tables in table listings. Currently
    applies only when using the AWS Glue metastore.
  - `false`
:::

(hive-thrift-metastore)=
## Thrift metastore configuration properties

In order to use a Hive Thrift metastore, you must configure the metastore with
`hive.metastore=thrift` and provide further details with the following
properties:

:::{list-table} Thrift metastore configuration properties
:widths: 35, 50, 15
:header-rows: 1

* - Property name
  - Description
  - Default
* - `hive.metastore.uri`
  - The URIs of the Hive metastore to connect to using the Thrift protocol.
    If a comma-separated list of URIs is provided, the first URI is used by
    default, and the rest of the URIs are fallback metastores. This property
    is required. Example: `thrift://192.0.2.3:9083` or
    `thrift://192.0.2.3:9083,thrift://192.0.2.4:9083`
  -
* - `hive.metastore.username`
  - The username Trino uses to access the Hive metastore.
  -
* - `hive.metastore.authentication.type`
  - Hive metastore authentication type. Possible values are `NONE` or
    `KERBEROS`.
  - `NONE`
* - `hive.metastore.thrift.client.connect-timeout`
  - Socket connect timeout for metastore client.
  - `10s`
* - `hive.metastore.thrift.client.read-timeout`
  - Socket read timeout for metastore client.
  - `10s`
* - `hive.metastore.thrift.impersonation.enabled`
  - Enable Hive metastore end user impersonation.
  -
* - `hive.metastore.thrift.use-spark-table-statistics-fallback`
  - Enable usage of table statistics generated by Apache Spark when Hive table
    statistics are not available.
  - `true`
* - `hive.metastore.thrift.delegation-token.cache-ttl`
  - Time to live delegation token cache for metastore.
  - `1h`
* - `hive.metastore.thrift.delegation-token.cache-maximum-size`
  - Delegation token cache maximum size.
  - `1000`
* - `hive.metastore.thrift.client.ssl.enabled`
  - Use SSL when connecting to metastore.
  - `false`
* - `hive.metastore.thrift.client.ssl.key`
  - Path to private key and client certification (key store).
  -
* - `hive.metastore.thrift.client.ssl.key-password`
  - Password for the private key.
  -
* - `hive.metastore.thrift.client.ssl.trust-certificate`
  - Path to the server certificate chain (trust store). Required when SSL is
    enabled.
  -
* - `hive.metastore.thrift.client.ssl.trust-certificate-password`
  - Password for the trust store.
  -
* - `hive.metastore.service.principal`
  - The Kerberos principal of the Hive metastore service.
  -
* - `hive.metastore.client.principal`
  - The Kerberos principal that Trino uses when connecting to the Hive metastore
    service.
  -
* - `hive.metastore.client.keytab`
  - Hive metastore client keytab location.
  -
* - `hive.metastore.thrift.delete-files-on-drop`
  - Actively delete the files for managed tables when performing drop table or
    partition operations, for cases when the metastore does not delete the
    files.
  - `false`
* - `hive.metastore.thrift.assume-canonical-partition-keys`
  - Allow the metastore to assume that the values of partition columns can be
    converted to string values. This can lead to performance improvements in
    queries which apply filters on the partition columns. Partition keys with a
    `TIMESTAMP` type do not get canonicalized.
  - `false`
* - `hive.metastore.thrift.client.socks-proxy`
  - SOCKS proxy to use for the Thrift Hive metastore.
  -
* - `hive.metastore.thrift.client.max-retries`
  - Maximum number of retry attempts for metastore requests.
  - `9`
* - `hive.metastore.thrift.client.backoff-scale-factor`
  - Scale factor for metastore request retry delay.
  - `2.0`
* - `hive.metastore.thrift.client.max-retry-time`
  - Total allowed time limit for a metastore request to be retried.
  - `30s`
* - `hive.metastore.thrift.client.min-backoff-delay`
  - Minimum delay between metastore request retries.
  - `1s`
* - `hive.metastore.thrift.client.max-backoff-delay`
  - Maximum delay between metastore request retries.
  - `1s`
* - `hive.metastore.thrift.txn-lock-max-wait`
  - Maximum time to wait to acquire hive transaction lock.
  - `10m`
* - `hive.metastore.thrift.catalog-name`
  - The term "Hive metastore catalog name" refers to the abstraction concept
    within Hive, enabling various systems to connect to distinct, independent
    catalogs stored in the metastore. By default, the catalog name in Hive
    metastore is set to "hive." When this configuration property is left empty,
    the default catalog of the Hive metastore will be accessed.
  -
:::

Use the following configuration properties for HTTP client transport mode, so
when the `hive.metastore.uri` uses the `http://` or `https://` protocol.

:::{list-table} Thrift metastore HTTP configuration properties
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `hive.metastore.http.client.authentication.type`
  - The authentication type to use with the HTTP client transport mode. When set
    to the only supported value of `BEARER`, the token configured in
    `hive.metastore.http.client.bearer-token` is used to authenticate to the
    metastore service.
* - `hive.metastore.http.client.bearer-token`
  - Bearer token to use for authentication with the metastore service when HTTPS
    transport mode is used by using a `https://` protocol in
    `hive.metastore.uri`. This must not be set with `http://`.
* - `hive.metastore.http.client.additional-headers`
  - Additional headers to send with metastore service requests. These headers
    must be comma-separated and delimited using `:`. For example,
    `header1:value1,header2:value2` sends two headers `header1` and `header2`
    with the values as `value1` and `value2`. Escape comma (`,`) or colon(`:`)
    characters in a header name or value with a backslash (`\`). Use
    `X-Databricks-Catalog-Name:[catalog_name]` to configure the required
    header values for Unity catalog.
:::

(hive-thrift-metastore-authentication)=
### Thrift metastore authentication

In a Kerberized Hadoop cluster, Trino connects to the Hive metastore Thrift
service using {abbr}`SASL (Simple Authentication and Security Layer)` and
authenticates using Kerberos. Kerberos authentication for the metastore is
configured in the connector's properties file using the following optional
properties:

:::{list-table} Hive metastore Thrift service authentication properties
:widths: 30, 55, 15
:header-rows: 1

* - Property value
  - Description
  - Default
* - `hive.metastore.authentication.type`
  - Hive metastore authentication type. One of `NONE` or `KERBEROS`. When using
    the default value of `NONE`, Kerberos authentication is disabled, and no
    other properties must be configured.

    When set to `KERBEROS` the Hive connector connects to the Hive metastore
    Thrift service using SASL and authenticate using Kerberos.
  - `NONE`
* - `hive.metastore.thrift.impersonation.enabled`
  - Enable Hive metastore end user impersonation. See
    [](hive-security-metastore-impersonation) for more information.
  - `false`
* - `hive.metastore.service.principal`
  - The Kerberos principal of the Hive metastore service. The coordinator uses
    this to authenticate the Hive metastore.

    The `_HOST` placeholder can be used in this property value. When connecting
    to the Hive metastore, the Hive connector substitutes in the hostname of the
    **metastore** server it is connecting to. This is useful if the metastore
    runs on multiple hosts.

    Example: `hive/hive-server-host@EXAMPLE.COM` or `hive/_HOST@EXAMPLE.COM`.
  -
* - `hive.metastore.client.principal`
  - The Kerberos principal that Trino uses when connecting to the Hive metastore
    service.

    Example: `trino/trino-server-node@EXAMPLE.COM` or `trino/_HOST@EXAMPLE.COM`.

    The `_HOST` placeholder can be used in this property value. When connecting
    to the Hive metastore, the Hive connector substitutes in the hostname of the
    **worker** node Trino is running on. This is useful if each worker node has
    its own Kerberos principal.

    Unless [](hive-security-metastore-impersonation) is enabled, the principal
    specified by `hive.metastore.client.principal` must have sufficient
    privileges to remove files and directories within the `hive/warehouse`
    directory.

    **Warning:** If the principal does have sufficient permissions, only the
    metadata is removed, and the data continues to consume disk space. This
    occurs because the Hive metastore is responsible for deleting the internal
    table data. When the metastore is configured to use Kerberos authentication,
    all of the HDFS operations performed by the metastore are impersonated.
    Errors deleting data are silently ignored.
  -
* - `hive.metastore.client.keytab`
  - The path to the keytab file that contains a key for the principal
      specified by `hive.metastore.client.principal`. This file must be
      readable by the operating system user running Trino.
  -
:::

The following sections describe the configuration properties and values needed
for the various authentication configurations needed to use the Hive metastore
Thrift service with the Hive connector.

#### Default `NONE` authentication without impersonation

```text
hive.metastore.authentication.type=NONE
```

The default authentication type for the Hive metastore is `NONE`. When the
authentication type is `NONE`, Trino connects to an unsecured Hive
metastore. Kerberos is not used.

(hive-security-metastore-impersonation)=
#### `KERBEROS` authentication with impersonation

```text
hive.metastore.authentication.type=KERBEROS
hive.metastore.thrift.impersonation.enabled=true
hive.metastore.service.principal=hive/hive-metastore-host.example.com@EXAMPLE.COM
hive.metastore.client.principal=trino@EXAMPLE.COM
hive.metastore.client.keytab=/etc/trino/hive.keytab
```

When the authentication type for the Hive metastore Thrift service is
`KERBEROS`, Trino connects as the Kerberos principal specified by the
property `hive.metastore.client.principal`. Trino authenticates this
principal using the keytab specified by the `hive.metastore.client.keytab`
property, and verifies that the identity of the metastore matches
`hive.metastore.service.principal`.

When using `KERBEROS` Metastore authentication with impersonation, the
principal specified by the `hive.metastore.client.principal` property must be
allowed to impersonate the current Trino user, as discussed in the section
[](hdfs-security-impersonation).

Keytab files must be distributed to every node in the Trino cluster.

(hive-glue-metastore)=
## AWS Glue catalog configuration properties

In order to use an AWS Glue catalog, you must configure your catalog file as
follows:

`hive.metastore=glue` and provide further details with the following
properties:

:::{list-table} AWS Glue catalog configuration properties
:widths: 35, 50, 15
:header-rows: 1

* - Property Name
  - Description
  - Default
* - `hive.metastore.glue.region`
  - AWS region of the Glue Catalog. This is required when not running in EC2, or
    when the catalog is in a different region. Example: `us-east-1`
  -
* - `hive.metastore.glue.endpoint-url`
  - Glue API endpoint URL (optional). Example:
    `https://glue.us-east-1.amazonaws.com`
  -
* - `hive.metastore.glue.sts.region`
  - AWS region of the STS service to authenticate with. This is required when
    running in a GovCloud region. Example: `us-gov-east-1`
  -
* - `hive.metastore.glue.sts.endpoint`
  - STS endpoint URL to use when authenticating to Glue (optional). Example:
    `https://sts.us-gov-east-1.amazonaws.com`
  -
* - `hive.metastore.glue.pin-client-to-current-region`
  - Pin Glue requests to the same region as the EC2 instance where Trino is
    running.
  - `false`
* - `hive.metastore.glue.max-connections`
  - Max number of concurrent connections to Glue.
  - `30`
* - `hive.metastore.glue.max-error-retries`
  - Maximum number of error retries for the Glue client.
  - `10`
* - `hive.metastore.glue.default-warehouse-dir`
  - Default warehouse directory for schemas created without an explicit
    `location` property.
  -
* - `hive.metastore.glue.use-web-identity-token-credentials-provider`
  - If you are running Trino on Amazon EKS, and authenticate using a Kubernetes
    service account, you can set this property to `true`. Setting to `true` forces
    Trino to not try using different credential providers from the default credential
    provider chain, and instead directly use credentials from the service account.
  - `false`
* - `hive.metastore.glue.aws-access-key`
  - AWS access key to use to connect to the Glue Catalog. If specified along
    with `hive.metastore.glue.aws-secret-key`, this parameter takes precedence
    over `hive.metastore.glue.iam-role`.
  -
* - `hive.metastore.glue.aws-secret-key`
  - AWS secret key to use to connect to the Glue Catalog. If specified along
    with `hive.metastore.glue.aws-access-key`, this parameter takes precedence
    over `hive.metastore.glue.iam-role`.
  -
* - `hive.metastore.glue.catalogid`
  - The ID of the Glue Catalog in which the metadata database resides.
  -
* - `hive.metastore.glue.iam-role`
  - ARN of an IAM role to assume when connecting to the Glue Catalog.
  -
* - `hive.metastore.glue.external-id`
  - External ID for the IAM role trust policy when connecting to the Glue
    Catalog.
  -
* - `hive.metastore.glue.partitions-segments`
  - Number of segments for partitioned Glue tables.
  - `5`
* - `hive.metastore.glue.skip-archive`
  - AWS Glue has the ability to archive older table versions and a user can
    roll back the table to any historical version if needed. By default, the
    Hive Connector backed by Glue will not skip the archival of older table
    versions.
  - `false`
:::

(iceberg-glue-catalog)=
### Iceberg-specific Glue catalog configuration properties

When using the Glue catalog, the Iceberg connector supports the same
{ref}`general Glue configuration properties <hive-glue-metastore>` as previously
described with the following additional property:

:::{list-table} Iceberg Glue catalog configuration property
:widths: 35, 50, 15
:header-rows: 1

* - Property name
  - Description
  - Default
* - `iceberg.glue.cache-table-metadata`
  - While updating the table in AWS Glue, store the table metadata with the
    purpose of accelerating `information_schema.columns` and
    `system.metadata.table_comments` queries.
  - `true`
  :::

## Iceberg-specific metastores

The Iceberg table format manages most metadata in metadata files in the object
storage itself. A small amount of metadata, however, still requires the use of a
metastore. In the Iceberg ecosystem, these smaller metastores are called Iceberg
metadata catalogs, or just catalogs.

You can use a general metastore such as an HMS or AWS Glue, or you can use the
Iceberg-specific REST, Nessie or JDBC metadata catalogs, as discussed in this
section.

(iceberg-rest-catalog)=
### REST catalog

In order to use the Iceberg REST catalog, configure the catalog type
with `iceberg.catalog.type=rest`, and provide further details with the
following properties:

:::{list-table} Iceberg REST catalog configuration properties
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `iceberg.rest-catalog.uri`
  - REST server API endpoint URI (required). Example:
    `http://iceberg-with-rest:8181`
* - `iceberg.rest-catalog.prefix`
  - The prefix for the resource path to use with the REST catalog server (optional).
    Example: `dev`
* - `iceberg.rest-catalog.warehouse`
  - Warehouse identifier/location for the catalog (optional). Example:
    `s3://my_bucket/warehouse_location`
* - `iceberg.rest-catalog.security`
  - The type of security to use (default: `NONE`).  `OAUTH2` requires either a
    `token` or `credential`. Example: `OAUTH2`
* - `iceberg.rest-catalog.session`
  - Session information included when communicating with the REST Catalog.
    Options are `NONE` or `USER` (default: `NONE`).
* - `iceberg.rest-catalog.oauth2.token`
  - The bearer token used for interactions with the server. A `token` or
    `credential` is required for `OAUTH2` security. Example: `AbCdEf123456`
* - `iceberg.rest-catalog.oauth2.credential`
  - The credential to exchange for a token in the OAuth2 client credentials flow
    with the server. A `token` or `credential` is required for `OAUTH2`
    security. Example: `AbCdEf123456`
* - `iceberg.rest-catalog.oauth2.scope`
  - Scope to be used when communicating with the REST Catalog. Applicable only
    when using `credential`.
* - `iceberg.rest-catalog.oauth2.server-uri`
  - The endpoint to retrieve access token from OAuth2 Server.
* - `iceberg.rest-catalog.vended-credentials-enabled`
  - Use credentials provided by the REST backend for file system access.
    Defaults to `false`.
* - `iceberg.rest-catalog.nested-namespace-enabled`
  - Support querying objects under nested namespace.
    Defaults to `false`.
* - `iceberg.rest-catalog.case-insensitive-name-matching`
  - Match namespace, table, and view names case insensitively. Defaults to `false`.
* - `iceberg.rest-catalog.case-insensitive-name-matching.cache-ttl`
  - [Duration](prop-type-duration) for which case-insensitive namespace, table, 
    and view names are cached. Defaults to `1m`.
  :::

The following example shows a minimal catalog configuration using an Iceberg
REST metadata catalog:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-with-rest:8181
```

`iceberg.security` must be `read_only` when connecting to Databricks Unity catalog
using an Iceberg REST catalog:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://dbc-12345678-9999.cloud.databricks.com/api/2.1/unity-catalog/iceberg
iceberg.security=read_only
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.token=***
```

The REST catalog supports [view management](sql-view-management) 
using the [Iceberg View specification](https://iceberg.apache.org/view-spec/).

The REST catalog does not support [materialized view management](sql-materialized-view-management).

(iceberg-jdbc-catalog)=
### JDBC catalog

The Iceberg JDBC catalog is supported for the Iceberg connector.  At a minimum,
`iceberg.jdbc-catalog.driver-class`, `iceberg.jdbc-catalog.connection-url`,
`iceberg.jdbc-catalog.default-warehouse-dir`, and
`iceberg.jdbc-catalog.catalog-name` must be configured. When using any
database besides PostgreSQL, a JDBC driver jar file must be placed in the plugin
directory.

:::{list-table} JDBC catalog configuration properties
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `iceberg.jdbc-catalog.driver-class`
  - JDBC driver class name.
* - `iceberg.jdbc-catalog.connection-url`
  - The URI to connect to the JDBC server.
* - `iceberg.jdbc-catalog.connection-user`
  - User name for JDBC client.
* - `iceberg.jdbc-catalog.connection-password`
  - Password for JDBC client.
* - `iceberg.jdbc-catalog.catalog-name`
  - Iceberg JDBC metastore catalog name.
* - `iceberg.jdbc-catalog.default-warehouse-dir`
  - The default warehouse directory to use for JDBC.
* - `iceberg.jdbc-catalog.schema-version`
  - JDBC catalog schema version.
    Valid values are `V0` or `V1`. Defaults to `V1`.
* - `iceberg.jdbc-catalog.retryable-status-codes`
  - On connection error to JDBC metastore, retry if
    it is one of these JDBC status codes.
    Valid value is a comma-separated list of status codes.
    Note: JDBC catalog always retries the following status
    codes: `08000,08003,08006,08007,40001`. Specify only
    additional codes (such as `57000,57P03,57P04` if using
    PostgreSQL driver) here.
:::

:::{warning}
The JDBC catalog may have compatibility issues if Iceberg introduces breaking
changes in the future. Consider the {ref}`REST catalog
<iceberg-rest-catalog>` as an alternative solution.

The JDBC catalog requires the metadata tables to already exist.
Refer to [Iceberg repository](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/jdbc/JdbcUtil.java)
for creating those tables.
:::

The following example shows a minimal catalog configuration using an
Iceberg JDBC metadata catalog:

```text
connector.name=iceberg
iceberg.catalog.type=jdbc
iceberg.jdbc-catalog.catalog-name=test
iceberg.jdbc-catalog.driver-class=org.postgresql.Driver
iceberg.jdbc-catalog.connection-url=jdbc:postgresql://example.net:5432/database
iceberg.jdbc-catalog.connection-user=admin
iceberg.jdbc-catalog.connection-password=test
iceberg.jdbc-catalog.default-warehouse-dir=s3://bucket
```

The JDBC catalog does not support [materialized view management](sql-materialized-view-management).

(iceberg-nessie-catalog)=
### Nessie catalog

In order to use a Nessie catalog, configure the catalog type with
`iceberg.catalog.type=nessie` and provide further details with the following
properties:

:::{list-table} Nessie catalog configuration properties
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `iceberg.nessie-catalog.uri`
  - Nessie API endpoint URI (required). Example:
    `https://localhost:19120/api/v2`
* - `iceberg.nessie-catalog.ref`
  - The branch/tag to use for Nessie. Defaults to `main`.
* - `iceberg.nessie-catalog.default-warehouse-dir`
  - Default warehouse directory for schemas created without an explicit
    `location` property. Example: `/tmp`
* - `iceberg.nessie-catalog.read-timeout`
  - The read timeout [duration](prop-type-duration) for requests to the Nessie
    server. Defaults to `25s`.
* - `iceberg.nessie-catalog.connection-timeout`
  - The connection timeout [duration](prop-type-duration) for connection
    requests to the Nessie server. Defaults to `5s`.
* - `iceberg.nessie-catalog.enable-compression`
  - Configure whether compression should be enabled or not for requests to the
    Nessie server. Defaults to `true`.
* - `iceberg.nessie-catalog.authentication.type`
  - The authentication type to use. Available value is `BEARER`. Defaults to no
    authentication.
* - `iceberg.nessie-catalog.authentication.token`
  - The token to use with `BEARER` authentication. Example:
`SXVLUXUhIExFQ0tFUiEK`
* - `iceberg.nessie-catalog.client-api-version`
  - Optional version of the Client API version to use. By default it is inferred from the `iceberg.nessie-catalog.uri` value.
    Valid values are `V1` or `V2`.
:::

```text
connector.name=iceberg
iceberg.catalog.type=nessie
iceberg.nessie-catalog.uri=https://localhost:19120/api/v2
iceberg.nessie-catalog.default-warehouse-dir=/tmp
```

The Nessie catalog does not support [view management](sql-view-management) or
[materialized view management](sql-materialized-view-management).

(iceberg-snowflake-catalog)=
### Snowflake catalog

In order to use a Snowflake catalog, configure the catalog type with
`iceberg.catalog.type=snowflake` and provide further details with the following
properties:

:::{list-table} Snowflake catalog configuration properties
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `iceberg.snowflake-catalog.account-uri`
  - Snowflake JDBC account URI (required). Example:
    `jdbc:snowflake://example123456789.snowflakecomputing.com`
* - `iceberg.snowflake-catalog.user`
  - Snowflake user (required).
* - `iceberg.snowflake-catalog.password`
  - Snowflake password (required).
* - `iceberg.snowflake-catalog.database`
  - Snowflake database name (required).
* - `iceberg.snowflake-catalog.role`
  - Snowflake role name
:::

```text
connector.name=iceberg
iceberg.catalog.type=snowflake
iceberg.snowflake-catalog.account-uri=jdbc:snowflake://example1234567890.snowflakecomputing.com
iceberg.snowflake-catalog.user=user
iceberg.snowflake-catalog.password=secret
iceberg.snowflake-catalog.database=db
```

When using the Snowflake catalog, data management tasks such as creating tables,
must be performed in Snowflake because using the catalog from external systems
like Trino only supports `SELECT` queries and other [read operations](sql-read-operations).

Additionally, the [Snowflake-created Iceberg
tables](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake)
do not expose partitioning information, which prevents efficient parallel reads
and therefore can have significant negative performance implications.

The Snowflake catalog does not support [view management](sql-view-management) or
[materialized view management](sql-materialized-view-management).

Further information is available in the [Snowflake catalog
documentation](https://docs.snowflake.com/en/user-guide/tables-iceberg-catalog).

(partition-projection)=
## Access tables with Athena partition projection metadata

[Partition projection](https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html)
is a feature of AWS Athena often used to speed up query processing with highly
partitioned tables when using the Hive connector.

Trino supports partition projection table properties stored in the Hive
metastore or Glue catalog, and it reimplements this functionality. Currently,
there is a limitation in comparison to AWS Athena for date projection, as it
only supports intervals of `DAYS`, `HOURS`, `MINUTES`, and `SECONDS`.

If there are any compatibility issues blocking access to a requested table when
partition projection is enabled, set the
`partition_projection_ignore` table property to `true` for a table to bypass
any errors.

Refer to {ref}`hive-table-properties` and {ref}`hive-column-properties` for
configuration of partition projection.

## Configure metastore for Avro

For catalogs using the Hive connector, you must add the following property
definition to the Hive metastore configuration file `hive-site.xml` and
restart the metastore service to enable first-class support for Avro tables when
using Hive 3.x:

```xml
<property>
     <!-- https://community.hortonworks.com/content/supportkb/247055/errorjavalangunsupportedoperationexception-storage.html -->
     <name>metastore.storage.schema.reader.impl</name>
     <value>org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader</value>
 </property>
```
