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

```{eval-rst}
.. list-table:: General metastore configuration properties
    :widths: 35, 50, 15
    :header-rows: 1

    * - Property Name
      - Description
      - Default
    * - ``hive.metastore``
      - The type of Hive metastore to use. Trino currently supports the default
        Hive Thrift metastore (``thrift``), and the AWS Glue Catalog (``glue``)
        as metadata sources. You must use this for all object storage catalogs
        except Iceberg.
      - ``thrift``
    * - ``iceberg.catalog.type``
      - The Iceberg table format manages most metadata in metadata files in the
        object storage itself. A small amount of metadata, however, still
        requires the use of a metastore. In the Iceberg ecosystem, these smaller
        metastores are called Iceberg metadata catalogs, or just catalogs. The
        examples in each subsection depict the contents of a Trino catalog file
        that uses the the Iceberg connector to configures different Iceberg
        metadata catalogs.

        You must set this property in all Iceberg catalog property files.
        Valid values are ``HIVE_METASTORE``, ``GLUE``, ``JDBC``, and ``REST``.
      -
    * - ``hive.metastore-cache.cache-partitions``
      - Enable caching for partition metadata. You can disable caching to avoid
        inconsistent behavior that results from it.
      - ``true``
    * - ``hive.metastore-cache-ttl``
      - Duration of how long cached metastore data is considered valid.
      - ``0s``
    * - ``hive.metastore-stats-cache-ttl``
      - Duration of how long cached metastore statistics are considered valid.
        If ``hive.metastore-cache-ttl`` is larger then it takes precedence
        over ``hive.metastore-stats-cache-ttl``.
      - ``5m``
    * - ``hive.metastore-cache-maximum-size``
      - Maximum number of metastore data objects in the Hive metastore cache.
      - ``10000``
    * - ``hive.metastore-refresh-interval``
      - Asynchronously refresh cached metastore data after access if it is older
        than this but is not yet expired, allowing subsequent accesses to see
        fresh data.
      -
    * - ``hive.metastore-refresh-max-threads``
      - Maximum threads used to refresh cached metastore data.
      - ``10``
    * - ``hive.metastore-timeout``
      - Timeout for Hive metastore requests.
      - ``10s``
    * - ``hive.hide-delta-lake-tables``
      - Controls whether to hide Delta Lake tables in table listings. Currently
        applies only when using the AWS Glue metastore.
      - ``false``
```

(hive-thrift-metastore)=

## Thrift metastore configuration properties

In order to use a Hive Thrift metastore, you must configure the metastore with
`hive.metastore=thrift` and provide further details with the following
properties:

```{eval-rst}
.. list-table:: Thrift metastore configuration properties
   :widths: 35, 50, 15
   :header-rows: 1

   * - Property name
     - Description
     - Default
   * - ``hive.metastore.uri``
     - The URIs of the Hive metastore to connect to using the Thrift protocol.
       If a comma-separated list of URIs is provided, the first URI is used by
       default, and the rest of the URIs are fallback metastores. This property
       is required. Example: ``thrift://192.0.2.3:9083`` or
       ``thrift://192.0.2.3:9083,thrift://192.0.2.4:9083``
     -
   * - ``hive.metastore.username``
     - The username Trino uses to access the Hive metastore.
     -
   * - ``hive.metastore.authentication.type``
     - Hive metastore authentication type. Possible values are ``NONE`` or
       ``KERBEROS``.
     - ``NONE``
   * - ``hive.metastore.thrift.impersonation.enabled``
     - Enable Hive metastore end user impersonation.
     -
   * - ``hive.metastore.thrift.use-spark-table-statistics-fallback``
     - Enable usage of table statistics generated by Apache Spark when Hive
       table statistics are not available.
     - ``true``
   * - ``hive.metastore.thrift.delegation-token.cache-ttl``
     - Time to live delegation token cache for metastore.
     - ``1h``
   * - ``hive.metastore.thrift.delegation-token.cache-maximum-size``
     - Delegation token cache maximum size.
     - ``1000``
   * - ``hive.metastore.thrift.client.ssl.enabled``
     - Use SSL when connecting to metastore.
     - ``false``
   * - ``hive.metastore.thrift.client.ssl.key``
     - Path to private key and client certification (key store).
     -
   * - ``hive.metastore.thrift.client.ssl.key-password``
     - Password for the private key.
     -
   * - ``hive.metastore.thrift.client.ssl.trust-certificate``
     - Path to the server certificate chain (trust store). Required when SSL is
       enabled.
     -
   * - ``hive.metastore.thrift.client.ssl.trust-certificate-password``
     - Password for the trust store.
     -
   * - ``hive.metastore.thrift.batch-fetch.enabled``
     - Enable fetching tables and views from all schemas in a single request.
     - ``true``
   * - ``hive.metastore.service.principal``
     - The Kerberos principal of the Hive metastore service.
     -
   * - ``hive.metastore.client.principal``
     - The Kerberos principal that Trino uses when connecting to the Hive
       metastore service.
     -
   * - ``hive.metastore.client.keytab``
     - Hive metastore client keytab location.
     -
   * - ``hive.metastore.thrift.delete-files-on-drop``
     - Actively delete the files for managed tables when performing drop table
       or partition operations, for cases when the metastore does not delete the
       files.
     - ``false``
   * - ``hive.metastore.thrift.assume-canonical-partition-keys``
     - Allow the metastore to assume that the values of partition columns can be
       converted to string values. This can lead to performance improvements in
       queries which apply filters on the partition columns. Partition keys with
       a ``TIMESTAMP`` type do not get canonicalized.
     - ``false``
   * - ``hive.metastore.thrift.client.socks-proxy``
     - SOCKS proxy to use for the Thrift Hive metastore.
     -
   * - ``hive.metastore.thrift.client.max-retries``
     - Maximum number of retry attempts for metastore requests.
     - ``9``
   * - ``hive.metastore.thrift.client.backoff-scale-factor``
     - Scale factor for metastore request retry delay.
     - ``2.0``
   * - ``hive.metastore.thrift.client.max-retry-time``
     - Total allowed time limit for a metastore request to be retried.
     - ``30s``
   * - ``hive.metastore.thrift.client.min-backoff-delay``
     - Minimum delay between metastore request retries.
     - ``1s``
   * - ``hive.metastore.thrift.client.max-backoff-delay``
     - Maximum delay between metastore request retries.
     - ``1s``
   * - ``hive.metastore.thrift.txn-lock-max-wait``
     - Maximum time to wait to acquire hive transaction lock.
     - ``10m``
```

(hive-glue-metastore)=

## AWS Glue catalog configuration properties

In order to use an AWS Glue catalog, you must configure your catalog file as
follows:

`hive.metastore=glue` and provide further details with the following
properties:

```{eval-rst}
.. list-table:: AWS Glue catalog configuration properties
    :widths: 35, 50, 15
    :header-rows: 1

    * - Property Name
      - Description
      - Default
    * - ``hive.metastore.glue.region``
      - AWS region of the Glue Catalog. This is required when not running in
        EC2, or when the catalog is in a different region. Example:
        ``us-east-1``
      -
    * - ``hive.metastore.glue.endpoint-url``
      - Glue API endpoint URL (optional). Example:
        ``https://glue.us-east-1.amazonaws.com``
      -
    * - ``hive.metastore.glue.sts.region``
      - AWS region of the STS service to authenticate with. This is required
        when running in a GovCloud region. Example: ``us-gov-east-1``
      -
    * - ``hive.metastore.glue.proxy-api-id``
      - The ID of the Glue Proxy API, when accessing Glue via an VPC endpoint in
        API Gateway.
      -
    * - ``hive.metastore.glue.sts.endpoint``
      - STS endpoint URL to use when authenticating to Glue (optional). Example:
        ``https://sts.us-gov-east-1.amazonaws.com``
      -
    * - ``hive.metastore.glue.pin-client-to-current-region``
      - Pin Glue requests to the same region as the EC2 instance where Trino is
        running.
      - ``false``
    * - ``hive.metastore.glue.max-connections``
      - Max number of concurrent connections to Glue.
      - ``30``
    * - ``hive.metastore.glue.max-error-retries``
      - Maximum number of error retries for the Glue client.
      - ``10``
    * - ``hive.metastore.glue.default-warehouse-dir``
      - Default warehouse directory for schemas created without an explicit
        ``location`` property.
      -
    * - ``hive.metastore.glue.aws-credentials-provider``
      - Fully qualified name of the Java class to use for obtaining AWS
        credentials. Can be used to supply a custom credentials provider.
      -
    * - ``hive.metastore.glue.aws-access-key``
      - AWS access key to use to connect to the Glue Catalog. If specified along
        with ``hive.metastore.glue.aws-secret-key``, this parameter takes
        precedence over ``hive.metastore.glue.iam-role``.
      -
    * - ``hive.metastore.glue.aws-secret-key``
      - AWS secret key to use to connect to the Glue Catalog. If specified along
        with ``hive.metastore.glue.aws-access-key``, this parameter takes
        precedence over ``hive.metastore.glue.iam-role``.
      -
    * - ``hive.metastore.glue.catalogid``
      - The ID of the Glue Catalog in which the metadata database resides.
      -
    * - ``hive.metastore.glue.iam-role``
      - ARN of an IAM role to assume when connecting to the Glue Catalog.
      -
    * - ``hive.metastore.glue.external-id``
      - External ID for the IAM role trust policy when connecting to the Glue
        Catalog.
      -
    * - ``hive.metastore.glue.partitions-segments``
      - Number of segments for partitioned Glue tables.
      - ``5``
    * - ``hive.metastore.glue.get-partition-threads``
      - Number of threads for parallel partition fetches from Glue.
      - ``20``
    * - ``hive.metastore.glue.read-statistics-threads``
      - Number of threads for parallel statistic fetches from Glue.
      - ``5``
    * - ``hive.metastore.glue.write-statistics-threads``
      - Number of threads for parallel statistic writes to Glue.
      - ``5``
```

(iceberg-glue-catalog)=

### Iceberg-specific Glue catalog configuration properties

When using the Glue catalog, the Iceberg connector supports the same
{ref}`general Glue configuration properties <hive-glue-metastore>` as previously
described with the following additional property:

```{eval-rst}
.. list-table:: Iceberg Glue catalog configuration property
  :widths: 35, 50, 15
  :header-rows: 1

  * - Property name
    - Description
    - Default
  * - ``iceberg.glue.skip-archive``
    - Skip archiving an old table version when creating a new version in a
      commit. See `AWS Glue Skip Archive
      <https://iceberg.apache.org/docs/latest/aws/#skip-archive>`_.
    - ``false``
```

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

```{eval-rst}
.. list-table:: Iceberg REST catalog configuration properties
  :widths: 40, 60
  :header-rows: 1

  * - Property name
    - Description
  * - ``iceberg.rest-catalog.uri``
    - REST server API endpoint URI (required).
      Example: ``http://iceberg-with-rest:8181``
  * - ``iceberg.rest-catalog.warehouse``
    - Warehouse identifier/location for the catalog (optional).
      Example: ``s3://my_bucket/warehouse_location``
  * - ``iceberg.rest-catalog.security``
    - The type of security to use (default: ``NONE``).  ``OAUTH2`` requires
      either a ``token`` or ``credential``. Example: ``OAUTH2``
  * - ``iceberg.rest-catalog.session``
    - Session information included when communicating with the REST Catalog.
      Options are ``NONE`` or ``USER`` (default: ``NONE``).
  * - ``iceberg.rest-catalog.oauth2.token``
    - The bearer token used for interactions with the server. A
      ``token`` or ``credential`` is required for ``OAUTH2`` security.
      Example: ``AbCdEf123456``
  * - ``iceberg.rest-catalog.oauth2.credential``
    - The credential to exchange for a token in the OAuth2 client credentials
      flow with the server. A ``token`` or ``credential`` is required for
      ``OAUTH2`` security. Example: ``AbCdEf123456``
```

The following example shows a minimal catalog configuration using an Iceberg
REST metadata catalog:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-with-rest:8181
```

The REST catalog does not support {doc}`views</sql/create-view>` or
{doc}`materialized views</sql/create-materialized-view>`.

(iceberg-jdbc-catalog)=

### JDBC catalog

The Iceberg REST catalog is supported for the Iceberg connector.  At a minimum,
`iceberg.jdbc-catalog.driver-class`, `iceberg.jdbc-catalog.connection-url`
and `iceberg.jdbc-catalog.catalog-name` must be configured. When using any
database besides PostgreSQL, a JDBC driver jar file must be placed in the plugin
directory.

:::{warning}
The JDBC catalog may have compatibility issues if Iceberg introduces breaking
changes in the future. Consider the {ref}`REST catalog
<iceberg-rest-catalog>` as an alternative solution.
:::

At a minimum, `iceberg.jdbc-catalog.driver-class`,
`iceberg.jdbc-catalog.connection-url`, and
`iceberg.jdbc-catalog.catalog-name` must be configured. When using any
database besides PostgreSQL, a JDBC driver jar file must be placed in the plugin
directory. The following example shows a minimal catalog configuration using an
Iceberg REST metadata catalog:

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

The JDBC catalog does not support {doc}`views</sql/create-view>` or
{doc}`materialized views</sql/create-materialized-view>`.

(iceberg-nessie-catalog)=

### Nessie catalog

In order to use a Nessie catalog, configure the catalog type with
`iceberg.catalog.type=nessie` and provide further details with the following
properties:

```{eval-rst}
.. list-table:: Nessie catalog configuration properties
  :widths: 40, 60
  :header-rows: 1

  * - Property name
    - Description
  * - ``iceberg.nessie-catalog.uri``
    - Nessie API endpoint URI (required).
      Example: ``https://localhost:19120/api/v1``
  * - ``iceberg.nessie-catalog.ref``
    - The branch/tag to use for Nessie, defaults to ``main``.
  * - ``iceberg.nessie-catalog.default-warehouse-dir``
    - Default warehouse directory for schemas created without an explicit
      ``location`` property. Example: ``/tmp``
```

```text
connector.name=iceberg
iceberg.catalog.type=nessie
iceberg.nessie-catalog.uri=https://localhost:19120/api/v1
iceberg.nessie-catalog.default-warehouse-dir=/tmp
```

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
