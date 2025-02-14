# BigQuery connector

```{raw} html
<img src="../_static/img/bigquery.png" class="connector-logo">
```

The BigQuery connector allows querying the data stored in [BigQuery](https://cloud.google.com/bigquery/). This can be used to join data between
different systems like BigQuery and Hive. The connector uses the [BigQuery
Storage API](https://cloud.google.com/bigquery/docs/reference/storage/) to
read the data from the tables.

## BigQuery Storage API

The Storage API streams data in parallel directly from BigQuery via gRPC without
using Google Cloud Storage as an intermediary.
It has a number of advantages over using the previous export-based read flow
that should generally lead to better read performance:

**Direct Streaming**

: It does not leave any temporary files in Google Cloud Storage. Rows are read
  directly from BigQuery servers using an Avro wire format.

**Column Filtering**

: The new API allows column filtering to only read the data you are interested in.
  [Backed by a columnar datastore](https://cloud.google.com/blog/products/bigquery/inside-capacitor-bigquerys-next-generation-columnar-storage-format),
  it can efficiently stream data without reading all columns.

**Dynamic Sharding**

: The API rebalances records between readers until they all complete. This means
  that all Map phases will finish nearly concurrently. See this blog article on
  [how dynamic sharding is similarly used in Google Cloud Dataflow](https://cloud.google.com/blog/products/gcp/no-shard-left-behind-dynamic-work-rebalancing-in-google-cloud-dataflow).

(bigquery-requirements)=
## Requirements

To connect to BigQuery, you need:

- To enable the [BigQuery Storage Read API](https://cloud.google.com/bigquery/docs/reference/storage/#enabling_the_api).

- Network access from your Trino coordinator and workers to the
  Google Cloud API service endpoint. This endpoint uses HTTPS, or port 443.

- To configure BigQuery so that the Trino coordinator and workers have [permissions
  in BigQuery](https://cloud.google.com/bigquery/docs/reference/storage#permissions).

- To set up authentication. Your authentiation options differ depending on whether
  you are using Dataproc/Google Compute Engine (GCE) or not.

  **On Dataproc/GCE** the authentication is done from the machine's role.

  **Outside Dataproc/GCE** you have 3 options:

  - Use a service account JSON key and `GOOGLE_APPLICATION_CREDENTIALS` as
    described in the Google Cloud authentication [getting started guide](https://cloud.google.com/docs/authentication/getting-started).
  - Set `bigquery.credentials-key` in the catalog properties file. It should
    contain the contents of the JSON file, encoded using base64.
  - Set `bigquery.credentials-file` in the catalog properties file. It should
    point to the location of the JSON file.

## Configuration

To configure the BigQuery connector, create a catalog properties file in
`etc/catalog` named `example.properties`, to mount the BigQuery connector as
the `example` catalog. Create the file with the following contents, replacing
the connection properties as appropriate for your setup:

```text
connector.name=bigquery
bigquery.project-id=<your Google Cloud Platform project id>
```

### Multiple GCP projects

The BigQuery connector can only access a single GCP project.Thus, if you have
data in multiple GCP projects, You need to create several catalogs, each
pointing to a different GCP project. For example, if you have two GCP projects,
one for the sales and one for analytics, you can create two properties files in
`etc/catalog` named `sales.properties` and `analytics.properties`, both
having `connector.name=bigquery` but with different `project-id`. This will
create the two catalogs, `sales` and `analytics` respectively.

(bigquery-arrow-serialization-support)=
### Arrow serialization support

This is a feature which introduces support for using Apache Arrow
as the serialization format when reading from BigQuery.  Please note there are
a few caveats:

- Using Apache Arrow serialization is enabled by default. Add
  `--add-opens=java.base/java.nio=ALL-UNNAMED` to the Trino
  {ref}`jvm-config`.

(bigquery-reading-from-views)=
### Reading from views

The connector has a preliminary support for reading from [BigQuery views](https://cloud.google.com/bigquery/docs/views-intro). Please note there are
a few caveats:

- Reading from views is disabled by default. In order to enable it, set the
  `bigquery.views-enabled` configuration property to `true`.
- BigQuery views are not materialized by default, which means that the
  connector needs to materialize them before it can read them. This process
  affects the read performance.
- The materialization process can also incur additional costs to your BigQuery bill.
- By default, the materialized views are created in the same project and
  dataset. Those can be configured by the optional `bigquery.view-materialization-project`
  and `bigquery.view-materialization-dataset` properties, respectively. The
  service account must have write permission to the project and the dataset in
  order to materialize the view.

### Configuration properties

:::{list-table} BigQuery configuration properties
:widths: 30, 55, 15
:header-rows: 1

* - Property name
  - Description
  - Default
* - `bigquery.project-id`
  - The Google Cloud Project ID where the data reside.
  - Taken from the service account
* - `bigquery.parent-project-id`
  - The project ID Google Cloud Project to bill for the export.
  - Taken from the service account
* - `bigquery.views-enabled`
  - Enables the connector to read from views and not only tables. Read
    [this section](bigquery-reading-from-views) before enabling this feature.
  - `false`
* - `bigquery.view-expire-duration`
  - Expire duration for the materialized view.
  - `24h`
* - `bigquery.view-materialization-project`
  - The project where the materialized view is going to be created.
  - The view's project
* - `bigquery.view-materialization-dataset`
  - The dataset where the materialized view is going to be created.
  - The view's project
* - `bigquery.skip-view-materialization`
  - Use REST API to access views instead of Storage API. BigQuery `BIGNUMERIC`
    and `TIMESTAMP` types are unsupported.
  - `false`
* - `bigquery.view-materialization-with-filter`
  - Use filter conditions when materializing views.
  - `false`
* - `bigquery.views-cache-ttl`
  - Duration for which the materialization of a view will be cached and reused.
    Set to `0ms` to disable the cache.
  - `15m`
* - `bigquery.metadata.cache-ttl`
  - Duration for which metadata retrieved from BigQuery is cached and reused.
    Set to `0ms` to disable the cache.
  - `0ms`
* - `bigquery.max-read-rows-retries`
  - The number of retries in case of retryable server issues.
  - `3`
* - `bigquery.credentials-key`
  - The base64 encoded credentials key.
  - None. See the [requirements](bigquery-requirements) section
* - `bigquery.credentials-file`
  - The path to the JSON credentials file.
  - None. See the [requirements](bigquery-requirements) section
* - `bigquery.case-insensitive-name-matching`
  - Match dataset and table names case-insensitively.
  - `false`
* - `bigquery.case-insensitive-name-matching.cache-ttl`
  - [Duration](prop-type-duration) for which case insensitive schema and table
    names are cached. Set to `0ms` to disable the cache.
  - `0ms`
* - `bigquery.query-results-cache.enabled`
  - Enable [query results cache](https://cloud.google.com/bigquery/docs/cached-results).
  - `false`
* - `bigquery.arrow-serialization.enabled`
  - Enable using Apache Arrow serialization when reading data from BigQuery.
    Read this [section](bigquery-arrow-serialization-support) before using this feature.
  - `true`
* - `bigquery.channel-pool.initial-size`
  - The initial size of the connection pool, also known as a channel pool,
    used for gRPC communication.
  - `1`
* - `bigquery.channel-pool.min-size`
  - The minimum number of connections in the connection pool, also known as a
    channel pool, used for gRPC communication.
  - `1`
* - `bigquery.channel-pool.max-size`
  - The maximum number of connections in the connection pool, also known as a
    channel pool, used for gRPC communication.
  - `1`
* - `bigquery.channel-pool.min-rpc-per-channel`
  - Threshold to start scaling down the channel pool.
    When the average of outstanding RPCs in a single minute drop below this
    threshold, channels are removed from the pool.
  - `0`
* - `bigquery.channel-pool.max-rpc-per-channel`
  - Threshold to start scaling up the channel pool.
    When the average of outstanding RPCs in a single minute surpass this
    threshold, channels are added to the pool.
  - `2147483647`
* - `bigquery.rpc-retries`
  - The maximum number of retry attempts to perform for the RPC calls.
    If this value is set to `0`, the value from 
    `bigquery.rpc-timeout` is used. 
    Retry is deactivated when both `bigquery.rpc-retries` and
    `bigquery.rpc-timeout` are `0`.
    If this value is positive, and the number of attempts exceeds
    `bigquery.rpc-retries` limit, retries stop even if
    the total retry time is still lower than `bigquery.rpc-timeout`.
  - `0`
* - `bigquery.rpc-timeout`
  - Timeout [duration](prop-type-duration) on when the retries for the
    RPC call should be given up completely. The higher the timeout, the
    more retries can be attempted. If this value is `0s`, then
    `bigquery.rpc-retries` is used to determine retries.
    Retry is deactivated when `bigquery.rpc-retries` and 
    `bigquery.rpc-timeout` are both `0`. 
    If this value is positive, and the retry duration has reached the timeout
    value, retries stop even if the number of attempts is lower than 
    the `bigquery.rpc-retries` value.
  - `0s`
* - `bigquery.rpc-retry-delay`
  - The delay [duration](prop-type-duration) before the first retry attempt
    for RPC calls.
  - `0s`
* - `bigquery.rpc-retry-delay-multiplier`
  - Controls the change in delay before the next retry.
    The retry delay of the previous call is multiplied by the
    `bigquery.rpc-retry-delay-multiplier` to calculate the retry delay
    for the next RPC call.
  - `1.0`
* - `bigquery.rpc-proxy.enabled`
  - Use a proxy for communication with BigQuery.
  - `false`
* - `bigquery.rpc-proxy.uri`
  - Proxy URI to use if connecting through a proxy.
  -
* - `bigquery.rpc-proxy.username`
  - Proxy user name to use if connecting through a proxy.
  -
* - `bigquery.rpc-proxy.password`
  - Proxy password to use if connecting through a proxy.
  -
* - `bigquery.rpc-proxy.keystore-path`
  - Keystore containing client certificates to present to proxy if connecting
    through a proxy. Only required if proxy uses mutual TLS.
  -
* - `bigquery.rpc-proxy.keystore-password`
  - Password of the keystore specified by `bigquery.rpc-proxy.keystore-path`.
  -
* - `bigquery.rpc-proxy.truststore-path`
  - Truststore containing certificates of the proxy server if connecting
    through a proxy.
  -
* - `bigquery.rpc-proxy.truststore-password`
  - Password of the truststore specified by `bigquery.rpc-proxy.truststore-path`.
  -
:::

(bigquery-fte-support)=
### Fault-tolerant execution support

The connector supports {doc}`/admin/fault-tolerant-execution` of query
processing. Read and write operations are both supported with any retry policy.


(bigquery-type-mapping)=
## Type mapping

Because Trino and BigQuery each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading or
writing data. Data types may not map the same way in both directions between
Trino and the data source. Refer to the following sections for type mapping in
each direction.

### BigQuery type to Trino type mapping

The connector maps BigQuery types to the corresponding Trino types according
to the following table:

:::{list-table} BigQuery type to Trino type mapping
:widths: 30, 30, 50
:header-rows: 1

* - BigQuery type
  - Trino type
  - Notes
* - `BOOLEAN`
  - `BOOLEAN`
  -
* - `INT64`
  - `BIGINT`
  - `INT`, `SMALLINT`, `INTEGER`, `BIGINT`, `TINYINT`, and `BYTEINT` are aliases
    for `INT64` in BigQuery.
* - `FLOAT64`
  - `DOUBLE`
  -
* - `NUMERIC`
  - `DECIMAL(P,S)`
  - The default precision and scale of `NUMERIC` is `(38, 9)`.
* - `BIGNUMERIC`
  - `DECIMAL(P,S)`
  - Precision > 38 is not supported. The default precision and scale of
    `BIGNUMERIC` is `(77, 38)`.
* - `DATE`
  - `DATE`
  -
* - `DATETIME`
  - `TIMESTAMP(6)`
  -
* - `STRING`
  - `VARCHAR`
  -
* - `BYTES`
  - `VARBINARY`
  -
* - `TIME`
  - `TIME(6)`
  -
* - `TIMESTAMP`
  - `TIMESTAMP(6) WITH TIME ZONE`
  - Time zone is UTC
* - `GEOGRAPHY`
  - `VARCHAR`
  - In [Well-known text
    (WKT)](https://wikipedia.org/wiki/Well-known_text_representation_of_geometry)
    format
* - `JSON`
  - `JSON`
  -
* - `ARRAY`
  - `ARRAY`
  -
* - `RECORD`
  - `ROW`
  -
:::

No other types are supported.

### Trino type to BigQuery type mapping

The connector maps Trino types to the corresponding BigQuery types according
to the following table:

:::{list-table} Trino type to BigQuery type mapping
:widths: 30, 30, 50
:header-rows: 1

* - Trino type
  - BigQuery type
  - Notes
* - `BOOLEAN`
  - `BOOLEAN`
  -
* - `VARBINARY`
  - `BYTES`
  -
* - `DATE`
  - `DATE`
  -
* - `DOUBLE`
  - `FLOAT`
  -
* - `BIGINT`
  - `INT64`
  - `INT`, `SMALLINT`, `INTEGER`, `BIGINT`, `TINYINT`, and
    `BYTEINT` are aliases for `INT64` in BigQuery.
* - `DECIMAL(P,S)`
  - `NUMERIC`
  - The default precision and scale of `NUMERIC` is `(38, 9)`.
* - `VARCHAR`
  - `STRING`
  -
* - `TIMESTAMP(6)`
  - `DATETIME`
  -
:::

No other types are supported.

## System tables

For each Trino table which maps to BigQuery view there exists a system table
which exposes BigQuery view definition. Given a BigQuery view `example_view`
you can send query `SELECT * example_view$view_definition` to see the SQL
which defines view in BigQuery.

(bigquery-special-columns)=
## Special columns

In addition to the defined columns, the BigQuery connector exposes
partition information in a number of hidden columns:

- `$partition_date`: Equivalent to `_PARTITIONDATE` pseudo-column in BigQuery
- `$partition_time`: Equivalent to `_PARTITIONTIME` pseudo-column in BigQuery

You can use these columns in your SQL statements like any other column. They
can be selected directly, or used in conditional statements. For example, you
can inspect the partition date and time for each record:

```
SELECT *, "$partition_date", "$partition_time"
FROM example.web.page_views;
```

Retrieve all records stored in the partition `_PARTITIONDATE = '2022-04-07'`:

```
SELECT *
FROM example.web.page_views
WHERE "$partition_date" = date '2022-04-07';
```

:::{note}
Two special partitions `__NULL__` and `__UNPARTITIONED__` are not supported.
:::

(bigquery-sql-support)=
## SQL support

The connector provides read and write access to data and metadata in the
BigQuery database. In addition to the
{ref}`globally available <sql-globally-available>` and
{ref}`read operation <sql-read-operations>` statements, the connector supports
the following features:

- {doc}`/sql/insert`
- {doc}`/sql/delete`
- {doc}`/sql/truncate`
- {doc}`/sql/create-table`
- {doc}`/sql/create-table-as`
- {doc}`/sql/drop-table`
- {doc}`/sql/create-schema`
- {doc}`/sql/drop-schema`
- {doc}`/sql/comment`

```{include} sql-delete-limitation.fragment
```

### Procedures

```{include} procedures-execute.fragment
```

### Table functions

The connector provides specific {doc}`table functions </functions/table>` to
access BigQuery.

(bigquery-query-function)=
#### `query(varchar) -> table`

The `query` function allows you to query the underlying BigQuery directly. It
requires syntax native to BigQuery, because the full query is pushed down and
processed by BigQuery. This can be useful for accessing native features which are
not available in Trino or for improving query performance in situations where
running a query natively may be faster.

```{include} query-passthrough-warning.fragment
```

For example, query the `example` catalog and group and concatenate all
employee IDs by manager ID:

```
SELECT
  *
FROM
  TABLE(
    example.system.query(
      query => 'SELECT
        manager_id, STRING_AGG(employee_id)
      FROM
        company.employees
      GROUP BY
        manager_id'
    )
  );
```

```{include} query-table-function-ordering.fragment
```

## Performance

The connector includes a number of performance improvements, detailed in the
following sections.

(bigquery-pushdown)=
### Pushdown

The connector supports pushdown for a number of operations:

- [](limit-pushdown) for access to tables and other objects when using the REST
  API to reduce CPU consumption in BigQuery and performance overall. Pushdown is
  not supported by the Storage API, used for the more common Trino-managed
  tables, and therefore not used for access with it.

## FAQ

### What is the Pricing for the Storage API?

See the [BigQuery pricing documentation](https://cloud.google.com/bigquery/pricing#storage-api).
