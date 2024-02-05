# Elasticsearch connector

```{raw} html
<img src="../_static/img/elasticsearch.png" class="connector-logo">
```

The Elasticsearch connector allows access to
[Elasticsearch](https://www.elastic.co/products/elasticsearch) data from Trino.
This document describes how to configure a catalog with the Elasticsearch
connector to run SQL queries against Elasticsearch.

## Requirements

- Elasticsearch 7.x or 8.x
- Network access from the Trino coordinator and workers to the Elasticsearch nodes.

## Configuration

To configure the Elasticsearch connector, create a catalog properties file
`etc/catalog/example.properties` with the following contents, replacing the
properties as appropriate for your setup:

```text
connector.name=elasticsearch
elasticsearch.host=localhost
elasticsearch.port=9200
elasticsearch.default-schema-name=default
```

The following table details all general configuration properties:

:::{list-table} Elasticsearch configuration properties
:widths: 35, 55, 10
:header-rows: 1

* - Property name
  - Description
  - Default
* - `elasticsearch.host`
  - The comma-separated list of host names for the Elasticsearch node to connect
    to. This property is required.
  -
* - `elasticsearch.port`
  - Port to use to connecto to Elasticsearch.
  - `9200`
* - `elasticsearch.default-schema-name`
  - The schema that contains all tables defined without a qualifying schema
    name.
  - `default`
* - `elasticsearch.scroll-size`
  - Sets the maximum number of hits that can be returned with each
    [Elasticsearch scroll
    request](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html#scroll-search-context).
  - `1000`
* - `elasticsearch.scroll-timeout`
  - [Duration](prop-type-duration) for Elasticsearch to keep the search context
    alive for scroll requests.
  - `1m`
* - `elasticsearch.request-timeout`
  - Timeout [duration](prop-type-duration) for all Elasticsearch requests.
  - `10s`
* - `elasticsearch.connect-timeout`
  - Timeout [duration](prop-type-duration) for all Elasticsearch connection
    attempts.
  - `1s`
* - `elasticsearch.backoff-init-delay`
  - The minimum [duration](prop-type-duration) between backpressure retry
    attempts for a single request to Elasticsearch. Setting it too low can
    overwhelm an already struggling cluster.
  - `500ms`
* - `elasticsearch.backoff-max-delay`
  - The maximum [duration](prop-type-duration) between backpressure retry
    attempts for a single request to Elasticsearch.
  - `20s`
* - `elasticsearch.max-retry-time`
  - The maximum [duration](prop-type-duration) across all retry attempts for a
    single request to Elasticsearch.
  - `30s`
* - `elasticsearch.node-refresh-interval`
  - [Duration](prop-type-duration) between requests to refresh the list of
    available Elasticsearch nodes.
  - `1m`
* - `elasticsearch.ignore-publish-address`
  - Disable using the address published by the Elasticsearch API to connect for
    queries. Some deployments map Elasticsearch ports to a random public port
    and enabling this property can help in these cases.
  - `false`
:::

### Authentication

The connection to Elasticsearch can use AWS or password authentication.

To enable AWS authentication and authorization using IAM policies, the
`elasticsearch.security` option must be set to `AWS`. Additionally, the
following options must be configured:

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `elasticsearch.aws.region`
  - AWS region of the Elasticsearch endpoint. This option is required.
* - `elasticsearch.aws.access-key`
  - AWS access key to use to connect to the Elasticsearch domain. If not set, the
    default AWS credentials provider chain is used.
* - `elasticsearch.aws.secret-key`
  - AWS secret key to use to connect to the Elasticsearch domain. If not set, the
    default AWS credentials provider chain is used.
* - `elasticsearch.aws.iam-role`
  - Optional ARN of an IAM role to assume to connect to Elasticsearch. Note that
    the configured IAM user must be able to assume this role.
* - `elasticsearch.aws.external-id`
  - Optional external ID to pass while assuming an AWS IAM role.
:::

To enable password authentication, the `elasticsearch.security` option must be set
to `PASSWORD`. Additionally the following options must be configured:

:::{list-table}
:widths: 45, 55
:header-rows: 1

* - Property name
  - Description
* - `elasticsearch.auth.user`
  - User name to use to connect to Elasticsearch.
* - `elasticsearch.auth.password`
  - Password to use to connect to Elasticsearch.
:::

### Connection security with TLS

The connector provides additional security options to connect to Elasticsearch
clusters with TLS enabled.

If your cluster has globally-trusted certificates, you should only need to
enable TLS. If you require custom configuration for certificates, the connector
supports key stores and trust stores in PEM or Java Key Store (JKS) format.

The available configuration values are listed in the following table:

:::{list-table} TLS Security Properties
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `elasticsearch.tls.enabled`
  - Enables TLS security.
* - `elasticsearch.tls.keystore-path`
  - The path to the [PEM](/security/inspect-pem) or [JKS](/security/inspect-jks)
    key store.
* - `elasticsearch.tls.truststore-path`
  - The path to [PEM](/security/inspect-pem) or [JKS](/security/inspect-jks)
    trust store.
* - `elasticsearch.tls.keystore-password`
  - The key password for the key store specified by
    `elasticsearch.tls.keystore-path`.
* - `elasticsearch.tls.truststore-password`
  - The key password for the trust store specified by
    `elasticsearch.tls.truststore-path`.
* - `elasticsearch.tls.verify-hostnames`
  - Flag to determine if the hostnames in the certificates must be verified. Defaults
    to `true`.
:::

(elasticesearch-type-mapping)=
## Type mapping

Because Trino and Elasticsearch each support types that the other does not, this
connector {ref}`maps some types <type-mapping-overview>` when reading data.

### Elasticsearch type to Trino type mapping

The connector maps Elasticsearch types to the corresponding Trino types
according to the following table:

:::{list-table} Elasticsearch type to Trino type mapping
:widths: 30, 30, 50
:header-rows: 1

* - Elasticsearch type
  - Trino type
  - Notes
* - `BOOLEAN`
  - `BOOLEAN`
  -
* - `DOUBLE`
  - `DOUBLE`
  -
* - `FLOAT`
  - `REAL`
  -
* - `BYTE`
  - `TINYINT`
  -
* - `SHORT`
  - `SMALLINT`
  -
* - `INTEGER`
  - `INTEGER`
  -
* - `LONG`
  - `BIGINT`
  -
* - `KEYWORD`
  - `VARCHAR`
  -
* - `TEXT`
  - `VARCHAR`
  -
* - `DATE`
  - `TIMESTAMP`
  - For more information, see [](elasticsearch-date-types).
* - `IPADDRESS`
  - `IP`
  -
:::

No other types are supported.

(elasticsearch-array-types)=

### Array types

Fields in Elasticsearch can contain [zero or more
values](https://www.elastic.co/guide/en/elasticsearch/reference/current/array.html),
but there is no dedicated array type. To indicate a field contains an array, it
can be annotated in a Trino-specific structure in the
[\_meta](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-meta-field.html)
section of the index mapping.

For example, you can have an Elasticsearch index that contains documents with the following structure:

```json
{
    "array_string_field": ["trino","the","lean","machine-ohs"],
    "long_field": 314159265359,
    "id_field": "564e6982-88ee-4498-aa98-df9e3f6b6109",
    "timestamp_field": "1987-09-17T06:22:48.000Z",
    "object_field": {
        "array_int_field": [86,75,309],
        "int_field": 2
    }
}
```

The array fields of this structure can be defined by using the following command to add the field
property definition to the `_meta.trino` property of the target index mapping with Elasticsearch available at `search.example.com:9200`:

```shell
curl --request PUT \
    --url search.example.com:9200/doc/_mapping \
    --header 'content-type: application/json' \
    --data '
{
    "_meta": {
        "trino":{
            "array_string_field":{
                "isArray":true
            },
            "object_field":{
                "array_int_field":{
                    "isArray":true
                }
            },
        }
    }
}'
```

:::{note}
It is not allowed to use `asRawJson` and `isArray` flags simultaneously for the same column.
:::

(elasticsearch-date-types)=
### Date types

The Elasticsearch connector supports only the default `date` type. All other
[date] formats including [built-in date formats] and [custom date formats] are
not supported. Dates with the [format] property are ignored.

### Raw JSON transform

Documents in Elasticsearch can include more complex structures that are not
represented in the mapping. For example, a single `keyword` field can have
widely different content including a single `keyword` value, an array, or a
multidimensional `keyword` array with any level of nesting.

The following command configures `array_string_field` mapping with Elasticsearch
available at `search.example.com:9200`:

```shell
curl --request PUT \
    --url search.example.com:9200/doc/_mapping \
    --header 'content-type: application/json' \
    --data '
{
    "properties": {
        "array_string_field":{
            "type": "keyword"
        }
    }
}'
```

All the following documents are legal for Elasticsearch with
`array_string_field` mapping:

```json
[
    {
        "array_string_field": "trino"
    },
    {
        "array_string_field": ["trino","is","the","best"]
    },
    {
        "array_string_field": ["trino",["is","the","best"]]
    },
    {
        "array_string_field": ["trino",["is",["the","best"]]]
    }
]
```

See the [Elasticsearch array
documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/array.html)
for more details.

Further, Elasticsearch supports types, such as
[dense_vector](https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html),
that are not supported in Trino. These and other types can cause parsing
exceptions for users that use of these types in Elasticsearch. To manage all of
these scenarios, you can transform fields to raw JSON by annotating it in a
Trino-specific structure in the
[\_meta](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-meta-field.html)
section of the index mapping. This indicates to Trino that the field, and all
nested fields beneath, need to be cast to a `VARCHAR` field that contains the
raw JSON content. These fields can be defined by using the following command to
add the field property definition to the `_meta.trino` property of the target
index mapping.

```shell
curl --request PUT \
    --url search.example.com:9200/doc/_mapping \
    --header 'content-type: application/json' \
    --data '
{
    "_meta": {
        "trino":{
            "array_string_field":{
                "asRawJson":true
            }
        }
    }
}'
```

This preceding configuration causes Trino to return the `array_string_field`
field as a `VARCHAR` containing raw JSON. You can parse these fields with the
[built-in JSON functions](/functions/json).

:::{note}
It is not allowed to use `asRawJson` and `isArray` flags simultaneously for the same column.
:::

## Special columns

The following hidden columns are available:

:::{list-table}
:widths: 25, 75
:header-rows: 1

* - Column
  - Description
* - `_id`
  - The Elasticsearch document ID.
* - `_score`
  - The document score returned by the Elasticsearch query.
* - `_source`
  - The source of the original document.
:::


(elasticsearch-full-text-queries)=
## Full text queries

Trino SQL queries can be combined with Elasticsearch queries by providing the [full text query]
as part of the table name, separated by a colon. For example:

```sql
SELECT * FROM "tweets: +trino SQL^2"
```

(elasticsearch-sql-support)=
## SQL support

The connector provides [globally available](sql-globally-available) and [read
operation](sql-read-operations) statements to access data and metadata in the
Elasticsearch catalog.

## Table functions

The connector provides specific {doc}`table functions </functions/table>` to
access Elasticsearch.

(elasticsearch-raw-query-function)=
### `raw_query(varchar) -> table`

The `raw_query` function allows you to query the underlying database directly.
This function requires [Elastic Query
DSL](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html)
syntax. The full DSL query is pushed down and processed in Elasticsearch. This
can be useful for accessing native features which are not available in Trino or
for improving query performance in situations where running a query natively may
be faster.

```{include} query-passthrough-warning.fragment
```

The `raw_query` function requires three parameters:

- `schema`: The schema in the catalog that the query is to be executed on.
- `index`: The index in Elasticsearch to be searched.
- `query`: The query to execute, written in [Elastic Query DSL](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html).

Once executed, the query returns a single row containing the resulting JSON
payload returned by Elasticsearch.

For example, query the `example` catalog and use the `raw_query` table function
to search for documents in the `orders` index where the country name is
`ALGERIA` as defined as a JSON-formatted query matcher and passed to the
`raw_query` table function in the `query` parameter:

```
SELECT
  *
FROM
  TABLE(
    example.system.raw_query(
      schema => 'sales',
      index => 'orders',
      query => '{
        "query": {
          "match": {
            "name": "ALGERIA"
          }
        }
      }'
    )
  );
```

```{include} query-table-function-ordering.fragment
```

## Performance

The connector includes a number of performance improvements, detailed in the
following sections.

### Parallel data access

The connector requests data from multiple nodes of the Elasticsearch cluster for
query processing in parallel.

### Predicate push down

The connector supports [predicate push down](predicate-pushdown) for the
following data types:

:::{list-table}
:widths: 50, 50
:header-rows: 1

* - Elasticsearch
  - Trino
* - `boolean`
  - `BOOLEAN`
* - `double`
  - `DOUBLE`
* - `float`
  - `REAL`
* - `byte`
  - `TINYINT`
* - `short`
  - `SMALLINT`
* - `integer`
  - `INTEGER`
* - `long`
  - `BIGINT`
* - `keyword`
  - `VARCHAR`
* - `date`
  - `TIMESTAMP`
:::

No other data types are supported for predicate push down.

[built-in date formats]: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html#built-in-date-formats
[custom date formats]: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html#custom-date-formats
[date]: https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
[format]: https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html#mapping-date-format
[full text query]: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#query-string-syntax
