# Loki connector

```{raw} html
<img src="../_static/img/loki.png" class="connector-logo">
```

The Loki connector allows querying log data stored in [Grafana
Loki](https://grafana.com/oss/loki/). This document describes how to configure a
catalog with the Loki connector to run SQL queries against Loki.

## Requirements

To connect to Loki, you need:

- Loki 3.1.0 or higher.
- Network access from the Trino coordinator and workers to Loki. Port 3100 is
  the default port.

## Configuration

The connector can query log data in Loki. Create a catalog properties file that
specifies the Loki connector by setting the `connector.name` to `loki`.

For example, to access a database as the `example` catalog, create the file
`etc/catalog/example.properties`.

```text
connector.name=loki
loki.uri=http://loki.example.com:3100
```

The following table contains a list of all available configuration properties.

:::{list-table} Loki configuration properties
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `loki.uri`
  - The URI endpoint for the Loki server that Trino cluster nodes use to access
    the Loki APIs.
* - `loki.query-timeout`
  - [Duration](prop-type-duration) that Trino waits for a result from Loki
    before the specific query request times out. Defaults to `10s`. A minimum of
    `1s` is required.
:::

(loki-type-mapping)=
## Type mapping

Because Trino and Loki each support types that the other does not, this
connector [modifies some types](type-mapping-overview) when reading data.

### Loki to Trino type mapping

Each log line in Loki is split up by the connector into three columns:

* `timestamp`
* `values`
* `labels`

These are separately mapped to Trino types:

:::{list-table} Loki log entry to Trino type mapping
:widths: 40, 60
:header-rows: 1

* - Loki type
  - Trino type
* - `timestamp`
  - `TIMESTAMP WITH TIME ZONE`
* - `values` for [log queries](https://grafana.com/docs/loki/latest/query/log_queries/)
  - `VARCHAR`
* - `values` for [metrics queries](https://grafana.com/docs/loki/latest/query/metric_queries/)
  - `DOUBLE`
* - `labels`
  - `MAP` with label names and values as `VARCHAR` key value pairs
:::

No other types are supported.

(loki-sql-support)=
## SQL support

The Loki connector does not provide access to any schema or tables. Instead you
must use the [query_range](loki-query-range) table function to return a table
representation of the desired log data. Use the data in the returned table like
any other table in a SQL query, including use of functions, joins, and other SQL
functionality.

(lok-table-functions)=
### Table functions

The connector provides the following [table function](/functions/table) to
access Loki.

(loki-query-range)=
### `query_range(varchar, timestamp, timestamp) -> table`

The `query_range` function allows you to query the log data in Loki with the
following parameters:

* The first parameter is a `varchar` string that uses valid
  [LogQL](https://grafana.com/docs/loki/latest/query/) query.
* The second parameter is a `timestamp` formatted data and time representing the
  start date and time of the log data range to query.
* The third parameter is a `timestamp` formatted data and time representing the
  end date and time of the log data range to query.

The table function is available in the `system` schema of the catalog using the
Loki connector, and returns a table with the columns `timestamp`, `value`, and
`labels` described in the [](loki-type-mapping) section.

The following query invokes the `query_range` table function in the `example`
catalog. It uses the LogQL query string `{origin="CA"}` to retrieve all log data
with the value `CA` for the `origin` label on the log entries. The timestamp
parameters set a range of all log entries from the first of January 2025. 

```sql
SELECT timestamp, value 
FROM
  TABLE(
    example.system.query_range(
      '{origin="CA"}',
      TIMESTAMP '2025-01-01 00:00:00',
      TIMESTAMP '2025-01-02 00:00:00'
    )
  )
;
```    

The query only returns the timestamp and value for each log entry, and omits the
label data in the `labels` column. The value is a `varchar` string since the
LoqQL query is a log query.

## Examples

The following examples show case combinations of
[LogQL](https://grafana.com/docs/loki/latest/query/) queries passed through the
table function with SQL accessing the data in the returned table. 

The following query uses a metrics query and therefore returns a `count` column
with double values, limiting the result data to the latest 100 values.

```sql
SELECT value AS count
FROM
  TABLE(
    example.system.query_range(
      'count_over_time({test="metrics_query"}[5m])',
      TIMESTAMP '2025-01-01 00:00:00',
      TIMESTAMP '2025-01-02 00:00:00'
    )
  )
ORDER BY timestamp DESC
LIMIT 100;
```

The following query accesses the value of the label named `province` and returns
it as separate column.

```sql
SELECT 
  timestamp, 
  value, 
  labels['province'] AS province
FROM
  TABLE(
    example.system.query_range(
      '{origin="CA"}',
      TIMESTAMP '2025-01-01 00:00:00',
      TIMESTAMP '2025-01-02 00:00:00'
    )
  )
;
