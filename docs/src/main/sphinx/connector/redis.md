# Redis connector

```{raw} html
<img src="../_static/img/redis.png" class="connector-logo">
```

The Redis connector allows querying of live data stored in [Redis](https://redis.io/). This can be
used to join data between different systems like Redis and Hive.

Each Redis key/value pair is presented as a single row in Trino. Rows can be
broken down into cells by using table definition files.

Currently, only Redis key of string and zset types are supported, only Redis value of
string and hash types are supported.

## Requirements

Requirements for using the connector in a catalog to connect to a Redis data
source are:

- Redis 2.8.0 or higher (Redis Cluster is not supported)
- Network access, by default on port 6379, from the Trino coordinator and
  workers to Redis.

## Configuration

To configure the Redis connector, create a catalog properties file
`etc/catalog/example.properties` with the following content, replacing the
properties as appropriate:

```text
connector.name=redis
redis.table-names=schema1.table1,schema1.table2
redis.nodes=host:port
```

### Multiple Redis servers

You can have as many catalogs as you need. If you have additional
Redis servers, simply add another properties file to `etc/catalog`
with a different name, making sure it ends in `.properties`.

## Configuration properties

The following configuration properties are available:

| Property name                       | Description                                                                                       |
| ----------------------------------- | ------------------------------------------------------------------------------------------------- |
| `redis.table-names`                 | List of all tables provided by the catalog                                                        |
| `redis.default-schema`              | Default schema name for tables                                                                    |
| `redis.nodes`                       | Location of the Redis server                                                                      |
| `redis.scan-count`                  | Redis parameter for scanning of the keys                                                          |
| `redis.max-keys-per-fetch`          | Get values associated with the specified number of keys in the redis command such as MGET(key...) |
| `redis.key-prefix-schema-table`     | Redis keys have schema-name:table-name prefix                                                     |
| `redis.key-delimiter`               | Delimiter separating schema_name and table_name if redis.key-prefix-schema-table is used          |
| `redis.table-description-dir`       | Directory containing table description files                                                      |
| `redis.table-description-cache-ttl` | The cache time for table description files                                                        |
| `redis.hide-internal-columns`       | Controls whether internal columns are part of the table schema or not                             |
| `redis.database-index`              | Redis database index                                                                              |
| `redis.user`                        | Redis server username                                                                             |
| `redis.password`                    | Redis server password                                                                             |

### `redis.table-names`

Comma-separated list of all tables provided by this catalog. A table name
can be unqualified (simple name) and is placed into the default schema
(see below), or qualified with a schema name (`<schema-name>.<table-name>`).

For each table defined, a table description file (see below) may
exist. If no table description file exists, the
table only contains internal columns (see below).

This property is optional; the connector relies on the table description files
specified in the `redis.table-description-dir` property.

### `redis.default-schema`

Defines the schema which will contain all tables that were defined without
a qualifying schema name.

This property is optional; the default is `default`.

### `redis.nodes`

The `hostname:port` pair for the Redis server.

This property is required; there is no default.

Redis Cluster is not supported.

### `redis.scan-count`

The internal COUNT parameter for the Redis SCAN command when connector is using
SCAN to find keys for the data. This parameter can be used to tune performance
of the Redis connector.

This property is optional; the default is `100`.

### `redis.max-keys-per-fetch`

The internal number of keys for the Redis MGET command and Pipeline HGETALL command
when connector is using these commands to find values of keys. This parameter can be
used to tune performance of the Redis connector.

This property is optional; the default is `100`.

### `redis.key-prefix-schema-table`

If true, only keys prefixed with the `schema-name:table-name` are scanned
for a table, and all other keys are filtered out.  If false, all keys are
scanned.

This property is optional; the default is `false`.

### `redis.key-delimiter`

The character used for separating `schema-name` and `table-name` when
`redis.key-prefix-schema-table` is `true`

This property is optional; the default is `:`.

### `redis.table-description-dir`

References a folder within Trino deployment that holds one or more JSON
files, which must end with `.json` and contain table description files.

Note that the table description files will only be used by the Trino coordinator
node.

This property is optional; the default is `etc/redis`.

### `redis.table-description-cache-ttl`

The Redis connector dynamically loads the table description files after waiting
for the time specified by this property. Therefore, there is no need to update
the `redis.table-names` property and restart the Trino service when adding,
updating, or deleting a file end with `.json` to `redis.table-description-dir`
folder.

This property is optional; the default is `5m`.

### `redis.hide-internal-columns`

In addition to the data columns defined in a table description file, the
connector maintains a number of additional columns for each table. If
these columns are hidden, they can still be used in queries, but they do not
show up in `DESCRIBE <table-name>` or `SELECT *`.

This property is optional; the default is `true`.

### `redis.database-index`

The Redis database to query.

This property is optional; the default is `0`.

### `redis.user`

The username for Redis server.

This property is optional; the default is `null`.

### `redis.password`

The password for password-protected Redis server.

This property is optional; the default is `null`.

## Internal columns

For each defined table, the connector maintains the following columns:

| Column name      | Type    | Description                                                                                                                                |
| ---------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `_key`           | VARCHAR | Redis key.                                                                                                                                 |
| `_value`         | VARCHAR | Redis value corresponding to the key.                                                                                                      |
| `_key_length`    | BIGINT  | Number of bytes in the key.                                                                                                                |
| `_value_length`  | BIGINT  | Number of bytes in the value.                                                                                                              |
| `_key_corrupt`   | BOOLEAN | True if the decoder could not decode the key for this row. When true, data columns mapped from the key should be treated as invalid.       |
| `_value_corrupt` | BOOLEAN | True if the decoder could not decode the message for this row. When true, data columns mapped from the value should be treated as invalid. |

For tables without a table definition file, the `_key_corrupt` and
`_value_corrupt` columns are `false`.

## Table definition files

With the Redis connector it is possible to further reduce Redis key/value pairs into
granular cells, provided the key/value string follows a particular format. This process
defines new columns that can be further queried from Trino.

A table definition file consists of a JSON definition for a table. The
name of the file can be arbitrary, but must end in `.json`.

```text
{
    "tableName": ...,
    "schemaName": ...,
    "key": {
        "dataFormat": ...,
        "fields": [
            ...
        ]
    },
    "value": {
        "dataFormat": ...,
        "fields": [
            ...
       ]
    }
}
```

| Field        | Required | Type        | Description                                                                       |
| ------------ | -------- | ----------- | --------------------------------------------------------------------------------- |
| `tableName`  | required | string      | Trino table name defined by this file.                                            |
| `schemaName` | optional | string      | Schema which will contain the table. If omitted, the default schema name is used. |
| `key`        | optional | JSON object | Field definitions for data columns mapped to the value key.                       |
| `value`      | optional | JSON object | Field definitions for data columns mapped to the value itself.                    |

Please refer to the [Kafka connector](/connector/kafka) page for the description of the `dataFormat` as well as various available decoders.

In addition to the above Kafka types, the Redis connector supports `hash` type for the `value` field which represent data stored in the Redis hash.

```text
{
    "tableName": ...,
    "schemaName": ...,
    "value": {
        "dataFormat": "hash",
        "fields": [
            ...
       ]
    }
}
```

## Type mapping

Because Trino and Redis each support types that the other does not, this
connector {ref}`maps some types <type-mapping-overview>` when reading data. Type
mapping depends on the RAW, CSV, JSON, and AVRO file formats.

### Row decoding

A decoder is used to map data to table columns.

The connector contains the following decoders:

- `raw`: Message is not interpreted; ranges of raw message bytes are mapped
  to table columns.
- `csv`: Message is interpreted as comma separated message, and fields are
  mapped to table columns.
- `json`: Message is parsed as JSON, and JSON fields are mapped to table
  columns.
- `avro`: Message is parsed based on an Avro schema, and Avro fields are
  mapped to table columns.

:::{note}
If no table definition file exists for a table, the `dummy` decoder is
used, which does not expose any columns.
:::

```{include} raw-decoder.fragment
```

```{include} csv-decoder.fragment
```

```{include} json-decoder.fragment
```

```{include} avro-decoder.fragment
```

(redis-sql-support)=

## SQL support

The connector provides {ref}`globally available <sql-globally-available>` and
{ref}`read operation <sql-read-operations>` statements to access data and
metadata in Redis.

## Performance

The connector includes a number of performance improvements, detailed in the
following sections.

(redis-pushdown)=

### Pushdown

```{include} pushdown-correctness-behavior.fragment
```

(redis-predicate-pushdown)=

#### Predicate pushdown support

The connector supports pushdown of keys of `string` type only, the `zset`
type is not supported. Key pushdown is not supported when multiple key fields
are defined in the table definition file.

The connector supports pushdown of equality predicates, such as `IN` or `=`.
Inequality predicates, such as `!=`, and range predicates, such as `>`,
`<`, or `BETWEEN` are not pushed down.

In the following example, the predicate of the first query is not pushed down
since `>` is a range predicate. The other queries are pushed down:

```sql
-- Not pushed down
SELECT * FROM nation WHERE redis_key > 'CANADA';
-- Pushed down
SELECT * FROM nation WHERE redis_key = 'CANADA';
SELECT * FROM nation WHERE redis_key IN ('CANADA', 'POLAND');
```
