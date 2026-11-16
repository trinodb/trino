---
myst:
  substitutions:
    default_domain_compaction_threshold: '`256`'
---

# YDB connector

The YDB connector allows querying row-oriented tables in a
[YDB](https://ydb.tech/) database.

## Requirements

To connect to YDB, you need:

- Network access from the Trino coordinator and workers to the YDB gRPC
  endpoint and to the endpoints returned by YDB discovery. Port 2135 is the
  default port for TLS connections.

## Configuration

To configure the YDB connector as the ``example`` catalog, create a file
named ``example.properties`` in ``etc/catalog``. Include the following
connection properties in the file:

```text
connector.name=ydb
connection-url=jdbc:ydb:grpcs://ydb.example.net:2135/database/path
```

The ``connection-url`` defines the connection information and parameters to
pass to the JDBC driver. Use `grpc` only for an endpoint that does not require
TLS. See the [YDB JDBC driver documentation](https://github.com/ydb-platform/ydb-jdbc-driver)
for the supported authentication options. You can use
{doc}`secrets </security/secrets>` to avoid using actual values in catalog
properties files.

Each catalog connects to one YDB database. The connector exposes one schema
named `default`, it does not map Trino schemas to YDB directories:

```sql
SHOW TABLES FROM example.default;
SELECT * FROM example.default.events;
```

YDB table names are database-relative paths. Quote a path containing `/` as
one Trino identifier, for example `example.default."analytics/events"`.

```{include} jdbc-common-configurations.fragment
```

```{include} jdbc-domain-compaction-threshold.fragment
```

(ydb-type-mapping)=
## Type mapping

Because Trino and YDB each support types that the other does not, this
connector {ref}`modifies some types <type-mapping-overview>` when reading data.

### YDB to Trino type mapping

The connector maps YDB types to the corresponding Trino types according to the
following table:

:::{list-table} YDB to Trino type mapping
:widths: 40, 60
:header-rows: 1

* - YDB type
  - Trino type
* - `Bool`
  - `BOOLEAN`
* - `Int8`
  - `TINYINT`
* - `Int16`
  - `SMALLINT`
* - `Int32`
  - `INTEGER`
* - `Int64`
  - `BIGINT`
* - `Uint8`
  - `SMALLINT`
* - `Uint16`
  - `INTEGER`
* - `Uint32`
  - `BIGINT`
* - `Uint64`
  - `DECIMAL(20, 0)`
* - `Float`
  - `REAL`
* - `Double`
  - `DOUBLE`
* - `Decimal(p, s)`
  - `DECIMAL(p, s)`
* - `Text`
  - `VARCHAR`
* - `Date`
  - `DATE`
* - `Date32`
  - `DATE`
:::

No other types are supported.

YDB decimal `NaN` and infinity values cannot be represented by Trino decimals
and fail when read.

```{include} jdbc-type-mapping.fragment
```

(ydb-sql-support)=
## SQL support

The connector provides read access to data and metadata in YDB. In addition
to the [globally available](sql-globally-available) and [read
operation](sql-read-operations) statements, the connector supports the
following features:

- [](ydb-procedures)

(ydb-procedures)=
### Procedures

```{include} jdbc-procedures-flush.fragment
```
```{include} procedures-execute.fragment
```

## Performance

The connector includes a number of performance improvements, detailed in the
following sections.

(ydb-pushdown)=
### Pushdown

The connector supports {ref}`predicate pushdown <predicate-pushdown>`.
