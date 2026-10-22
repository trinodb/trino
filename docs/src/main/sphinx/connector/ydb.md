# YDB connector

The YDB connector queries row-oriented tables in a
[YDB](https://ydb.tech/) database using the YDB JDBC driver and YQL.
The coordinator and workers must be able to reach the database's gRPC
endpoint and the endpoints returned by YDB discovery.

## Configuration

Create `etc/catalog/example.properties` with the following configuration:

```properties
connector.name=ydb
connection-url=jdbc:ydb:grpcs://ydb.example.net:2135/database/path?useQueryService=true
```

Use `grpc` only for an endpoint that does not require TLS. Configure
authentication using the
[YDB JDBC connection options](https://github.com/ydb-platform/ydb-jdbc-driver).
Use {doc}`secrets </security/secrets>` for credentials.

Each catalog connects to one YDB database. The connector exposes one virtual
schema named `default`; it does not map Trino schemas to YDB directories:

```sql
SHOW TABLES FROM example.default;
SELECT * FROM example.default.events;
```

YDB table names are database-relative paths. Quote a path containing `/` as
one Trino identifier, for example `example.default."analytics/events"`.
Schema creation, deletion, and renaming are not supported.

## Type mapping

| YDB type | Trino type |
| --- | --- |
| `Bool` | `boolean` |
| `Int8`, `Int16`, `Int32`, `Int64` | `tinyint`, `smallint`, `integer`, `bigint` |
| `Uint8`, `Uint16`, `Uint32` | `smallint`, `integer`, `bigint` |
| `Uint64` | `decimal(20,0)` |
| `Float`, `Double` | `real`, `double` |
| `Utf8`, `Text` | Unbounded `varchar` |
| `String`, `Bytes` | `varbinary` |
| `Decimal(p,s)` | `decimal(p,s)` |
| `Date`, `Date32` | `date` |
| `Datetime`, `Datetime64`, `Timestamp`, `Timestamp64` | `timestamp(6)` |

Unsigned integers retain their numeric value, including the full `Uint64`
range from `0` to `18446744073709551615`. Binary values are not decoded as
UTF-8. Timestamp values use UTC rather than the coordinator's or worker's
default time zone.

YDB decimal NaN and infinity values cannot be represented by Trino decimals
and fail when read. YDB decimal precision is limited to 35.

Unsupported source types are omitted by default. The JDBC
`unsupported-type-handling=CONVERT_TO_VARCHAR` option exposes them as
read-only strings without predicate pushdown.

## Limitations

The connector is read-only. Creating, changing, or dropping tables and
schemas, `INSERT`, `UPDATE`, `DELETE`, and `MERGE` are not supported.
The `system.execute` native SQL procedure is also disabled.

Joins, aggregations, scalar expressions, sorting, and limits are evaluated
by Trino. Predicates use the JDBC connector's type-specific pushdown
controllers; unsigned, floating-point, decimal, and legacy temporal values
remain in Trino where native parameter or comparison semantics differ.

The connector defaults the JDBC driver option `cacheConnectionsInDriver` to
`false` because JDBC 2.4.1 can race cached context lookup against last-close
shutdown. Do not override this option to `true` on this driver version.
It also defaults `replaceJdbcInByYqlList` to `false` because JDBC 2.4.1's
optimized list parameter cannot bind native Timestamp64 SDK values.
