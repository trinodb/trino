# YDB connector

The YDB connector queries and modifies row-oriented tables in a
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
default time zone. Writing fractional seconds to a YDB `Datetime` or
`Datetime64` column fails rather than truncating the value.

YDB decimal NaN and infinity values cannot be represented by Trino decimals
and fail when read. YDB decimal precision is limited to 35.

New tables use `Date32` for `date`, `Timestamp64` for `timestamp(3)` and
`timestamp(6)`, `Text` for `varchar`, and `Bytes` for `varbinary`.
Timestamp precision is reported as six digits when the table is read again.
Other timestamp precisions, `time`, time zone types, `char`, and container
types do not have write mappings.

Unsupported source types are omitted by default. The JDBC
`unsupported-type-handling=CONVERT_TO_VARCHAR` option exposes them as
read-only strings without predicate or JOIN pushdown.

## Creating tables

YDB tables require an ordered primary key. Set the `primary_key` table
property explicitly:

```sql
CREATE TABLE example.default.events (
    tenant bigint,
    event_id bigint,
    payload varchar
)
WITH (primary_key = ARRAY['tenant', 'event_id']);

CREATE TABLE example.default.events_copy
WITH (primary_key = ARRAY['tenant', 'event_id'])
AS SELECT tenant, event_id, payload FROM example.default.events;
```

The connector validates missing, empty, duplicate, and unknown primary-key
columns. It does not add hidden keys to user tables. Primary-key columns
must satisfy YDB's native type restrictions.

## Writes and transactions

The connector supports `INSERT`, `UPDATE`, `DELETE`, and `MERGE`.
Pushed-down `UPDATE` and `DELETE` obtain their affected-row counts using
YQL `RETURNING`, not a separate count query.

Changing a physical primary-key column with `UPDATE` or a `MERGE` update
clause is rejected because [YQL UPDATE cannot change primary
keys](https://ydb.tech/docs/en/yql/reference/syntax/update).
The primary-key column order comes from JDBC metadata, not the physical
column order.

Transactional `INSERT` writes to a staging table and copies its rows into
the target when the query finishes. Opting into
`insert.non-transactional-insert.enabled=true` writes directly to the
target; a failed insert can leave previously committed batches.

`MERGE` uses one writer and one JDBC transaction for its insert, delete,
and update operations. It rolls back that transaction on a write failure
or cancellation before commit. The source and target scans performed by
Trino are not part of that JDBC transaction. YDB transaction size and
duration limits apply. A lost commit response can leave the commit outcome
unknown; the connector does not replay the transaction.

Query and task retries for writes are not supported. DDL and DML are not
replayed by a connector-owned retry loop.

Table and column comments, views, column renaming, column type changes,
column defaults, and `TRUNCATE` are not supported.

## Pushdown

JOIN pushdown is disabled by default. Enable it for a catalog with
`join-pushdown.enabled=true`, or for a session:

```sql
SET SESSION example.join_pushdown_enabled = true;
```

The connector supports `INNER`, `LEFT`, `RIGHT`, and `FULL` equality JOINs
on direct columns with compatible value mappings: booleans, signed and
unsigned integers, floating-point values, text, bytes, dates, and
timestamps. Multiple equality conditions are supported.

SQL NULL keys never match. Floating-point NaN keys do not match, consistent
with Trino's ordinary equality operator; positive and negative zero match.
The connector normalizes these keys before constructing the YQL JOIN.

Native decimal keys, forced string mappings, inequalities, null-safe
equality, and unsupported computed or cast expressions are evaluated by
Trino. Arithmetic that can differ on overflow also stays in Trino, even
when projected below a JOIN.

The connector also supports limit pushdown and selected predicates,
aggregations, and Top-N operations. Unsigned, floating-point, decimal,
and legacy temporal predicates are restricted where parameter conversion,
ordering, or range semantics differ. Integral and decimal `sum` and `avg`,
floating-point grouping and extrema, and unsafe casts stay in Trino.
