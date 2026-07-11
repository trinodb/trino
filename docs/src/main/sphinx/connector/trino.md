---
myst:
  substitutions:
    default_domain_compaction_threshold: '`256`'
---

# Trino connector

The Trino connector queries a remote Trino cluster through JDBC and exposes it
as a local read-only catalog.

## Requirements

- The remote Trino cluster must be reachable through the Trino JDBC driver.
- The connector is tested when the local and remote clusters run the same Trino
  release. Compatibility across different Trino releases is not guaranteed.

## Configuration

Create a catalog properties file such as `etc/catalog/remote.properties`:

```properties
connector.name=trino
connection-url=jdbc:trino://remote-trino-host:443/catalog_name
connection-user=myuser
connection-password=mypassword
```

Each connector instance maps one remote catalog to one local catalog.

Append `?SSL=true` for TLS connections:

```properties
connection-url=jdbc:trino://remote-host:443/catalog_name?SSL=true
```

### Connector configuration properties

:::{list-table}
:widths: 35, 55, 10
:header-rows: 1

* - Property name
  - Description
  - Default
* - `connection-url`
  - JDBC URL for the remote Trino cluster. The URL path must include the remote
    catalog and can include a default schema.
  - Required
* - `unsupported-type-handling`
  - Final fallback for unsupported types. Allowed values are `IGNORE` and
    `CONVERT_TO_VARCHAR`.
  - `CONVERT_TO_VARCHAR`
* - `remote-delegation.enabled`
  - Enable Trino-native SQL rendering for compatible remote fragments.
  - `true`
* - `statistics.enabled`
  - Retrieve remote table statistics with `SHOW STATS`.
  - `true`
:::

```{include} jdbc-authentication.fragment
```

```{include} jdbc-common-configurations.fragment
```

```{include} query-comment-format.fragment
```

```{include} jdbc-domain-compaction-threshold.fragment
```

```{include} jdbc-case-insensitive-matching.fragment
```

## Querying

After configuration, the remote catalog is visible through the local catalog:

```sql
SHOW SCHEMAS FROM remote;
SHOW TABLES FROM remote.myschema;
SELECT * FROM remote.myschema.mytable LIMIT 10;
```

### Cross-catalog joins

```sql
SELECT l.id, r.metric
FROM local_catalog.schema.table l
JOIN remote.external_schema.external_table r
    ON l.id = r.id;
```

## Type mapping

The connector uses four transport modes and an unsupported outcome:

- **Native**: JDBC preserves the type exactly
- **VARCHAR transport**: unsupported scalar temporal or interval values are
  projected as `VARCHAR` and decoded back to the original logical type
- **VARBINARY transport**: top-level sketch types are projected as `VARBINARY`
  and decoded back to the original logical type
- **JSON transport**: unsupported structural columns are recursively rewritten,
  projected as JSON text, and decoded back to the original logical type
- **Unsupported**: only types that still cannot be represented safely

### Native reads

Native scalar reads include:

- `boolean`
- `tinyint`, `smallint`, `integer`, `bigint`
- `real`, `double`, `decimal(p,s)`
- `number`
- `char`, `varchar`, `varbinary`
- `date`
- exact `time(p<=9)` and `timestamp(p<=9)`
- `uuid`, `json`, `ipaddress`

Native complex reads are allowed only when every descendant leaf is natively
readable. Date leaves, fractional `time(p>0)` leaves, and timestamp leaves
inside structural types use JSON transport because the JDBC complex-value
representation cannot preserve them exactly.

Examples:

- `array(varchar)`
- `map(varchar, bigint)`
- `row(id uuid, payload json)`
- `row(events array(map(varchar, varchar)))`

### VARCHAR transport

Top-level scalar fallback currently covers:

- `time with time zone`
- `timestamp(p) with time zone` at every precision; the wire value carries
  the UTC instant and original zone ID separately
- `interval year to month`
- `interval day to second`
- high-precision `time(p)` and `timestamp(p)` when JDBC is not exact

These columns still appear locally as their original logical Trino types.

### JSON transport

If a structural column contains a descendant that cannot be read natively, the
connector rewrites the whole column through JSON transport and reconstructs the
original logical type locally.

Examples:

- `array(timestamp(3))`
- `array(timestamp(12))`
- `array(time(3))`
- `array(date)`
- `map(varchar, interval day to second)`
- `row(ts timestamp(12), attrs map(varchar, varchar))`

JSON transport contract:

- `map(varchar, v)` stays a JSON object
- `map(non-varchar, v)` is normalized to `array(row(key, value))`
- row fields are encoded positionally and reconstructed by the declared row type
- nested unsupported scalar leaves are encoded through string surrogates

### VARBINARY transport

Top-level statistical sketch types are projected as `VARBINARY` and decoded
back to the original logical Trino type locally.

Current coverage:

- `HyperLogLog`
- `P4HyperLogLog`
- `qdigest(T)`
- `setdigest`
- `tdigest`

### Unsupported

`unsupported-type-handling` remains the final fallback for types that still
cannot be represented safely. This mainly applies to opaque or connector-specific
types without an explicit transport rule, and nested descendants that do not yet
have a safe recursive transport contract.

## Query passthrough

Execute a manual top-level query statement on the remote Trino:

```sql
SELECT *
FROM TABLE(
    remote.system.query(
        query => 'SELECT * FROM information_schema.tables LIMIT 10'
    )
)
```

`system.query` is an explicit bypass for the normal connector planning path:

- the inner SQL is sent without expression rewriting; the connector can strip
  a trailing semicolon and wrap the query to assign stable output column aliases
- the connector still infers output columns and applies the normal type
  mapping and transport rules to the result
- only top-level query statements (`SELECT`, `WITH`, `VALUES`, `TABLE`) are
  accepted; this syntactic check does not prove that invoked functions or table
  functions are free of side effects, so remote access control and read-only
  credentials are the execution boundary
- while normal table access maps 1:1 to the configured remote catalog,
  passthrough SQL is an explicit escape hatch from that mapping
- remote query preparation and execution failures are returned directly;
  explicit passthrough SQL is not a fallback candidate

```{include} query-table-function-ordering.fragment
```

## SQL support

The connector provides {ref}`globally available <sql-globally-available>` and
{ref}`read operation <sql-read-operations>` statements to access data and
metadata in the remote Trino catalog. Write operations and DDL statements are
not supported.

## Performance

### Pushdown

The connector supports:

- predicate pushdown
- projection pushdown
- Trino-native remote SQL delegation for compatible casts, comparisons, boolean
  logic, arithmetic, `LIKE`, `IN`, regexp, JSON, date/time functions,
  dereference, subscript, and aggregation expressions
- `LIMIT` pushdown
- partial `ORDER BY ... LIMIT` pushdown with local ordering verification
- aggregation pushdown for `count`, `count distinct`, `count_if`,
  `checksum`, `min/max`, `sum`, and `avg` on supported types
- same-remote join pushdown for supported join shapes

```{include} join-pushdown-enabled-true.fragment
```

Remote delegation delegates compatible remote subtrees and leaves unsupported
expressions as local fallback when that preserves semantics. Disabling it
(`remote-delegation.enabled=false`) leaves only the baseline JDBC pushdown
path enabled. The equivalent catalog session property is
`remote_delegation_enabled`.

Pushdown behavior for transport-backed columns is split:

- scalar `VARCHAR` transport (`time(p>9)`, `timestamp(p>9)`,
  `timestamp(p) with time zone`, `time with time zone`, interval types)
  keeps tuple-domain predicate pushdown enabled via typed bind expressions such
  as `CAST(? AS timestamp(12))` or
  `INTERVAL '0.001' SECOND * CAST(? AS BIGINT)`
- structural `JSON` transport remains `DISABLE_PUSHDOWN`
- sketch `VARBINARY` transport remains `DISABLE_PUSHDOWN`

This keeps remote filtering available where the connector can still bind the
original logical type safely, while avoiding pushdown on carrier-only
transports whose semantics would otherwise drift from the original remote
column.

### Statistics

`getTableStatistics()` queries remote Trino with `SHOW STATS FOR <table>` and
feeds the result into the local optimizer. If the remote side cannot provide
statistics, the connector falls back to unknown statistics.

`AUTOMATIC` join pushdown requires remote column statistics, including the
distinct-value count for join keys. If the remote catalog only reports a table
row count, the join remains local.

## Security

All remote SQL executes with credentials resolved by the standard JDBC
credential provider described in the data source authentication section. Named
extra credentials are used only when explicitly configured with
`user-credential-name` and `password-credential-name`.

The connector does not transparently forward the local Trino user's identity,
role, session properties, or arbitrary extra credentials to the remote Trino
cluster.

Not supported:

- transparent end-user identity propagation
- remote session property forwarding
- role delegation
- arbitrary extra credential forwarding

Session-sensitive functions and casts such as `current_timestamp`,
`current_date`, `current_time`, current time zone functions, locale-sensitive
date formatting, and casts that depend on the session start date are evaluated
locally. Time-zone-dependent expressions such as `from_iso8601_timestamp` are
delegated only when local and remote time zones match. Expressions with explicit
time zone operands, such as `AT TIME ZONE 'Asia/Seoul'`, can be delegated when
the rendered SQL is otherwise compatible.

## Limitations

- Standard table access and metadata operations are read-only
- `system.query` accepts only top-level query statements; top-level writes
  (DDL, DML, `CALL`) are rejected before remote execution, while functions
  and table functions remain governed by remote access control
- `CALL system.execute(...)` is inherited from the base JDBC framework but
  is denied by this connector's read-only access control
- Remote delegation probes `CHAR` to `VARCHAR` cast semantics and trims
  legacy remote padding when such casts are pushed down
- Cross-cluster joins can only be improved with pushdown and statistics; the
  connector cannot remove the structural cost of federating between clusters
