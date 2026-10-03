# YDB Trino Adapter — roadmap

This roadmap tracks the connector against Trino 483
`BaseConnectorTest`/`BaseConnectorSmokeTest`. A green test is meaningful only
when it exercises the advertised behavior. Empty overrides and false capability
flags are test debt, not support.

## Trino 483 dependency migration

Trino 483 is the latest stable release published by both the
[official Trino releases](https://github.com/trinodb/trino/releases/tag/483)
and [Maven Central](https://repo.maven.apache.org/maven2/io/trino/trino-spi/maven-metadata.xml).
It requires 64-bit Java 25, with a minimum patch level of 25.0.1; the module and
CI continue to use Java 25.

The 479-to-483 source migration consists of the following upstream contract
changes:

- `ColumnMetadata.getComment()` now returns `Optional<String>`;
- JDBC merge rollback registration now accepts `Consumer<Runnable>`;
- JDBC create-table rollback registration uses the same callback shape, and
  destination-table cleanup is now separate from temporary-table cleanup; the
  adapter inherits both implementations;
- page-sink provider methods now receive optional table credentials, which are
  intentionally unused because YDB writes remain authenticated through their
  JDBC connection;
- `MODULO_FUNCTION_NAME` replaces the deprecated
  `MODULUS_FUNCTION_NAME` connector-expression constant.

The SPI also removed the deprecated session-taking `Type.getObject` overload
and `Type.appendTo`; the adapter already uses the retained
`Type.getObject(Block, int)` method.

`QueryBuilder` is source-identical between 479 and 483, and the relevant
`JdbcPageSink`/`JdbcMergeSink` constructors are unchanged. The
`TestingConnectorBehavior` enum has no additions, removals, renamed values, or
default changes. The active inherited-test delta adds
`testVarcharEqualityPushdownIgnoresTrailingSpaces`; it must pass against YDB
before this upgrade can be declared green. The new materialized-view `WHEN
STALE` tests remain skipped by the existing unsupported materialized-view
capability. Trino 483 removes the inherited `testDropTableIfExists`,
`testMaterializedViewWhenStale`, `testShowCreateInformationSchema`,
`testShowCreateInformationSchemaTable`, `testShowInformationSchemaTables`, and
`testSymbolAliasing` methods, plus the duplicate smoke-test information-schema
method.

Historical 479-to-483 migration validation with Temurin 25.0.2:

- clean test compilation: 16 production sources and 5 test sources compiled;
- package with tests skipped: successful;
- dependency audit: every Trino release-coupled artifact resolves to 483;
- Docker-backed tests: 0 completed. The existing Colima default profile is
  unavailable during that run. These historical counts do not validate the
  subsequent changes below.

## Pre-upgrade DML baseline (Trino 479)

PR #240 adds DELETE, UPDATE, row-level UPDATE, and MERGE. The following pieces
have been verified locally against a real YDB test container:

- typed predicates for DELETE and UPDATE, including `varchar` values;
- exact affected-row counts through YQL `RETURNING`, without a racy pre-count;
- `Float`/`Double` write mappings during MERGE;
- real JDBC primary-key metadata, including `KEY_SEQ` ordering;
- concurrent updates identified by the test-only hidden primary key;
- smoke MERGE and row-level UPDATE without disabling their behavior flags;
- retry classification through the YDB SDK status model, with fresh merge
  connections and rollback-before-close.

GitHub Actions is green on PR #240: 314 tests run, 0 failed, 0 errors, 84
skipped. This includes 36 smoke tests (4 skipped) and 278 connector tests (80
skipped). The inherited `testMergeLarge` runs without an override and completes
within the CI budget. An isolated local Colima run previously exceeded 11
minutes, so MERGE scalability remains a production concern rather than a CI
failure.

## MERGE contract after connector review

`JdbcMergeSink` discards native metadata in its operation-specific handles and
retains connections until finish/abort. Trino 483 does not call merge-sink abort
from operator close and does not enforce the connector's writer limit for MERGE.
A single-writer setting cannot establish statement atomicity.

The connector therefore requires explicit `non_transactional_merge=true` for
row-level writes. `YdbMergeSink` uses native mappings and bounded, owned batch
transactions, preserves operation and composite-key order, handles NULL keys,
and closes all resources before returning. Earlier successful batches remain
committed on failure; no replay is attempted. Physical-key changes fail with
`NOT_SUPPORTED` before execution, as required by the
[YQL UPDATE contract](https://ydb.tech/docs/en/yql/reference/syntax/update).

Unit tests cover order, nullable keys, rollback, partial batch failure and
resource closure; production-client tests cover composite-key MERGE and default
INSERT staging. Full inherited tests must also pass before a release claim.
Statement-atomic MERGE remains future work, requiring coordinator finalization
or verified upstream planner and cancellation changes, not an implicit promise.

## P1 — retry and transaction hardening

- Do not wrap a merge sink in connector-owned replay after any driver or YDB
  failure. A future retry design requires staging or an operation ID that proves
  replay safety. See
  [YDB SDK error handling](https://ydb.tech/docs/en/reference/ydb-sdk/error_handling).
- The connector defaults `cacheConnectionsInDriver=false` for JDBC 2.4.1.
  `YdbDriver.connect` obtains a cached context before connection registration,
  while the last connection close can remove and shut down that same context.
  CI exposed a terminated session-pool executor during a concurrent UPDATE.
  Per-connection context ownership avoids this race without replay. URL options
  override Properties; explicitly re-enabling this cache is not supported.
  See the [pinned driver lifecycle](https://github.com/ydb-platform/ydb-jdbc-driver/blob/v2.4.1/jdbc/src/main/java/tech/ydb/jdbc/YdbDriver.java).
- Do not replay buffered INSERT pages after `JdbcPageSink` may already have
  committed an internal batch.
- Add tests for partial sink failures and abort cleanup.

Concurrent `ALTER TABLE ... ADD COLUMN` statements on one table can be rejected
by YDB with `OVERLOADED` (400060) and the specific issue `path is under
operation` in `EPathStateAlter`. The inherited Trino test permits only this
exact connector-specific conflict and still verifies every successfully added
column. This contract does not add an `ADD COLUMN` retry. The connector uses
Trino's one-shot `BaseJdbcClient` statement execution for DDL, so a transient
failure cannot replay a non-idempotent statement on the same connection. See
the [YDB status-code contract](https://ydb.tech/docs/en/reference/ydb-sdk/ydb-status-codes).

## P2 — remove remaining test debt

Audit every inherited-test override and every `hasBehavior` exception. For each
unsupported behavior, record:

1. the exact YDB/YQL or Trino limitation;
2. an authoritative documentation link or tracked upstream issue;
3. a focused negative test that proves the connector fails clearly.

New Trino date columns use signed `Date32`; the connector tests no longer
exclude negative dates. Legacy YDB `Date` starts at the Unix epoch and is
handled separately; see
[primitive types](https://ydb.tech/docs/en/yql/reference/types/primitive).
CHAR is rejected with the focused inherited contract
(`Unsupported column type: char(3)`) because YDB has no fixed-width string
primitive and mapping it to `Text` would lose Trino padding semantics.

Nullable `ADD COLUMN` remains supported. The separate
`SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT` capability remains false because the
connector explicitly rejects `ADD COLUMN ... NOT NULL`; Trino's inherited
`testAddNotNullColumnToEmptyTable` verifies that rejection. Current YQL syntax
documents `NOT NULL`, but the capability must not be enabled until the YDB
image used by the test suite passes Trino's complete empty-table,
non-empty-table, and nullability-metadata contract. Column defaults remain a
separate capability. See YDB
[`ALTER TABLE ... ADD COLUMN`](https://ydb.tech/docs/en/yql/reference/syntax/alter_table/columns).

The former empty default-column INSERT override has been replaced with a
YDB-native row-table fixture. The inherited `testInsertForDefaultColumn` now
verifies omitted literal defaults, explicit values and nulls, and reordered
insert columns. YDB supports literal defaults on row-oriented tables, but the
Trino 483 `SUPPORTS_DEFAULT_COLUMN_VALUE` behavior remains false because its
group also advertises CREATE, ADD, NOT NULL/default, and MERGE contracts that
the connector does not fully implement. SET and DROP have child capabilities
that inherit from this behavior and also remain false. See YDB
[`CREATE TABLE`](https://ydb.tech/docs/en/yql/reference/syntax/create_table).

## Later capability work

- one configured YDB database per Trino catalog, with the virtual `default` schema; schema DDL remains unsupported;
- List/Dict/Struct mappings for Trino ARRAY/MAP/ROW;
- views, comments, rename column, and type changes after checking current YQL
  semantics;
- complete real-YDB validation of default transactional INSERT staging;
- complete the Trino 483 default-column behavior group before advertising it.

## Validation ladder

Use JDK 25 at patch level 25.0.1 or newer and the Docker/Testcontainers
environment documented in the root `AGENTS.md`:

```bash
mvn -f ydb-trino-adapter/pom.xml -DskipTests compile
mvn -f ydb-trino-adapter/pom.xml -Dtest='TestYdbConnectorTest#testName' test
mvn -f ydb-trino-adapter/pom.xml -Dtest=TestYdbConnectorSmokeTest test
mvn --batch-mode --update-snapshots -f ydb-trino-adapter/pom.xml clean test
```

Always report the test count and skipped count. Do not disable Docker-backed
tests, weaken CI, or add a no-op override to obtain a green result.
