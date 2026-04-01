# Ducklake Testing Upgrade Plan

## Current State (Updated 2026-03-31)

| Metric | Ducklake | Iceberg (reference) |
|--------|----------|---------------------|
| Test files | 9 | 153 |
| Test count | 152 | ~2000+ |
| Query-runner integration tests | 1 (104 methods) | 42 files |
| BaseConnectorTest coverage | none (read-only) | full (226 methods) |
| End-to-end SQL tests | 104 | extensive |
| Cross-engine compatibility tests | none | N/A |

**Tier 1 (query-runner integration tests) is COMPLETE.** `TestDucklakeIntegration` provides 104 end-to-end test methods covering metadata, types, NULLs, predicates, partitioning, schema evolution, complex type dereferences, joins, set operations, EXPLAIN, aggregations, edge cases, and write-rejection.

Test tables (11 total, created by `DucklakeCatalogGenerator`):
- `simple_table` (5 rows) — primitives
- `array_table` (5 rows) — array type
- `partitioned_table` (5 rows) — identity partitioned by region
- `temporal_partitioned_table` (6 rows) — year/month partition
- `daily_partitioned_table` (5 rows) — year/month/day partition
- `nested_table` (3 rows) — struct, map, nested arrays, complex struct
- `wide_types_table` (3 rows) — tinyint through varbinary
- `nullable_table` (4 rows) — NULLs in every column type
- `empty_table` (0 rows) — empty result handling
- `schema_evolution_table` (4 rows) — column added after initial data
- `aggregation_table` (30 rows) — GROUP BY, HAVING, window functions

## DuckDB Ducklake Extension Tests

The DuckDB ducklake extension has **~370 SQLLogicTest files** across 48 categories at `ducklake-main/test/sql/`. Format: plain SQL with inline expected results. Categories include:

- **Read-relevant (Trino can test now):** types (11), partitioning (13), stats (11), general (13), schema_evolution, time_travel (2), constraints (3), metadata (9)
- **Write-relevant (future):** insert (3), delete (11), update (7), merge (6), alter (26), compaction (31), add_files (33), transaction (13)
- **DuckDB-specific (not directly portable):** data_inlining (32), deletion_inlining (17), macros (11), sorted_table (26), settings (6)

## Upgrade Strategy

Three tiers, from highest to lowest impact:

### Tier 1: Query-Runner Integration Tests (highest priority)

Build a `DucklakeQueryRunner` and a `BaseDucklakeConnectorTest` that exercises the read path through Trino's full SQL stack.

**Pattern to follow:** `IcebergQueryRunner` + `BaseIcebergConnectorSmokeTest`

**Key files:**
- `testing/trino-testing/src/main/java/io/trino/testing/BaseConnectorSmokeTest.java` — smoke test base (lighter than BaseConnectorTest)
- `testing/trino-testing/src/main/java/io/trino/testing/AbstractTestQueryFramework.java` — provides `assertQuery()`, `assertQueryReturnsEmptyResult()`, `computeActual()`
- `plugin/trino-iceberg/src/test/java/io/trino/plugin/iceberg/IcebergQueryRunner.java` — query runner builder pattern
- `plugin/trino-iceberg/src/test/java/io/trino/plugin/iceberg/BaseIcebergConnectorSmokeTest.java` — smoke test example

**Implementation:**

```
src/test/java/io/trino/plugin/ducklake/
  DucklakeQueryRunner.java          — boots DistributedQueryRunner with ducklake catalog
  TestDucklakeConnectorTest.java    — extends AbstractTestQueryFramework, read-path tests
```

`DucklakeQueryRunner` needs to:
1. Run `DucklakeCatalogGenerator.generateTestCatalog()` to create test data via DuckDB
2. Install the ducklake connector with catalog properties pointing at the generated SQLite DB
3. Set the default schema to `test_schema`

`TestDucklakeConnectorTest` should cover:
- `SELECT *` from each test table
- `SELECT` with WHERE predicates (partition pruning, stats pruning)
- `SELECT` with complex types (struct, map, array, nested)
- `SHOW SCHEMAS`, `SHOW TABLES`, `DESCRIBE table`
- `SELECT` with aggregations, GROUP BY, ORDER BY, LIMIT
- `EXPLAIN` to verify plan nodes
- Schema evolution reads (missing columns return NULL)
- Dynamic filter interaction via joins

**Override strategy for read-only connector:**
Since Ducklake is read-only, do NOT extend `BaseConnectorTest` (which requires write support). Instead extend `AbstractTestQueryFramework` directly and write targeted read-path tests.

### Tier 2: Cross-Engine Compatibility Tests (DuckDB writes, Trino reads)

Use DuckDB's ducklake extension test SQL as a source of truth for data generation, then verify Trino can read the results correctly.

**Approach:**

```
src/test/java/io/trino/plugin/ducklake/
  DucklakeCrossEngineTestGenerator.java   — runs DuckDB SQL to produce catalogs
  TestDucklakeCrossEngineReads.java       — Trino reads and verifies
```

**How it works:**
1. `DucklakeCrossEngineTestGenerator` executes DuckDB SQL statements (from ducklake extension tests or derived) via JDBC to produce a SQLite catalog + Parquet files
2. `TestDucklakeCrossEngineReads` boots a Trino query runner pointed at that catalog
3. Tests run Trino SELECTs and compare results to known-good values

**Priority test categories to port (read-side relevant):**

| DuckDB test category | Files | What to verify in Trino |
|---------------------|-------|------------------------|
| `types/all_types.test` | 1 | All supported type roundtrips |
| `types/struct.test` | 1 | Nested struct reads, NULL handling |
| `types/list.test` | 1 | Array reads |
| `types/map.test` | 1 | Map reads |
| `types/json.test` | 1 | JSON-as-VARCHAR reads |
| `types/floats.test` | 1 | NaN, infinity handling |
| `partitioning/basic_partitioning.test` | 1 | Identity partition pruning |
| `partitioning/year_month_day.test` | 1 | Temporal partition reads |
| `partitioning/multi_key_partition.test` | 1 | Multi-key partitioning |
| `general/` | 13 | Basic CRUD readback |
| `delete/basic_delete.test` | 1 | Merge-on-read delete filtering |
| `stats/cardinality.test` | 1 | Stats-driven planning |
| `schema_evolution/` | varies | Column add/drop/rename reads |

**Example test pattern:**
```java
@Test
public void testReadAllTypes()
{
    // DucklakeCrossEngineTestGenerator already created the catalog with:
    //   CREATE TABLE ducklake.data_types AS FROM all_types
    //   (generated from DuckDB's all_types table)

    // Verify Trino can read each supported type
    assertQuery("SELECT typeof(bool_col) FROM data_types LIMIT 1", "VALUES 'boolean'");
    assertQuery("SELECT count(*) FROM data_types", "VALUES 1");
    // ... etc
}
```

### Tier 3: Future — Full Bidirectional Tests (when write support lands)

When Ducklake gains write support in Trino:

| Test mode | Writer | Reader | Purpose |
|-----------|--------|--------|---------|
| Duck→Trino | DuckDB | Trino | Read compatibility (Tier 2, available now) |
| Trino→Trino | Trino | Trino | Standard connector tests (Tier 1 with writes) |
| Trino→Duck | Trino | DuckDB | Write compatibility |
| Duck→Trino→Duck | Both | Both | Full roundtrip |

At that point, extend `BaseConnectorTest` for full write-path coverage (226 test methods).

## Reference: Key Trino Testing APIs

```java
// Boot a test server
DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
queryRunner.installPlugin(new DucklakePlugin());
queryRunner.createCatalog("ducklake", "ducklake", properties);

// In test methods (from AbstractTestQueryFramework):
assertQuery("SELECT * FROM table", "VALUES (1, 'a'), (2, 'b')");
assertQueryReturnsEmptyResult("SELECT * FROM table WHERE false");
computeActual("SELECT * FROM table");  // returns MaterializedResult
assertUpdate("INSERT INTO ...", expectedRowCount);  // for write tests
assertQueryFails("BAD SQL", "Expected error.*");
```

## Reference: DuckDB SQLLogicTest Format

```
statement ok           — execute SQL, expect success
statement error        — execute SQL, expect failure
query [TYPES]          — execute SELECT, verify results
----                   — separator before expected output
<tab-separated rows>   — expected data
```

Types: `I`=integer, `T`=text, `R`=real, `D`=date, etc.

Source: `ducklake-main/test/sql/` (370 files, 48 categories)

## SQLLogicTest Runner Option

### Can we run DuckDB's .test files directly against Trino?

**Short answer: not directly.** The DuckDB test files use DuckDB-specific SQL syntax:
- `FROM table` (without SELECT)
- `EXCLUDE (col1, col2)` in SELECT
- `ATTACH 'ducklake:...'` / `DETACH`
- `test_all_types()`, `range()`, `glob()` table functions
- `BEGIN` / `COMMIT` / `ROLLBACK` (DuckDB transaction syntax)
- `SET VARIABLE` / `getvariable()`
- `test-env` directives with `__TEST_DIR__` and `{UUID}` placeholders

These would all fail against Trino's SQL parser.

### Available Java sqllogictest libraries

| Library | Maven | Notes |
|---------|-------|-------|
| `net.hydromatic:sql-logic-test` | Yes | Standard sqllogictest parser, minimal deps |
| `risinglightdb/sqllogictest-driver-jdbc` | GitHub | JDBC-based runner |
| Apache Calcite's variant | Internal | Not standalone |

Trino does **not** use any sqllogictest library today.

### Recommended approach: two-phase test runner

Rather than adapting the DuckDB .test files to run against Trino, use them as a **data generation spec**:

**Phase 1 (DuckDB JDBC):** Parse the `.test` file, run the `statement ok` / `INSERT` / `CREATE TABLE` blocks against DuckDB via JDBC to produce a SQLite catalog + Parquet files. This is the "write side."

**Phase 2 (Trino QueryRunner):** Run the `query` blocks against Trino (with SQL syntax adaptation) to verify reads match expected results. Adaptations needed:
- `FROM table` → `SELECT * FROM table`
- `EXCLUDE (cols)` → explicit column list
- Skip DuckDB-only directives (`ATTACH`, `DETACH`, `BEGIN/COMMIT`, `test-env`)
- Map DuckDB type names to Trino type names in result comparison

**Why this is better than a raw sqllogictest runner:**
- No need to modify the DuckDB test files (they stay upstream-compatible)
- Trino SQL differences are handled in a thin adapter layer
- The expensive part (test data generation) only runs once per test class
- Trino's `MaterializedResult` comparison is more robust than text-based diffing

A future enhancement could use `net.hydromatic:sql-logic-test` for the parser (to avoid writing our own .test file parser), with a custom executor that routes statements to DuckDB or Trino based on the phase.

## Custom Two-Phase Test Runner Design

The `.test` file format is simple enough to parse ourselves (~200-300 lines). Key design:

```
DucklakeSqlLogicTestRunner
  ├── TestFileParser            — line-based parser for .test format
  │     directives: statement ok/error, query <types>, ----, require, test-env
  ├── DuckDbWriteExecutor       — runs write-side SQL via DuckDB JDBC
  │     handles: CREATE, INSERT, ALTER, DELETE, UPDATE, ATTACH, BEGIN/COMMIT
  ├── TrinoReadExecutor         — runs read-side SQL via Trino QueryRunner
  │     handles: SELECT/query blocks with result comparison
  └── SqlAdapter                — minimal DuckDB→Trino SQL rewrites
        FROM xyz → SELECT * FROM xyz
        EXCLUDE → explicit column list
        range(N) → UNNEST(sequence(0, N-1))
        skip: ATTACH, DETACH, test-env, require
```

**Test permutation matrix** (all use same parsed .test files):

| Mode | Write engine | Read engine | When |
|------|-------------|-------------|------|
| Duck→Trino | DuckDB JDBC | Trino QueryRunner | Now (read-only connector) |
| Duck→Duck | DuckDB JDBC | DuckDB JDBC | Baseline / reference |
| Trino→Trino | Trino QueryRunner | Trino QueryRunner | When write support lands |
| Trino→Duck | Trino QueryRunner | DuckDB JDBC | When write support lands |

Each mode uses the same test files — only the executor routing changes.

**Note on sqlglot:** Could be used via GraalVM/WASM for SQL translation if the manual `SqlAdapter` becomes unwieldy. For now the rewrite list is short enough to handle with string manipulation. Revisit if we find many more DuckDB-isms in practice.

## Priority: Ducklake Tests vs Iceberg Test Patterns

**Ducklake extension tests are the primary source of truth.** They define what the format does, they're maintained by the DuckDB team, and they can be synced as the extension evolves. Our goal is to run their full suite (read-side) against Trino and keep it replaceable when they update.

**Iceberg/Trino test patterns are secondary — use selectively.** We don't need to replicate Iceberg's 153-file test suite. What we want from Trino's testing framework is:

| From Trino/Iceberg | Why | How much |
|---------------------|-----|----------|
| `DucklakeQueryRunner` | Boots a real Trino server for end-to-end SQL | One class, essential |
| `AbstractTestQueryFramework` | `assertQuery()`, `computeActual()`, result comparison | Base class, essential |
| A handful of Trino-perspective tests | Verify SHOW SCHEMAS, DESCRIBE, EXPLAIN plans, information_schema, dynamic filter join behavior, stats-driven planning | ~20-30 hand-written test methods |
| `BaseConnectorTest` / `BaseConnectorSmokeTest` | 226 standard connector tests | **Not now** — requires write support. Adopt when writes land. |

The split:
- **~90% of test coverage** should come from the ducklake extension test files via the two-phase runner (Duck writes, Trino reads). These are authoritative, comprehensive, and auto-updatable.
- **~10% of test coverage** should be hand-written Trino-perspective tests that verify things DuckDB tests don't exercise: Trino plan shapes, dynamic filters across joins, information_schema queries, connector SPI behavior, stats propagation to the optimizer.

## Execution Order

1. **Now:** Build `DucklakeQueryRunner` + small `TestDucklakeConnectorTest` (Trino-perspective tests — SHOW, DESCRIBE, EXPLAIN, joins, dynamic filters)
2. **Priority:** Build the two-phase test runner and wire up the ducklake extension test suite (Duck→Trino reads). This is the bulk of coverage.
3. **Later:** Expand Trino-perspective tests as needed (plan quality, edge cases)
4. **On write support:** Extend to `BaseConnectorTest` and enable all four write/read permutations (Tier 3)

## Implementation Notes for Next Session

**Starting point for Tier 1:**
1. Create `DucklakeQueryRunner.java` modeled after `IcebergQueryRunner.java` (line 97: `Builder extends DistributedQueryRunner.Builder`)
2. The `DucklakeCatalogGenerator` already creates test data — just need to wire it into the query runner's `@BeforeAll`
3. Create `TestDucklakeConnectorTest.java` extending `AbstractTestQueryFramework`
4. Start with simple SELECTs and SHOW commands, then add predicate/type/join tests

**Starting point for the test runner (Tier 2+):**
1. Parser: iterate lines, match `^statement (ok|error)`, `^query ([A-Z]+)`, `^----`, `^#`, `^require`, `^test-env`
2. Accumulate SQL lines between directives
3. Route to DuckDB or Trino executor based on directive type and current phase
4. For `query` blocks: execute SQL, compare tab-separated results with expected output

**DuckDB test files most worth porting first:**
- `ducklake_basic.test` — sanity check
- `types/struct.test`, `types/list.test`, `types/map.test` — complex type reads
- `partitioning/basic_partitioning.test`, `partitioning/year_month_day.test` — partition pruning
- `delete/basic_delete.test` — merge-on-read deletes
- `general/` (13 files) — broad coverage

## DuckDB Data Inlining and the Trino Connector

### Problem

DuckDB's ducklake extension **inlines small data** directly in the SQLite catalog database instead of writing Parquet files. The default threshold is 10 rows (`data_inlining_row_limit = 10`). When data is inlined:
- No Parquet files are created
- Data is stored as virtual files named `ducklake_inlined_data_<id>` in the catalog
- The Trino connector **cannot read inlined data** — it reads Parquet files directly

This means that any table with ≤10 rows will be invisible to Trino unless inlining is disabled.

### Current Workaround

The SLT test runner disables inlining via the ATTACH option:
```sql
ATTACH 'ducklake:sqlite:catalog.db' AS ducklake (DATA_PATH '/path', DATA_INLINING_ROW_LIMIT 0)
```

### DuckDB Settings for Inlining (reference)

| Method | Syntax | Scope |
|--------|--------|-------|
| ATTACH option | `DATA_INLINING_ROW_LIMIT 0` | Per-catalog, at creation time |
| Persistent option | `CALL ducklake.set_option('data_inlining_row_limit', 0)` | Global, per-schema, or per-table |
| Session default | `SET ducklake_default_data_inlining_row_limit = 0` | Session-level default |
| Flush on demand | `CALL ducklake_flush_inlined_data('catalog')` | Writes inlined data to Parquet |

Priority: ATTACH option > set_option > session default > hardcoded default (10).

### Future Considerations

1. **Production users**: Anyone using DuckDB with default settings will have small tables inlined. The Trino connector should either:
   - Support reading inlined data from the SQLite catalog (requires new code path)
   - Clearly document that `DATA_INLINING_ROW_LIMIT 0` must be set when creating catalogs intended for Trino
   - Detect inlined data and surface a clear error message

2. **DuckDB 1.5.1 fixes**: Improved inlining correctness for updates/deletes over inlined data. No changes to the setting mechanism itself.

3. **Inline row limit bug**: There were reports of the `data_inlining_row_limit` setting not being respected in DuckDB 1.5.0. This may be resolved in 1.5.1+ (the DuckDB team bumped the ducklake extension).
