# Lane Audit: Parquet

## Lane Summary

- Audit status: `manual-pass-complete`
- Section start commit: `Add Parquet environment`
- Section end commit: `Reorganize Delta Databricks compatibility tests`
- Introduced JUnit suites: `SuiteParquet`.
- Extended existing suites: none.
- Retired legacy suites: `SuiteParquet`, `launcher and Tempto product-test framework`.
- Environment classes introduced: `ParquetEnvironment`.
- Method status counts: verified `2`, intentional difference `0`, needs follow-up `0`.

## Semantic Audit Status

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: `HDP to Hive 3.1 migration`.
- Needs-follow-up methods: none currently identified in this source/history pass.

## Environment Semantic Audit

### `ParquetEnvironment`

- Environment semantic audit status: `complete`
- Legacy environment source:
  `testing/trino-product-tests-launcher/src/main/java/io/trino/tests/product/launcher/env/environment/EnvMultinodeParquet.java`
- Current environment class:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/parquet/ParquetEnvironment.java`
- Container/service inventory parity:
  - Legacy environment extended `StandardMultinode` plus `Hadoop`.
  - Current environment narrows that to one Trino container plus one Hadoop container.
  - The runtime surface still exposes the same three catalogs needed by the lane: `hive`, `tpch`, and `tpcds`.
- Config/resource parity:
  - Legacy launcher mounted connector property files from `conf/environment/multinode-parquet`.
  - Current environment builds those catalogs directly in code.
  - `tpch` and `tpcds` remain connector-only catalogs; `hive` remains the HDFS-backed catalog.
- Startup ordering and dependency parity:
  - Both environments start Hadoop before Trino and route Trino against the Hive metastore/HDFS topology.
- Ports/endpoints/auth/certs parity:
  - No auth/cert-specific behavior exists in either lane shape.
- Catalog/session default parity:
  - Current `createTrinoConnection()` defaults to `hive.default`, but the tests explicitly `USE hive.tpch` and `USE hive.tpcds` just as the legacy tests did.
- Image/runtime differences:
  - The old launcher multinode shape is replaced by direct JUnit/Testcontainers orchestration.
- Any observed difference:
  - Dataset provisioning moved from shared Tempto helper `createTpcdsAndTpchDatasets("hive")` to current `ParquetEnvironment`-backed inline table creation in `initializeDatasets(...)`.
- Classification: `verified`
- Reviewer note:
  - The environment is semantically faithful. The implementation is narrower and code-driven, but it still provides the same catalogs and underlying Hive/HDFS behavior that the Parquet SQL-based tests require.

## Suite Semantic Audit

### `SuiteParquet`

- Suite semantic audit status: `complete`
- CI bucket: `hive-storage`
- Relationship to lane: `owned by this lane`.
- Reviewer note:
  - Compared directly against legacy `SuiteParquet`. Both suites run a single Parquet-focused environment with the same effective tag pair: `ConfiguredFeatures` + `Parquet`.

## Ported Test Classes

### `TestParquetJunit`


- Owning migration commit: `Migrate TestParquet to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/parquet/TestParquetJunit.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/TestParquet.java`
- Class-level environment requirement: `ParquetEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `Parquet`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `2`. Current methods: `2`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly. Deleted query resources in
  the same migration commit: `src/main/resources/sql-tests/testcases/hive_tpch/q15.{sql,result}`,
  `src/main/resources/sql-tests/testcases/tpcds/q24_1.{sql,result}`,
  `src/main/resources/sql-tests/testcases/tpcds/q72.{sql,result}`.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testTpcds`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/TestParquet.java` ->
  `testTpcds`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/parquet/TestParquetJunit.java` ->
  `TestParquetJunit.testTpcds`
- Mapping type: `direct`
- Environment parity: Current class requires `ParquetEnvironment`. Routed by source review into `SuiteParquet` run 1.
- Tag parity: Current tags: `ConfiguredFeatures`, `Parquet`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: Legacy setup shape: `USE`. Current setup shape: `USE`, `initializeDatasets`.
- Action parity: Legacy action shape: `Resources.toString`, `getResource`, `Resources.readLines`, `stream`, `filter`, `line.startsWith`, `collect`, `toImmutableList`, `onTrino`, `executeQuery`. Current action shape: `Resources.toString`, `getResource`, `Resources.readLines`, `stream`, `filter`, `line.startsWith`, `toList`, `env.executeTrinoInSession`, `session.executeUpdate`, `session.executeQuery`.
- Assertion parity: Legacy assertion helpers: `assertResults`. Current assertion helpers: `assertResults`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [Resources.toString, getResource, Resources.readLines, stream, filter, line.startsWith, collect, toImmutableList, onTrino, executeQuery] vs current [initializeDatasets, Resources.toString, getResource, Resources.readLines, stream, filter, line.startsWith, toList, env.executeTrinoInSession, session.executeUpdate, session.executeQuery]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [Resources.toString, getResource, Resources.readLines, stream, filter, line.startsWith, collect, toImmutableList, onTrino, executeQuery], verbs [USE]. Current flow summary -> helpers [initializeDatasets, Resources.toString, getResource, Resources.readLines, stream, filter, line.startsWith, toList, env.executeTrinoInSession, session.executeUpdate, session.executeQuery], verbs [USE].
- Audit status: `verified`

##### `testTpch`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/TestParquet.java` ->
  `testTpch`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/parquet/TestParquetJunit.java` ->
  `TestParquetJunit.testTpch`
- Mapping type: `direct`
- Environment parity: Current class requires `ParquetEnvironment`. Routed by source review into `SuiteParquet` run 1.
- Tag parity: Current tags: `ConfiguredFeatures`, `Parquet`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: Legacy setup shape: `USE`. Current setup shape: `USE`, `initializeDatasets`.
- Action parity: Legacy action shape: `Resources.toString`, `getResource`, `Resources.readLines`, `stream`, `filter`, `line.startsWith`, `collect`, `toImmutableList`, `onTrino`, `executeQuery`. Current action shape: `Resources.toString`, `getResource`, `Resources.readLines`, `stream`, `filter`, `line.startsWith`, `toList`, `env.executeTrinoInSession`, `session.executeUpdate`, `session.executeQuery`.
- Assertion parity: Legacy assertion helpers: `assertResults`. Current assertion helpers: `assertResults`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [Resources.toString, getResource, Resources.readLines, stream, filter, line.startsWith, collect, toImmutableList, onTrino, executeQuery] vs current [initializeDatasets, Resources.toString, getResource, Resources.readLines, stream, filter, line.startsWith, toList, env.executeTrinoInSession, session.executeUpdate, session.executeQuery]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [Resources.toString, getResource, Resources.readLines, stream, filter, line.startsWith, collect, toImmutableList, onTrino, executeQuery], verbs [USE]. Current flow summary -> helpers [initializeDatasets, Resources.toString, getResource, Resources.readLines, stream, filter, line.startsWith, toList, env.executeTrinoInSession, session.executeUpdate, session.executeQuery], verbs [USE].
- Audit status: `verified`
