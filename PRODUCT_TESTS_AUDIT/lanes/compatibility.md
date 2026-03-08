# Lane Audit: Compatibility

## Lane Summary

- Audit status: `manual-pass-complete`
- Section start commit: `Add Compatibility suite`
- Section end commit: `Remove legacy SuiteCompatibility`
- Introduced JUnit suites: `SuiteCompatibility`.
- Extended existing suites: none.
- Retired legacy suites: `SuiteCompatibility`.
- Environment classes introduced: none.
- Method status counts: verified `5`, intentional difference `0`, needs follow-up `0`.

## Semantic Audit Status

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: `HDP to Hive 3.1 migration`.
- Needs-follow-up methods: none.
- Reviewer note: The Iceberg compatibility method remains a faithful direct port, and the Hive view compatibility helper
  now again asserts through both the current and compatibility Trino coordinators.

## Environment Semantic Audit

### `HiveSparkEnvironment`

- Environment semantic audit status: `complete`
- Legacy environment source:
  `testing/trino-product-tests-launcher/src/main/java/io/trino/tests/product/launcher/env/environment/EnvMultinodeHiveCaching.java`
  plus the legacy `SuiteCompatibility` compatibility-server topology.
- Current environment class:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/HiveSparkEnvironment.java`
- Container/service inventory parity: Current environment keeps the Spark-backed Hive compatibility shape needed by
  `TestHiveViewCompatibility`, including a dedicated compatibility Trino coordinator alongside the current Trino
  coordinator.
- Config/resource parity: Hive/Spark resources remain present through the current environment wiring, including the
  second-query executor path needed for the legacy dual-coordinator assertions.
- Startup ordering and dependency parity: Current startup remains Spark/Hive-oriented and supports the same table/view preparation flow used by the migrated tests.
- Ports/endpoints/auth/certs parity: No lane-specific auth/cert drift identified.
- Catalog/session default parity: The legacy lane used `onTrino()` and `onCompatibilityTestServer()`; current lane now
  provides both `executeTrino*` and `executeCompatibilityTrino*` helpers for the same dual-coordinator flow.
- Image/runtime differences: `HDP -> Hive 3.1` is the approved runtime difference for the Hive-side stack.
- Any observed difference: The current environment builds and manages both Trino coordinators directly in code instead
  of relying on the legacy launcher compatibility-server wiring.
- Classification: `verified`
- Reviewer note: This environment preserves the dual-executor assertion shape that the legacy compatibility tests
  relied on.

### `SparkIcebergCompatibilityEnvironment`

- Environment semantic audit status: `complete`
- Legacy environment source:
  `testing/trino-product-tests-launcher/src/main/java/io/trino/tests/product/launcher/env/environment/EnvMultinodeCompatibility.java`
  as used by legacy `SuiteCompatibility`.
- Current environment class:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/SparkIcebergCompatibilityEnvironment.java`
- Container/service inventory parity: Current environment preserves the essential split between a compatibility Trino endpoint and the current Trino endpoint for Iceberg time-travel validation.
- Config/resource parity: The current environment keeps the Iceberg compatibility wiring needed to create data through the compatibility endpoint and read it through the current endpoint.
- Startup ordering and dependency parity: Compatibility Trino is still started before the current assertions run; the lane’s single Iceberg method still has both executor paths available.
- Ports/endpoints/auth/certs parity: No unresolved auth/cert drift identified.
- Catalog/session default parity: Current environment explicitly provides both `executeCompatibilityTrino*` and current `executeTrino*`, matching the legacy test intent.
- Image/runtime differences: none beyond the approved framework replacement.
- Any observed difference: The current port adds explicit `try/finally` cleanup and typed `QueryResult` handling, but the environment semantics remain the same.
- Classification: `verified`
- Reviewer note: This environment is semantically faithful for the one Iceberg compatibility method in this lane.

## Suite Semantic Audit

### `SuiteCompatibility`

- Suite semantic audit status: `complete`
- Legacy suite source:
  `testing/trino-product-tests-launcher/src/main/java/io/trino/tests/product/launcher/suite/suites/SuiteCompatibility.java`
- Current suite class:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteCompatibility.java`
- Environment runs parity:
  - Legacy suite ran Hive view compatibility through a compatibility-server path and Iceberg format-version compatibility through a dedicated compatibility environment.
  - Current suite still has two runs:
    - `HiveSparkEnvironment` with tag `HiveViewCompatibility`
    - `SparkIcebergCompatibilityEnvironment` with tag `IcebergFormatVersionCompatibility`
- Include/exclude tag parity: Current include tags match the two legacy coverage families. No exclude-tag drift identified.
- Class/method coverage parity:
  - `TestIcebergFormatVersionCompatibility` remains fully represented.
  - `TestHiveViewCompatibility` keeps the same four test methods and now again asserts through both the current and
    compatibility Trino coordinators.
- CI placement parity: Current suite is routed in `pt` bucket `delta-lake`. This is a scheduling change only and does not alter suite membership.
- Any observed difference: Current wiring uses explicit compatibility-environment helpers instead of the old launcher
  executor abstraction.
- Classification: `verified`
- Reviewer note: The suite structure is faithful and the Hive view compatibility portion again preserves the legacy
  dual-coordinator comparison layer.

## Ported Test Classes

### `TestHiveViewCompatibility`

- Owning migration commit: `Migrate TestHiveViewCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveViewCompatibility.java`
- Legacy class removed in same migration commit:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestHiveViewCompatibility.java`
- Class-level environment requirement: `HiveSparkEnvironment`.
- Class-level tags: `HiveSpark`, `HiveViewCompatibility`.
- Method inventory complete: Yes. Legacy methods: `4`. Current methods: `4`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`.
- Method statuses present: `verified`.
- Reviewer note: All four methods still exist, and the current helper now again asserts both `env.executeTrino(...)`
  and `env.executeCompatibilityTrino(...)`.

#### Methods

##### `testSelectOnView`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestHiveViewCompatibility.java` ->
  `testSelectOnView`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveViewCompatibility.java` ->
  `TestHiveViewCompatibility.testSelectOnView`
- Mapping type: `direct`
- Setup parity: Both versions create the view and issue the same two queries.
- Action parity: SQL text and row-shape intent are preserved.
- Assertion parity: Legacy asserted both `onTrino()` and `onCompatibilityTestServer()` through `assertViewQuery(...)`.
  Current JUnit does the same through `env.executeTrino(...)` and `env.executeCompatibilityTrino(...)`.
- Cleanup parity: Current JUnit adds explicit `finally` cleanup; legacy cleanup was less explicit.
- Any observed difference: Current JUnit uses explicit compatibility-environment helpers instead of the old executor
  abstraction.
- Classification: `verified`

##### `testSelectOnViewFromDifferentSchema`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestHiveViewCompatibility.java` ->
  `testSelectOnViewFromDifferentSchema`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveViewCompatibility.java` ->
  `TestHiveViewCompatibility.testSelectOnViewFromDifferentSchema`
- Mapping type: `direct`
- Setup parity: Both versions create the schema and view in the same logical shape.
- Action parity: Query shape is preserved.
- Assertion parity: Legacy compared both current Trino and the compatibility server result; current JUnit now checks
  both current Trino and the compatibility coordinator.
- Cleanup parity: Current JUnit explicitly drops the schema in `finally`.
- Any observed difference: Current JUnit uses explicit compatibility-environment helpers instead of the old executor
  abstraction.
- Classification: `verified`

##### `testExistingView`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestHiveViewCompatibility.java` ->
  `testExistingView`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveViewCompatibility.java` ->
  `TestHiveViewCompatibility.testExistingView`
- Mapping type: `direct`
- Setup parity: Both versions create the initial view before attempting a duplicate create.
- Action parity: Both versions test duplicate view creation.
- Assertion parity: Legacy asserted the duplicate-create failure through `onTrino()` while the existing view came from
  the compatibility server. Current JUnit preserves that split via the compatibility-environment helpers.
- Cleanup parity: Current JUnit drops the view in `finally`.
- Any observed difference: Current JUnit uses explicit compatibility-environment helpers instead of the old executor
  abstraction.
- Classification: `verified`

##### `testCommentOnViewColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestHiveViewCompatibility.java` ->
  `testCommentOnViewColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveViewCompatibility.java` ->
  `TestHiveViewCompatibility.testCommentOnViewColumn`
- Mapping type: `direct`
- Setup parity: Both versions create two views for comment-on-column checks.
- Action parity: Both versions apply column comments and re-read the views.
- Assertion parity: Legacy checked both compatibility-server and current Trino readability after the comments. Current
  JUnit does the same via its helper.
- Cleanup parity: Current JUnit adds explicit `finally` cleanup for both views.
- Any observed difference: Current JUnit uses explicit compatibility-environment helpers instead of the old executor
  abstraction.
- Classification: `verified`

### `TestIcebergFormatVersionCompatibility`

- Owning migration commit: `Migrate TestIcebergFormatVersionCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergFormatVersionCompatibility.java`
- Legacy class removed in same migration commit:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergFormatVersionCompatibility.java`
- Class-level environment requirement: `SparkIcebergCompatibilityEnvironment`.
- Class-level tags: `Iceberg`, `IcebergFormatVersionCompatibility`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: none beyond the framework replacement.
- Method statuses present: `verified`.
- Reviewer note: Manual source comparison completed. The JUnit port preserves the compatibility-server write path, snapshot lookup, current-Trino snapshot verification, and time-travel read assertion exactly. The only changes are typed environment helpers and explicit `try/finally` cleanup.

#### Methods

##### `testTrinoTimeTravelReadTableCreatedByEarlyVersionTrino`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergFormatVersionCompatibility.java` ->
  `testTrinoTimeTravelReadTableCreatedByEarlyVersionTrino`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergFormatVersionCompatibility.java` ->
  `TestIcebergFormatVersionCompatibility.testTrinoTimeTravelReadTableCreatedByEarlyVersionTrino`
- Mapping type: `direct`
- Setup parity: Both versions create the same randomly named Iceberg table through the compatibility Trino endpoint and insert the same three values.
- Action parity: Both versions read the latest snapshot from the `$snapshots` table through the compatibility endpoint, verify that current Trino sees the same snapshot id, and then compare the time-travel result against the compatibility-server base read.
- Assertion parity: Result-shape assertions are unchanged.
- Cleanup parity: Current JUnit adds explicit `try/finally` cleanup; legacy cleanup was trailing teardown code.
- Any observed difference: Current JUnit uses typed environment helpers and wraps cleanup in `finally`, but the semantic behavior is preserved.
- Classification: `verified`
