# Lane Audit: Gcs

## Lane Summary

- Audit status: `manual-pass-complete`
- Section start commit: `Add GCS environment`
- Section end commit: `Remove legacy SuiteGcs`
- Introduced JUnit suites: `SuiteGcs`.
- Extended existing suites: none.
- Retired legacy suites: `SuiteGcs`.
- Environment classes introduced: `GcsEnvironment`.
- Method status counts: verified `13`, intentional difference `0`, needs follow-up `0`.

## Semantic Audit Status

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: `HDP to Hive 3.1 migration`.
- Needs-follow-up methods: none currently identified in this source/history pass.

## Environment Semantic Audit
### `GcsEnvironment`
- Environment semantic audit status: `complete`
- Legacy environment source:
  `testing/trino-product-tests-launcher/src/main/java/io/trino/tests/product/launcher/env/environment/EnvMultinodeGcs.java`
  plus legacy helper coverage in `BaseTestTableFormats`.
- Current environment class:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/gcs/GcsEnvironment.java`
- Container/service inventory parity:
  - Legacy launcher environment ran Trino, Hadoop/Hive metastore, and Spark with GCS wiring.
  - Current environment keeps the same effective three-service topology with one Trino container, one Hadoop container, and one Spark container.
- Config/resource parity:
  - Legacy launcher mounted `multinode-gcs` config files and connector definitions.
  - Current environment applies the same GCS/Hive overrides by copying generated XML/config fragments into Hadoop and setting GCS credentials on Hadoop, Spark, and Trino directly.
  - Current catalogs remain `hive`, `iceberg`, `delta`, and `tpch`, which matches the needs of the three GCS test classes.
- Startup ordering and dependency parity:
  - Hadoop starts first, then Spark, then Trino in both effective shapes.
- Ports/endpoints/auth/certs parity:
  - No additional auth/cert drift beyond GCS credential injection itself.
- Catalog/session default parity:
  - Current environment routes all three product-test families through the same GCS-backed warehouse and explicit catalog names, matching the legacy lane intent.
- Image/runtime differences:
  - Launcher orchestration is replaced by direct JUnit/Testcontainers orchestration.
- Any observed difference:
  - Shared table-format coverage moved from inheritance on `BaseTestTableFormats` to static helpers in `GcsTableFormatsTestUtils`.
- Classification: `verified`
- Reviewer note:
  - The current environment is semantically faithful. The wiring is code-driven rather than launcher-driven, but the same object-store, Spark, Hive, and Trino integration points are present.

## Suite Semantic Audit
### `SuiteGcs`
- Suite semantic audit status: `complete`
- CI bucket: `cloud-object-store`
- Relationship to lane: `owned by this lane`.
- Reviewer note:
  - Compared directly against legacy `SuiteGcs`. Both versions run the same three tag slices (`DeltaLakeGcs`, `IcebergGcs`, `HiveGcs`) against one GCS-backed environment family.

## Ported Test Classes

### `TestHiveGcs`


- Owning migration commit: `Migrate TestHiveGcs to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveGcs.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestHiveGcs.java`
- Class-level environment requirement: `GcsEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `HiveGcs`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `4`. Current methods: `4`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testInsertTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestHiveGcs.java` ->
  `testInsertTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveGcs.java` ->
  `TestHiveGcs.testInsertTable`
- Mapping type: `direct`
- Environment parity: Current class requires `GcsEnvironment`. Routed by source review into `SuiteGcs` run 3.
- Tag parity: Current tags: `ConfiguredFeatures`, `HiveGcs`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: Legacy setup shape: `super.testCreateAndInsertTable`. Current setup shape: `GcsTableFormatsTestUtils.testCreateAndInsertTable`.
- Action parity: Legacy action shape: none. Current action shape: none. Legacy delegate calls: `super.testCreateAndInsertTable`. Current delegate calls: `GcsTableFormatsTestUtils.testCreateAndInsertTable`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testCreateAndInsertTable] vs current [GcsTableFormatsTestUtils.testCreateAndInsertTable]; helper calls differ: legacy [super.testCreateAndInsertTable] vs current [GcsTableFormatsTestUtils.testCreateAndInsertTable]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testCreateAndInsertTable], verbs [none]. Current flow summary -> helpers [GcsTableFormatsTestUtils.testCreateAndInsertTable], verbs [none].
- Audit status: `verified`

##### `testPathContainsSpecialCharacter`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestHiveGcs.java` ->
  `testPathContainsSpecialCharacter`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveGcs.java` ->
  `TestHiveGcs.testPathContainsSpecialCharacter`
- Mapping type: `direct`
- Environment parity: Current class requires `GcsEnvironment`. Routed by source review into `SuiteGcs` run 3.
- Tag parity: Current tags: `ConfiguredFeatures`, `HiveGcs`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `super.testPathContainsSpecialCharacter`. Current action shape: `GcsTableFormatsTestUtils.testPathContainsSpecialCharacter`. Legacy delegate calls: `super.testPathContainsSpecialCharacter`. Current delegate calls: `GcsTableFormatsTestUtils.testPathContainsSpecialCharacter`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testPathContainsSpecialCharacter] vs current [GcsTableFormatsTestUtils.testPathContainsSpecialCharacter]; helper calls differ: legacy [super.testPathContainsSpecialCharacter] vs current [GcsTableFormatsTestUtils.testPathContainsSpecialCharacter]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testPathContainsSpecialCharacter], verbs [none]. Current flow summary -> helpers [GcsTableFormatsTestUtils.testPathContainsSpecialCharacter], verbs [none].
- Audit status: `verified`

##### `testLocationContainsDiscouragedCharacter`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestHiveGcs.java` ->
  `testLocationContainsDiscouragedCharacter`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveGcs.java` ->
  `TestHiveGcs.testLocationContainsDiscouragedCharacter`
- Mapping type: `direct`
- Environment parity: Current class requires `GcsEnvironment`. Routed by source review into `SuiteGcs` run 3.
- Tag parity: Current tags: `ConfiguredFeatures`, `HiveGcs`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `super.testLocationContainsDiscouragedCharacter`. Current action shape: `GcsTableFormatsTestUtils.testLocationContainsDiscouragedCharacter`. Legacy delegate calls: `super.testLocationContainsDiscouragedCharacter`. Current delegate calls: `GcsTableFormatsTestUtils.testLocationContainsDiscouragedCharacter`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testLocationContainsDiscouragedCharacter] vs current [GcsTableFormatsTestUtils.testLocationContainsDiscouragedCharacter]; helper calls differ: legacy [super.testLocationContainsDiscouragedCharacter] vs current [GcsTableFormatsTestUtils.testLocationContainsDiscouragedCharacter]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testLocationContainsDiscouragedCharacter], verbs [none]. Current flow summary -> helpers [GcsTableFormatsTestUtils.testLocationContainsDiscouragedCharacter], verbs [none].
- Audit status: `verified`

##### `testSparkReadingTrinoData`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestHiveGcs.java` ->
  `testSparkReadingTrinoData`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveGcs.java` ->
  `TestHiveGcs.testSparkReadingTrinoData`
- Mapping type: `direct`
- Environment parity: Current class requires `GcsEnvironment`. Routed by source review into `SuiteGcs` run 3.
- Tag parity: Current tags: `ConfiguredFeatures`, `HiveGcs`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: Legacy setup shape: `super.testSparkCompatibilityOnTrinoCreatedTable`. Current setup shape: `GcsTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable`.
- Action parity: Legacy action shape: none. Current action shape: none. Legacy delegate calls: `super.testSparkCompatibilityOnTrinoCreatedTable`. Current delegate calls: `GcsTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testSparkCompatibilityOnTrinoCreatedTable] vs current [GcsTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable]; helper calls differ: legacy [super.testSparkCompatibilityOnTrinoCreatedTable] vs current [GcsTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testSparkCompatibilityOnTrinoCreatedTable], verbs [none]. Current flow summary -> helpers [GcsTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable], verbs [none].
- Audit status: `verified`

### `TestIcebergGcs`


- Owning migration commit: `Migrate TestIcebergGcs to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergGcs.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergGcs.java`
- Class-level environment requirement: `GcsEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `IcebergGcs`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `5`. Current methods: `5`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testCreateAndSelectNationTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergGcs.java` ->
  `testCreateAndSelectNationTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergGcs.java` ->
  `TestIcebergGcs.testCreateAndSelectNationTable`
- Mapping type: `direct`
- Environment parity: Current class requires `GcsEnvironment`. Routed by source review into `SuiteGcs` run 2.
- Tag parity: Current tags: `ConfiguredFeatures`, `IcebergGcs`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: Legacy setup shape: `super.testCreateAndSelectNationTable`. Current setup shape: `GcsTableFormatsTestUtils.testCreateAndSelectNationTable`.
- Action parity: Legacy action shape: none. Current action shape: none. Legacy delegate calls: `super.testCreateAndSelectNationTable`. Current delegate calls: `GcsTableFormatsTestUtils.testCreateAndSelectNationTable`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testCreateAndSelectNationTable] vs current [GcsTableFormatsTestUtils.testCreateAndSelectNationTable]; helper calls differ: legacy [super.testCreateAndSelectNationTable] vs current [GcsTableFormatsTestUtils.testCreateAndSelectNationTable]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testCreateAndSelectNationTable], verbs [none]. Current flow summary -> helpers [GcsTableFormatsTestUtils.testCreateAndSelectNationTable], verbs [none].
- Audit status: `verified`

##### `testBasicWriteOperations`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergGcs.java` ->
  `testBasicWriteOperations`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergGcs.java` ->
  `TestIcebergGcs.testBasicWriteOperations`
- Mapping type: `direct`
- Environment parity: Current class requires `GcsEnvironment`. Routed by source review into `SuiteGcs` run 2.
- Tag parity: Current tags: `ConfiguredFeatures`, `IcebergGcs`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `super.testBasicWriteOperations`. Current action shape: `GcsTableFormatsTestUtils.testBasicWriteOperations`. Legacy delegate calls: `super.testBasicWriteOperations`. Current delegate calls: `GcsTableFormatsTestUtils.testBasicWriteOperations`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testBasicWriteOperations] vs current [GcsTableFormatsTestUtils.testBasicWriteOperations]; helper calls differ: legacy [super.testBasicWriteOperations] vs current [GcsTableFormatsTestUtils.testBasicWriteOperations]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testBasicWriteOperations], verbs [none]. Current flow summary -> helpers [GcsTableFormatsTestUtils.testBasicWriteOperations], verbs [none].
- Audit status: `verified`

##### `testPathContainsSpecialCharacter`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergGcs.java` ->
  `testPathContainsSpecialCharacter`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergGcs.java` ->
  `TestIcebergGcs.testPathContainsSpecialCharacter`
- Mapping type: `direct`
- Environment parity: Current class requires `GcsEnvironment`. Routed by source review into `SuiteGcs` run 2.
- Tag parity: Current tags: `ConfiguredFeatures`, `IcebergGcs`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `super.testPathContainsSpecialCharacter`. Current action shape: `GcsTableFormatsTestUtils.testPathContainsSpecialCharacter`. Legacy delegate calls: `super.testPathContainsSpecialCharacter`. Current delegate calls: `GcsTableFormatsTestUtils.testPathContainsSpecialCharacter`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testPathContainsSpecialCharacter] vs current [GcsTableFormatsTestUtils.testPathContainsSpecialCharacter]; helper calls differ: legacy [super.testPathContainsSpecialCharacter] vs current [GcsTableFormatsTestUtils.testPathContainsSpecialCharacter]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testPathContainsSpecialCharacter], verbs [none]. Current flow summary -> helpers [GcsTableFormatsTestUtils.testPathContainsSpecialCharacter], verbs [none].
- Audit status: `verified`

##### `testLocationContainsDiscouragedCharacter`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergGcs.java` ->
  `testLocationContainsDiscouragedCharacter`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergGcs.java` ->
  `TestIcebergGcs.testLocationContainsDiscouragedCharacter`
- Mapping type: `direct`
- Environment parity: Current class requires `GcsEnvironment`. Routed by source review into `SuiteGcs` run 2.
- Tag parity: Current tags: `ConfiguredFeatures`, `IcebergGcs`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `super.testLocationContainsDiscouragedCharacter`. Current action shape: `GcsTableFormatsTestUtils.testLocationContainsDiscouragedCharacter`. Legacy delegate calls: `super.testLocationContainsDiscouragedCharacter`. Current delegate calls: `GcsTableFormatsTestUtils.testLocationContainsDiscouragedCharacter`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testLocationContainsDiscouragedCharacter] vs current [GcsTableFormatsTestUtils.testLocationContainsDiscouragedCharacter]; helper calls differ: legacy [super.testLocationContainsDiscouragedCharacter] vs current [GcsTableFormatsTestUtils.testLocationContainsDiscouragedCharacter]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testLocationContainsDiscouragedCharacter], verbs [none]. Current flow summary -> helpers [GcsTableFormatsTestUtils.testLocationContainsDiscouragedCharacter], verbs [none].
- Audit status: `verified`

##### `testSparkReadingTrinoData`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergGcs.java` ->
  `testSparkReadingTrinoData`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergGcs.java` ->
  `TestIcebergGcs.testSparkReadingTrinoData`
- Mapping type: `direct`
- Environment parity: Current class requires `GcsEnvironment`. Routed by source review into `SuiteGcs` run 2.
- Tag parity: Current tags: `ConfiguredFeatures`, `IcebergGcs`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: Legacy setup shape: `super.testSparkCompatibilityOnTrinoCreatedTable`. Current setup shape: `GcsTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable`.
- Action parity: Legacy action shape: none. Current action shape: none. Legacy delegate calls: `super.testSparkCompatibilityOnTrinoCreatedTable`. Current delegate calls: `GcsTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testSparkCompatibilityOnTrinoCreatedTable] vs current [GcsTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable]; helper calls differ: legacy [super.testSparkCompatibilityOnTrinoCreatedTable] vs current [GcsTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testSparkCompatibilityOnTrinoCreatedTable], verbs [none]. Current flow summary -> helpers [GcsTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable], verbs [none].
- Audit status: `verified`

### `TestDeltaLakeGcs`


- Owning migration commit: `Migrate TestDeltaLakeGcs to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeGcs.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeGcs.java`
- Class-level environment requirement: `GcsEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `DeltaLakeGcs`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `4`. Current methods: `4`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testCreateAndSelectNationTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeGcs.java` ->
  `testCreateAndSelectNationTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeGcs.java` ->
  `TestDeltaLakeGcs.testCreateAndSelectNationTable`
- Mapping type: `direct`
- Environment parity: Current class requires `GcsEnvironment`. Routed by source review into `SuiteGcs` run 1.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeGcs`, `ProfileSpecificTests`. Tag routing matches the
  current suite selection.
- Setup parity: Legacy setup shape: `super.testCreateAndSelectNationTable`. Current setup shape: `GcsTableFormatsTestUtils.testCreateAndSelectNationTable`.
- Action parity: Legacy action shape: none. Current action shape: none. Legacy delegate calls: `super.testCreateAndSelectNationTable`. Current delegate calls: `GcsTableFormatsTestUtils.testCreateAndSelectNationTable`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testCreateAndSelectNationTable] vs current [GcsTableFormatsTestUtils.testCreateAndSelectNationTable]; helper calls differ: legacy [super.testCreateAndSelectNationTable] vs current [GcsTableFormatsTestUtils.testCreateAndSelectNationTable]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testCreateAndSelectNationTable], verbs [none]. Current flow summary -> helpers [GcsTableFormatsTestUtils.testCreateAndSelectNationTable], verbs [none].
- Audit status: `verified`

##### `testBasicWriteOperations`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeGcs.java` ->
  `testBasicWriteOperations`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeGcs.java` ->
  `TestDeltaLakeGcs.testBasicWriteOperations`
- Mapping type: `direct`
- Environment parity: Current class requires `GcsEnvironment`. Routed by source review into `SuiteGcs` run 1.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeGcs`, `ProfileSpecificTests`. Tag routing matches the
  current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `super.testBasicWriteOperations`. Current action shape: `GcsTableFormatsTestUtils.testBasicWriteOperations`. Legacy delegate calls: `super.testBasicWriteOperations`. Current delegate calls: `GcsTableFormatsTestUtils.testBasicWriteOperations`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testBasicWriteOperations] vs current [GcsTableFormatsTestUtils.testBasicWriteOperations]; helper calls differ: legacy [super.testBasicWriteOperations] vs current [GcsTableFormatsTestUtils.testBasicWriteOperations]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testBasicWriteOperations], verbs [none]. Current flow summary -> helpers [GcsTableFormatsTestUtils.testBasicWriteOperations], verbs [none].
- Audit status: `verified`

##### `testPathContainsSpecialCharacter`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeGcs.java` ->
  `testPathContainsSpecialCharacter`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeGcs.java` ->
  `TestDeltaLakeGcs.testPathContainsSpecialCharacter`
- Mapping type: `direct`
- Environment parity: Current class requires `GcsEnvironment`. Routed by source review into `SuiteGcs` run 1.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeGcs`, `ProfileSpecificTests`. Tag routing matches the
  current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `super.testPathContainsSpecialCharacter`. Current action shape: `GcsTableFormatsTestUtils.testPathContainsSpecialCharacter`. Legacy delegate calls: `super.testPathContainsSpecialCharacter`. Current delegate calls: `GcsTableFormatsTestUtils.testPathContainsSpecialCharacter`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testPathContainsSpecialCharacter] vs current [GcsTableFormatsTestUtils.testPathContainsSpecialCharacter]; helper calls differ: legacy [super.testPathContainsSpecialCharacter] vs current [GcsTableFormatsTestUtils.testPathContainsSpecialCharacter]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testPathContainsSpecialCharacter], verbs [none]. Current flow summary -> helpers [GcsTableFormatsTestUtils.testPathContainsSpecialCharacter], verbs [none].
- Audit status: `verified`

##### `testLocationContainsDiscouragedCharacter`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeGcs.java` ->
  `testLocationContainsDiscouragedCharacter`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeGcs.java` ->
  `TestDeltaLakeGcs.testLocationContainsDiscouragedCharacter`
- Mapping type: `direct`
- Environment parity: Current class requires `GcsEnvironment`. Routed by source review into `SuiteGcs` run 1.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeGcs`, `ProfileSpecificTests`. Tag routing matches the
  current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `super.testLocationContainsDiscouragedCharacter`. Current action shape: `GcsTableFormatsTestUtils.testLocationContainsDiscouragedCharacter`. Legacy delegate calls: `super.testLocationContainsDiscouragedCharacter`. Current delegate calls: `GcsTableFormatsTestUtils.testLocationContainsDiscouragedCharacter`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testLocationContainsDiscouragedCharacter] vs current [GcsTableFormatsTestUtils.testLocationContainsDiscouragedCharacter]; helper calls differ: legacy [super.testLocationContainsDiscouragedCharacter] vs current [GcsTableFormatsTestUtils.testLocationContainsDiscouragedCharacter]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testLocationContainsDiscouragedCharacter], verbs [none]. Current flow summary -> helpers [GcsTableFormatsTestUtils.testLocationContainsDiscouragedCharacter], verbs [none].
- Audit status: `verified`
