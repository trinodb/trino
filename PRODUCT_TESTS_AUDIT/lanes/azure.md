# Lane Audit: Azure

## Lane Summary

- Audit status: `complete`
- Section start commit: `Add Azure environment`
- Section end commit: `Remove legacy SuiteAzure`
- Introduced JUnit suites: `SuiteAzure`.
- Extended existing suites: none.
- Retired legacy suites: `SuiteAzure`.
- Environment classes introduced: `AzureEnvironment`.
- Method status counts: verified `20`, intentional difference `0`, needs follow-up `0`.

## Semantic Audit Status

- Manual review note: the legacy Azure lane was re-read from `SuiteAzure`, `EnvMultinodeAzure`, `BaseTestTableFormats`, `TestHiveAzure`, `TestIcebergAzure`, `TestDeltaLakeAzure`, `TestAzureBlobFileSystem`, and `TestAbfsSyncPartitionMetadata`, then compared against the current JUnit/Testcontainers sources in this lane.

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: `HDP to Hive 3.1 migration`.
- Needs-follow-up methods: none identified in the manual source-body pass for this lane.

## Environment Semantic Audit
### `AzureEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of legacy `EnvMultinodeAzure` against current `AzureEnvironment`.
- Container/service inventory parity: preserved. Both environments run Hadoop/Hive metastore, Spark, and Trino with Azure-backed warehouse storage and dedicated `hive`, `delta`, `iceberg`, and `tpch` catalogs.
- Config/resource wiring parity: preserved in intent. Legacy launcher copied prepared connector/property files and generated temp overrides into containers; current environment writes the same Azure account/access-key settings directly into Hadoop, Spark, and Trino container configuration at startup.
- Startup dependency parity: preserved. Hadoop starts first, Spark depends on Hadoop, and Trino waits on the metastore-backed Hadoop services before the tests run.
- Endpoint/auth parity: preserved. Both use ABFS access-key authentication via `ABFS_CONTAINER`, `ABFS_ACCOUNT`, and `ABFS_ACCESS_KEY`.
- Catalog/session defaults parity: preserved in intent, with approved implementation drift. Current catalogs are created programmatically instead of mounted property files, but the same catalog set and Azure filesystem settings are present.
- Recorded differences: outside-Docker execution, JUnit/Testcontainers framework replacement, and code-driven config generation instead of launcher-time file mounting.
- Reviewer note: no Azure-environment fidelity gap is currently identified; the differences are implementation-level and remain within the approved migration envelope.

## Suite Semantic Audit
### `SuiteAzure`
- Suite semantic audit status: `complete`
- CI bucket: `cloud-object-store`
- Relationship to lane: `owned by this lane`.
- Legacy/current basis: manual comparison of legacy launcher `SuiteAzure` and current `SuiteAzure`.
- Environment-run parity: preserved. Legacy launcher had three runs on `EnvMultinodeAzure`; current suite has three runs on `AzureEnvironment`.
- Tag parity: preserved. The same slices are executed: `(ConfiguredFeatures, Azure)`, `(ConfiguredFeatures, DeltaLakeAzure)`, and `(ConfiguredFeatures, IcebergAzure)`.
- Coverage-intent parity: preserved. The current suite still partitions Hive/Azure, Delta/Azure, and Iceberg/Azure coverage exactly the way the legacy launcher suite did.
- Recorded differences: current execution is direct JUnit suite execution instead of launcher-driven Docker-side execution.
- Reviewer note: no Azure suite-level fidelity gap is currently identified.

## Ported Test Classes

### `TestHiveAzure`


- Owning migration commit: `Migrate TestHiveAzure to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveAzure.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestHiveAzure.java`
- Class-level environment requirement: `AzureEnvironment`.
- Class-level tags: `Azure`, `ConfiguredFeatures`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `3`. Current methods: `3`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope. The current class is a thin JUnit wrapper around `AzureTableFormatsTestUtils`, mirroring the legacy thin wrapper around `BaseTestTableFormats`.
- Method statuses present: `verified`. Manual source-body comparisons below were confirmed against the legacy class and the current helper-backed JUnit method bodies.

#### Methods

##### `testInsertTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestHiveAzure.java` ->
  `testInsertTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveAzure.java` ->
  `TestHiveAzure.testInsertTable`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 1.
- Tag parity: Current tags: `Azure`, `ConfiguredFeatures`, `ProfileSpecificTests`. Tag routing matches the current suite
  selection.
- Setup parity: Legacy setup shape: `super.testCreateAndInsertTable`. Current setup shape: `AzureTableFormatsTestUtils.testCreateAndInsertTable`.
- Action parity: Legacy action shape: none. Current action shape: none. Legacy delegate calls: `super.testCreateAndInsertTable`. Current delegate calls: `AzureTableFormatsTestUtils.testCreateAndInsertTable`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testCreateAndInsertTable] vs current [AzureTableFormatsTestUtils.testCreateAndInsertTable]; helper calls differ: legacy [super.testCreateAndInsertTable] vs current [AzureTableFormatsTestUtils.testCreateAndInsertTable]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testCreateAndInsertTable], verbs [none]. Current flow summary -> helpers [AzureTableFormatsTestUtils.testCreateAndInsertTable], verbs [none].
- Audit status: `verified`

##### `testPathContainsSpecialCharacter`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestHiveAzure.java` ->
  `testPathContainsSpecialCharacter`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveAzure.java` ->
  `TestHiveAzure.testPathContainsSpecialCharacter`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 1.
- Tag parity: Current tags: `Azure`, `ConfiguredFeatures`, `ProfileSpecificTests`. Tag routing matches the current suite
  selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `super.testPathContainsSpecialCharacter`. Current action shape: `AzureTableFormatsTestUtils.testPathContainsSpecialCharacter`. Legacy delegate calls: `super.testPathContainsSpecialCharacter`. Current delegate calls: `AzureTableFormatsTestUtils.testPathContainsSpecialCharacter`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testPathContainsSpecialCharacter] vs current [AzureTableFormatsTestUtils.testPathContainsSpecialCharacter]; helper calls differ: legacy [super.testPathContainsSpecialCharacter] vs current [AzureTableFormatsTestUtils.testPathContainsSpecialCharacter]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testPathContainsSpecialCharacter], verbs [none]. Current flow summary -> helpers [AzureTableFormatsTestUtils.testPathContainsSpecialCharacter], verbs [none].
- Audit status: `verified`

##### `testSparkReadingTrinoData`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestHiveAzure.java` ->
  `testSparkReadingTrinoData`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveAzure.java` ->
  `TestHiveAzure.testSparkReadingTrinoData`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 1.
- Tag parity: Current tags: `Azure`, `ConfiguredFeatures`, `ProfileSpecificTests`. Tag routing matches the current suite
  selection.
- Setup parity: Legacy setup shape: `super.testSparkCompatibilityOnTrinoCreatedTable`. Current setup shape: `AzureTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable`.
- Action parity: Legacy action shape: none. Current action shape: none. Legacy delegate calls: `super.testSparkCompatibilityOnTrinoCreatedTable`. Current delegate calls: `AzureTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testSparkCompatibilityOnTrinoCreatedTable] vs current [AzureTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable]; helper calls differ: legacy [super.testSparkCompatibilityOnTrinoCreatedTable] vs current [AzureTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testSparkCompatibilityOnTrinoCreatedTable], verbs [none]. Current flow summary -> helpers [AzureTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable], verbs [none].
- Audit status: `verified`

### `TestAzureBlobFileSystem`


- Owning migration commit: `Migrate TestAzureBlobFileSystem to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestAzureBlobFileSystem.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestAzureBlobFileSystem.java`
- Class-level environment requirement: `AzureEnvironment`.
- Class-level tags: `Azure`, `ConfiguredFeatures`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`. The current class still exercises Hive/Trino path handling directly; the stack change is the only class-scope approved difference.
- Method statuses present: `verified`. Manual source-body comparisons below were confirmed against the legacy class and the current helper-backed JUnit method bodies.

#### Methods

##### `testPathContainsSpecialCharacter`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestAzureBlobFileSystem.java` ->
  `testPathContainsSpecialCharacter`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestAzureBlobFileSystem.java` ->
  `TestAzureBlobFileSystem.testPathContainsSpecialCharacter`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 1.
- Tag parity: Current tags: `Azure`, `ConfiguredFeatures`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onHive`, `executeQuery`, `BY`, `onTrino`, `ImmutableList.of`. Current action shape: `SELECT`, `randomNameSuffix`, `env.getSchemaLocation`, `env.executeHiveUpdate`, `BY`, `env.executeTrinoUpdate`, `List.of`, `env.executeHive`, `expected.toArray`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onHive, executeQuery, BY, onTrino, ImmutableList.of] vs current [randomNameSuffix, env.getSchemaLocation, env.executeHiveUpdate, BY, env.executeTrinoUpdate, List.of, env.executeHive, expected.toArray, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onHive, executeQuery, BY, onTrino, ImmutableList.of], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.getSchemaLocation, env.executeHiveUpdate, BY, env.executeTrinoUpdate, List.of, env.executeHive, expected.toArray, env.executeTrino], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

### `TestAbfsSyncPartitionMetadata`


- Owning migration commit: `Migrate TestAbfsSyncPartitionMetadata to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java`
- Class-level environment requirement: `AzureEnvironment`.
- Class-level tags: `Azure`, `ConfiguredFeatures`.
- Method inventory complete: Yes. Legacy methods: `9`. Current methods: `9`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`. The current class still exercises Hive/Trino path handling directly; the stack change is the only class-scope approved difference.
- Method statuses present: `verified`. Manual source-body comparisons below were confirmed against the legacy class and the current helper-backed JUnit method bodies.

#### Methods

##### `testAddPartition`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java` ->
  `testAddPartition`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java` ->
  `TestAbfsSyncPartitionMetadata.testAddPartition`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 1.
- Tag parity: Current tags: `Azure`, `ConfiguredFeatures`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `super.testAddPartition`. Current action shape: `CALL`, `SELECT`, `prepare`, `env.executeTrinoUpdate`, `system.sync_partition_metadata`, `schemaName`, `env.executeTrino`, `qualifiedTableName`, `tableLocation`. Legacy delegate calls: `super.testAddPartition`. Current delegate calls: none.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: `assertPartitions`, `row`, `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: `cleanup`.
- Any observed difference, however small: delegate calls differ: legacy [super.testAddPartition] vs current [none]; helper calls differ: legacy [super.testAddPartition] vs current [prepare, env.executeTrinoUpdate, system.sync_partition_metadata, schemaName, env.executeTrino, qualifiedTableName, tableLocation, cleanup]; SQL verbs differ: legacy [none] vs current [CALL, SELECT]; assertion helpers differ: legacy [none] vs current [assertPartitions, row, assertThatThrownBy, hasMessageContaining]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [super.testAddPartition], verbs [none]. Current flow summary -> helpers [prepare, env.executeTrinoUpdate, system.sync_partition_metadata, schemaName, env.executeTrino, qualifiedTableName, tableLocation, cleanup], verbs [CALL, SELECT].
- Audit status: `verified`

##### `testAddPartitionContainingCharactersThatNeedUrlEncoding`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java` ->
  `testAddPartitionContainingCharactersThatNeedUrlEncoding`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java` ->
  `TestAbfsSyncPartitionMetadata.testAddPartitionContainingCharactersThatNeedUrlEncoding`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 1.
- Tag parity: Current tags: `Azure`, `ConfiguredFeatures`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `super.testAddPartitionContainingCharactersThatNeedUrlEncoding`. Current action shape: `CALL`, `ensureSchemaExists`, `env.executeTrinoUpdate`, `qualifiedTableName`, `format`, `s`, `WITH`, `VALUES`, `getTableLocation`, `system.sync_partition_metadata`, `schemaName`. Legacy delegate calls: `super.testAddPartitionContainingCharactersThatNeedUrlEncoding`. Current delegate calls: none.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: `assertPartitions`, `row`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: `DROP`, `cleanup`.
- Any observed difference, however small: delegate calls differ: legacy [super.testAddPartitionContainingCharactersThatNeedUrlEncoding] vs current [none]; helper calls differ: legacy [super.testAddPartitionContainingCharactersThatNeedUrlEncoding] vs current [ensureSchemaExists, env.executeTrinoUpdate, qualifiedTableName, format, s, WITH, VALUES, getTableLocation, system.sync_partition_metadata, schemaName, cleanup]; SQL verbs differ: legacy [none] vs current [DROP, CREATE, INSERT, CALL]; assertion helpers differ: legacy [none] vs current [assertPartitions, row]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [super.testAddPartitionContainingCharactersThatNeedUrlEncoding], verbs [none]. Current flow summary -> helpers [ensureSchemaExists, env.executeTrinoUpdate, qualifiedTableName, format, s, WITH, VALUES, getTableLocation, system.sync_partition_metadata, schemaName, cleanup], verbs [DROP, CREATE, INSERT, CALL].
- Audit status: `verified`

##### `testDropPartition`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java` ->
  `testDropPartition`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java` ->
  `TestAbfsSyncPartitionMetadata.testDropPartition`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 1.
- Tag parity: Current tags: `Azure`, `ConfiguredFeatures`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: none. Current action shape: `CALL`, `prepare`, `env.executeTrinoUpdate`, `system.sync_partition_metadata`, `schemaName`. Legacy delegate calls: `super.testDropPartition`. Current delegate calls: none.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: `assertPartitions`, `row`, `assertData`.
- Cleanup parity: Legacy cleanup shape: `super.testDropPartition`. Current cleanup shape: `DROP`, `cleanup`.
- Any observed difference, however small: delegate calls differ: legacy [super.testDropPartition] vs current [none]; helper calls differ: legacy [super.testDropPartition] vs current [prepare, env.executeTrinoUpdate, system.sync_partition_metadata, schemaName, cleanup]; SQL verbs differ: legacy [none] vs current [CALL, DROP]; assertion helpers differ: legacy [none] vs current [assertPartitions, row, assertData]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [super.testDropPartition], verbs [none]. Current flow summary -> helpers [prepare, env.executeTrinoUpdate, system.sync_partition_metadata, schemaName, cleanup], verbs [CALL, DROP].
- Audit status: `verified`

##### `testDropPartitionContainingCharactersThatNeedUrlEncoding`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java` ->
  `testDropPartitionContainingCharactersThatNeedUrlEncoding`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java` ->
  `TestAbfsSyncPartitionMetadata.testDropPartitionContainingCharactersThatNeedUrlEncoding`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 1.
- Tag parity: Current tags: `Azure`, `ConfiguredFeatures`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: none. Current action shape: `CALL`, `ensureSchemaExists`, `env.executeTrinoUpdate`, `qualifiedTableName`, `format`, `s`, `WITH`, `VALUES`, `getTableLocation`, `system.sync_partition_metadata`, `schemaName`. Legacy delegate calls: `super.testDropPartitionContainingCharactersThatNeedUrlEncoding`. Current delegate calls: none.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: `assertPartitions`, `row`.
- Cleanup parity: Legacy cleanup shape: `super.testDropPartitionContainingCharactersThatNeedUrlEncoding`. Current cleanup shape: `DROP`, `DELETE`, `cleanup`.
- Any observed difference, however small: delegate calls differ: legacy [super.testDropPartitionContainingCharactersThatNeedUrlEncoding] vs current [none]; helper calls differ: legacy [super.testDropPartitionContainingCharactersThatNeedUrlEncoding] vs current [ensureSchemaExists, env.executeTrinoUpdate, qualifiedTableName, format, s, WITH, VALUES, getTableLocation, system.sync_partition_metadata, schemaName, cleanup]; SQL verbs differ: legacy [none] vs current [DROP, CREATE, INSERT, CALL, DELETE]; assertion helpers differ: legacy [none] vs current [assertPartitions, row]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [super.testDropPartitionContainingCharactersThatNeedUrlEncoding], verbs [none]. Current flow summary -> helpers [ensureSchemaExists, env.executeTrinoUpdate, qualifiedTableName, format, s, WITH, VALUES, getTableLocation, system.sync_partition_metadata, schemaName, cleanup], verbs [DROP, CREATE, INSERT, CALL, DELETE].
- Audit status: `verified`

##### `testFullSyncPartition`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java` ->
  `testFullSyncPartition`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java` ->
  `TestAbfsSyncPartitionMetadata.testFullSyncPartition`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 1.
- Tag parity: Current tags: `Azure`, `ConfiguredFeatures`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `super.testFullSyncPartition`. Current action shape: `CALL`, `prepare`, `env.executeTrinoUpdate`, `system.sync_partition_metadata`, `schemaName`. Legacy delegate calls: `super.testFullSyncPartition`. Current delegate calls: none.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: `assertPartitions`, `row`, `assertData`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: `cleanup`.
- Any observed difference, however small: delegate calls differ: legacy [super.testFullSyncPartition] vs current [none]; helper calls differ: legacy [super.testFullSyncPartition] vs current [prepare, env.executeTrinoUpdate, system.sync_partition_metadata, schemaName, cleanup]; SQL verbs differ: legacy [none] vs current [CALL]; assertion helpers differ: legacy [none] vs current [assertPartitions, row, assertData]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [super.testFullSyncPartition], verbs [none]. Current flow summary -> helpers [prepare, env.executeTrinoUpdate, system.sync_partition_metadata, schemaName, cleanup], verbs [CALL].
- Audit status: `verified`

##### `testInvalidSyncMode`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java` ->
  `testInvalidSyncMode`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java` ->
  `TestAbfsSyncPartitionMetadata.testInvalidSyncMode`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 1.
- Tag parity: Current tags: `Azure`, `ConfiguredFeatures`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `super.testInvalidSyncMode`. Current action shape: `CALL`, `prepare`, `env.executeTrino`, `system.sync_partition_metadata`, `schemaName`. Legacy delegate calls: `super.testInvalidSyncMode`. Current delegate calls: none.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: `cleanup`.
- Any observed difference, however small: delegate calls differ: legacy [super.testInvalidSyncMode] vs current [none]; helper calls differ: legacy [super.testInvalidSyncMode] vs current [prepare, env.executeTrino, system.sync_partition_metadata, schemaName, cleanup]; SQL verbs differ: legacy [none] vs current [CALL]; assertion helpers differ: legacy [none] vs current [assertThatThrownBy, hasMessageContaining]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [super.testInvalidSyncMode], verbs [none]. Current flow summary -> helpers [prepare, env.executeTrino, system.sync_partition_metadata, schemaName, cleanup], verbs [CALL].
- Audit status: `verified`

##### `testMixedCasePartitionNames`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java` ->
  `testMixedCasePartitionNames`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java` ->
  `TestAbfsSyncPartitionMetadata.testMixedCasePartitionNames`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 1.
- Tag parity: Current tags: `Azure`, `ConfiguredFeatures`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `super.testMixedCasePartitionNames`. Current action shape: `CALL`, `prepare`, `tableLocation`, `makeDirectory`, `format`, `copyOrcFileToDirectory`, `env.executeTrinoUpdate`, `system.sync_partition_metadata`, `schemaName`. Legacy delegate calls: `super.testMixedCasePartitionNames`. Current delegate calls: none.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: `assertPartitions`, `row`, `assertData`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testMixedCasePartitionNames] vs current [none]; helper calls differ: legacy [super.testMixedCasePartitionNames] vs current [prepare, tableLocation, makeDirectory, format, copyOrcFileToDirectory, env.executeTrinoUpdate, system.sync_partition_metadata, schemaName]; SQL verbs differ: legacy [none] vs current [CALL]; assertion helpers differ: legacy [none] vs current [assertPartitions, row, assertData]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [super.testMixedCasePartitionNames], verbs [none]. Current flow summary -> helpers [prepare, tableLocation, makeDirectory, format, copyOrcFileToDirectory, env.executeTrinoUpdate, system.sync_partition_metadata, schemaName], verbs [CALL].
- Audit status: `verified`

##### `testConflictingMixedCasePartitionNames`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java` ->
  `testConflictingMixedCasePartitionNames`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java` ->
  `TestAbfsSyncPartitionMetadata.testConflictingMixedCasePartitionNames`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 1.
- Tag parity: Current tags: `Azure`, `ConfiguredFeatures`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `super.testConflictingMixedCasePartitionNames`. Current action shape: `CALL`, `tableLocation`, `prepare`, `makeDirectory`, `format`, `copyOrcFileToDirectory`, `env.executeTrino`, `system.sync_partition_metadata`, `schemaName`. Legacy delegate calls: `super.testConflictingMixedCasePartitionNames`. Current delegate calls: none.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `assertPartitions`, `row`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testConflictingMixedCasePartitionNames] vs current [none]; helper calls differ: legacy [super.testConflictingMixedCasePartitionNames] vs current [tableLocation, prepare, makeDirectory, format, copyOrcFileToDirectory, env.executeTrino, system.sync_partition_metadata, schemaName]; SQL verbs differ: legacy [none] vs current [CALL]; assertion helpers differ: legacy [none] vs current [assertThatThrownBy, hasMessageContaining, assertPartitions, row]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [super.testConflictingMixedCasePartitionNames], verbs [none]. Current flow summary -> helpers [tableLocation, prepare, makeDirectory, format, copyOrcFileToDirectory, env.executeTrino, system.sync_partition_metadata, schemaName], verbs [CALL].
- Audit status: `verified`

##### `testSyncPartitionMetadataWithNullArgument`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java` ->
  `testSyncPartitionMetadataWithNullArgument`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestAbfsSyncPartitionMetadata.java` ->
  `TestAbfsSyncPartitionMetadata.testSyncPartitionMetadataWithNullArgument`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 1.
- Tag parity: Current tags: `Azure`, `ConfiguredFeatures`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `super.testSyncPartitionMetadataWithNullArgument`. Current action shape: `CALL`, `env.executeTrino`, `system.sync_partition_metadata`. Legacy delegate calls: `super.testSyncPartitionMetadataWithNullArgument`. Current delegate calls: none.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: `assertThatThrownBy`, `hasMessageMatching`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testSyncPartitionMetadataWithNullArgument] vs current [none]; helper calls differ: legacy [super.testSyncPartitionMetadataWithNullArgument] vs current [env.executeTrino, system.sync_partition_metadata]; SQL verbs differ: legacy [none] vs current [CALL]; assertion helpers differ: legacy [none] vs current [assertThatThrownBy, hasMessageMatching]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [super.testSyncPartitionMetadataWithNullArgument], verbs [none]. Current flow summary -> helpers [env.executeTrino, system.sync_partition_metadata], verbs [CALL].
- Audit status: `verified`

### `TestIcebergAzure`


- Owning migration commit: `Migrate TestIcebergAzure to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergAzure.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergAzure.java`
- Class-level environment requirement: `AzureEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `IcebergAzure`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `4`. Current methods: `4`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope. The current class is a thin JUnit wrapper around `AzureTableFormatsTestUtils`, mirroring the legacy thin wrapper around `BaseTestTableFormats`.
- Method statuses present: `verified`. Manual source-body comparisons below were confirmed against the legacy class and the current helper-backed JUnit method bodies.

#### Methods

##### `testCreateAndSelectNationTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergAzure.java` ->
  `testCreateAndSelectNationTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergAzure.java` ->
  `TestIcebergAzure.testCreateAndSelectNationTable`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 3.
- Tag parity: Current tags: `ConfiguredFeatures`, `IcebergAzure`, `ProfileSpecificTests`. Tag routing matches the
  current suite selection.
- Setup parity: Legacy setup shape: `super.testCreateAndSelectNationTable`. Current setup shape: `AzureTableFormatsTestUtils.testCreateAndSelectNationTable`.
- Action parity: Legacy action shape: none. Current action shape: none. Legacy delegate calls: `super.testCreateAndSelectNationTable`. Current delegate calls: `AzureTableFormatsTestUtils.testCreateAndSelectNationTable`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testCreateAndSelectNationTable] vs current [AzureTableFormatsTestUtils.testCreateAndSelectNationTable]; helper calls differ: legacy [super.testCreateAndSelectNationTable] vs current [AzureTableFormatsTestUtils.testCreateAndSelectNationTable]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testCreateAndSelectNationTable], verbs [none]. Current flow summary -> helpers [AzureTableFormatsTestUtils.testCreateAndSelectNationTable], verbs [none].
- Audit status: `verified`

##### `testBasicWriteOperations`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergAzure.java` ->
  `testBasicWriteOperations`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergAzure.java` ->
  `TestIcebergAzure.testBasicWriteOperations`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 3.
- Tag parity: Current tags: `ConfiguredFeatures`, `IcebergAzure`, `ProfileSpecificTests`. Tag routing matches the
  current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `super.testBasicWriteOperations`. Current action shape: `AzureTableFormatsTestUtils.testBasicWriteOperations`. Legacy delegate calls: `super.testBasicWriteOperations`. Current delegate calls: `AzureTableFormatsTestUtils.testBasicWriteOperations`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testBasicWriteOperations] vs current [AzureTableFormatsTestUtils.testBasicWriteOperations]; helper calls differ: legacy [super.testBasicWriteOperations] vs current [AzureTableFormatsTestUtils.testBasicWriteOperations]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testBasicWriteOperations], verbs [none]. Current flow summary -> helpers [AzureTableFormatsTestUtils.testBasicWriteOperations], verbs [none].
- Audit status: `verified`

##### `testPathContainsSpecialCharacter`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergAzure.java` ->
  `testPathContainsSpecialCharacter`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergAzure.java` ->
  `TestIcebergAzure.testPathContainsSpecialCharacter`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 3.
- Tag parity: Current tags: `ConfiguredFeatures`, `IcebergAzure`, `ProfileSpecificTests`. Tag routing matches the
  current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `super.testPathContainsSpecialCharacter`. Current action shape: `AzureTableFormatsTestUtils.testPathContainsSpecialCharacter`. Legacy delegate calls: `super.testPathContainsSpecialCharacter`. Current delegate calls: `AzureTableFormatsTestUtils.testPathContainsSpecialCharacter`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testPathContainsSpecialCharacter] vs current [AzureTableFormatsTestUtils.testPathContainsSpecialCharacter]; helper calls differ: legacy [super.testPathContainsSpecialCharacter] vs current [AzureTableFormatsTestUtils.testPathContainsSpecialCharacter]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testPathContainsSpecialCharacter], verbs [none]. Current flow summary -> helpers [AzureTableFormatsTestUtils.testPathContainsSpecialCharacter], verbs [none].
- Audit status: `verified`

##### `testSparkReadingTrinoData`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergAzure.java` ->
  `testSparkReadingTrinoData`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergAzure.java` ->
  `TestIcebergAzure.testSparkReadingTrinoData`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 3.
- Tag parity: Current tags: `ConfiguredFeatures`, `IcebergAzure`, `ProfileSpecificTests`. Tag routing matches the
  current suite selection.
- Setup parity: Legacy setup shape: `super.testSparkCompatibilityOnTrinoCreatedTable`. Current setup shape: `AzureTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable`.
- Action parity: Legacy action shape: none. Current action shape: none. Legacy delegate calls: `super.testSparkCompatibilityOnTrinoCreatedTable`. Current delegate calls: `AzureTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testSparkCompatibilityOnTrinoCreatedTable] vs current [AzureTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable]; helper calls differ: legacy [super.testSparkCompatibilityOnTrinoCreatedTable] vs current [AzureTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testSparkCompatibilityOnTrinoCreatedTable], verbs [none]. Current flow summary -> helpers [AzureTableFormatsTestUtils.testSparkCompatibilityOnTrinoCreatedTable], verbs [none].
- Audit status: `verified`

### `TestDeltaLakeAzure`


- Owning migration commit: `Migrate TestDeltaLakeAzure to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeAzure.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeAzure.java`
- Class-level environment requirement: `AzureEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `DeltaLakeAzure`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `3`. Current methods: `3`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope. The current class is a thin JUnit wrapper around `AzureTableFormatsTestUtils`, mirroring the legacy thin wrapper around `BaseTestTableFormats`.
- Method statuses present: `verified`. Manual source-body comparisons below were confirmed against the legacy class and the current helper-backed JUnit method bodies.

#### Methods

##### `testCreateAndSelectNationTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeAzure.java` ->
  `testCreateAndSelectNationTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeAzure.java` ->
  `TestDeltaLakeAzure.testCreateAndSelectNationTable`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 2.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeAzure`, `ProfileSpecificTests`. Tag routing matches the
  current suite selection.
- Setup parity: Legacy setup shape: `super.testCreateAndSelectNationTable`. Current setup shape: `AzureTableFormatsTestUtils.testCreateAndSelectNationTable`.
- Action parity: Legacy action shape: none. Current action shape: none. Legacy delegate calls: `super.testCreateAndSelectNationTable`. Current delegate calls: `AzureTableFormatsTestUtils.testCreateAndSelectNationTable`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testCreateAndSelectNationTable] vs current [AzureTableFormatsTestUtils.testCreateAndSelectNationTable]; helper calls differ: legacy [super.testCreateAndSelectNationTable] vs current [AzureTableFormatsTestUtils.testCreateAndSelectNationTable]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testCreateAndSelectNationTable], verbs [none]. Current flow summary -> helpers [AzureTableFormatsTestUtils.testCreateAndSelectNationTable], verbs [none].
- Audit status: `verified`

##### `testBasicWriteOperations`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeAzure.java` ->
  `testBasicWriteOperations`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeAzure.java` ->
  `TestDeltaLakeAzure.testBasicWriteOperations`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 2.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeAzure`, `ProfileSpecificTests`. Tag routing matches the
  current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `super.testBasicWriteOperations`. Current action shape: `AzureTableFormatsTestUtils.testBasicWriteOperations`. Legacy delegate calls: `super.testBasicWriteOperations`. Current delegate calls: `AzureTableFormatsTestUtils.testBasicWriteOperations`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testBasicWriteOperations] vs current [AzureTableFormatsTestUtils.testBasicWriteOperations]; helper calls differ: legacy [super.testBasicWriteOperations] vs current [AzureTableFormatsTestUtils.testBasicWriteOperations]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testBasicWriteOperations], verbs [none]. Current flow summary -> helpers [AzureTableFormatsTestUtils.testBasicWriteOperations], verbs [none].
- Audit status: `verified`

##### `testPathContainsSpecialCharacter`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeAzure.java` ->
  `testPathContainsSpecialCharacter`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeAzure.java` ->
  `TestDeltaLakeAzure.testPathContainsSpecialCharacter`
- Mapping type: `direct`
- Environment parity: Current class requires `AzureEnvironment`. Routed by source review into `SuiteAzure` run 2.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeAzure`, `ProfileSpecificTests`. Tag routing matches the
  current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `super.testPathContainsSpecialCharacter`. Current action shape: `AzureTableFormatsTestUtils.testPathContainsSpecialCharacter`. Legacy delegate calls: `super.testPathContainsSpecialCharacter`. Current delegate calls: `AzureTableFormatsTestUtils.testPathContainsSpecialCharacter`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: delegate calls differ: legacy [super.testPathContainsSpecialCharacter] vs current [AzureTableFormatsTestUtils.testPathContainsSpecialCharacter]; helper calls differ: legacy [super.testPathContainsSpecialCharacter] vs current [AzureTableFormatsTestUtils.testPathContainsSpecialCharacter]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [super.testPathContainsSpecialCharacter], verbs [none]. Current flow summary -> helpers [AzureTableFormatsTestUtils.testPathContainsSpecialCharacter], verbs [none].
- Audit status: `verified`
