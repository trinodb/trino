# Lane Audit: Delta Lake

## Lane Summary

- Audit status: `manual-pass-complete`
- Section start commit: `Add Delta Lake environments`
- Section end commit: `Remove legacy SuiteDeltaLakeOss`
- Introduced JUnit suites: `SuiteDeltaLakeOss`.
- Extended existing suites: `SuiteDeltaLakeOss`.
- Retired legacy suites: `SuiteDeltaLakeOss`.
- Environment classes introduced: `DeltaLakeMinioCachingEnvironment`, `HiveDeltaLakeMinioEnvironment`.
- Method status counts: verified `191`, intentional difference `7`, needs follow-up `0`.

## Semantic Audit Status

- Manual review note: the Delta Lake lane was freshly re-read end to end in the final pass, including the remaining
  helper-backed delegate methods, the current-only Databricks-side checkpoint coverage, and the current/legacy suite
  and environment sources.

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: `HDP to Hive 3.1 migration`.
- Needs-follow-up methods: none currently identified in this source/history pass.

## Environment Semantic Audit
### `DeltaLakeMinioEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of legacy `EnvMultinodeMinioDataLake` against current
  `DeltaLakeMinioEnvironment`.
- Container/service inventory parity: preserved in intent. Both provide Minio-backed object storage, Hive metastore,
  Spark Delta services, and Trino with Delta Lake plus Hive interoperability on the same S3-compatible storage.
- Recorded differences:
  - Legacy launcher ran this slice on the shared multinode data-lake environment with extra Iceberg and memory
    connectors; current environment narrows the runtime to the Delta/Hive/TPCH services required by the migrated
    JUnit lane.
  - Current environment configures Minio, Spark, and Trino directly in code instead of mounting launcher property files.
- Reviewer note: the environment is narrower than the old shared launcher stack, but no additional Delta Lake fidelity
  gap is currently identified.

### `DeltaLakeOssEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of legacy `EnvSinglenodeDeltaLakeKerberizedHdfs` against current
  `DeltaLakeOssEnvironment`.
- Container/service inventory parity: preserved in intent for the current HDFS-read slice. Both provide Hadoop/HDFS,
  Spark Delta, and Trino with a metastore-backed Delta Lake catalog over HDFS storage.
- Recorded differences:
  - Legacy launcher wrapped the HDFS coverage in a kerberized launcher environment; current JUnit environment keeps the
    shared HDFS/Spark/Trino data path but drops the launcher-only Kerberos scaffolding for the single migrated
    `DeltaLakeHdfs` read test.
  - Current environment configures the services directly in code instead of mounting launcher property files.
- Reviewer note: the current environment is a simplification of the old launcher shell, but no unresolved fidelity gap
  is currently identified for the migrated HDFS-read coverage.

### `HiveDeltaLakeMinioEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of the legacy `DELTA_LAKE_OSS` suite run on `EnvSinglenodeDeltaLakeOss`
  against current `HiveDeltaLakeMinioEnvironment`.
- Catalog/redirection parity: preserved in intent. Both provide paired Hive and Delta catalogs over shared Minio-backed
  storage so the redirect tests exercise cross-catalog visibility and handoff.
- Recorded differences: current environment is extracted as a dedicated redirect-oriented class on top of the Minio
  Delta infrastructure instead of sharing the broader launcher environment and mounted configuration files.
- Reviewer note: no additional environment-specific fidelity gap is currently identified.

### `DeltaLakeMinioCachingEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of legacy `EnvMultinodeMinioDataLakeCaching` against current
  `DeltaLakeMinioCachingEnvironment`.
- Storage/cache parity: preserved in intent. Both provide Minio-backed Delta storage plus explicit filesystem cache
  configuration for the Alluxio-caching lane.
- Recorded differences:
  - Legacy launcher ran caching on the shared multinode data-lake stack; current environment extracts the same cache
    semantics into a dedicated Testcontainers environment.
  - Current environment creates the Minio bucket and Trino cache configuration directly in code instead of mounting
    separate cached and non-cached launcher property files.
- Reviewer note: no additional environment-specific fidelity gap is currently identified.

## Suite Semantic Audit
### `SuiteDeltaLakeOss`
- Suite semantic audit status: `complete`
- CI bucket: `delta-lake`
- Relationship to lane: `owned by this lane`.
- Reviewer note: compared directly against legacy `SuiteDeltaLakeOss`; the current suite preserves the four-run Minio,
  HDFS, redirect, and caching structure for the migrated Delta Lake lane.

## Ported Test Classes

### `TestDeltaLakeActiveFilesCache`


- Owning migration commit: `Migrate TestDeltaLakeActiveFilesCache to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeActiveFilesCache.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeActiveFilesCache.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testRefreshTheFilesCacheWhenTableIsRecreated`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeActiveFilesCache.java` ->
  `testRefreshTheFilesCacheWhenTableIsRecreated`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeActiveFilesCache.java` ->
  `TestDeltaLakeActiveFilesCache.testRefreshTheFilesCacheWhenTableIsRecreated`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `CALL`, `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `s`, `WITH`, `storage`, `removeS3Directory`, `onDelta`, `system.flush_metadata_cache`. Current action shape: `SELECT`, `CALL`, `randomNameSuffix`, `env.executeTrinoUpdate`, `format`, `s`, `WITH`, `env.getBucketName`, `env.executeTrino`, `env.executeSparkUpdate`, `storage`, `removeS3Directory`, `system.flush_metadata_cache`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `assertQueryFailure`, `hasMessageContaining`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, format, s, WITH, dropDeltaTableWithRetry, storage, removeS3Directory, onDelta, system.flush_metadata_cache] vs current [randomNameSuffix, env.executeTrinoUpdate, format, s, WITH, env.getBucketName, env.executeTrino, env.executeSparkUpdate, storage, removeS3Directory, system.flush_metadata_cache]; SQL verbs differ: legacy [CREATE, INSERT, SELECT, CALL, DROP] vs current [CREATE, INSERT, SELECT, DROP, CALL]; assertion helpers differ: legacy [assertThat, containsOnly, row, assertQueryFailure, hasMessageContaining] vs current [assertThat, containsOnly, row, assertThatThrownBy, hasMessageContaining]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, format, s, WITH, dropDeltaTableWithRetry, storage, removeS3Directory, onDelta, system.flush_metadata_cache], verbs [CREATE, INSERT, SELECT, CALL, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, format, s, WITH, env.getBucketName, env.executeTrino, env.executeSparkUpdate, storage, removeS3Directory, system.flush_metadata_cache], verbs [CREATE, INSERT, SELECT, DROP, CALL].
- Audit status: `verified`

### `TestDeltaLakeAlterTableCompatibility`


- Owning migration commit: `Migrate TestDeltaLakeAlterTableCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `10`. Current methods: `9`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly. Cross-class or merged legacy
  coverage accounted for: `testTrinoAlterTablePreservesGeneratedColumn` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibilityDatabricks.java` ->
  `testTrinoAlterTablePreservesGeneratedColumn`.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testAddColumnWithCommentOnTrino`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java` ->
  `testAddColumnWithCommentOnTrino`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java` ->
  `TestDeltaLakeAlterTableCompatibility.testAddColumnWithCommentOnTrino`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`, `COMMENT`. Current setup shape: `CREATE`, `ALTER`, `COMMENT`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `s`, `WITH`, `getColumnCommentOnTrino`, `getColumnCommentOnDelta`. Current action shape: `randomNameSuffix`, `env.executeTrinoUpdate`, `format`, `s`, `WITH`, `env.getBucketName`, `getColumnCommentOnTrino`, `getColumnCommentOnSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, format, s, WITH, getColumnCommentOnTrino, getColumnCommentOnDelta] vs current [randomNameSuffix, env.executeTrinoUpdate, format, s, WITH, env.getBucketName, getColumnCommentOnTrino, getColumnCommentOnSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, format, s, WITH, getColumnCommentOnTrino, getColumnCommentOnDelta], verbs [CREATE, ALTER, COMMENT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, format, s, WITH, env.getBucketName, getColumnCommentOnTrino, getColumnCommentOnSpark], verbs [CREATE, ALTER, COMMENT, DROP].
- Audit status: `verified`

##### `testRenameColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java` ->
  `testRenameColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java` ->
  `TestDeltaLakeAlterTableCompatibility.testRenameColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `format`, `s`, `TBLPROPERTIES`, `VALUES`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `format`, `s`, `TBLPROPERTIES`, `env.getBucketName`, `VALUES`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, format, s, TBLPROPERTIES, VALUES, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, format, s, TBLPROPERTIES, env.getBucketName, VALUES, env.executeTrino]; SQL verbs differ: legacy [CREATE, INSERT, SELECT, ALTER] vs current [CREATE, INSERT, SELECT, ALTER, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, format, s, TBLPROPERTIES, VALUES, onTrino, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT, ALTER]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, format, s, TBLPROPERTIES, env.getBucketName, VALUES, env.executeTrino], verbs [CREATE, INSERT, SELECT, ALTER, DROP].
- Audit status: `verified`

##### `testRenamePartitionedColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java` ->
  `testRenamePartitionedColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java` ->
  `TestDeltaLakeAlterTableCompatibility.testRenamePartitionedColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `format`, `s`, `BY`, `TBLPROPERTIES`, `VALUES`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `format`, `s`, `BY`, `TBLPROPERTIES`, `env.getBucketName`, `VALUES`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, format, s, BY, TBLPROPERTIES, VALUES, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, format, s, BY, TBLPROPERTIES, env.getBucketName, VALUES, env.executeTrino]; SQL verbs differ: legacy [CREATE, INSERT, SELECT, ALTER] vs current [CREATE, INSERT, SELECT, ALTER, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, format, s, BY, TBLPROPERTIES, VALUES, onTrino, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT, ALTER]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, format, s, BY, TBLPROPERTIES, env.getBucketName, VALUES, env.executeTrino], verbs [CREATE, INSERT, SELECT, ALTER, DROP].
- Audit status: `verified`

##### `testDropNotNullConstraint`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java` ->
  `testDropNotNullConstraint`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java` ->
  `TestDeltaLakeAlterTableCompatibility.testDropNotNullConstraint`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `ALTER`, `INSERT`, `SELECT`, `testDropNotNullConstraint`, `randomNameSuffix`, `onDelta`, `onTrino`. Current action shape: `ALTER`, `INSERT`, `SELECT`, `testDropNotNullConstraint`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.executeTrinoUpdate`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the shared helper is semantically unchanged; it still iterates the `id`, `name`, and `none` column-mapping modes, drops two NOT NULL constraints in Spark and two in Trino, inserts all-null rows from both engines, and verifies both readers return the duplicated null row set before dropping the table.
- Known intentional difference: None.
- Reviewer note: The top-level method invokes the shared helper three times for the three mapping modes. That helper creates a partitioned Delta table with four NOT NULL columns, relaxes the constraints from both engines, inserts nulls from both engines, verifies Spark and Trino both see two all-null rows, and drops the table.
- Audit status: `verified`

##### `testCommentOnTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java` ->
  `testCommentOnTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java` ->
  `TestDeltaLakeAlterTableCompatibility.testCommentOnTable`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`. Current setup shape: `CREATE`, `COMMENT`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `s`, `WITH`, `getTableCommentOnTrino`, `getTableCommentOnDelta`. Current action shape: `randomNameSuffix`, `env.executeTrinoUpdate`, `format`, `s`, `WITH`, `env.getBucketName`, `getTableCommentOnTrino`, `getTableCommentOnSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, format, s, WITH, getTableCommentOnTrino, getTableCommentOnDelta] vs current [randomNameSuffix, env.executeTrinoUpdate, format, s, WITH, env.getBucketName, getTableCommentOnTrino, getTableCommentOnSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, format, s, WITH, getTableCommentOnTrino, getTableCommentOnDelta], verbs [CREATE, COMMENT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, format, s, WITH, env.getBucketName, getTableCommentOnTrino, getTableCommentOnSpark], verbs [CREATE, COMMENT, DROP].
- Audit status: `verified`

##### `testCommentOnColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java` ->
  `testCommentOnColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java` ->
  `TestDeltaLakeAlterTableCompatibility.testCommentOnColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`. Current setup shape: `CREATE`, `COMMENT`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `s`, `WITH`, `getColumnCommentOnTrino`, `getColumnCommentOnDelta`. Current action shape: `randomNameSuffix`, `env.executeTrinoUpdate`, `format`, `s`, `WITH`, `env.getBucketName`, `getColumnCommentOnTrino`, `getColumnCommentOnSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, format, s, WITH, getColumnCommentOnTrino, getColumnCommentOnDelta] vs current [randomNameSuffix, env.executeTrinoUpdate, format, s, WITH, env.getBucketName, getColumnCommentOnTrino, getColumnCommentOnSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, format, s, WITH, getColumnCommentOnTrino, getColumnCommentOnDelta], verbs [CREATE, COMMENT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, format, s, WITH, env.getBucketName, getColumnCommentOnTrino, getColumnCommentOnSpark], verbs [CREATE, COMMENT, DROP].
- Audit status: `verified`

##### `testTrinoPreservesReaderAndWriterVersions`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java` ->
  `testTrinoPreservesReaderAndWriterVersions`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java` ->
  `TestDeltaLakeAlterTableCompatibility.testTrinoPreservesReaderAndWriterVersions`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`, `ALTER`, `INSERT`, `SET`. Current setup shape: `CREATE`, `COMMENT`, `ALTER`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `UPDATE`, `MERGE`, `SHOW`, `randomNameSuffix`, `onDelta`, `executeQuery`, `format`, `s`, `TBLPROPERTIES`, `onTrino`, `VALUES`, `ON`, `getOnlyElement`, `rows`, `minReaderVersion.get`, `minWriterVersion.get`. Current action shape: `UPDATE`, `MERGE`, `SHOW`, `randomNameSuffix`, `env.executeSparkUpdate`, `format`, `s`, `TBLPROPERTIES`, `env.getBucketName`, `env.executeTrinoUpdate`, `VALUES`, `ON`, `getOnlyElement`, `env.executeSpark`, `rows`, `minReaderVersion.get`, `minWriterVersion.get`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `DROP`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, format, s, TBLPROPERTIES, onTrino, VALUES, ON, getOnlyElement, rows, minReaderVersion.get, minWriterVersion.get] vs current [randomNameSuffix, env.executeSparkUpdate, format, s, TBLPROPERTIES, env.getBucketName, env.executeTrinoUpdate, VALUES, ON, getOnlyElement, env.executeSpark, rows, minReaderVersion.get, minWriterVersion.get]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, format, s, TBLPROPERTIES, onTrino, VALUES, ON, getOnlyElement, rows, minReaderVersion.get, minWriterVersion.get], verbs [CREATE, COMMENT, ALTER, INSERT, UPDATE, SET, DELETE, MERGE, SHOW, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, format, s, TBLPROPERTIES, env.getBucketName, env.executeTrinoUpdate, VALUES, ON, getOnlyElement, env.executeSpark, rows, minReaderVersion.get, minWriterVersion.get], verbs [CREATE, COMMENT, ALTER, INSERT, UPDATE, SET, DELETE, MERGE, SHOW, DROP].
- Audit status: `verified`

##### `testTrinoPreservesTableFeature`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java` ->
  `testTrinoPreservesTableFeature`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java` ->
  `TestDeltaLakeAlterTableCompatibility.testTrinoPreservesTableFeature`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`, `ALTER`, `INSERT`, `SET`. Current setup shape: `CREATE`, `COMMENT`, `ALTER`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `UPDATE`, `MERGE`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `onTrino`, `VALUES`, `ON`, `getTablePropertyOnDelta`. Current action shape: `UPDATE`, `MERGE`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `env.executeTrinoUpdate`, `VALUES`, `ON`, `getTablePropertyOnSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino, VALUES, ON, getTablePropertyOnDelta, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, VALUES, ON, getTablePropertyOnSpark]; SQL verbs differ: legacy [CREATE, COMMENT, ALTER, INSERT, UPDATE, SET, DELETE, MERGE] vs current [CREATE, COMMENT, ALTER, INSERT, UPDATE, SET, DELETE, MERGE, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino, VALUES, ON, getTablePropertyOnDelta, dropDeltaTableWithRetry], verbs [CREATE, COMMENT, ALTER, INSERT, UPDATE, SET, DELETE, MERGE]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, VALUES, ON, getTablePropertyOnSpark], verbs [CREATE, COMMENT, ALTER, INSERT, UPDATE, SET, DELETE, MERGE, DROP].
- Audit status: `verified`

##### `testTypeWideningInteger`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java` ->
  `testTypeWideningInteger`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeAlterTableCompatibility.java` ->
  `TestDeltaLakeAlterTableCompatibility.testTypeWideningInteger`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`. Current setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`.
- Action parity: Legacy action shape: `SELECT`, `UPDATE`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `onTrino`. Current action shape: `SELECT`, `UPDATE`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino], verbs [CREATE, INSERT, SELECT, ALTER, UPDATE, SET, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrino], verbs [CREATE, INSERT, SELECT, ALTER, UPDATE, SET, DROP].
- Audit status: `verified`

### `TestDeltaLakeCaseInsensitiveMapping`


- Owning migration commit: `Migrate TestDeltaLakeCaseInsensitiveMapping to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCaseInsensitiveMapping.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCaseInsensitiveMapping.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `6`. Current methods: `4`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly. Cross-class or merged legacy
  coverage accounted for: `testGeneratedColumnWithNonLowerCaseColumnName` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCaseInsensitiveMappingDatabricks.java` ->
  `testGeneratedColumnWithNonLowerCaseColumnName`; `testIdentityColumnWithNonLowerCaseColumnName` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCaseInsensitiveMappingDatabricks.java` ->
  `testIdentityColumnWithNonLowerCaseColumnName`.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testNonLowercaseColumnNames`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCaseInsensitiveMapping.java` ->
  `testNonLowercaseColumnNames`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCaseInsensitiveMapping.java` ->
  `TestDeltaLakeCaseInsensitiveMapping.testNonLowercaseColumnNames`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SHOW`, `randomNameSuffix`, `onDelta`, `executeQuery`, `BY`, `VALUES`, `onTrino`. Current action shape: `SHOW`, `randomNameSuffix`, `env.executeSparkUpdate`, `BY`, `env.getBucketName`, `VALUES`, `env.executeTrinoUpdate`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, BY, VALUES, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, VALUES, env.executeTrinoUpdate, env.executeTrino]; SQL verbs differ: legacy [CREATE, INSERT, SHOW] vs current [CREATE, INSERT, SHOW, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, BY, VALUES, onTrino, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SHOW]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, VALUES, env.executeTrinoUpdate, env.executeTrino], verbs [CREATE, INSERT, SHOW, DROP].
- Audit status: `verified`

##### `testNonLowercaseFieldNames`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCaseInsensitiveMapping.java` ->
  `testNonLowercaseFieldNames`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCaseInsensitiveMapping.java` ->
  `TestDeltaLakeCaseInsensitiveMapping.testNonLowercaseFieldNames`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `SHOW`, `randomNameSuffix`, `onDelta`, `executeQuery`, `onTrino`, `ImmutableList.of`. Current action shape: `SELECT`, `SHOW`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `env.executeTrinoUpdate`, `env.executeTrino`, `ImmutableList.of`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, onTrino, ImmutableList.of, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, env.executeTrinoUpdate, env.executeTrino, ImmutableList.of, env.executeSpark]; SQL verbs differ: legacy [CREATE, INSERT, SELECT, SHOW] vs current [CREATE, INSERT, SELECT, SHOW, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, onTrino, ImmutableList.of, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT, SHOW]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, env.executeTrinoUpdate, env.executeTrino, ImmutableList.of, env.executeSpark], verbs [CREATE, INSERT, SELECT, SHOW, DROP].
- Audit status: `verified`

##### `testColumnCommentWithNonLowerCaseColumnName`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCaseInsensitiveMapping.java` ->
  `testColumnCommentWithNonLowerCaseColumnName`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCaseInsensitiveMapping.java` ->
  `TestDeltaLakeCaseInsensitiveMapping.testColumnCommentWithNonLowerCaseColumnName`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`. Current setup shape: `CREATE`, `COMMENT`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onDelta`, `executeQuery`, `getColumnCommentOnTrino`, `getColumnCommentOnDelta`, `onTrino`. Current action shape: `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `getColumnCommentOnTrino`, `getColumnCommentOnSpark`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, getColumnCommentOnTrino, getColumnCommentOnDelta, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, getColumnCommentOnTrino, getColumnCommentOnSpark, env.executeTrinoUpdate]; SQL verbs differ: legacy [CREATE, COMMENT] vs current [CREATE, COMMENT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, getColumnCommentOnTrino, getColumnCommentOnDelta, onTrino, dropDeltaTableWithRetry], verbs [CREATE, COMMENT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, getColumnCommentOnTrino, getColumnCommentOnSpark, env.executeTrinoUpdate], verbs [CREATE, COMMENT, DROP].
- Audit status: `verified`

##### `testNotNullColumnWithNonLowerCaseColumnName`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCaseInsensitiveMapping.java` ->
  `testNotNullColumnWithNonLowerCaseColumnName`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCaseInsensitiveMapping.java` ->
  `TestDeltaLakeCaseInsensitiveMapping.testNotNullColumnWithNonLowerCaseColumnName`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`, `INSERT`. Current setup shape: `CREATE`, `COMMENT`, `INSERT`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onDelta`, `executeQuery`, `onTrino`. Current action shape: `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThatThrownBy`, `hasMessageContaining`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, env.executeTrinoUpdate]; SQL verbs differ: legacy [CREATE, COMMENT, INSERT] vs current [CREATE, COMMENT, INSERT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, onTrino, dropDeltaTableWithRetry], verbs [CREATE, COMMENT, INSERT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, env.executeTrinoUpdate], verbs [CREATE, COMMENT, INSERT, DROP].
- Audit status: `verified`

### `TestDeltaLakeChangeDataFeedCompatibility`


- Owning migration commit: `Migrate TestDeltaLakeChangeDataFeedCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `17`. Current methods: `17`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testUpdateTableWithCdf`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `testUpdateTableWithCdf`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `TestDeltaLakeChangeDataFeedCompatibility.testUpdateTableWithCdf`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `SHOW`, `UPDATE`, `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `getOnlyValue`, `toString`, `onDelta`, `VALUES`, `table_changes`. Current action shape: `SHOW`, `UPDATE`, `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `env.getBucketName`, `env.executeTrino`, `getOnlyValue`, `toString`, `env.executeSparkUpdate`, `VALUES`, `env.executeSpark`, `table_changes`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `contains`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, getOnlyValue, toString, onDelta, VALUES, table_changes] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, env.executeTrino, getOnlyValue, toString, env.executeSparkUpdate, VALUES, env.executeSpark, table_changes]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, getOnlyValue, toString, onDelta, VALUES, table_changes], verbs [CREATE, SHOW, INSERT, UPDATE, SET, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, env.executeTrino, getOnlyValue, toString, env.executeSparkUpdate, VALUES, env.executeSpark, table_changes], verbs [CREATE, SHOW, INSERT, UPDATE, SET, SELECT, DROP].
- Audit status: `verified`

##### `testUpdateTableWithChangeDataFeedWriterFeature`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `testUpdateTableWithChangeDataFeedWriterFeature`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `TestDeltaLakeChangeDataFeedCompatibility.testUpdateTableWithChangeDataFeedWriterFeature`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `SET`, `ALTER`.
- Action parity: Legacy action shape: `SHOW`, `UPDATE`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `onTrino`, `getOnlyValue`, `toString`, `getTablePropertiesOnDelta`, `properties.getOrDefault`, `equals`, `VALUES`, `table_changes`, `entry`, `doesNotContainKey`, `doesNotContain`. Current action shape: `SHOW`, `UPDATE`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `env.executeTrino`, `getOnlyValue`, `toString`, `getTablePropertiesOnSpark`, `properties.getOrDefault`, `equals`, `VALUES`, `env.executeTrinoUpdate`, `env.executeSpark`, `table_changes`, `entry`, `doesNotContainKey`, `doesNotContain`, `hasStackTraceContaining`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `containsOnly`, `row`, `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThat`, `contains`, `containsOnly`, `row`, `assertThatThrownBy`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino, getOnlyValue, toString, getTablePropertiesOnDelta, properties.getOrDefault, equals, VALUES, table_changes, entry, doesNotContainKey, doesNotContain, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, env.executeTrino, getOnlyValue, toString, getTablePropertiesOnSpark, properties.getOrDefault, equals, VALUES, env.executeTrinoUpdate, env.executeSpark, table_changes, entry, doesNotContainKey, doesNotContain, hasStackTraceContaining]; SQL verbs differ: legacy [CREATE, SHOW, INSERT, UPDATE, SET, SELECT, ALTER] vs current [CREATE, SHOW, INSERT, UPDATE, SET, SELECT, ALTER, DROP]; assertion helpers differ: legacy [assertThat, contains, containsOnly, row, assertQueryFailure, hasMessageMatching] vs current [assertThat, contains, containsOnly, row, assertThatThrownBy]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino, getOnlyValue, toString, getTablePropertiesOnDelta, properties.getOrDefault, equals, VALUES, table_changes, entry, doesNotContainKey, doesNotContain, dropDeltaTableWithRetry], verbs [CREATE, SHOW, INSERT, UPDATE, SET, SELECT, ALTER]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, env.executeTrino, getOnlyValue, toString, getTablePropertiesOnSpark, properties.getOrDefault, equals, VALUES, env.executeTrinoUpdate, env.executeSpark, table_changes, entry, doesNotContainKey, doesNotContain, hasStackTraceContaining], verbs [CREATE, SHOW, INSERT, UPDATE, SET, SELECT, ALTER, DROP].
- Audit status: `verified`

##### `testUpdateCdfTableWithNonLowercaseColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `testUpdateCdfTableWithNonLowercaseColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `TestDeltaLakeChangeDataFeedCompatibility.testUpdateCdfTableWithNonLowercaseColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `onTrino`, `VALUES`, `builder`, `add`, `build`, `table_changes`, `TABLE`, `system.table_changes`. Current action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `env.executeTrinoUpdate`, `VALUES`, `builder`, `add`, `build`, `env.executeSpark`, `table_changes`, `env.executeTrino`, `TABLE`, `system.table_changes`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino, VALUES, builder, add, build, table_changes, TABLE, system.table_changes, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, VALUES, builder, add, build, env.executeSpark, table_changes, env.executeTrino, TABLE, system.table_changes]; SQL verbs differ: legacy [CREATE, INSERT, UPDATE, SET, SELECT] vs current [CREATE, INSERT, UPDATE, SET, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino, VALUES, builder, add, build, table_changes, TABLE, system.table_changes, dropDeltaTableWithRetry], verbs [CREATE, INSERT, UPDATE, SET, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, VALUES, builder, add, build, env.executeSpark, table_changes, env.executeTrino, TABLE, system.table_changes], verbs [CREATE, INSERT, UPDATE, SET, SELECT, DROP].
- Audit status: `verified`

##### `testUpdatePartitionedTableWithCdf`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `testUpdatePartitionedTableWithCdf`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `TestDeltaLakeChangeDataFeedCompatibility.testUpdatePartitionedTableWithCdf`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `BY`, `TBLPROPERTIES`, `getTablePropertiesOnDelta`, `properties.getOrDefault`, `equals`, `VALUES`, `onTrino`, `table_changes`. Current action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `BY`, `env.getBucketName`, `TBLPROPERTIES`, `getTablePropertiesOnSpark`, `properties.getOrDefault`, `equals`, `VALUES`, `env.executeTrinoUpdate`, `env.executeSpark`, `table_changes`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, getTablePropertiesOnDelta, properties.getOrDefault, equals, VALUES, onTrino, table_changes, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, TBLPROPERTIES, getTablePropertiesOnSpark, properties.getOrDefault, equals, VALUES, env.executeTrinoUpdate, env.executeSpark, table_changes]; SQL verbs differ: legacy [CREATE, INSERT, UPDATE, SET, SELECT] vs current [CREATE, INSERT, UPDATE, SET, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, getTablePropertiesOnDelta, properties.getOrDefault, equals, VALUES, onTrino, table_changes, dropDeltaTableWithRetry], verbs [CREATE, INSERT, UPDATE, SET, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, TBLPROPERTIES, getTablePropertiesOnSpark, properties.getOrDefault, equals, VALUES, env.executeTrinoUpdate, env.executeSpark, table_changes], verbs [CREATE, INSERT, UPDATE, SET, SELECT, DROP].
- Audit status: `verified`

##### `testUpdateTableWithManyRowsInsertedInTheSameQueryAndCdfEnabled`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `testUpdateTableWithManyRowsInsertedInTheSameQueryAndCdfEnabled`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `TestDeltaLakeChangeDataFeedCompatibility.testUpdateTableWithManyRowsInsertedInTheSameQueryAndCdfEnabled`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `onTrino`, `table_changes`. Current action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `env.executeTrinoUpdate`, `env.executeSpark`, `table_changes`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, table_changes, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrinoUpdate, env.executeSpark, table_changes]; SQL verbs differ: legacy [CREATE, INSERT, UPDATE, SET, SELECT] vs current [CREATE, INSERT, UPDATE, SET, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, table_changes, dropDeltaTableWithRetry], verbs [CREATE, INSERT, UPDATE, SET, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrinoUpdate, env.executeSpark, table_changes], verbs [CREATE, INSERT, UPDATE, SET, SELECT, DROP].
- Audit status: `verified`

##### `testUpdatePartitionedTableWithManyRowsInsertedInTheSameRequestAndCdfEnabled`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `testUpdatePartitionedTableWithManyRowsInsertedInTheSameRequestAndCdfEnabled`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `TestDeltaLakeChangeDataFeedCompatibility.testUpdatePartitionedTableWithManyRowsInsertedInTheSameRequestAndCdfEnabled`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `BY`, `TBLPROPERTIES`, `onTrino`, `table_changes`. Current action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `BY`, `env.getBucketName`, `TBLPROPERTIES`, `env.executeTrinoUpdate`, `env.executeSpark`, `table_changes`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, onTrino, table_changes, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, env.executeSpark, table_changes]; SQL verbs differ: legacy [CREATE, INSERT, UPDATE, SET, SELECT] vs current [CREATE, INSERT, UPDATE, SET, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, onTrino, table_changes, dropDeltaTableWithRetry], verbs [CREATE, INSERT, UPDATE, SET, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, env.executeSpark, table_changes], verbs [CREATE, INSERT, UPDATE, SET, SELECT, DROP].
- Audit status: `verified`

##### `testUpdatePartitionedTableCdfEnabledAndPartitioningColumnUpdated`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `testUpdatePartitionedTableCdfEnabledAndPartitioningColumnUpdated`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `TestDeltaLakeChangeDataFeedCompatibility.testUpdatePartitionedTableCdfEnabledAndPartitioningColumnUpdated`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `BY`, `TBLPROPERTIES`, `onTrino`, `table_changes`. Current action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `BY`, `env.getBucketName`, `TBLPROPERTIES`, `env.executeTrinoUpdate`, `env.executeSpark`, `table_changes`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, onTrino, table_changes, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, env.executeSpark, table_changes]; SQL verbs differ: legacy [CREATE, INSERT, UPDATE, SET, SELECT] vs current [CREATE, INSERT, UPDATE, SET, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, onTrino, table_changes, dropDeltaTableWithRetry], verbs [CREATE, INSERT, UPDATE, SET, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, env.executeSpark, table_changes], verbs [CREATE, INSERT, UPDATE, SET, SELECT, DROP].
- Audit status: `verified`

##### `testUpdateTableWithCdfEnabledAfterTableIsAlreadyCreated`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `testUpdateTableWithCdfEnabledAfterTableIsAlreadyCreated`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `TestDeltaLakeChangeDataFeedCompatibility.testUpdateTableWithCdfEnabledAfterTableIsAlreadyCreated`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `SET`, `ALTER`.
- Action parity: Legacy action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `VALUES`, `onTrino`, `TBLPROPERTIES`, `get`, `table_changes`, `format`. Current action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `VALUES`, `env.executeTrinoUpdate`, `TBLPROPERTIES`, `env.executeSpark`, `getRows`, `get`, `getValue`, `table_changes`, `hasStackTraceContaining`, `format`.
- Assertion parity: Legacy assertion helpers: `row`, `assertQueryFailure`, `hasMessageMatching`, `assertThat`, `containsOnly`. Current assertion helpers: `assertThatThrownBy`, `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, VALUES, onTrino, TBLPROPERTIES, get, table_changes, format, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, VALUES, env.executeTrinoUpdate, TBLPROPERTIES, env.executeSpark, getRows, get, getValue, table_changes, hasStackTraceContaining, format]; SQL verbs differ: legacy [CREATE, INSERT, UPDATE, SET, ALTER, SELECT] vs current [CREATE, INSERT, UPDATE, SET, ALTER, SELECT, DROP]; assertion helpers differ: legacy [row, assertQueryFailure, hasMessageMatching, assertThat, containsOnly] vs current [assertThatThrownBy, assertThat, containsOnly, row]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, VALUES, onTrino, TBLPROPERTIES, get, table_changes, format, dropDeltaTableWithRetry], verbs [CREATE, INSERT, UPDATE, SET, ALTER, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, VALUES, env.executeTrinoUpdate, TBLPROPERTIES, env.executeSpark, getRows, get, getValue, table_changes, hasStackTraceContaining, format], verbs [CREATE, INSERT, UPDATE, SET, ALTER, SELECT, DROP].
- Audit status: `verified`

##### `testDeleteFromTableWithCdf`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `testDeleteFromTableWithCdf`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `TestDeltaLakeChangeDataFeedCompatibility.testDeleteFromTableWithCdf`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `getTablePropertiesOnDelta`, `properties.getOrDefault`, `equals`, `VALUES`, `onTrino`, `table_changes`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `getTablePropertiesOnSpark`, `properties.getOrDefault`, `equals`, `VALUES`, `env.executeTrinoUpdate`, `env.executeSpark`, `table_changes`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, getTablePropertiesOnDelta, properties.getOrDefault, equals, VALUES, onTrino, table_changes, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, getTablePropertiesOnSpark, properties.getOrDefault, equals, VALUES, env.executeTrinoUpdate, env.executeSpark, table_changes]; SQL verbs differ: legacy [CREATE, INSERT, DELETE, SELECT] vs current [CREATE, INSERT, DELETE, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, getTablePropertiesOnDelta, properties.getOrDefault, equals, VALUES, onTrino, table_changes, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, getTablePropertiesOnSpark, properties.getOrDefault, equals, VALUES, env.executeTrinoUpdate, env.executeSpark, table_changes], verbs [CREATE, INSERT, DELETE, SELECT, DROP].
- Audit status: `verified`

##### `testMergeUpdateIntoTableWithCdfEnabled`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `testMergeUpdateIntoTableWithCdfEnabled`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `TestDeltaLakeChangeDataFeedCompatibility.testMergeUpdateIntoTableWithCdfEnabled`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `MERGE`, `UPDATE`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `onTrino`, `ON`, `table_changes`. Current action shape: `MERGE`, `UPDATE`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `env.executeTrinoUpdate`, `ON`, `env.executeSpark`, `table_changes`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, ON, INSERT, table_changes, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrinoUpdate, ON, INSERT, env.executeSpark, table_changes]; SQL verbs differ: legacy [CREATE, INSERT, MERGE, UPDATE, SET, SELECT] vs current [CREATE, INSERT, MERGE, UPDATE, SET, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, ON, INSERT, table_changes, dropDeltaTableWithRetry], verbs [CREATE, INSERT, MERGE, UPDATE, SET, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrinoUpdate, ON, INSERT, env.executeSpark, table_changes], verbs [CREATE, INSERT, MERGE, UPDATE, SET, SELECT, DROP].
- Audit status: `verified`

##### `testMergeDeleteIntoTableWithCdfEnabled`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `testMergeDeleteIntoTableWithCdfEnabled`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `TestDeltaLakeChangeDataFeedCompatibility.testMergeDeleteIntoTableWithCdfEnabled`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `MERGE`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `onTrino`, `ON`, `table_changes`. Current action shape: `MERGE`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `env.executeTrinoUpdate`, `ON`, `env.executeSpark`, `table_changes`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, ON, INSERT, table_changes, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrinoUpdate, ON, INSERT, env.executeSpark, table_changes]; SQL verbs differ: legacy [CREATE, INSERT, MERGE, DELETE, SELECT] vs current [CREATE, INSERT, MERGE, DELETE, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, ON, INSERT, table_changes, dropDeltaTableWithRetry], verbs [CREATE, INSERT, MERGE, DELETE, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrinoUpdate, ON, INSERT, env.executeSpark, table_changes], verbs [CREATE, INSERT, MERGE, DELETE, SELECT, DROP].
- Audit status: `verified`

##### `testMergeMixedDeleteAndUpdateIntoTableWithCdfEnabled`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `testMergeMixedDeleteAndUpdateIntoTableWithCdfEnabled`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `TestDeltaLakeChangeDataFeedCompatibility.testMergeMixedDeleteAndUpdateIntoTableWithCdfEnabled`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `MERGE`, `UPDATE`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `getTablePropertiesOnDelta`, `properties.getOrDefault`, `equals`, `VALUES`, `onTrino`, `ON`, `table_changes`. Current action shape: `MERGE`, `UPDATE`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `getTablePropertiesOnSpark`, `properties.getOrDefault`, `equals`, `VALUES`, `env.executeTrinoUpdate`, `ON`, `env.executeSpark`, `table_changes`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, getTablePropertiesOnDelta, properties.getOrDefault, equals, VALUES, onTrino, ON, INSERT, table_changes, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, getTablePropertiesOnSpark, properties.getOrDefault, equals, VALUES, env.executeTrinoUpdate, ON, INSERT, env.executeSpark, table_changes]; SQL verbs differ: legacy [CREATE, INSERT, MERGE, DELETE, UPDATE, SET, SELECT] vs current [CREATE, INSERT, MERGE, DELETE, UPDATE, SET, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, getTablePropertiesOnDelta, properties.getOrDefault, equals, VALUES, onTrino, ON, INSERT, table_changes, dropDeltaTableWithRetry], verbs [CREATE, INSERT, MERGE, DELETE, UPDATE, SET, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, getTablePropertiesOnSpark, properties.getOrDefault, equals, VALUES, env.executeTrinoUpdate, ON, INSERT, env.executeSpark, table_changes], verbs [CREATE, INSERT, MERGE, DELETE, UPDATE, SET, SELECT, DROP].
- Audit status: `verified`

##### `testDeleteFromNullPartitionWithCdfEnabled`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `testDeleteFromNullPartitionWithCdfEnabled`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `TestDeltaLakeChangeDataFeedCompatibility.testDeleteFromNullPartitionWithCdfEnabled`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `BY`, `TBLPROPERTIES`, `onTrino`, `table_changes`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `BY`, `env.getBucketName`, `TBLPROPERTIES`, `env.executeTrinoUpdate`, `env.executeTrino`, `env.executeSpark`, `table_changes`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, onTrino, table_changes, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, env.executeTrino, env.executeSpark, table_changes]; SQL verbs differ: legacy [CREATE, INSERT, DELETE, SELECT] vs current [CREATE, INSERT, DELETE, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, onTrino, table_changes, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, env.executeTrino, env.executeSpark, table_changes], verbs [CREATE, INSERT, DELETE, SELECT, DROP].
- Audit status: `verified`

##### `testTurningOnAndOffCdfFromTrino`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `testTurningOnAndOffCdfFromTrino`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `TestDeltaLakeChangeDataFeedCompatibility.testTurningOnAndOffCdfFromTrino`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `SET`, `ALTER`.
- Action parity: Legacy action shape: `SHOW`, `UPDATE`, `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `getOnlyValue`, `toString`, `onDelta`, `VALUES`, `table_changes`. Current action shape: `SHOW`, `UPDATE`, `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `env.getBucketName`, `env.executeTrino`, `getOnlyValue`, `toString`, `env.executeSparkUpdate`, `VALUES`, `env.executeSpark`, `table_changes`, `hasStackTraceContaining`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `containsOnly`, `row`, `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThat`, `contains`, `containsOnly`, `row`, `assertThatThrownBy`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, getOnlyValue, toString, onDelta, VALUES, table_changes] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, env.executeTrino, getOnlyValue, toString, env.executeSparkUpdate, VALUES, env.executeSpark, table_changes, hasStackTraceContaining]; assertion helpers differ: legacy [assertThat, contains, containsOnly, row, assertQueryFailure, hasMessageMatching] vs current [assertThat, contains, containsOnly, row, assertThatThrownBy]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, getOnlyValue, toString, onDelta, VALUES, table_changes], verbs [CREATE, SHOW, INSERT, UPDATE, SET, SELECT, ALTER, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, env.executeTrino, getOnlyValue, toString, env.executeSparkUpdate, VALUES, env.executeSpark, table_changes, hasStackTraceContaining], verbs [CREATE, SHOW, INSERT, UPDATE, SET, SELECT, ALTER, DROP].
- Audit status: `verified`

##### `testThatCdfDoesntWorkWhenPropertyIsNotSet`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `testThatCdfDoesntWorkWhenPropertyIsNotSet`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `TestDeltaLakeChangeDataFeedCompatibility.testThatCdfDoesntWorkWhenPropertyIsNotSet`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `SHOW`, `INSERT`. Current setup shape: `CREATE`, `SHOW`, `INSERT`.
- Action parity: Legacy action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `assertThereIsNoCdfFileGenerated`, `assertThatThereIsNoChangeDataFiles`, `onTrino`, `onDelta`. Current action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `assertThereIsNoCdfFileGenerated`, `assertThatThereIsNoChangeDataFiles`, `env.executeTrinoUpdate`, `env.executeTrino`, `env.executeSparkUpdate`, `env.executeSpark`, `env.createMinioClient`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `doesNotContain`, `contains`, `containsOnly`. Current assertion helpers: `assertThat`, `doesNotContain`, `contains`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the helper is semantically unchanged; it still runs two cases, one with no CDF property and one with `change_data_feed_enabled = false`, verifies `SHOW CREATE TABLE` reflects that state, performs cross-engine updates after inserts, confirms the final rows, and checks that `_change_data` remains empty. The JUnit port replaces the legacy S3 client with a scoped Minio client.
- Known intentional difference: None.
- Reviewer note: The top-level method allocates two table names and runs the shared helper twice. That helper creates a table without active CDF, inserts three rows, updates one row from Trino and one from Spark, verifies the final contents, confirms no `_change_data` objects were written, and drops the table.
- Audit status: `verified`

##### `testTrinoCanReadCdfEntriesGeneratedByDelta`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `testTrinoCanReadCdfEntriesGeneratedByDelta`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `TestDeltaLakeChangeDataFeedCompatibility.testTrinoCanReadCdfEntriesGeneratedByDelta`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `MERGE`, `UPDATE`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `ON`, `onTrino`, `TABLE`, `system.table_changes`, `table_changes`. Current action shape: `MERGE`, `UPDATE`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `ON`, `env.executeSpark`, `builder`, `add`, `build`, `env.executeTrino`, `TABLE`, `system.table_changes`, `table_changes`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, ON, INSERT, onTrino, TABLE, system.table_changes, table_changes, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, ON, INSERT, env.executeSpark, builder, add, build, env.executeTrino, TABLE, system.table_changes, table_changes]; SQL verbs differ: legacy [CREATE, INSERT, MERGE, DELETE, UPDATE, SET, SELECT] vs current [CREATE, INSERT, MERGE, DELETE, UPDATE, SET, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, ON, INSERT, onTrino, TABLE, system.table_changes, table_changes, dropDeltaTableWithRetry], verbs [CREATE, INSERT, MERGE, DELETE, UPDATE, SET, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, ON, INSERT, env.executeSpark, builder, add, build, env.executeTrino, TABLE, system.table_changes, table_changes], verbs [CREATE, INSERT, MERGE, DELETE, UPDATE, SET, SELECT, DROP].
- Audit status: `verified`

##### `testDeltaCanReadCdfEntriesGeneratedByTrino`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `testDeltaCanReadCdfEntriesGeneratedByTrino`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeChangeDataFeedCompatibility.java` ->
  `TestDeltaLakeChangeDataFeedCompatibility.testDeltaCanReadCdfEntriesGeneratedByTrino`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `MERGE`, `UPDATE`, `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `VALUES`, `ON`, `builder`, `add`, `build`, `TABLE`, `system.table_changes`, `onDelta`, `table_changes`. Current action shape: `MERGE`, `UPDATE`, `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `env.getBucketName`, `VALUES`, `ON`, `env.executeTrino`, `builder`, `add`, `build`, `TABLE`, `system.table_changes`, `env.executeSpark`, `table_changes`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, VALUES, ON, INSERT, builder, add, build, TABLE, system.table_changes, onDelta, table_changes, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, VALUES, ON, INSERT, env.executeTrino, builder, add, build, TABLE, system.table_changes, env.executeSpark, table_changes, env.executeSparkUpdate]; SQL verbs differ: legacy [CREATE, INSERT, MERGE, DELETE, UPDATE, SET, SELECT] vs current [CREATE, INSERT, MERGE, DELETE, UPDATE, SET, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, VALUES, ON, INSERT, builder, add, build, TABLE, system.table_changes, onDelta, table_changes, dropDeltaTableWithRetry], verbs [CREATE, INSERT, MERGE, DELETE, UPDATE, SET, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, VALUES, ON, INSERT, env.executeTrino, builder, add, build, TABLE, system.table_changes, env.executeSpark, table_changes, env.executeSparkUpdate], verbs [CREATE, INSERT, MERGE, DELETE, UPDATE, SET, SELECT, DROP].
- Audit status: `verified`

### `TestDeltaLakeCheckConstraintCompatibility`


- Owning migration commit: `Migrate TestDeltaLakeCheckConstraintCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckConstraintCompatibility.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckConstraintCompatibility.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `8`. Current methods: `8`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testCheckConstraintInsertCompatibility`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckConstraintCompatibility.java` ->
  `testCheckConstraintInsertCompatibility`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckConstraintCompatibility.java` ->
  `TestDeltaLakeCheckConstraintCompatibility.testCheckConstraintInsertCompatibility`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`, `INSERT`. Current setup shape: `CREATE`, `ALTER`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `CHECK`, `VALUES`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `CHECK`, `VALUES`, `env.executeTrino`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `assertThatThrownBy`, `hasMessageMatching`, `hasMessageContaining`. Current assertion helpers: `assertThat`, `containsOnly`, `assertSparkCheckConstraintViolation`, `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, CHECK, VALUES, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, CHECK, VALUES, env.executeTrino, env.executeTrinoUpdate]; SQL verbs differ: legacy [CREATE, ALTER, INSERT, SELECT] vs current [CREATE, ALTER, INSERT, SELECT, DROP]; assertion helpers differ: legacy [assertThat, containsOnly, assertThatThrownBy, hasMessageMatching, hasMessageContaining] vs current [assertThat, containsOnly, assertSparkCheckConstraintViolation, assertThatThrownBy, hasMessageContaining]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, CHECK, VALUES, onTrino, dropDeltaTableWithRetry], verbs [CREATE, ALTER, INSERT, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, CHECK, VALUES, env.executeTrino, env.executeTrinoUpdate], verbs [CREATE, ALTER, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testCheckConstraintUpdateCompatibility`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckConstraintCompatibility.java` ->
  `testCheckConstraintUpdateCompatibility`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckConstraintCompatibility.java` ->
  `TestDeltaLakeCheckConstraintCompatibility.testCheckConstraintUpdateCompatibility`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`, `INSERT`, `SET`. Current setup shape: `CREATE`, `ALTER`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `SELECT`, `UPDATE`, `randomNameSuffix`, `onDelta`, `executeQuery`, `CHECK`, `onTrino`. Current action shape: `SELECT`, `UPDATE`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `CHECK`, `env.executeTrinoUpdate`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `assertThatThrownBy`, `hasMessageMatching`, `hasMessageContaining`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `assertSparkCheckConstraintViolation`, `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, CHECK, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, CHECK, env.executeTrinoUpdate, env.executeTrino]; SQL verbs differ: legacy [CREATE, ALTER, INSERT, SELECT, UPDATE, SET] vs current [CREATE, ALTER, INSERT, SELECT, UPDATE, SET, DROP]; assertion helpers differ: legacy [assertThat, containsOnly, row, assertThatThrownBy, hasMessageMatching, hasMessageContaining] vs current [assertThat, containsOnly, row, assertSparkCheckConstraintViolation, assertThatThrownBy, hasMessageContaining]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, CHECK, onTrino, dropDeltaTableWithRetry], verbs [CREATE, ALTER, INSERT, SELECT, UPDATE, SET]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, CHECK, env.executeTrinoUpdate, env.executeTrino], verbs [CREATE, ALTER, INSERT, SELECT, UPDATE, SET, DROP].
- Audit status: `verified`

##### `testCheckConstraintMergeCompatibility`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckConstraintCompatibility.java` ->
  `testCheckConstraintMergeCompatibility`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckConstraintCompatibility.java` ->
  `TestDeltaLakeCheckConstraintCompatibility.testCheckConstraintMergeCompatibility`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`, `INSERT`, `SET`. Current setup shape: `CREATE`, `ALTER`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `SELECT`, `MERGE`, `UPDATE`, `randomNameSuffix`, `onDelta`, `executeQuery`, `CHECK`, `onTrino`, `VALUES`, `ON`. Current action shape: `SELECT`, `MERGE`, `UPDATE`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `CHECK`, `env.executeTrinoUpdate`, `VALUES`, `env.executeTrino`, `ON`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `assertThatThrownBy`, `hasMessageMatching`, `hasMessageContaining`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `assertSparkCheckConstraintViolation`, `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, CHECK, onTrino, VALUES, ON, INSERT, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, CHECK, env.executeTrinoUpdate, VALUES, env.executeTrino, ON, INSERT]; SQL verbs differ: legacy [CREATE, ALTER, INSERT, SELECT, MERGE, UPDATE, SET] vs current [CREATE, ALTER, INSERT, SELECT, MERGE, UPDATE, SET, DROP]; assertion helpers differ: legacy [assertThat, containsOnly, row, assertThatThrownBy, hasMessageMatching, hasMessageContaining] vs current [assertThat, containsOnly, row, assertSparkCheckConstraintViolation, assertThatThrownBy, hasMessageContaining]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, CHECK, onTrino, VALUES, ON, INSERT, dropDeltaTableWithRetry], verbs [CREATE, ALTER, INSERT, SELECT, MERGE, UPDATE, SET]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, CHECK, env.executeTrinoUpdate, VALUES, env.executeTrino, ON, INSERT], verbs [CREATE, ALTER, INSERT, SELECT, MERGE, UPDATE, SET, DROP].
- Audit status: `verified`

##### `testCheckConstraintWriterFeature`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckConstraintCompatibility.java` ->
  `testCheckConstraintWriterFeature`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckConstraintCompatibility.java` ->
  `TestDeltaLakeCheckConstraintCompatibility.testCheckConstraintWriterFeature`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`, `INSERT`. Current setup shape: `CREATE`, `ALTER`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `CHECK`, `onTrino`, `getTablePropertiesOnDelta`, `entry`, `doesNotContainKey`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `CHECK`, `env.executeTrinoUpdate`, `getTablePropertiesOnSpark`, `entry`, `doesNotContainKey`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageContaining`, `assertThat`, `contains`, `containsOnly`, `row`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `assertThat`, `contains`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, CHECK, onTrino, getTablePropertiesOnDelta, entry, doesNotContainKey, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, CHECK, env.executeTrinoUpdate, getTablePropertiesOnSpark, entry, doesNotContainKey, env.executeTrino]; SQL verbs differ: legacy [CREATE, ALTER, INSERT, SELECT] vs current [CREATE, ALTER, INSERT, SELECT, DROP]; assertion helpers differ: legacy [assertQueryFailure, hasMessageContaining, assertThat, contains, containsOnly, row] vs current [assertThatThrownBy, hasMessageContaining, assertThat, contains, containsOnly, row]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, CHECK, onTrino, getTablePropertiesOnDelta, entry, doesNotContainKey, dropDeltaTableWithRetry], verbs [CREATE, ALTER, INSERT, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, CHECK, env.executeTrinoUpdate, getTablePropertiesOnSpark, entry, doesNotContainKey, env.executeTrino], verbs [CREATE, ALTER, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testCheckConstraintUnknownCondition`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckConstraintCompatibility.java` ->
  `testCheckConstraintUnknownCondition`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckConstraintCompatibility.java` ->
  `TestDeltaLakeCheckConstraintCompatibility.testCheckConstraintUnknownCondition`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`, `INSERT`. Current setup shape: `CREATE`, `ALTER`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `CHECK`, `VALUES`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `CHECK`, `VALUES`, `env.executeTrinoUpdate`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThatThrownBy`, `hasMessageMatching`, `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertSparkCheckConstraintViolation`, `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, CHECK, VALUES, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, CHECK, VALUES, env.executeTrinoUpdate, env.executeTrino]; SQL verbs differ: legacy [CREATE, ALTER, INSERT, SELECT] vs current [CREATE, ALTER, INSERT, SELECT, DROP]; assertion helpers differ: legacy [assertThatThrownBy, hasMessageMatching, assertThat, containsOnly, row] vs current [assertSparkCheckConstraintViolation, assertThat, containsOnly, row]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, CHECK, VALUES, onTrino, dropDeltaTableWithRetry], verbs [CREATE, ALTER, INSERT, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, CHECK, VALUES, env.executeTrinoUpdate, env.executeTrino], verbs [CREATE, ALTER, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testCheckConstraintAcrossColumns`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckConstraintCompatibility.java` ->
  `testCheckConstraintAcrossColumns`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckConstraintCompatibility.java` ->
  `TestDeltaLakeCheckConstraintCompatibility.testCheckConstraintAcrossColumns`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`, `INSERT`. Current setup shape: `CREATE`, `ALTER`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `CHECK`, `onTrino`, `VALUES`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `CHECK`, `env.executeTrinoUpdate`, `VALUES`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `assertThatThrownBy`, `hasMessageMatching`, `hasMessageContaining`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `assertSparkCheckConstraintViolation`, `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, CHECK, onTrino, VALUES, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, CHECK, env.executeTrinoUpdate, VALUES, env.executeTrino]; SQL verbs differ: legacy [CREATE, ALTER, INSERT, SELECT] vs current [CREATE, ALTER, INSERT, SELECT, DROP]; assertion helpers differ: legacy [assertThat, containsOnly, row, assertThatThrownBy, hasMessageMatching, hasMessageContaining] vs current [assertThat, containsOnly, row, assertSparkCheckConstraintViolation, assertThatThrownBy, hasMessageContaining]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, CHECK, onTrino, VALUES, dropDeltaTableWithRetry], verbs [CREATE, ALTER, INSERT, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, CHECK, env.executeTrinoUpdate, VALUES, env.executeTrino], verbs [CREATE, ALTER, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testMetadataOperationsRetainCheckConstraint`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckConstraintCompatibility.java` ->
  `testMetadataOperationsRetainCheckConstraint`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckConstraintCompatibility.java` ->
  `TestDeltaLakeCheckConstraintCompatibility.testMetadataOperationsRetainCheckConstraint`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`, `COMMENT`. Current setup shape: `CREATE`, `ALTER`, `COMMENT`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onDelta`, `executeQuery`, `CHECK`, `onTrino`. Current action shape: `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `CHECK`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, CHECK, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, CHECK, env.executeTrinoUpdate]; SQL verbs differ: legacy [CREATE, ALTER, COMMENT] vs current [CREATE, ALTER, COMMENT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, CHECK, onTrino, dropDeltaTableWithRetry], verbs [CREATE, ALTER, COMMENT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, CHECK, env.executeTrinoUpdate], verbs [CREATE, ALTER, COMMENT, DROP].
- Audit status: `verified`

##### `testUnsupportedCheckConstraintExpression`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckConstraintCompatibility.java` ->
  `testUnsupportedCheckConstraintExpression`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckConstraintCompatibility.java` ->
  `TestDeltaLakeCheckConstraintCompatibility.testUnsupportedCheckConstraintExpression`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`, `INSERT`, `SET`, `COMMENT`. Current setup shape: `CREATE`, `ALTER`, `INSERT`, `SET`, `COMMENT`.
- Action parity: Legacy action shape: `UPDATE`, `MERGE`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `CHECK`, `abs`, `VALUES`, `onTrino`, `ON`. Current action shape: `UPDATE`, `MERGE`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `CHECK`, `abs`, `VALUES`, `env.executeTrinoUpdate`, `ON`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageContaining`, `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, CHECK, abs, VALUES, onTrino, ON, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, CHECK, abs, VALUES, env.executeTrinoUpdate, ON, env.executeTrino]; SQL verbs differ: legacy [CREATE, ALTER, INSERT, UPDATE, SET, DELETE, MERGE, COMMENT, SELECT] vs current [CREATE, ALTER, INSERT, UPDATE, SET, DELETE, MERGE, COMMENT, SELECT, DROP]; assertion helpers differ: legacy [assertQueryFailure, hasMessageContaining, assertThat, containsOnly, row] vs current [assertThatThrownBy, hasMessageContaining, assertThat, containsOnly, row]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, CHECK, abs, VALUES, onTrino, ON, dropDeltaTableWithRetry], verbs [CREATE, ALTER, INSERT, UPDATE, SET, DELETE, MERGE, COMMENT, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, CHECK, abs, VALUES, env.executeTrinoUpdate, ON, env.executeTrino], verbs [CREATE, ALTER, INSERT, UPDATE, SET, DELETE, MERGE, COMMENT, SELECT, DROP].
- Audit status: `verified`

### `TestDeltaLakeCheckpointsCompatibility`


- Owning migration commit: `Migrate TestDeltaLakeCheckpointsCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `18`. Current methods: `11`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly. Cross-class or merged legacy
  coverage accounted for: `testDatabricksCheckpointMinMaxStatisticsForRowType` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsDatabricksCompatibility.java` ->
  `testDatabricksCheckpointMinMaxStatisticsForRowType`; `testDatabricksCheckpointNullStatisticsForRowType` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsDatabricksCompatibility.java` ->
  `testDatabricksCheckpointNullStatisticsForRowType`; `testDatabricksUsesCheckpointInterval` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsDatabricksCompatibility.java` ->
  `testDatabricksUsesCheckpointInterval`; `testDatabricksWriteStatsAsJsonEnabled` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsDatabricksCompatibility.java` ->
  `testDatabricksWriteStatsAsJsonEnabled`; `testDatabricksWriteStatsAsStructEnabled` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsDatabricksCompatibility.java` ->
  `testDatabricksWriteStatsAsStructEnabled`; `testTrinoCheckpointMinMaxStatisticsForRowType` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsDatabricksCompatibility.java` ->
  `testTrinoCheckpointMinMaxStatisticsForRowType`; `testTrinoCheckpointNullStatisticsForRowType` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsDatabricksCompatibility.java` ->
  `testTrinoCheckpointNullStatisticsForRowType`.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testSparkCanReadTrinoCheckpoint`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `testSparkCanReadTrinoCheckpoint`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `TestDeltaLakeCheckpointsCompatibility.testSparkCanReadTrinoCheckpoint`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `format`, `BY`, `VALUES`, `onTrino`, `ImmutableList.of`, `listCheckpointFiles`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `format`, `BY`, `env.getBucketName`, `VALUES`, `env.executeTrinoUpdate`, `ImmutableList.of`, `listCheckpointFiles`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `isEmpty`, `containsOnly`, `hasSize`. Current assertion helpers: `row`, `assertThat`, `isEmpty`, `containsOnly`, `hasSize`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, format, BY, VALUES, onTrino, ImmutableList.of, listCheckpointFiles, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, format, BY, env.getBucketName, VALUES, env.executeTrinoUpdate, ImmutableList.of, listCheckpointFiles, env.executeSpark, env.executeTrino]; SQL verbs differ: legacy [CREATE, INSERT, DELETE, SELECT] vs current [CREATE, INSERT, DELETE, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, format, BY, VALUES, onTrino, ImmutableList.of, listCheckpointFiles, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, format, BY, env.getBucketName, VALUES, env.executeTrinoUpdate, ImmutableList.of, listCheckpointFiles, env.executeSpark, env.executeTrino], verbs [CREATE, INSERT, DELETE, SELECT, DROP].
- Audit status: `verified`

##### `testSparkCanReadTrinoCheckpointWithMultiplePartitionColumns`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `testSparkCanReadTrinoCheckpointWithMultiplePartitionColumns`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `TestDeltaLakeCheckpointsCompatibility.testSparkCanReadTrinoCheckpointWithMultiplePartitionColumns`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `CALL`, `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `s`, `DECIMAL`, `TIMESTAMP`, `WITH`, `CAST`, `igDecimal`, `Date.valueOf`, `ImmutableList.of`, `onDelta`, `system.flush_metadata_cache`, `date_format`, `format_datetime`, `selectTrinoTimestamps.rows`, `stream`, `map`, `collect`, `toImmutableList`. Current action shape: `SELECT`, `CALL`, `randomNameSuffix`, `env.executeTrinoUpdate`, `format`, `s`, `DECIMAL`, `TIMESTAMP`, `WITH`, `env.getBucketName`, `CAST`, `igDecimal`, `Date.valueOf`, `ImmutableList.of`, `env.executeSpark`, `system.flush_metadata_cache`, `env.executeTrino`, `date_format`, `format_datetime`, `selectTrinoTimestamps.getRows`, `stream`, `map`, `Row.fromList`, `row.getValues`, `collect`, `toImmutableList`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, format, s, DECIMAL, TIMESTAMP, WITH, CAST, igDecimal, Date.valueOf, ImmutableList.of, onDelta, system.flush_metadata_cache, date_format, format_datetime, selectTrinoTimestamps.rows, stream, map, collect, toImmutableList, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeTrinoUpdate, format, s, DECIMAL, TIMESTAMP, WITH, env.getBucketName, CAST, igDecimal, Date.valueOf, ImmutableList.of, env.executeSpark, system.flush_metadata_cache, env.executeTrino, date_format, format_datetime, selectTrinoTimestamps.getRows, stream, map, Row.fromList, row.getValues, collect, toImmutableList, env.executeSparkUpdate]; SQL verbs differ: legacy [CREATE, INSERT, SELECT, CALL] vs current [CREATE, INSERT, SELECT, CALL, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, format, s, DECIMAL, TIMESTAMP, WITH, CAST, igDecimal, Date.valueOf, ImmutableList.of, onDelta, system.flush_metadata_cache, date_format, format_datetime, selectTrinoTimestamps.rows, stream, map, collect, toImmutableList, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT, CALL]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, format, s, DECIMAL, TIMESTAMP, WITH, env.getBucketName, CAST, igDecimal, Date.valueOf, ImmutableList.of, env.executeSpark, system.flush_metadata_cache, env.executeTrino, date_format, format_datetime, selectTrinoTimestamps.getRows, stream, map, Row.fromList, row.getValues, collect, toImmutableList, env.executeSparkUpdate], verbs [CREATE, INSERT, SELECT, CALL, DROP].
- Audit status: `verified`

##### `testTrinoUsesCheckpointInterval`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `testTrinoUsesCheckpointInterval`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `TestDeltaLakeCheckpointsCompatibility.testTrinoUsesCheckpointInterval`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SHOW`, `INSERT`, `DELETE`, `ALTER`, `SELECT`, `trinoUsesCheckpointInterval`, `fillWithInserts`, `listCheckpointFiles`, `onDelta`, `onTrino`. Current action shape: `SHOW`, `INSERT`, `DELETE`, `ALTER`, `SELECT`, `trinoUsesCheckpointInterval`, `fillWithInserts`, `listCheckpointFiles`, `env.executeSparkUpdate`, `env.executeTrinoUpdate`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `isEmpty`, `hasSize`, `hasNoRows`. Current assertion helpers: `assertThat`, `contains`, `isEmpty`, `hasSize`, `hasNoRows`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the helper is semantically unchanged; it still creates a Spark table with checkpoint interval 5, verifies Trino sees that interval in `SHOW CREATE TABLE`, fills to the first checkpoint with Trino inserts, mixes more Trino and Spark writes plus a delete, changes the interval to 2, and verifies two more checkpoints appear. The JUnit port swaps the executor wiring to `env.execute*`.
- Known intentional difference: None.
- Reviewer note: This method exercises the shared checkpoint-interval helper with plain `delta.checkpointInterval = 5`. The helper creates the table, validates the visible property, drives checkpoint creation through mixed-engine writes, and drops the table.
- Audit status: `verified`

##### `testTrinoUsesCheckpointIntervalWithTableFeature`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `testTrinoUsesCheckpointIntervalWithTableFeature`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `TestDeltaLakeCheckpointsCompatibility.testTrinoUsesCheckpointIntervalWithTableFeature`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SHOW`, `INSERT`, `DELETE`, `ALTER`, `SELECT`, `trinoUsesCheckpointInterval`, `fillWithInserts`, `listCheckpointFiles`, `onDelta`, `onTrino`. Current action shape: `SHOW`, `INSERT`, `DELETE`, `ALTER`, `SELECT`, `trinoUsesCheckpointInterval`, `fillWithInserts`, `listCheckpointFiles`, `env.executeSparkUpdate`, `env.executeTrinoUpdate`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `isEmpty`, `hasSize`, `hasNoRows`. Current assertion helpers: `assertThat`, `contains`, `isEmpty`, `hasSize`, `hasNoRows`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the helper is semantically unchanged; it still creates a Spark table with checkpoint interval 5, verifies Trino sees that interval in `SHOW CREATE TABLE`, fills to the first checkpoint with Trino inserts, mixes more Trino and Spark writes plus a delete, changes the interval to 2, and verifies two more checkpoints appear. The only method-specific variation is the extra `delta.feature.columnMapping = supported` table property.
- Known intentional difference: None.
- Reviewer note: This method exercises the shared checkpoint-interval helper with both `delta.checkpointInterval = 5` and the column-mapping table feature enabled. The helper creates the table, validates the visible property, drives checkpoint creation through mixed-engine writes, and drops the table.
- Audit status: `verified`

##### `testTrinoWriteStatsAsJsonDisabled`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `testTrinoWriteStatsAsJsonDisabled`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `TestDeltaLakeCheckpointsCompatibility.testTrinoWriteStatsAsJsonDisabled`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `INSERT`, `SHOW`, `randomNameSuffix`, `testWriteStatsAsJsonDisabled`, `onDelta`, `onTrino`. Current action shape: `INSERT`, `SHOW`, `randomNameSuffix`, `testWriteStatsAsJsonDisabled`, `env.executeSparkUpdate`, `env.executeTrinoUpdate`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the shared helper is semantically unchanged; it still creates a Spark table with `delta.checkpoint.writeStatsAsJson = false`, performs one insert through the selected executor, and verifies Trino `SHOW STATS` exposes partition-level counts but no JSON-backed min/max data. The JUnit port swaps the executor to `env.executeTrinoUpdate`.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: This variant drives the shared helper through Trino writes. The helper creates the table with JSON checkpoint stats disabled, inserts one row, verifies the reduced `SHOW STATS` output from Trino, and drops the table.
- Audit status: `verified`

##### `testSparkWriteStatsAsJsonDisabled`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `testSparkWriteStatsAsJsonDisabled`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `TestDeltaLakeCheckpointsCompatibility.testSparkWriteStatsAsJsonDisabled`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `INSERT`, `SHOW`, `randomNameSuffix`, `testWriteStatsAsJsonDisabled`, `onDelta`, `onTrino`. Current action shape: `INSERT`, `SHOW`, `randomNameSuffix`, `testWriteStatsAsJsonDisabled`, `env.executeSparkUpdate`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the shared helper is semantically unchanged; it still creates a Spark table with `delta.checkpoint.writeStatsAsJson = false`, performs one insert through the selected executor, and verifies Trino `SHOW STATS` exposes partition-level counts but no JSON-backed min/max data. The JUnit port swaps the executor to `env.executeSparkUpdate`.
- Known intentional difference: None.
- Reviewer note: This variant drives the shared helper through Spark writes. The helper creates the table with JSON checkpoint stats disabled, inserts one row, verifies the reduced `SHOW STATS` output from Trino, and drops the table.
- Audit status: `verified`

##### `testTrinoWriteStatsAsStructDisabled`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `testTrinoWriteStatsAsStructDisabled`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `TestDeltaLakeCheckpointsCompatibility.testTrinoWriteStatsAsStructDisabled`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `INSERT`, `SHOW`, `randomNameSuffix`, `testWriteStatsAsStructDisabled`, `onDelta`, `onTrino`. Current action shape: `INSERT`, `SHOW`, `randomNameSuffix`, `testWriteStatsAsStructDisabled`, `env.executeSparkUpdate`, `env.executeTrinoUpdate`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the shared helper is semantically unchanged; it still creates a Spark table with both JSON and struct checkpoint stats disabled, performs one insert through the selected executor, and verifies Trino `SHOW STATS` returns only null statistics. The JUnit port swaps the executor to `env.executeTrinoUpdate`.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: This variant drives the shared helper through Trino writes. The helper creates the table with both checkpoint stat encodings disabled, inserts one row, verifies the fully null `SHOW STATS` output from Trino, and drops the table.
- Audit status: `verified`

##### `testSparkWriteStatsAsStructDisabled`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `testSparkWriteStatsAsStructDisabled`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `TestDeltaLakeCheckpointsCompatibility.testSparkWriteStatsAsStructDisabled`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `INSERT`, `SHOW`, `randomNameSuffix`, `testWriteStatsAsStructDisabled`, `onDelta`, `onTrino`. Current action shape: `INSERT`, `SHOW`, `randomNameSuffix`, `testWriteStatsAsStructDisabled`, `env.executeSparkUpdate`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the shared helper is semantically unchanged; it still creates a Spark table with both JSON and struct checkpoint stats disabled, performs one insert through the selected executor, and verifies Trino `SHOW STATS` returns only null statistics. The JUnit port swaps the executor to `env.executeSparkUpdate`.
- Known intentional difference: None.
- Reviewer note: This variant drives the shared helper through Spark writes. The helper creates the table with both checkpoint stat encodings disabled, inserts one row, verifies the fully null `SHOW STATS` output from Trino, and drops the table.
- Audit status: `verified`

##### `testTrinoWriteStatsAsJsonEnabled`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `testTrinoWriteStatsAsJsonEnabled`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `TestDeltaLakeCheckpointsCompatibility.testTrinoWriteStatsAsJsonEnabled`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `INSERT`, `ALTER`, `SHOW`, `randomNameSuffix`, `testWriteStatsAsJsonEnabled`, `onDelta`, `onTrino`. Current action shape: `INSERT`, `ALTER`, `SHOW`, `randomNameSuffix`, `testWriteStatsAsJsonEnabled`, `env.executeSparkUpdate`, `env.executeTrinoUpdate`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the shared helper is semantically unchanged; it still creates a Spark table with struct stats enabled, writes two rows through the chosen executor, flips the checkpoint properties to JSON mode, writes a third row, and verifies Trino `SHOW STATS` exposes the expected JSON-backed statistics. The JUnit port swaps the executor to `env.executeTrinoUpdate`.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: This variant drives the shared helper through Trino writes. The helper creates the table, toggles checkpoint stats from struct to JSON after two inserts, performs the final insert, verifies the Trino stats output for the selected type, and drops the table.
- Audit status: `verified`

##### `testTrinoWriteStatsAsStructEnabled`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `testTrinoWriteStatsAsStructEnabled`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `TestDeltaLakeCheckpointsCompatibility.testTrinoWriteStatsAsStructEnabled`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `INSERT`, `SHOW`, `randomNameSuffix`, `testWriteStatsAsStructEnabled`, `onDelta`, `onTrino`. Current action shape: `INSERT`, `SHOW`, `randomNameSuffix`, `testWriteStatsAsStructEnabled`, `env.executeSparkUpdate`, `env.executeTrinoUpdate`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the shared helper is semantically unchanged; it still creates a Spark table with struct stats enabled, performs one insert through the chosen executor, and verifies Trino `SHOW STATS` exposes the expected struct-backed statistics. The JUnit port swaps the executor to `env.executeTrinoUpdate`.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: This variant drives the shared helper through Trino writes. The helper creates the table with struct stats enabled, inserts one row, verifies the Trino stats output includes the expected counts, and drops the table.
- Audit status: `verified`

##### `testV2CheckpointMultipleSidecars`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `testV2CheckpointMultipleSidecars`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `TestDeltaLakeCheckpointsCompatibility.testV2CheckpointMultipleSidecars`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `SET`, `CREATE`. Current setup shape: `SET`, `CREATE`.
- Action parity: Legacy action shape: `INSERT`, `SELECT`, `testV2CheckpointMultipleSidecars`, `randomNameSuffix`, `onDelta`, `onTrino`, `listSidecarFiles`. Current action shape: `INSERT`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `env.executeSpark`, `env.executeTrino`, `listSidecarFiles`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `hasSize`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `isNotEmpty`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the current JUnit method inlines the old helper and keeps the same V2 checkpoint setup: it sets the top-level format and part size, creates a partitioned table with `delta.checkpointPolicy = v2`, inserts two rows, verifies both engines read them, and confirms sidecar files exist. The current assertion only checks non-emptiness of the sidecar directory, whereas the legacy helper asserted exactly two sidecar files.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: The legacy wrapper delegated to a helper that ran both `json` and `parquet` top-level formats. The current parameterized method performs that same setup inline for each format, verifies Spark and Trino read the inserted rows, checks for V2 sidecar files, and drops the table.
- Audit status: `verified`

### `TestDeltaLakeCheckpointsDatabricksCompatibility`


- Owning migration commit: `Migrate TestDeltaLakeCheckpointsCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsDatabricksCompatibility.java`
- Legacy class removed in same migration commit:
    - None. This commit added new JUnit-side verification coverage rather than removing a legacy class.
- Class-level environment requirement: `DeltaLakeDatabricksEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`.
- Method inventory complete: Not applicable. No legacy class or resource source exists for this new verification
  coverage.
- Legacy helper/resource dependencies accounted for: New JUnit-side verification coverage without a removed legacy
  counterpart.
- Intentional differences summary: `HDP to Hive 3.1 migration`, `JUnit/AssertJ/Testcontainers framework replacement`
- Method statuses present: `intentional difference`.

#### Methods

##### `testDatabricksUsesCheckpointInterval`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `testDatabricksUsesCheckpointInterval`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsDatabricksCompatibility.java` ->
  `TestDeltaLakeCheckpointsDatabricksCompatibility.testDatabricksUsesCheckpointInterval`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133`, `SuiteDeltaLakeDatabricks143`, `SuiteDeltaLakeDatabricks154`, `SuiteDeltaLakeDatabricks164`, and `SuiteDeltaLakeDatabricks173`; the runtime-specific tags on this method narrow additional assertions to the 143/154/164 suites.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `DeltaLakeDatabricks143`,
  `DeltaLakeDatabricks154`, `DeltaLakeDatabricks164`, `ProfileSpecificTests`. Tag routing matches the shared Databricks suites in 133/173 plus the runtime-specific 143/154/164 suites.
- Setup parity: No legacy counterpart. Current setup shape: `CREATE`, `INSERT`.
- Action parity: No legacy counterpart. Current action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `env.executeTrinoSql`, `format`, `s`, `WITH`, `env.getBucketName`, `env.executeDatabricksSql`, `getOnlyValue`, `VALUES`, `listCheckpointFiles`, `hasNoRows`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `contains`, `isEmpty`, `hasSize`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `HDP to Hive 3.1 migration`, `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only flow summary -> helpers [randomNameSuffix, env.executeTrinoSql, format, s, WITH, env.getBucketName, env.executeDatabricksSql, getOnlyValue, VALUES, listCheckpointFiles, hasNoRows, dropDeltaTableWithRetry], verbs [CREATE, SHOW, INSERT, SELECT].
- Audit status: `intentional difference`

##### `testTrinoCheckpointMinMaxStatisticsForRowType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `testTrinoCheckpointMinMaxStatisticsForRowType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsDatabricksCompatibility.java` ->
  `TestDeltaLakeCheckpointsDatabricksCompatibility.testTrinoCheckpointMinMaxStatisticsForRowType`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks143`, `SuiteDeltaLakeDatabricks154`, and `SuiteDeltaLakeDatabricks164` for the runtime-specific tags carried by this method.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: No legacy counterpart. Current setup shape: `CREATE`, `INSERT`.
- Action parity: No legacy counterpart. Current action shape: `DELETE`, `EXPLAIN`, `SELECT`, `randomNameSuffix`, `testCheckpointMinMaxStatisticsForRowType`, `env.executeDatabricksSql`, `env.executeTrinoSql`, `listCheckpointFiles`, `assertTransactionLogVersion`, `assertLastEntryIsCheckpointed`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `hasSize`, `matches`, `containsOnly`, `row`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: `DROP`.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `HDP to Hive 3.1 migration`, `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only coverage. The shared helper creates a Databricks table with a nested row column and checkpoint interval 1, inserts four rows, deletes one row through the supplied Trino executor to force a checkpoint, verifies Databricks can answer `max(root.entry_one)` from a `LocalTableScan`, verifies both engines return the same min/max result, and drops the table.
- Audit status: `intentional difference`

##### `testDatabricksCheckpointMinMaxStatisticsForRowType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `testDatabricksCheckpointMinMaxStatisticsForRowType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsDatabricksCompatibility.java` ->
  `TestDeltaLakeCheckpointsDatabricksCompatibility.testDatabricksCheckpointMinMaxStatisticsForRowType`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: No legacy counterpart. Current setup shape: `CREATE`, `INSERT`.
- Action parity: No legacy counterpart. Current action shape: `DELETE`, `EXPLAIN`, `SELECT`, `randomNameSuffix`, `testCheckpointMinMaxStatisticsForRowType`, `env.executeDatabricksSql`, `env.executeTrinoSql`, `listCheckpointFiles`, `assertTransactionLogVersion`, `assertLastEntryIsCheckpointed`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `hasSize`, `matches`, `containsOnly`, `row`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: `DROP`.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `HDP to Hive 3.1 migration`, `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only coverage. The shared helper creates a Databricks table with a nested row column and checkpoint interval 1, inserts four rows, deletes one row through the supplied Databricks executor to force a checkpoint, verifies Databricks can answer `max(root.entry_one)` from a `LocalTableScan`, verifies both engines return the same min/max result, and drops the table.
- Audit status: `intentional difference`

##### `testTrinoCheckpointNullStatisticsForRowType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `testTrinoCheckpointNullStatisticsForRowType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsDatabricksCompatibility.java` ->
  `TestDeltaLakeCheckpointsDatabricksCompatibility.testTrinoCheckpointNullStatisticsForRowType`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: No legacy counterpart. Current setup shape: `CREATE`, `INSERT`.
- Action parity: No legacy counterpart. Current action shape: `DELETE`, `EXPLAIN`, `SELECT`, `randomNameSuffix`, `testCheckpointNullStatisticsForRowType`, `env.executeDatabricksSql`, `env.executeTrinoSql`, `listCheckpointFiles`, `assertTransactionLogVersion`, `assertLastEntryIsCheckpointed`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `hasSize`, `matches`, `containsOnly`, `row`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: `DROP`.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `HDP to Hive 3.1 migration`, `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only coverage. The shared helper creates a Databricks table with nullable nested row fields and checkpoint interval 1, inserts four rows including one all-null nested row, deletes one row through the supplied Trino executor to force a checkpoint, verifies Databricks can answer `count(root.entry_two)` from a `LocalTableScan`, verifies both engines return the same non-null count, and drops the table.
- Audit status: `intentional difference`

##### `testDatabricksCheckpointNullStatisticsForRowType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `testDatabricksCheckpointNullStatisticsForRowType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsDatabricksCompatibility.java` ->
  `TestDeltaLakeCheckpointsDatabricksCompatibility.testDatabricksCheckpointNullStatisticsForRowType`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: No legacy counterpart. Current setup shape: `CREATE`, `INSERT`.
- Action parity: No legacy counterpart. Current action shape: `DELETE`, `EXPLAIN`, `SELECT`, `randomNameSuffix`, `testCheckpointNullStatisticsForRowType`, `env.executeDatabricksSql`, `env.executeTrinoSql`, `listCheckpointFiles`, `assertTransactionLogVersion`, `assertLastEntryIsCheckpointed`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `hasSize`, `matches`, `containsOnly`, `row`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: `DROP`.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `HDP to Hive 3.1 migration`, `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only coverage. The shared helper creates a Databricks table with nullable nested row fields and checkpoint interval 1, inserts four rows including one all-null nested row, deletes one row through the supplied Databricks executor to force a checkpoint, verifies Databricks can answer `count(root.entry_two)` from a `LocalTableScan`, verifies both engines return the same non-null count, and drops the table.
- Audit status: `intentional difference`

##### `testDatabricksWriteStatsAsJsonEnabled`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `testDatabricksWriteStatsAsJsonEnabled`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsDatabricksCompatibility.java` ->
  `TestDeltaLakeCheckpointsDatabricksCompatibility.testDatabricksWriteStatsAsJsonEnabled`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: No legacy counterpart. Current setup shape: `CREATE`.
- Action parity: No legacy counterpart. Current action shape: `INSERT`, `ALTER`, `SHOW`, `randomNameSuffix`, `testWriteStatsAsJsonEnabled`, `env.executeDatabricksSql`, `env.executeTrinoSql`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: `DROP`.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `HDP to Hive 3.1 migration`, `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only coverage. The shared helper creates a Databricks table with struct checkpoint stats enabled, writes two rows through Databricks, flips the checkpoint properties to JSON mode, writes a third row through Databricks, verifies Trino `SHOW STATS` exposes the expected JSON-backed values, and drops the table.
- Audit status: `intentional difference`

##### `testDatabricksWriteStatsAsStructEnabled`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsCompatibility.java` ->
  `testDatabricksWriteStatsAsStructEnabled`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCheckpointsDatabricksCompatibility.java` ->
  `TestDeltaLakeCheckpointsDatabricksCompatibility.testDatabricksWriteStatsAsStructEnabled`
- Mapping type: `new coverage helper`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: No legacy counterpart. Current setup shape: `CREATE`.
- Action parity: No legacy counterpart. Current action shape: `INSERT`, `SHOW`, `randomNameSuffix`, `testWriteStatsAsStructEnabled`, `env.executeDatabricksSql`, `env.executeTrinoSql`.
- Assertion parity: No legacy counterpart. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: No legacy counterpart. Current cleanup shape: `DROP`.
- Any observed difference, however small: Current-only verification coverage; no legacy parity comparison applies.
- Known intentional difference: `HDP to Hive 3.1 migration`, `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Current-only coverage. The shared helper creates a Databricks table with struct checkpoint stats enabled, writes one row through Databricks, verifies Trino `SHOW STATS` exposes the expected struct-backed counts, and drops the table.
- Audit status: `intentional difference`

### `TestDeltaLakeCloneTableCompatibility`


- Owning migration commit: `Migrate TestDeltaLakeCloneTableCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `7`. Current methods: `9`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testTableChangesOnShallowCloneTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java` ->
  `testTableChangesOnShallowCloneTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java` ->
  `TestDeltaLakeCloneTableCompatibility.testTableChangesOnShallowCloneTable`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `VALUES`, `TBLPROPERTIES`, `getFilesFromTableDirectory`, `ImmutableList.of`, `onTrino`, `TABLE`, `system.table_changes`, `table_changes`. Current action shape: `UPDATE`, `SELECT`, `env.getBucketName`, `randomNameSuffix`, `env.executeSparkUpdate`, `VALUES`, `TBLPROPERTIES`, `getFilesFromTableDirectory`, `List.of`, `env.executeTrino`, `TABLE`, `system.table_changes`, `env.executeSpark`, `table_changes`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEmpty`, `hasSize`, `row`, `containsOnly`. Current assertion helpers: `assertThat`, `isEmpty`, `hasSize`, `row`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, VALUES, TBLPROPERTIES, getFilesFromTableDirectory, ImmutableList.of, onTrino, TABLE, system.table_changes, table_changes] vs current [env.getBucketName, randomNameSuffix, env.executeSparkUpdate, VALUES, TBLPROPERTIES, getFilesFromTableDirectory, List.of, env.executeTrino, TABLE, system.table_changes, env.executeSpark, table_changes, env.executeTrinoUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, VALUES, TBLPROPERTIES, getFilesFromTableDirectory, ImmutableList.of, onTrino, TABLE, system.table_changes, table_changes], verbs [CREATE, INSERT, UPDATE, SET, SELECT, DROP]. Current flow summary -> helpers [env.getBucketName, randomNameSuffix, env.executeSparkUpdate, VALUES, TBLPROPERTIES, getFilesFromTableDirectory, List.of, env.executeTrino, TABLE, system.table_changes, env.executeSpark, table_changes, env.executeTrinoUpdate], verbs [CREATE, INSERT, UPDATE, SET, SELECT, DROP].
- Audit status: `verified`

##### `testShallowCloneTableDrop`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java` ->
  `testShallowCloneTableDrop`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java` ->
  `TestDeltaLakeCloneTableCompatibility.testShallowCloneTableDrop`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `VALUES`, `onTrino`. Current action shape: `SELECT`, `env.getBucketName`, `randomNameSuffix`, `env.executeSparkUpdate`, `VALUES`, `env.executeSpark`, `env.executeTrino`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, VALUES, onTrino] vs current [env.getBucketName, randomNameSuffix, env.executeSparkUpdate, VALUES, env.executeSpark, env.executeTrino, env.executeTrinoUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, VALUES, onTrino], verbs [DROP, CREATE, INSERT, SELECT]. Current flow summary -> helpers [env.getBucketName, randomNameSuffix, env.executeSparkUpdate, VALUES, env.executeSpark, env.executeTrino, env.executeTrinoUpdate], verbs [DROP, CREATE, INSERT, SELECT].
- Audit status: `verified`

##### `testVacuumOnShallowCloneTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java` ->
  `testVacuumOnShallowCloneTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java` ->
  `TestDeltaLakeCloneTableCompatibility.testVacuumOnShallowCloneTable`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `VACUUM`, `UPDATE`, `CALL`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `getActiveDataFiles`, `getFilesFromTableDirectory`, `table`, `getToBeVacuumedDataFilesFromDryRun`, `hasSameElementsAs`, `doesNotContainAnyElementsOf`, `onTrino`, `system.vacuum`, `containsExactlyInAnyOrderElementsOf`, `ImmutableList.of`, `rows`. Current action shape: `VACUUM`, `UPDATE`, `SELECT`, `env.getBucketName`, `randomNameSuffix`, `env.executeSparkUpdate`, `TBLPROPERTIES`, `VALUES`, `getActiveDataFiles`, `getFilesFromTableDirectory`, `table`, `getToBeVacuumedDataFilesFromDryRun`, `hasSameElementsAs`, `doesNotContainAnyElementsOf`, `executeTrinoVacuumWithZeroRetention`, `containsExactlyInAnyOrderElementsOf`, `List.of`, `env.executeSpark`, `env.executeTrino`, `rows`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `hasSize`, `isEqualTo`, `row`, `containsOnly`. Current assertion helpers: `assertThat`, `hasSize`, `isEqualTo`, `row`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, getActiveDataFiles, getFilesFromTableDirectory, table, getToBeVacuumedDataFilesFromDryRun, hasSameElementsAs, doesNotContainAnyElementsOf, onTrino, system.vacuum, containsExactlyInAnyOrderElementsOf, ImmutableList.of, rows] vs current [env.getBucketName, randomNameSuffix, env.executeSparkUpdate, TBLPROPERTIES, VALUES, getActiveDataFiles, getFilesFromTableDirectory, table, getToBeVacuumedDataFilesFromDryRun, hasSameElementsAs, doesNotContainAnyElementsOf, executeTrinoVacuumWithZeroRetention, containsExactlyInAnyOrderElementsOf, List.of, env.executeSpark, env.executeTrino, rows, env.executeTrinoUpdate]; SQL verbs differ: legacy [VACUUM, CREATE, INSERT, UPDATE, SET, CALL, SELECT, DROP] vs current [VACUUM, CREATE, INSERT, UPDATE, SET, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, getActiveDataFiles, getFilesFromTableDirectory, table, getToBeVacuumedDataFilesFromDryRun, hasSameElementsAs, doesNotContainAnyElementsOf, onTrino, system.vacuum, containsExactlyInAnyOrderElementsOf, ImmutableList.of, rows], verbs [VACUUM, CREATE, INSERT, UPDATE, SET, CALL, SELECT, DROP]. Current flow summary -> helpers [env.getBucketName, randomNameSuffix, env.executeSparkUpdate, TBLPROPERTIES, VALUES, getActiveDataFiles, getFilesFromTableDirectory, table, getToBeVacuumedDataFilesFromDryRun, hasSameElementsAs, doesNotContainAnyElementsOf, executeTrinoVacuumWithZeroRetention, containsExactlyInAnyOrderElementsOf, List.of, env.executeSpark, env.executeTrino, rows, env.executeTrinoUpdate], verbs [VACUUM, CREATE, INSERT, UPDATE, SET, SELECT, DROP].
- Audit status: `verified`

##### `testReadFromSchemaChangedShallowCloneTablePartitioned`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java` ->
  `testReadFromSchemaChangedShallowCloneTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java` ->
  `TestDeltaLakeCloneTableCompatibility.testReadFromSchemaChangedShallowCloneTablePartitioned`
- Mapping type: `renamed`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`, `SHALLOW CLONE`. Current setup shape: `CREATE`, `INSERT`, `ALTER`, `SHALLOW CLONE`.
- Action parity: Legacy action shape: `SELECT`, `testReadSchemaChangedCloneTable`, `randomNameSuffix`, `VERSION AS OF`, `onDelta`, `onTrino`. Current action shape: `SELECT`, `testReadSchemaChangedCloneTable`, `randomNameSuffix`, `VERSION AS OF`, `env.executeSparkUpdate`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the helper is semantically unchanged for the OSS shallow-clone case; it creates the base table, adds and later drops columns across versions, creates versioned shallow clones, and verifies Spark and Trino can read each clone and snapshot shape. The JUnit OSS split now exposes the partitioned and non-partitioned shallow-clone cases as separate methods.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: The shared helper creates a column-mapping table, inserts versioned rows across schema changes, creates shallow clones at versions 1 through 4, verifies Spark and Trino read the expected row shapes for each version, and drops the created tables.
- Audit status: `verified`

##### `testReadFromSchemaChangedShallowCloneTableNonPartitioned`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java` ->
  `testReadFromSchemaChangedDeepCloneTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java` ->
  `TestDeltaLakeCloneTableCompatibility.testReadFromSchemaChangedShallowCloneTableNonPartitioned`
- Mapping type: `renamed`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`, `SHALLOW CLONE`. Current setup shape: `CREATE`, `INSERT`, `ALTER`, `SHALLOW CLONE`.
- Action parity: Legacy action shape: `SELECT`, `testReadSchemaChangedCloneTable`, `randomNameSuffix`, `VERSION AS OF`, `onDelta`, `onTrino`. Current action shape: `SELECT`, `testReadSchemaChangedCloneTable`, `randomNameSuffix`, `VERSION AS OF`, `env.executeSparkUpdate`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the helper is semantically unchanged for the OSS shallow-clone case; it creates the base table, adds and later drops columns across versions, creates versioned shallow clones, and verifies Spark and Trino can read each clone and snapshot shape. In the current split the non-partitioned shallow-clone path is carried by this dedicated method, while the legacy deep-clone method remains separately represented in the Databricks-only class.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: The shared helper creates a column-mapping table, inserts versioned rows across schema changes, creates shallow clones at versions 1 through 4, verifies Spark and Trino read the expected row shapes for each version, and drops the created tables.
- Audit status: `verified`

##### `testShallowCloneTableMergeNonPartitioned`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java` ->
  `testShallowCloneTableMerge`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java` ->
  `TestDeltaLakeCloneTableCompatibility.testShallowCloneTableMergeNonPartitioned`
- Mapping type: `split`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SHALLOW`. Current setup shape: `CREATE`, `INSERT`, `SHALLOW`.
- Action parity: Legacy action shape: `SELECT`, `UPDATE`, `MERGE`, `DROP`, `randomNameSuffix`, `testShallowCloneTableMerge`, `onDelta`, `executeQuery`, `format`, `onTrino`. Current action shape: `SELECT`, `UPDATE`, `MERGE`, `DROP`, `randomNameSuffix`, `testShallowCloneTableMerge`, `env.executeSparkUpdate`, `format`, `env.getBucketName`, `env.executeTrino`, `env.executeTrinoUpdate`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, testShallowCloneTableMerge, onDelta, executeQuery, format, onTrino] vs current [randomNameSuffix, testShallowCloneTableMerge, env.executeSparkUpdate, format, env.getBucketName, env.executeTrino, env.executeTrinoUpdate, env.executeSpark]; the current split keeps the non-partitioned helper path explicit instead of multiplexing the legacy single method with an internal partitioned/non-partitioned loop.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, testShallowCloneTableMerge, onDelta, executeQuery, format, onTrino], verbs [CREATE, INSERT, SHALLOW, SELECT, UPDATE, MERGE, DROP]. Current flow summary -> helpers [randomNameSuffix, testShallowCloneTableMerge, env.executeSparkUpdate, format, env.getBucketName, env.executeTrino, env.executeTrinoUpdate, env.executeSpark], verbs [CREATE, INSERT, SHALLOW, SELECT, UPDATE, MERGE, DROP].
- Audit status: `verified`

##### `testShallowCloneTableMergePartitioned`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java` ->
  `testShallowCloneTableMerge`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java` ->
  `TestDeltaLakeCloneTableCompatibility.testShallowCloneTableMergePartitioned`
- Mapping type: `split`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SHALLOW`. Current setup shape: `CREATE`, `INSERT`, `SHALLOW`.
- Action parity: Legacy action shape: `SELECT`, `UPDATE`, `MERGE`, `DROP`, `randomNameSuffix`, `testShallowCloneTableMerge`, `onDelta`, `executeQuery`, `format`, `onTrino`. Current action shape: `SELECT`, `UPDATE`, `MERGE`, `DROP`, `randomNameSuffix`, `testShallowCloneTableMerge`, `env.executeSparkUpdate`, `format`, `env.getBucketName`, `env.executeTrino`, `env.executeTrinoUpdate`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, testShallowCloneTableMerge, onDelta, executeQuery, format, onTrino] vs current [randomNameSuffix, testShallowCloneTableMerge, env.executeSparkUpdate, format, env.getBucketName, env.executeTrino, env.executeTrinoUpdate, env.executeSpark]; the current split keeps the partitioned helper path explicit instead of multiplexing the legacy single method with an internal partitioned/non-partitioned loop.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, testShallowCloneTableMerge, onDelta, executeQuery, format, onTrino], verbs [CREATE, INSERT, SHALLOW, SELECT, UPDATE, MERGE, DROP]. Current flow summary -> helpers [randomNameSuffix, testShallowCloneTableMerge, env.executeSparkUpdate, format, env.getBucketName, env.executeTrino, env.executeTrinoUpdate, env.executeSpark], verbs [CREATE, INSERT, SHALLOW, SELECT, UPDATE, MERGE, DROP].
- Audit status: `verified`

##### `testReadShallowCloneTableWithSourceDeletionVectorPartitioned`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java` ->
  `testReadShallowCloneTableWithSourceDeletionVector`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java` ->
  `TestDeltaLakeCloneTableCompatibility.testReadShallowCloneTableWithSourceDeletionVectorPartitioned`
- Mapping type: `split`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `OPTIMIZE`, `DELETE`, `SHALLOW`. Current setup shape: `CREATE`, `INSERT`, `OPTIMIZE`, `DELETE`, `SHALLOW`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `testReadShallowCloneTableWithSourceDeletionVector`, `onDelta`, `executeQuery`, `onTrino`, `getDeletionVectorType`. Current action shape: `SELECT`, `randomNameSuffix`, `testReadShallowCloneTableWithSourceDeletionVector`, `env.executeSparkUpdate`, `env.getBucketName`, `env.executeSpark`, `env.executeTrino`, `getDeletionVectorType`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `isEqualTo`, `isNotEqualTo`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `isEqualTo`, `isNotEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, testReadShallowCloneTableWithSourceDeletionVector, onDelta, executeQuery, onTrino, getDeletionVectorType] vs current [randomNameSuffix, testReadShallowCloneTableWithSourceDeletionVector, env.executeSparkUpdate, env.getBucketName, env.executeSpark, env.executeTrino, getDeletionVectorType]; the current split keeps the partitioned helper path explicit instead of multiplexing the legacy single method with an internal partitioned/non-partitioned loop.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, testReadShallowCloneTableWithSourceDeletionVector, onDelta, executeQuery, onTrino, getDeletionVectorType], verbs [CREATE, INSERT, OPTIMIZE, DELETE, SHALLOW, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, testReadShallowCloneTableWithSourceDeletionVector, env.executeSparkUpdate, env.getBucketName, env.executeSpark, env.executeTrino, getDeletionVectorType], verbs [CREATE, INSERT, OPTIMIZE, DELETE, SHALLOW, SELECT, DROP].
- Audit status: `verified`

##### `testReadShallowCloneTableWithSourceDeletionVectorNonPartitioned`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java` ->
  `testReadShallowCloneTableWithSourceDeletionVector`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCloneTableCompatibility.java` ->
  `TestDeltaLakeCloneTableCompatibility.testReadShallowCloneTableWithSourceDeletionVectorNonPartitioned`
- Mapping type: `split`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `OPTIMIZE`, `DELETE`, `SHALLOW`. Current setup shape: `CREATE`, `INSERT`, `OPTIMIZE`, `DELETE`, `SHALLOW`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `testReadShallowCloneTableWithSourceDeletionVector`, `onDelta`, `executeQuery`, `onTrino`, `getDeletionVectorType`. Current action shape: `SELECT`, `randomNameSuffix`, `testReadShallowCloneTableWithSourceDeletionVector`, `env.executeSparkUpdate`, `env.getBucketName`, `env.executeSpark`, `env.executeTrino`, `getDeletionVectorType`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `isEqualTo`, `isNotEqualTo`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `isEqualTo`, `isNotEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, testReadShallowCloneTableWithSourceDeletionVector, onDelta, executeQuery, onTrino, getDeletionVectorType] vs current [randomNameSuffix, testReadShallowCloneTableWithSourceDeletionVector, env.executeSparkUpdate, env.getBucketName, env.executeSpark, env.executeTrino, getDeletionVectorType]; the current split keeps the non-partitioned helper path explicit instead of multiplexing the legacy single method with an internal partitioned/non-partitioned loop.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, testReadShallowCloneTableWithSourceDeletionVector, onDelta, executeQuery, onTrino, getDeletionVectorType], verbs [CREATE, INSERT, OPTIMIZE, DELETE, SHALLOW, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, testReadShallowCloneTableWithSourceDeletionVector, env.executeSparkUpdate, env.getBucketName, env.executeSpark, env.executeTrino, getDeletionVectorType], verbs [CREATE, INSERT, OPTIMIZE, DELETE, SHALLOW, SELECT, DROP].
- Audit status: `verified`

### `TestDeltaLakeColumnMappingMode`


- Owning migration commit: `Migrate TestDeltaLakeColumnMappingMode to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `31`. Current methods: `31`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testColumnMappingModeNone`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testColumnMappingModeNone`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testColumnMappingModeNone`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `struct`, `ImmutableList.of`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `struct`, `ImmutableList.of`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, struct, ImmutableList.of, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, struct, ImmutableList.of, env.executeSpark, env.executeTrino]; SQL verbs differ: legacy [CREATE, INSERT, SELECT] vs current [CREATE, INSERT, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, struct, ImmutableList.of, onTrino, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, struct, ImmutableList.of, env.executeSpark, env.executeTrino], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testColumnMappingModeTableFeature`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testColumnMappingModeTableFeature`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testColumnMappingModeTableFeature`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `ALTER`.
- Action parity: Legacy action shape: `SHOW`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `onTrino`, `getTablePropertiesOnDelta`, `entry`, `doesNotContainKey`, `getOnlyValue`, `doesNotContain`. Current action shape: `SHOW`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `env.executeTrinoUpdate`, `getTablePropertiesOnSpark`, `entry`, `doesNotContainKey`, `env.executeTrino`, `getOnlyValue`, `doesNotContain`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino, getTablePropertiesOnDelta, entry, doesNotContainKey, getOnlyValue, doesNotContain, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, getTablePropertiesOnSpark, entry, doesNotContainKey, env.executeTrino, getOnlyValue, doesNotContain]; SQL verbs differ: legacy [CREATE, INSERT, ALTER, SHOW] vs current [CREATE, INSERT, ALTER, SHOW, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino, getTablePropertiesOnDelta, entry, doesNotContainKey, getOnlyValue, doesNotContain, dropDeltaTableWithRetry], verbs [CREATE, INSERT, ALTER, SHOW]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, getTablePropertiesOnSpark, entry, doesNotContainKey, env.executeTrino, getOnlyValue, doesNotContain], verbs [CREATE, INSERT, ALTER, SHOW, DROP].
- Audit status: `verified`

##### `testTrinoColumnMappingModeReaderAndWriterVersion`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testTrinoColumnMappingModeReaderAndWriterVersion`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testTrinoColumnMappingModeReaderAndWriterVersion`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SHOW`, `testColumnMappingModeReaderAndWriterVersion`, `assertTableReaderAndWriterVersion`, `onTrino`. Current action shape: `SHOW`, `testColumnMappingModeReaderAndWriterVersion`, `assertTableReaderAndWriterVersion`, `env.executeTrinoUpdate`, `env.executeTrino`, `env.getBucketName`.
- Assertion parity: Legacy assertion helpers: `assertTableReaderAndWriterVersion`. Current assertion helpers: `assertTableReaderAndWriterVersion`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the shared helper is semantically unchanged; it still creates a Trino table in the selected column-mapping mode, verifies `delta.minReaderVersion = 2` and the expected writer version via table properties, and drops the table. The JUnit port routes the create/drop through `env.executeTrinoUpdate`.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: The Trino variant creates the table through Trino with the requested `column_mapping_mode`, checks the reader/writer versions recorded in table properties, and drops the table.
- Audit status: `verified`

##### `testChangingColumnMappingModeViaCreateOrReplaceTableOnTrino`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testChangingColumnMappingModeViaCreateOrReplaceTableOnTrino`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testChangingColumnMappingModeViaCreateOrReplaceTableOnTrino`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`. Current action shape: `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `env.getBucketName`.
- Assertion parity: Legacy assertion helpers: `assertTableReaderAndWriterVersion`. Current assertion helpers: `assertTableReaderAndWriterVersion`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH], verbs [CREATE, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName], verbs [CREATE, DROP].
- Audit status: `verified`

##### `testChangingColumnMappingModeViaCreateOrReplaceTableOnDelta`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testChangingColumnMappingModeViaCreateOrReplaceTableOnDelta`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testChangingColumnMappingModeViaCreateOrReplaceTableOnDelta`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `onTrino`. Current action shape: `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertTableReaderAndWriterVersion`. Current assertion helpers: `assertTableReaderAndWriterVersion`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino], verbs [CREATE, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate], verbs [CREATE, DROP].
- Audit status: `verified`

##### `testDeltaColumnMappingModeReaderAndWriterVersion`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testDeltaColumnMappingModeReaderAndWriterVersion`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testDeltaColumnMappingModeReaderAndWriterVersion`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SHOW`, `testColumnMappingModeReaderAndWriterVersion`, `assertTableReaderAndWriterVersion`, `onDelta`, `onTrino`. Current action shape: `SHOW`, `testColumnMappingModeReaderAndWriterVersion`, `assertTableReaderAndWriterVersion`, `env.executeSparkUpdate`, `env.executeTrino`, `env.getBucketName`.
- Assertion parity: Legacy assertion helpers: `assertTableReaderAndWriterVersion`. Current assertion helpers: `assertTableReaderAndWriterVersion`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the shared helper is semantically unchanged; it still creates a Spark table in the selected column-mapping mode, verifies `delta.minReaderVersion = 2` and the expected writer version via table properties, and drops the table. The JUnit port routes the create/drop through `env.executeSparkUpdate`.
- Known intentional difference: None.
- Reviewer note: The Spark variant creates the table through Spark with the requested `delta.columnMapping.mode`, checks the reader/writer versions recorded in table properties, and drops the table.
- Audit status: `verified`

##### `testTrinoColumnMappingMode`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testTrinoColumnMappingMode`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testTrinoColumnMappingMode`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `INSERT`, `ALTER`, `SELECT`, `DESCRIBE`, `testColumnMappingMode`, `onTrino`, `onDelta`. Current action shape: `INSERT`, `ALTER`, `SELECT`, `DESCRIBE`, `testColumnMappingMode`, `env.executeTrinoUpdate`, `env.executeSparkUpdate`, `env.executeSpark`, `env.executeTrino`, `env.getBucketName`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the shared helper is semantically unchanged; it still seeds nested-array and row data from both engines, verifies point lookups and predicate pushdown, renames top-level and nested columns from Spark, verifies `DESCRIBE` exposes the renamed schema, and checks both engines read the renamed columns. The JUnit port routes the DDL/DML through `env.execute*`.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: The Trino variant creates the initial column-mapping table through Trino, then the shared helper exercises cross-engine inserts, predicate reads, renames, schema introspection, and post-rename reads before dropping the table.
- Audit status: `verified`

##### `testDeltaColumnMappingMode`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testDeltaColumnMappingMode`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testDeltaColumnMappingMode`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `INSERT`, `ALTER`, `SELECT`, `DESCRIBE`, `testColumnMappingMode`, `onDelta`, `onTrino`. Current action shape: `INSERT`, `ALTER`, `SELECT`, `DESCRIBE`, `testColumnMappingMode`, `env.executeSparkUpdate`, `env.executeTrinoUpdate`, `env.executeSpark`, `env.executeTrino`, `env.getBucketName`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the shared helper is semantically unchanged; it still seeds nested-array and row data from both engines, verifies point lookups and predicate pushdown, renames top-level and nested columns from Spark, verifies `DESCRIBE` exposes the renamed schema, and checks both engines read the renamed columns. The JUnit port routes the DDL/DML through `env.execute*`.
- Known intentional difference: None.
- Reviewer note: The Spark variant creates the initial column-mapping table through Spark, then the shared helper exercises cross-engine inserts, predicate reads, renames, schema introspection, and post-rename reads before dropping the table.
- Audit status: `verified`

##### `testColumnMappingModeNameWithNonLowerCaseColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testColumnMappingModeNameWithNonLowerCaseColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testColumnMappingModeNameWithNonLowerCaseColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `COMMENT`. Current setup shape: `CREATE`, `INSERT`, `COMMENT`.
- Action parity: Legacy action shape: `SELECT`, `SHOW`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `ImmutableList.of`, `onTrino`, `getColumnCommentOnTrino`, `getColumnCommentOnDelta`. Current action shape: `SELECT`, `SHOW`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `ImmutableList.of`, `env.executeSpark`, `env.executeTrino`, `env.executeTrinoUpdate`, `getColumnCommentOnTrino`, `getColumnCommentOnSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`, `isEqualTo`. Current assertion helpers: `row`, `assertThat`, `containsOnly`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, ImmutableList.of, onTrino, getColumnCommentOnTrino, getColumnCommentOnDelta, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, ImmutableList.of, env.executeSpark, env.executeTrino, env.executeTrinoUpdate, getColumnCommentOnTrino, getColumnCommentOnSpark]; SQL verbs differ: legacy [CREATE, INSERT, SELECT, SHOW, COMMENT] vs current [CREATE, INSERT, SELECT, SHOW, COMMENT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, ImmutableList.of, onTrino, getColumnCommentOnTrino, getColumnCommentOnDelta, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT, SHOW, COMMENT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, ImmutableList.of, env.executeSpark, env.executeTrino, env.executeTrinoUpdate, getColumnCommentOnTrino, getColumnCommentOnSpark], verbs [CREATE, INSERT, SELECT, SHOW, COMMENT, DROP].
- Audit status: `verified`

##### `testColumnMappingModeNameCreatePartitionTableAsSelect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testColumnMappingModeNameCreatePartitionTableAsSelect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testColumnMappingModeNameCreatePartitionTableAsSelect`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `onDelta`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `env.getBucketName`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, onDelta] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, onDelta], verbs [CREATE, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, env.executeTrino, env.executeSpark], verbs [CREATE, SELECT, DROP].
- Audit status: `verified`

##### `testCreateTableWithCommentsColumnMappingModeName`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testCreateTableWithCommentsColumnMappingModeName`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testCreateTableWithCommentsColumnMappingModeName`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`. Current setup shape: `CREATE`, `COMMENT`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `getTableCommentOnTrino`, `getTableCommentOnDelta`, `getColumnCommentOnTrino`, `getColumnCommentOnDelta`. Current action shape: `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `env.getBucketName`, `getTableCommentOnTrino`, `getTableCommentOnSpark`, `getColumnCommentOnTrino`, `getColumnCommentOnSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, getTableCommentOnTrino, getTableCommentOnDelta, getColumnCommentOnTrino, getColumnCommentOnDelta] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, getTableCommentOnTrino, getTableCommentOnSpark, getColumnCommentOnTrino, getColumnCommentOnSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, getTableCommentOnTrino, getTableCommentOnDelta, getColumnCommentOnTrino, getColumnCommentOnDelta], verbs [CREATE, COMMENT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, getTableCommentOnTrino, getTableCommentOnSpark, getColumnCommentOnTrino, getColumnCommentOnSpark], verbs [CREATE, COMMENT, DROP].
- Audit status: `verified`

##### `testColumnMappingModeNameCommentOnTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testColumnMappingModeNameCommentOnTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testColumnMappingModeNameCommentOnTable`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`. Current setup shape: `CREATE`, `COMMENT`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `onTrino`, `getTableCommentOnTrino`, `getTableCommentOnDelta`. Current action shape: `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `env.executeTrinoUpdate`, `getTableCommentOnTrino`, `getTableCommentOnSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino, getTableCommentOnTrino, getTableCommentOnDelta, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, getTableCommentOnTrino, getTableCommentOnSpark]; SQL verbs differ: legacy [CREATE, COMMENT] vs current [CREATE, COMMENT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino, getTableCommentOnTrino, getTableCommentOnDelta, dropDeltaTableWithRetry], verbs [CREATE, COMMENT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, getTableCommentOnTrino, getTableCommentOnSpark], verbs [CREATE, COMMENT, DROP].
- Audit status: `verified`

##### `testColumnMappingModeNameCommentOnColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testColumnMappingModeNameCommentOnColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testColumnMappingModeNameCommentOnColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`, `ALTER`. Current setup shape: `CREATE`, `COMMENT`, `ALTER`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `getColumnCommentOnTrino`, `getColumnCommentOnDelta`, `onDelta`. Current action shape: `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `env.getBucketName`, `getColumnCommentOnTrino`, `getColumnCommentOnSpark`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, getColumnCommentOnTrino, getColumnCommentOnDelta, onDelta, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, getColumnCommentOnTrino, getColumnCommentOnSpark, env.executeSparkUpdate]; SQL verbs differ: legacy [CREATE, COMMENT, ALTER] vs current [CREATE, COMMENT, ALTER, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, getColumnCommentOnTrino, getColumnCommentOnDelta, onDelta, dropDeltaTableWithRetry], verbs [CREATE, COMMENT, ALTER]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, getColumnCommentOnTrino, getColumnCommentOnSpark, env.executeSparkUpdate], verbs [CREATE, COMMENT, ALTER, DROP].
- Audit status: `verified`

##### `testColumnMappingModeNameAddColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testColumnMappingModeNameAddColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testColumnMappingModeNameAddColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `ALTER`. Current setup shape: `ALTER`.
- Action parity: Legacy action shape: `CREATE`, `INSERT`, `ALTER`, `DESCRIBE`, `REPLACE TABLE`, `SELECT`, `testColumnMappingModeAddColumn`, `onTrino`, `onDelta`, `map`. Current action shape: `CREATE`, `INSERT`, `ALTER`, `DESCRIBE`, `REPLACE TABLE`, `SELECT`, `testColumnMappingModeAddColumn`, `env.executeTrinoUpdate`, `env.executeSparkUpdate`, `env.executeSpark`, `env.executeTrino`, `map`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `isEqualTo`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the shared helper is semantically unchanged; it still creates a one-column table, inserts two seed rows, adds four columns through mixed-engine ALTER statements, verifies `DESCRIBE` and cross-engine reads expose the new columns, checks `delta.columnMapping.maxColumnId`, replaces the table partitioned by the added column, and re-reads it. The JUnit port routes the DDL/DML through `env.execute*`.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: This Trino-led variant uses the shared helper to add one scalar, one array, one map, and one row column across engines, then verifies schema, values, and column-id bookkeeping before dropping the table.
- Audit status: `verified`

##### `testColumnMappingModeIdAddColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testColumnMappingModeIdAddColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testColumnMappingModeIdAddColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `ALTER`. Current setup shape: `ALTER`.
- Action parity: Legacy action shape: `CREATE`, `INSERT`, `ALTER`, `DESCRIBE`, `REPLACE TABLE`, `SELECT`, `testColumnMappingModeAddColumn`, `onDelta`, `onTrino`, `array`. Current action shape: `CREATE`, `INSERT`, `ALTER`, `DESCRIBE`, `REPLACE TABLE`, `SELECT`, `testColumnMappingModeAddColumn`, `env.executeSparkUpdate`, `env.executeTrinoUpdate`, `env.executeSpark`, `env.executeTrino`, `array`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `isEqualTo`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the shared helper is semantically unchanged; it still creates a one-column table, inserts two seed rows, adds four columns through mixed-engine ALTER statements, verifies `DESCRIBE` and cross-engine reads expose the new columns, checks `delta.columnMapping.maxColumnId`, replaces the table partitioned by the added column, and re-reads it. The JUnit port routes the DDL/DML through `env.execute*`.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: This Spark-led variant uses the shared helper to add one scalar, one array, one map, and one row column across engines, then verifies schema, values, and column-id bookkeeping before dropping the table.
- Audit status: `verified`

##### `testTrinoColumnMappingModeNameAddColumnWithExistingNonLowerCaseColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testTrinoColumnMappingModeNameAddColumnWithExistingNonLowerCaseColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testTrinoColumnMappingModeNameAddColumnWithExistingNonLowerCaseColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `onTrino`, `getColumnNamesOnDelta`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `env.executeTrinoUpdate`, `env.executeTrino`, `getColumnNamesOnSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `containsExactly`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `containsExactly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino, getColumnNamesOnDelta, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, env.executeTrino, getColumnNamesOnSpark]; SQL verbs differ: legacy [CREATE, INSERT, ALTER, SELECT] vs current [CREATE, INSERT, ALTER, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino, getColumnNamesOnDelta, dropDeltaTableWithRetry], verbs [CREATE, INSERT, ALTER, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, env.executeTrino, getColumnNamesOnSpark], verbs [CREATE, INSERT, ALTER, SELECT, DROP].
- Audit status: `verified`

##### `testShowStatsFromJsonForColumnMappingMode`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testShowStatsFromJsonForColumnMappingMode`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testShowStatsFromJsonForColumnMappingMode`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SHOW`, `ANALYZE`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `onTrino`, `ImmutableList.of`. Current action shape: `SHOW`, `ANALYZE`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `env.executeTrino`, `ImmutableList.of`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, ImmutableList.of, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrino, ImmutableList.of, env.executeTrinoUpdate]; SQL verbs differ: legacy [CREATE, INSERT, SHOW, ANALYZE] vs current [CREATE, INSERT, SHOW, ANALYZE, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, ImmutableList.of, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SHOW, ANALYZE]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrino, ImmutableList.of, env.executeTrinoUpdate], verbs [CREATE, INSERT, SHOW, ANALYZE, DROP].
- Audit status: `verified`

##### `testShowStatsFromParquetForColumnMappingModeName`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testShowStatsFromParquetForColumnMappingModeName`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testShowStatsFromParquetForColumnMappingModeName`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SHOW`, `ANALYZE`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `onTrino`, `ImmutableList.of`. Current action shape: `SHOW`, `ANALYZE`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `env.executeTrino`, `ImmutableList.of`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, ImmutableList.of, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrino, ImmutableList.of, env.executeTrinoUpdate]; SQL verbs differ: legacy [CREATE, INSERT, SHOW, ANALYZE] vs current [CREATE, INSERT, SHOW, ANALYZE, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, ImmutableList.of, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SHOW, ANALYZE]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrino, ImmutableList.of, env.executeTrinoUpdate], verbs [CREATE, INSERT, SHOW, ANALYZE, DROP].
- Audit status: `verified`

##### `testShowStatsOnPartitionedForColumnMappingModeId`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testShowStatsOnPartitionedForColumnMappingModeId`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testShowStatsOnPartitionedForColumnMappingModeId`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SHOW`, `randomNameSuffix`, `onDelta`, `executeQuery`, `BY`, `TBLPROPERTIES`, `VALUES`, `onTrino`, `ImmutableList.of`. Current action shape: `SHOW`, `randomNameSuffix`, `env.executeSparkUpdate`, `BY`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `env.executeTrino`, `ImmutableList.of`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, VALUES, onTrino, ImmutableList.of, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrino, ImmutableList.of]; SQL verbs differ: legacy [CREATE, INSERT, SHOW] vs current [CREATE, INSERT, SHOW, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, VALUES, onTrino, ImmutableList.of, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SHOW]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrino, ImmutableList.of], verbs [CREATE, INSERT, SHOW, DROP].
- Audit status: `verified`

##### `testProjectionPushdownDmlWithColumnMappingMode`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testProjectionPushdownDmlWithColumnMappingMode`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testProjectionPushdownDmlWithColumnMappingMode`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `MERGE`, `UPDATE`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `struct`, `ON`, `onTrino`. Current action shape: `MERGE`, `UPDATE`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `struct`, `ON`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, struct, ON, INSERT, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, struct, ON, INSERT, env.executeTrino]; SQL verbs differ: legacy [CREATE, INSERT, MERGE, DELETE, UPDATE, SET, SELECT] vs current [CREATE, INSERT, MERGE, DELETE, UPDATE, SET, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, struct, ON, INSERT, onTrino, dropDeltaTableWithRetry], verbs [CREATE, INSERT, MERGE, DELETE, UPDATE, SET, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, struct, ON, INSERT, env.executeTrino], verbs [CREATE, INSERT, MERGE, DELETE, UPDATE, SET, SELECT, DROP].
- Audit status: `verified`

##### `testChangeColumnMappingAndShowStatsForColumnMappingMode`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testChangeColumnMappingAndShowStatsForColumnMappingMode`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testChangeColumnMappingAndShowStatsForColumnMappingMode`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`. Current setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`.
- Action parity: Legacy action shape: `ANALYZE`, `SHOW`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `onTrino`. Current action shape: `ANALYZE`, `SHOW`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `env.executeTrinoUpdate`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrinoUpdate, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, dropDeltaTableWithRetry], verbs [CREATE, INSERT, ANALYZE, SHOW, ALTER, SET, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrinoUpdate, env.executeTrino], verbs [CREATE, INSERT, ANALYZE, SHOW, ALTER, SET, DROP].
- Audit status: `verified`

##### `testChangeColumnMappingMode`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testChangeColumnMappingMode`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testChangeColumnMappingMode`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`, `SET`. Current setup shape: `CREATE`, `ALTER`, `SET`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `formatted`. Current action shape: `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `hasStackTraceContaining`, `formatted`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageContaining`. Current assertion helpers: `assertThatThrownBy`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, formatted, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, hasStackTraceContaining, formatted]; SQL verbs differ: legacy [CREATE, ALTER, SET] vs current [CREATE, ALTER, SET, DROP]; assertion helpers differ: legacy [assertQueryFailure, hasMessageContaining] vs current [assertThatThrownBy]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, formatted, dropDeltaTableWithRetry], verbs [CREATE, ALTER, SET]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, hasStackTraceContaining, formatted], verbs [CREATE, ALTER, SET, DROP].
- Audit status: `verified`

##### `testRecalculateStatsForColumnMappingModeIdAndNoInitialStatistics`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testRecalculateStatsForColumnMappingModeIdAndNoInitialStatistics`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testRecalculateStatsForColumnMappingModeIdAndNoInitialStatistics`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `SHOW`, `ANALYZE`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `ImmutableList.of`, `VALUES`, `onTrino`. Current action shape: `SELECT`, `SHOW`, `ANALYZE`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `ImmutableList.of`, `VALUES`, `env.executeSpark`, `env.executeTrino`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, ImmutableList.of, VALUES, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, ImmutableList.of, VALUES, env.executeSpark, env.executeTrino, env.executeTrinoUpdate]; SQL verbs differ: legacy [CREATE, INSERT, SELECT, SHOW, ANALYZE] vs current [CREATE, INSERT, SELECT, SHOW, ANALYZE, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, ImmutableList.of, VALUES, onTrino, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT, SHOW, ANALYZE]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, ImmutableList.of, VALUES, env.executeSpark, env.executeTrino, env.executeTrinoUpdate], verbs [CREATE, INSERT, SELECT, SHOW, ANALYZE, DROP].
- Audit status: `verified`

##### `testMergeUpdateWithColumnMapping`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testMergeUpdateWithColumnMapping`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testMergeUpdateWithColumnMapping`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `MERGE`, `UPDATE`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `onTrino`, `ON`. Current action shape: `MERGE`, `UPDATE`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `env.executeTrinoUpdate`, `ON`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, ON, INSERT, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrinoUpdate, ON, INSERT, env.executeSpark]; SQL verbs differ: legacy [CREATE, INSERT, MERGE, UPDATE, SET, SELECT] vs current [CREATE, INSERT, MERGE, UPDATE, SET, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, ON, INSERT, dropDeltaTableWithRetry], verbs [CREATE, INSERT, MERGE, UPDATE, SET, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrinoUpdate, ON, INSERT, env.executeSpark], verbs [CREATE, INSERT, MERGE, UPDATE, SET, SELECT, DROP].
- Audit status: `verified`

##### `testMergeDeleteWithColumnMapping`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testMergeDeleteWithColumnMapping`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testMergeDeleteWithColumnMapping`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `MERGE`, `randomNameSuffix`, `onDelta`, `executeQuery`, `BY`, `TBLPROPERTIES`, `onTrino`, `VALUES`, `ROW`, `ImmutableList.of`, `array`, `struct`, `ON`. Current action shape: `MERGE`, `randomNameSuffix`, `env.executeSparkUpdate`, `BY`, `env.getBucketName`, `TBLPROPERTIES`, `env.executeTrinoUpdate`, `VALUES`, `ROW`, `ImmutableList.of`, `array`, `struct`, `ON`.
- Assertion parity: Legacy assertion helpers: `assertDeltaTrinoTableEquals`, `row`. Current assertion helpers: `assertDeltaTrinoTableEquals`, `row`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, onTrino, VALUES, ROW, ImmutableList.of, array, struct, ON, INSERT, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, VALUES, ROW, ImmutableList.of, array, struct, ON, INSERT]; SQL verbs differ: legacy [CREATE, INSERT, MERGE, DELETE] vs current [CREATE, INSERT, MERGE, DELETE, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, onTrino, VALUES, ROW, ImmutableList.of, array, struct, ON, INSERT, dropDeltaTableWithRetry], verbs [CREATE, INSERT, MERGE, DELETE]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, VALUES, ROW, ImmutableList.of, array, struct, ON, INSERT], verbs [CREATE, INSERT, MERGE, DELETE, DROP].
- Audit status: `verified`

##### `testDropLastNonPartitionColumnWithColumnMappingMode`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testDropLastNonPartitionColumnWithColumnMappingMode`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testDropLastNonPartitionColumnWithColumnMappingMode`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`. Current setup shape: `CREATE`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `onDelta`. Current action shape: `SELECT`, `randomNameSuffix`, `env.getBucketName`, `env.executeTrinoUpdate`, `WITH`, `env.executeSparkUpdate`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `row`, `assertThat`, `containsOnly`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, onDelta] vs current [randomNameSuffix, env.getBucketName, env.executeTrinoUpdate, WITH, env.executeSparkUpdate, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, onDelta], verbs [CREATE, SELECT, ALTER, DROP]. Current flow summary -> helpers [randomNameSuffix, env.getBucketName, env.executeTrinoUpdate, WITH, env.executeSparkUpdate, env.executeTrino, env.executeSpark], verbs [CREATE, SELECT, ALTER, DROP].
- Audit status: `verified`

##### `testTrinoExtendedStatisticsDropAndAddColumnWithColumnMappingMode`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testTrinoExtendedStatisticsDropAndAddColumnWithColumnMappingMode`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testTrinoExtendedStatisticsDropAndAddColumnWithColumnMappingMode`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `ALTER`.
- Action parity: Legacy action shape: `ANALYZE`, `SHOW`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `onTrino`. Current action shape: `ANALYZE`, `SHOW`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `env.executeTrinoUpdate`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrinoUpdate, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, dropDeltaTableWithRetry], verbs [CREATE, INSERT, ANALYZE, SHOW, ALTER, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrinoUpdate, env.executeTrino], verbs [CREATE, INSERT, ANALYZE, SHOW, ALTER, DROP].
- Audit status: `verified`

##### `testDropNonLowercaseColumnWithColumnMappingMode`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testDropNonLowercaseColumnWithColumnMappingMode`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testDropNonLowercaseColumnWithColumnMappingMode`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `ALTER`.
- Action parity: Legacy action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `BY`, `TBLPROPERTIES`, `getColumnNamesOnDelta`, `onTrino`, `VALUES`, `ImmutableList.of`. Current action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `BY`, `env.getBucketName`, `TBLPROPERTIES`, `getColumnNamesOnSpark`, `env.executeTrinoUpdate`, `VALUES`, `env.executeTrino`, `ImmutableList.of`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsExactly`, `containsOnly`, `row`, `assertThatThrownBy`, `hasMessageContaining`. Current assertion helpers: `assertThat`, `containsExactly`, `containsOnly`, `row`, `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, getColumnNamesOnDelta, onTrino, VALUES, ImmutableList.of, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, TBLPROPERTIES, getColumnNamesOnSpark, env.executeTrinoUpdate, VALUES, env.executeTrino, ImmutableList.of]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, getColumnNamesOnDelta, onTrino, VALUES, ImmutableList.of, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SHOW, ALTER, DROP, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, TBLPROPERTIES, getColumnNamesOnSpark, env.executeTrinoUpdate, VALUES, env.executeTrino, ImmutableList.of], verbs [CREATE, INSERT, SHOW, ALTER, DROP, SELECT].
- Audit status: `verified`

##### `testTrinoRenameColumnWithColumnMappingModeName`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testTrinoRenameColumnWithColumnMappingModeName`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testTrinoRenameColumnWithColumnMappingModeName`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `ALTER`. Current setup shape: `ALTER`.
- Action parity: Legacy action shape: `CREATE`, `INSERT`, `ALTER`, `DROP COLUMN`, `DESCRIBE`, `SELECT`, `testRenameColumnWithColumnMappingMode`, `onTrino`, `onDelta`. Current action shape: `CREATE`, `INSERT`, `ALTER`, `DROP COLUMN`, `DESCRIBE`, `SELECT`, `testRenameColumnWithColumnMappingMode`, `env.executeTrinoUpdate`, `env.executeSparkUpdate`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `isEqualTo`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the shared helper is semantically unchanged; it still creates a partitioned table, inserts one row, renames the data and partition columns through the supplied renamer, verifies `delta.columnMapping.maxColumnId` stays stable, checks `DESCRIBE` and cross-engine reads, drops `id`, renames `new_data` back to `id`, verifies old data does not reappear, and drops the table. The JUnit port routes the DDL/DML through `env.execute*`.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: This Trino-led variant uses the shared helper to rename both a regular column and the partition column, then verifies schema, data preservation, and non-restoration of dropped-column data before dropping the table.
- Audit status: `verified`

##### `testSparkRenameColumnWithColumnMappingModeId`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testSparkRenameColumnWithColumnMappingModeId`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testSparkRenameColumnWithColumnMappingModeId`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `ALTER`. Current setup shape: `ALTER`.
- Action parity: Legacy action shape: `CREATE`, `INSERT`, `ALTER`, `DROP COLUMN`, `DESCRIBE`, `SELECT`, `testRenameColumnWithColumnMappingMode`, `onDelta`, `onTrino`. Current action shape: `CREATE`, `INSERT`, `ALTER`, `DROP COLUMN`, `DESCRIBE`, `SELECT`, `testRenameColumnWithColumnMappingMode`, `env.executeSparkUpdate`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `isEqualTo`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the shared helper is semantically unchanged; it still creates a partitioned table, inserts one row, renames the data and partition columns through the supplied renamer, verifies `delta.columnMapping.maxColumnId` stays stable, checks `DESCRIBE` and cross-engine reads, drops `id`, renames `new_data` back to `id`, verifies old data does not reappear, and drops the table. The JUnit port routes the DDL/DML through `env.execute*`.
- Known intentional difference: None.
- Reviewer note: This Spark-led variant uses the shared helper to rename both a regular column and the partition column, then verifies schema, data preservation, and non-restoration of dropped-column data before dropping the table.
- Audit status: `verified`

##### `testRenameNonLowercaseColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `testRenameNonLowercaseColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeColumnMappingMode.java` ->
  `TestDeltaLakeColumnMappingMode.testRenameNonLowercaseColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`, `INSERT`, `ALTER`. Current setup shape: `CREATE`, `COMMENT`, `INSERT`, `ALTER`.
- Action parity: Legacy action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `BY`, `TBLPROPERTIES`, `onTrino`, `VALUES`, `getColumnNamesOnDelta`, `getColumnCommentOnDelta`. Current action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `BY`, `env.getBucketName`, `TBLPROPERTIES`, `env.executeTrinoUpdate`, `VALUES`, `env.executeTrino`, `getColumnNamesOnSpark`, `getColumnCommentOnSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `containsExactly`, `isEqualTo`, `assertQueryFailure`, `hasMessageContaining`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `containsExactly`, `isEqualTo`, `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, onTrino, VALUES, getColumnNamesOnDelta, getColumnCommentOnDelta, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, VALUES, env.executeTrino, getColumnNamesOnSpark, getColumnCommentOnSpark]; SQL verbs differ: legacy [CREATE, COMMENT, INSERT, SHOW, ALTER, SELECT] vs current [CREATE, COMMENT, INSERT, SHOW, ALTER, SELECT, DROP]; assertion helpers differ: legacy [assertThat, containsOnly, row, containsExactly, isEqualTo, assertQueryFailure, hasMessageContaining] vs current [assertThat, containsOnly, row, containsExactly, isEqualTo, assertThatThrownBy, hasMessageContaining]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, onTrino, VALUES, getColumnNamesOnDelta, getColumnCommentOnDelta, dropDeltaTableWithRetry], verbs [CREATE, COMMENT, INSERT, SHOW, ALTER, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, VALUES, env.executeTrino, getColumnNamesOnSpark, getColumnCommentOnSpark], verbs [CREATE, COMMENT, INSERT, SHOW, ALTER, SELECT, DROP].
- Audit status: `verified`

### `TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility`


- Owning migration commit: `Migrate TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `5`. Current methods: `5`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testCreateOrReplaceTableOnDeltaWithSchemaChange`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility.java` ->
  `testCreateOrReplaceTableOnDeltaWithSchemaChange`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility.java` ->
  `TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility.testCreateOrReplaceTableOnDeltaWithSchemaChange`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `performInsert`. Current setup shape: `CREATE`, `performInsert`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `with`, `onDelta`, `CAST`, `to_iso8601`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `with`, `env.getBucketName`, `env.executeSparkUpdate`, `CAST`, `env.executeTrino`, `to_iso8601`.
- Assertion parity: Legacy assertion helpers: `assertTransactionLogVersion`, `assertThat`, `containsOnly`. Current assertion helpers: `assertTransactionLogVersion`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, with, performInsert, onDelta, CAST, to_iso8601, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeTrinoUpdate, with, env.getBucketName, performInsert, env.executeSparkUpdate, CAST, env.executeTrino, to_iso8601]; SQL verbs differ: legacy [CREATE, SELECT] vs current [CREATE, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, with, performInsert, onDelta, CAST, to_iso8601, dropDeltaTableWithRetry], verbs [CREATE, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, with, env.getBucketName, performInsert, env.executeSparkUpdate, CAST, env.executeTrino, to_iso8601], verbs [CREATE, SELECT, DROP].
- Audit status: `verified`

##### `testCreateOrReplaceTableOnTrinoWithSchemaChange`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility.java` ->
  `testCreateOrReplaceTableOnTrinoWithSchemaChange`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility.java` ->
  `TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility.testCreateOrReplaceTableOnTrinoWithSchemaChange`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `performInsert`. Current setup shape: `CREATE`, `performInsert`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `with`, `onDelta`, `CAST`, `TIMESTAMP`, `date_format`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `with`, `env.getBucketName`, `CAST`, `TIMESTAMP`, `env.executeSpark`, `date_format`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `assertTransactionLogVersion`, `assertThat`, `containsOnly`. Current assertion helpers: `assertTransactionLogVersion`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, with, performInsert, onDelta, CAST, TIMESTAMP, date_format, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeTrinoUpdate, with, env.getBucketName, performInsert, CAST, TIMESTAMP, env.executeSpark, date_format, env.executeSparkUpdate]; SQL verbs differ: legacy [CREATE, SELECT] vs current [CREATE, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, with, performInsert, onDelta, CAST, TIMESTAMP, date_format, dropDeltaTableWithRetry], verbs [CREATE, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, with, env.getBucketName, performInsert, CAST, TIMESTAMP, env.executeSpark, date_format, env.executeSparkUpdate], verbs [CREATE, SELECT, DROP].
- Audit status: `verified`

##### `testCreateOrReplaceTableAndInsertOnTrinoWithSchemaChange`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility.java` ->
  `testCreateOrReplaceTableAndInsertOnTrinoWithSchemaChange`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility.java` ->
  `TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility.testCreateOrReplaceTableAndInsertOnTrinoWithSchemaChange`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `performInsert`. Current setup shape: `CREATE`, `performInsert`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `with`, `CAST`, `TIMESTAMP`, `onDelta`, `date_format`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `with`, `env.getBucketName`, `CAST`, `TIMESTAMP`, `env.executeSpark`, `date_format`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`. Current assertion helpers: `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, with, performInsert, CAST, TIMESTAMP, onDelta, date_format, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeTrinoUpdate, with, env.getBucketName, performInsert, CAST, TIMESTAMP, env.executeSpark, date_format, env.executeSparkUpdate]; SQL verbs differ: legacy [CREATE, SELECT] vs current [CREATE, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, with, performInsert, CAST, TIMESTAMP, onDelta, date_format, dropDeltaTableWithRetry], verbs [CREATE, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, with, env.getBucketName, performInsert, CAST, TIMESTAMP, env.executeSpark, date_format, env.executeSparkUpdate], verbs [CREATE, SELECT, DROP].
- Audit status: `verified`

##### `testCreateOrReplaceTableWithSchemaChangeOnCheckpoint`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility.java` ->
  `testCreateOrReplaceTableWithSchemaChangeOnCheckpoint`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility.java` ->
  `TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility.testCreateOrReplaceTableWithSchemaChangeOnCheckpoint`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `performInsert`. Current setup shape: `CREATE`, `INSERT`, `performInsert`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `with`, `ImmutableList.builder`, `expected.addAll`, `onDelta`, `CAST`, `expected.add`, `to_iso8601`, `expected.build`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `with`, `env.getBucketName`, `ImmutableList.builder`, `expected.addAll`, `env.executeSparkUpdate`, `CAST`, `expected.add`, `env.executeTrino`, `to_iso8601`, `expected.build`.
- Assertion parity: Legacy assertion helpers: `assertLastEntryIsCheckpointed`, `row`, `assertTransactionLogVersion`, `assertThat`, `containsOnly`. Current assertion helpers: `assertLastEntryIsCheckpointed`, `row`, `assertTransactionLogVersion`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, with, ImmutableList.builder, expected.addAll, performInsert, onDelta, CAST, expected.add, to_iso8601, expected.build, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeTrinoUpdate, with, env.getBucketName, ImmutableList.builder, expected.addAll, performInsert, env.executeSparkUpdate, CAST, expected.add, env.executeTrino, to_iso8601, expected.build]; SQL verbs differ: legacy [CREATE, SELECT, INSERT] vs current [CREATE, SELECT, INSERT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, with, ImmutableList.builder, expected.addAll, performInsert, onDelta, CAST, expected.add, to_iso8601, expected.build, dropDeltaTableWithRetry], verbs [CREATE, SELECT, INSERT]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, with, env.getBucketName, ImmutableList.builder, expected.addAll, performInsert, env.executeSparkUpdate, CAST, expected.add, env.executeTrino, to_iso8601, expected.build], verbs [CREATE, SELECT, INSERT, DROP].
- Audit status: `verified`

##### `testCreateOrReplaceTableWithOnlyFeaturesChangedOnTrino`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility.java` ->
  `testCreateOrReplaceTableWithOnlyFeaturesChangedOnTrino`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility.java` ->
  `TestDeltaLakeCreateOrReplaceTableAsSelectCompatibility.testCreateOrReplaceTableWithOnlyFeaturesChangedOnTrino`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `onDelta`, `hasNoRows`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `env.getBucketName`, `env.executeSpark`, `hasNoRows`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, onDelta, hasNoRows] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, env.executeSpark, hasNoRows]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, onDelta, hasNoRows], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, env.executeSpark, hasNoRows], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

### `TestDeltaLakeCreateTableAsSelectCompatibility`


- Owning migration commit: `Migrate TestDeltaLakeCreateTableAsSelectCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateTableAsSelectCompatibility.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateTableAsSelectCompatibility.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `5`. Current methods: `3`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly. Cross-class or merged legacy
  coverage accounted for: `testTrinoTimestampsWithDatabricks` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateTableAsSelectCompatibilityDatabricks.java` ->
  `testTrinoTimestampsWithDatabricks`; `testTrinoTypesWithDatabricks` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateTableAsSelectCompatibilityDatabricks.java` ->
  `testTrinoTypesWithDatabricks`.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testCreateFromTrinoWithDefaultPartitionValues`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateTableAsSelectCompatibility.java` ->
  `testCreateFromTrinoWithDefaultPartitionValues`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateTableAsSelectCompatibility.java` ->
  `TestDeltaLakeCreateTableAsSelectCompatibility.testCreateFromTrinoWithDefaultPartitionValues`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `VALUES`, `ImmutableList.of`, `onDelta`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `env.getBucketName`, `VALUES`, `ImmutableList.of`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isEqualTo`, `row`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, VALUES, ImmutableList.of, onDelta] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, VALUES, ImmutableList.of, env.executeSpark, env.executeTrino]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat, isEqualTo, row, containsOnly]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, VALUES, ImmutableList.of, onDelta], verbs [CREATE, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, VALUES, ImmutableList.of, env.executeSpark, env.executeTrino], verbs [CREATE, SELECT, DROP].
- Audit status: `verified`

##### `testCreateTableWithUnsupportedPartitionType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateTableAsSelectCompatibility.java` ->
  `testCreateTableWithUnsupportedPartitionType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateTableAsSelectCompatibility.java` ->
  `TestDeltaLakeCreateTableAsSelectCompatibility.testCreateTableWithUnsupportedPartitionType`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `USE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `formatted`, `onTrino`, `executeQuery`, `WITH`, `map`, `onDelta`, `BY`, `array`, `named_struct`. Current action shape: `SELECT`, `randomNameSuffix`, `formatted`, `env.getBucketName`, `env.executeTrinoUpdate`, `WITH`, `map`, `env.executeSparkUpdate`, `BY`, `array`, `hasStackTraceContaining`, `named_struct`.
- Assertion parity: Legacy assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `row`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, formatted, onTrino, executeQuery, WITH, map, onDelta, BY, array, named_struct, dropDeltaTableWithRetry] vs current [randomNameSuffix, formatted, env.getBucketName, env.executeTrinoUpdate, WITH, map, env.executeSparkUpdate, BY, array, hasStackTraceContaining, named_struct]; SQL verbs differ: legacy [CREATE, SELECT, USE] vs current [CREATE, SELECT, DROP]; assertion helpers differ: legacy [assertThatThrownBy, hasMessageContaining, row, hasMessageMatching] vs current [assertThatThrownBy, hasMessageContaining, row]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, formatted, onTrino, executeQuery, WITH, map, onDelta, BY, array, named_struct, dropDeltaTableWithRetry], verbs [CREATE, SELECT, USE]. Current flow summary -> helpers [randomNameSuffix, formatted, env.getBucketName, env.executeTrinoUpdate, WITH, map, env.executeSparkUpdate, BY, array, hasStackTraceContaining, named_struct], verbs [CREATE, SELECT, DROP].
- Audit status: `verified`

##### `testCreateTableAsSelectWithAllPartitionColumns`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateTableAsSelectCompatibility.java` ->
  `testCreateTableAsSelectWithAllPartitionColumns`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateTableAsSelectCompatibility.java` ->
  `TestDeltaLakeCreateTableAsSelectCompatibility.testCreateTableAsSelectWithAllPartitionColumns`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `USE`. Current setup shape: `CREATE`, `USE`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `onDelta`, `BY`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `env.getBucketName`, `env.executeSparkUpdate`, `BY`, `hasStackTraceContaining`.
- Assertion parity: Legacy assertion helpers: `assertThatThrownBy`, `hasMessageContaining`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, onDelta, BY, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, env.executeSparkUpdate, BY, hasStackTraceContaining]; SQL verbs differ: legacy [CREATE, SELECT, USE] vs current [CREATE, SELECT, USE, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, onDelta, BY, dropDeltaTableWithRetry], verbs [CREATE, SELECT, USE]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, env.executeSparkUpdate, BY, hasStackTraceContaining], verbs [CREATE, SELECT, USE, DROP].
- Audit status: `verified`

### `TestDeltaLakeDeleteCompatibility`


- Owning migration commit: `Migrate TestDeltaLakeDeleteCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `19`. Current methods: `16`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly. Cross-class or merged legacy
  coverage accounted for: `testDeleteCompatibility` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibilityDatabricks.java` ->
  `testTruncateTable` and in Databricks lane
  `TestDeltaLakeWriteDatabricksCompatibilityDatabricks.testDeleteCompatibility`; `testDeletionVectorsTruncateTable` ->
  covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibilityDatabricks.java` ->
  `testDeletionVectorsTruncateTable`; `testTruncateTable` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `testTruncateTable`.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testDeleteOnEnforcedConstraintsReturnsRowsCount`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `testDeleteOnEnforcedConstraintsReturnsRowsCount`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `TestDeltaLakeDeleteCompatibility.testDeleteOnEnforcedConstraintsReturnsRowsCount`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `BY`, `VALUES`, `onTrino`, `hasNoRows`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `BY`, `env.getBucketName`, `VALUES`, `env.executeTrinoUpdate`, `env.executeSpark`, `hasNoRows`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, BY, VALUES, onTrino, hasNoRows, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, VALUES, env.executeTrinoUpdate, env.executeSpark, hasNoRows]; SQL verbs differ: legacy [CREATE, INSERT, DELETE, SELECT] vs current [CREATE, INSERT, DELETE, SELECT, DROP]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat, isEqualTo]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, BY, VALUES, onTrino, hasNoRows, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, VALUES, env.executeTrinoUpdate, env.executeSpark, hasNoRows], verbs [CREATE, INSERT, DELETE, SELECT, DROP].
- Audit status: `verified`

##### `testDeleteOnAppendOnlyWriterFeature`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `testDeleteOnAppendOnlyWriterFeature`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `TestDeltaLakeDeleteCompatibility.testDeleteOnAppendOnlyWriterFeature`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `SET`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `getTablePropertiesOnDelta`, `entry`, `onTrino`, `hasNoRows`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `getTablePropertiesOnSpark`, `entry`, `hasStackTraceContaining`, `env.executeTrinoUpdate`, `env.executeSpark`, `env.executeTrino`, `hasNoRows`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `assertQueryFailure`, `hasMessageContaining`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `contains`, `assertThatThrownBy`, `hasMessageContaining`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, getTablePropertiesOnDelta, entry, onTrino, hasNoRows, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, getTablePropertiesOnSpark, entry, hasStackTraceContaining, env.executeTrinoUpdate, env.executeSpark, env.executeTrino, hasNoRows]; SQL verbs differ: legacy [CREATE, INSERT, DELETE, SET, SELECT, ALTER] vs current [CREATE, INSERT, DELETE, SET, SELECT, ALTER, DROP]; assertion helpers differ: legacy [assertThat, contains, assertQueryFailure, hasMessageContaining, containsOnly, row] vs current [assertThat, contains, assertThatThrownBy, hasMessageContaining, containsOnly, row]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, getTablePropertiesOnDelta, entry, onTrino, hasNoRows, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SET, SELECT, ALTER]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, getTablePropertiesOnSpark, entry, hasStackTraceContaining, env.executeTrinoUpdate, env.executeSpark, env.executeTrino, hasNoRows], verbs [CREATE, INSERT, DELETE, SET, SELECT, ALTER, DROP].
- Audit status: `verified`

##### `testTrinoDeletionVectors`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `testTrinoDeletionVectors`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `TestDeltaLakeDeleteCompatibility.testTrinoDeletionVectors`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `onDelta`, `getTablePropertyOnDelta`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `env.getBucketName`, `env.executeTrino`, `env.executeSpark`, `getTablePropertyOnSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `isEqualTo`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `DROP`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, onDelta, getTablePropertyOnDelta] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, env.executeTrino, env.executeSpark, getTablePropertyOnSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, onDelta, getTablePropertyOnDelta], verbs [CREATE, INSERT, DELETE, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, env.executeTrino, env.executeSpark, getTablePropertyOnSpark], verbs [CREATE, INSERT, DELETE, SELECT, DROP].
- Audit status: `verified`

##### `testDeletionVectors`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `testDeletionVectors`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `TestDeltaLakeDeleteCompatibility.testDeletionVectors`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `SELECT`, `SHOW`, `UPDATE`, `MERGE`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `onTrino`, `ON`. Current action shape: `SELECT`, `SHOW`, `UPDATE`, `MERGE`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `env.executeSpark`, `env.executeTrino`, `env.executeTrinoUpdate`, `ON`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `contains`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `contains`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, ON, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeSpark, env.executeTrino, env.executeTrinoUpdate, ON]; SQL verbs differ: legacy [CREATE, INSERT, DELETE, SELECT, SHOW, UPDATE, SET, MERGE] vs current [CREATE, INSERT, DELETE, SELECT, SHOW, UPDATE, SET, MERGE, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, ON, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SELECT, SHOW, UPDATE, SET, MERGE]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeSpark, env.executeTrino, env.executeTrinoUpdate, ON], verbs [CREATE, INSERT, DELETE, SELECT, SHOW, UPDATE, SET, MERGE, DROP].
- Audit status: `verified`

##### `testDeletionVectorsWithPartitionedTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `testDeletionVectorsWithPartitionedTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `TestDeltaLakeDeleteCompatibility.testDeletionVectorsWithPartitionedTable`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `BY`, `TBLPROPERTIES`, `VALUES`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `BY`, `TBLPROPERTIES`, `VALUES`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `DROP`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, VALUES, onTrino] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, BY, TBLPROPERTIES, VALUES, env.executeSpark, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, VALUES, onTrino], verbs [CREATE, INSERT, DELETE, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, BY, TBLPROPERTIES, VALUES, env.executeSpark, env.executeTrino], verbs [CREATE, INSERT, DELETE, SELECT, DROP].
- Audit status: `verified`

##### `testDeletionVectorsWithRandomPrefix`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `testDeletionVectorsWithRandomPrefix`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `TestDeltaLakeDeleteCompatibility.testDeletionVectorsWithRandomPrefix`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `env.executeSpark`, `env.executeTrino`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeSpark, env.executeTrino, env.executeTrinoUpdate]; SQL verbs differ: legacy [CREATE, INSERT, DELETE, SELECT] vs current [CREATE, INSERT, DELETE, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeSpark, env.executeTrino, env.executeTrinoUpdate], verbs [CREATE, INSERT, DELETE, SELECT, DROP].
- Audit status: `verified`

##### `testDisableDeletionVectors`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `testDisableDeletionVectors`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `TestDeltaLakeDeleteCompatibility.testDisableDeletionVectors`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`. Current setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `onTrino`, `hasNoRows`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `env.executeSpark`, `env.executeTrino`, `hasNoRows`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, hasNoRows, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeSpark, env.executeTrino, hasNoRows]; SQL verbs differ: legacy [CREATE, INSERT, DELETE, ALTER, SET, SELECT] vs current [CREATE, INSERT, DELETE, ALTER, SET, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, hasNoRows, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, ALTER, SET, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeSpark, env.executeTrino, hasNoRows], verbs [CREATE, INSERT, DELETE, ALTER, SET, SELECT, DROP].
- Audit status: `verified`

##### `testDeletionVectorsWithCheckpointInterval`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `testDeletionVectorsWithCheckpointInterval`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `TestDeltaLakeDeleteCompatibility.testDeletionVectorsWithCheckpointInterval`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeSpark, env.executeTrino]; SQL verbs differ: legacy [CREATE, INSERT, DELETE, SELECT] vs current [CREATE, INSERT, DELETE, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeSpark, env.executeTrino], verbs [CREATE, INSERT, DELETE, SELECT, DROP].
- Audit status: `verified`

##### `testDeletionVectorsMergeDelete`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `testDeletionVectorsMergeDelete`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `TestDeltaLakeDeleteCompatibility.testDeletionVectorsMergeDelete`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `MERGE`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `explode`, `sequence`, `ON`, `ImmutableList.of`, `onTrino`. Current action shape: `SELECT`, `MERGE`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `explode`, `sequence`, `ON`, `ImmutableList.of`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `contains`. Current assertion helpers: `row`, `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, explode, sequence, ON, ImmutableList.of, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, explode, sequence, ON, ImmutableList.of, env.executeSpark, env.executeTrino]; SQL verbs differ: legacy [CREATE, INSERT, SELECT, MERGE, DELETE] vs current [CREATE, INSERT, SELECT, MERGE, DELETE, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, explode, sequence, ON, ImmutableList.of, onTrino, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT, MERGE, DELETE]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, explode, sequence, ON, ImmutableList.of, env.executeSpark, env.executeTrino], verbs [CREATE, INSERT, SELECT, MERGE, DELETE, DROP].
- Audit status: `verified`

##### `testDeletionVectorsLargeNumbers`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `testDeletionVectorsLargeNumbers`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `TestDeltaLakeDeleteCompatibility.testDeletionVectorsLargeNumbers`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `explode`, `sequence`, `ImmutableList.of`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `explode`, `sequence`, `ImmutableList.of`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `contains`. Current assertion helpers: `row`, `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, explode, sequence, ImmutableList.of, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, explode, sequence, ImmutableList.of, env.executeSpark, env.executeTrino]; SQL verbs differ: legacy [CREATE, INSERT, SELECT, DELETE] vs current [CREATE, INSERT, SELECT, DELETE, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, explode, sequence, ImmutableList.of, onTrino, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT, DELETE]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, explode, sequence, ImmutableList.of, env.executeSpark, env.executeTrino], verbs [CREATE, INSERT, SELECT, DELETE, DROP].
- Audit status: `verified`

##### `testChangeDataFeedWithDeletionVectors`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `testChangeDataFeedWithDeletionVectors`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `TestDeltaLakeDeleteCompatibility.testChangeDataFeedWithDeletionVectors`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `onTrino`, `VALUES`, `table_changes`, `TABLE`, `system.table_changes`. Current action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `env.executeTrinoUpdate`, `VALUES`, `env.executeSpark`, `table_changes`, `env.executeTrino`, `TABLE`, `system.table_changes`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino, VALUES, table_changes, TABLE, system.table_changes] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, VALUES, env.executeSpark, table_changes, env.executeTrino, TABLE, system.table_changes]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino, VALUES, table_changes, TABLE, system.table_changes], verbs [CREATE, INSERT, UPDATE, SET, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, env.executeTrinoUpdate, VALUES, env.executeSpark, table_changes, env.executeTrino, TABLE, system.table_changes], verbs [CREATE, INSERT, UPDATE, SET, SELECT].
- Audit status: `verified`

##### `testDeletionVectorsAcrossAddFile`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `testDeletionVectorsAcrossAddFile`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `TestDeltaLakeDeleteCompatibility.testDeletionVectorsAcrossAddFile`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `BY`, `TBLPROPERTIES`, `VALUES`, `ImmutableList.of`, `onTrino`, `count`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `BY`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `ImmutableList.of`, `env.executeSpark`, `env.executeTrino`, `count`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, VALUES, ImmutableList.of, onTrino, count, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, TBLPROPERTIES, VALUES, ImmutableList.of, env.executeSpark, env.executeTrino, count]; SQL verbs differ: legacy [CREATE, INSERT, DELETE, SELECT] vs current [CREATE, INSERT, DELETE, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, BY, TBLPROPERTIES, VALUES, ImmutableList.of, onTrino, count, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, TBLPROPERTIES, VALUES, ImmutableList.of, env.executeSpark, env.executeTrino, count], verbs [CREATE, INSERT, DELETE, SELECT, DROP].
- Audit status: `verified`

##### `testDeletionVectorsDeleteFrom`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `testDeletionVectorsDeleteFrom`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `TestDeltaLakeDeleteCompatibility.testDeletionVectorsDeleteFrom`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `DELETE`, `SELECT`, `testDeletionVectorsDeleteAll`, `randomNameSuffix`, `onDelta`, `onTrino`. Current action shape: `DELETE`, `SELECT`, `testDeletionVectorsDeleteAll`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `hasRowsCount`, `hasNoRows`. Current assertion helpers: `assertThat`, `hasRowsCount`, `hasNoRows`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the shared helper is semantically unchanged; it still creates a deletion-vector-enabled table, inserts 1,000 rows, runs the caller-supplied delete action, verifies both engines see an empty table, and drops it. The JUnit port swaps the delete executor to `env.executeSparkUpdate`.
- Known intentional difference: None.
- Reviewer note: The top-level method supplies a Spark-side `DELETE FROM default.<table>` action to the shared helper. That helper creates the deletion-vector table, bulk-loads rows, executes the delete, confirms both engines see no remaining rows, and drops the table.
- Audit status: `verified`

##### `testDeletionVectorsOptimize`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `testDeletionVectorsOptimize`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `TestDeltaLakeDeleteCompatibility.testDeletionVectorsOptimize`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `ImmutableList.of`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `ImmutableList.of`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `contains`. Current assertion helpers: `row`, `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, ImmutableList.of, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, ImmutableList.of, env.executeTrino, env.executeSpark]; SQL verbs differ: legacy [CREATE, INSERT, DELETE, SELECT] vs current [CREATE, INSERT, DELETE, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, ImmutableList.of, onTrino, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, ImmutableList.of, env.executeTrino, env.executeSpark], verbs [CREATE, INSERT, DELETE, SELECT, DROP].
- Audit status: `verified`

##### `testDeletionVectorsAbsolutePath`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `testDeletionVectorsAbsolutePath`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `TestDeltaLakeDeleteCompatibility.testDeletionVectorsAbsolutePath`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `ImmutableList.of`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `ImmutableList.of`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `contains`. Current assertion helpers: `row`, `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, ImmutableList.of, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, ImmutableList.of, env.executeSpark, env.executeTrino]; SQL verbs differ: legacy [CREATE, INSERT, DELETE, SELECT] vs current [CREATE, INSERT, DELETE, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, ImmutableList.of, onTrino, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, ImmutableList.of, env.executeSpark, env.executeTrino], verbs [CREATE, INSERT, DELETE, SELECT, DROP].
- Audit status: `verified`

##### `testDeletionVectorsWithChangeDataFeed`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `testDeletionVectorsWithChangeDataFeed`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `TestDeltaLakeDeleteCompatibility.testDeletionVectorsWithChangeDataFeed`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `table_changes`, `onTrino`, `TABLE`, `system.table_changes`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `env.executeSpark`, `table_changes`, `env.executeTrino`, `TABLE`, `system.table_changes`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `assertQueryFailure`, `hasMessageContaining`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, table_changes, onTrino, TABLE, system.table_changes, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeSpark, table_changes, env.executeTrino, TABLE, system.table_changes]; SQL verbs differ: legacy [CREATE, INSERT, DELETE, SELECT] vs current [CREATE, INSERT, DELETE, SELECT, DROP]; assertion helpers differ: legacy [assertThat, containsOnly, row, assertQueryFailure, hasMessageContaining] vs current [assertThat, containsOnly, row, assertThatThrownBy, hasMessageContaining]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, table_changes, onTrino, TABLE, system.table_changes, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeSpark, table_changes, env.executeTrino, TABLE, system.table_changes], verbs [CREATE, INSERT, DELETE, SELECT, DROP].
- Audit status: `verified`

### `TestDeltaLakeDropTableCompatibility`


- Owning migration commit: `Migrate TestDeltaLakeDropTableCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDropTableCompatibility.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDropTableCompatibility.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testDropTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDropTableCompatibility.java` ->
  `testDropTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDropTableCompatibility.java` ->
  `TestDeltaLakeDropTableCompatibility.testDropTable`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE SCHEMA`, `USE`, `CREATE TABLE`, `listObjects`. Current setup shape: `CREATE SCHEMA`, `USE`, `CREATE TABLE`, `listObjects`.
- Action parity: Legacy action shape: `DROP`, `testDropTableAccuracy`, `randomNameSuffix`, `Optional.of`, `Optional.empty`, `format`, `onTrino`, `onDelta`, `s3.listObjectsV2`. Current action shape: `DROP`, `randomNameSuffix`, `Optional.of`, `Optional.empty`, `format`, `env.executeTrinoUpdate`, `env.executeSparkUpdate`, `env.createMinioClient`, `minioClient.listObjects`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isNotEmpty`, `isEmpty`. Current assertion helpers: `assertThat`, `isNotEmpty`, `isEmpty`.
- Cleanup parity: Legacy cleanup shape: `DROP TABLE`, `DROP SCHEMA`. Current cleanup shape: `DROP TABLE`, `DROP SCHEMA`.
- Any observed difference, however small: the current JUnit method inlines the old `testDropTableAccuracy` helper and switches the object-store check from the AWS SDK paginator to `MinioClient.listObjects`; the creator/dropper matrix, managed-versus-explicit-location expectations, and cleanup remain the same.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: For each creator/dropper/explicit-location combination, the test creates a schema and table, verifies data files exist, drops the table from the chosen engine, checks whether files remain based on managed versus external location semantics, and then removes the table and schema in cleanup.
- Audit status: `verified`

### `TestDeltaLakeInsertCompatibility`


- Owning migration commit: `Migrate TestDeltaLakeInsertCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `12`. Current methods: `9`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly. Cross-class or merged legacy
  coverage accounted for: `testCompression` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibilityDatabricks.java` ->
  `testCompression`; `testPartitionedInsertCompatibility` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibilityDatabricks.java` ->
  `testPartitionedInsertCompatibility`; `testWritesToTableWithGeneratedColumnFails` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibilityDatabricks.java` ->
  `testWritesToTableWithGeneratedColumnFails`.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testInsertCompatibility`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `testInsertCompatibility`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `TestDeltaLakeInsertCompatibility.testInsertCompatibility`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `VALUES`, `onTrino`, `ImmutableList.of`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `VALUES`, `env.executeTrinoUpdate`, `ImmutableList.of`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, VALUES, onTrino, ImmutableList.of, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, VALUES, env.executeTrinoUpdate, ImmutableList.of, env.executeSpark, env.executeTrino]; SQL verbs differ: legacy [CREATE, INSERT, SELECT] vs current [CREATE, INSERT, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, VALUES, onTrino, ImmutableList.of, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, VALUES, env.executeTrinoUpdate, ImmutableList.of, env.executeSpark, env.executeTrino], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testTimestampInsertCompatibility`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `testTimestampInsertCompatibility`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `TestDeltaLakeInsertCompatibility.testTimestampInsertCompatibility`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `TIMESTAMP`, `WITH`, `onDelta`, `builder`, `add`, `build`, `date_format`, `format_datetime`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `TIMESTAMP`, `WITH`, `env.getBucketName`, `env.executeSparkUpdate`, `builder`, `add`, `build`, `env.executeSpark`, `date_format`, `env.executeTrino`, `format_datetime`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, TIMESTAMP, WITH, onDelta, builder, add, build, date_format, format_datetime] vs current [randomNameSuffix, env.executeTrinoUpdate, TIMESTAMP, WITH, env.getBucketName, env.executeSparkUpdate, builder, add, build, env.executeSpark, date_format, env.executeTrino, format_datetime]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, TIMESTAMP, WITH, onDelta, builder, add, build, date_format, format_datetime], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, TIMESTAMP, WITH, env.getBucketName, env.executeSparkUpdate, builder, add, build, env.executeSpark, date_format, env.executeTrino, format_datetime], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testTimestampWithTimeZonePartitionedInsertCompatibility`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `testTimestampWithTimeZonePartitionedInsertCompatibility`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `TestDeltaLakeInsertCompatibility.testTimestampWithTimeZonePartitionedInsertCompatibility`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `onDelta`, `builder`, `add`, `build`, `date_format`, `format_datetime`, `getOnlyValue`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `env.getBucketName`, `env.executeSparkUpdate`, `builder`, `add`, `build`, `env.executeSpark`, `date_format`, `env.executeTrino`, `format_datetime`, `getOnlyValue`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`, `contains`. Current assertion helpers: `row`, `assertThat`, `containsOnly`, `contains`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, onDelta, builder, add, build, date_format, format_datetime, getOnlyValue] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, env.executeSparkUpdate, builder, add, build, env.executeSpark, date_format, env.executeTrino, format_datetime, getOnlyValue]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, onDelta, builder, add, build, date_format, format_datetime, getOnlyValue], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, env.executeSparkUpdate, builder, add, build, env.executeSpark, date_format, env.executeTrino, format_datetime, getOnlyValue], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testTrinoPartitionedDifferentOrderInsertCompatibility`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `testTrinoPartitionedDifferentOrderInsertCompatibility`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `TestDeltaLakeInsertCompatibility.testTrinoPartitionedDifferentOrderInsertCompatibility`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `VALUES`, `ImmutableList.of`, `onDelta`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `env.getBucketName`, `VALUES`, `ImmutableList.of`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, VALUES, ImmutableList.of, onDelta] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, VALUES, ImmutableList.of, env.executeSpark, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, VALUES, ImmutableList.of, onDelta], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, VALUES, ImmutableList.of, env.executeSpark, env.executeTrino], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testDeltaPartitionedDifferentOrderInsertCompatibility`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `testDeltaPartitionedDifferentOrderInsertCompatibility`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `TestDeltaLakeInsertCompatibility.testDeltaPartitionedDifferentOrderInsertCompatibility`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `BY`, `VALUES`, `ImmutableList.of`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `BY`, `env.getBucketName`, `VALUES`, `ImmutableList.of`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, BY, VALUES, ImmutableList.of, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, VALUES, ImmutableList.of, env.executeSpark, env.executeTrino]; SQL verbs differ: legacy [CREATE, INSERT, SELECT] vs current [CREATE, INSERT, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, BY, VALUES, ImmutableList.of, onTrino, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, VALUES, ImmutableList.of, env.executeSpark, env.executeTrino], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testInsertNonLowercaseColumnsCompatibility`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `testInsertNonLowercaseColumnsCompatibility`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `TestDeltaLakeInsertCompatibility.testInsertNonLowercaseColumnsCompatibility`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `VALUES`, `onTrino`, `ImmutableList.of`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `VALUES`, `env.executeTrinoUpdate`, `ImmutableList.of`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, VALUES, onTrino, ImmutableList.of, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, VALUES, env.executeTrinoUpdate, ImmutableList.of, env.executeSpark, env.executeTrino]; SQL verbs differ: legacy [CREATE, INSERT, SELECT] vs current [CREATE, INSERT, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, VALUES, onTrino, ImmutableList.of, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, VALUES, env.executeTrinoUpdate, ImmutableList.of, env.executeSpark, env.executeTrino], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testInsertNestedNonLowercaseColumnsCompatibility`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `testInsertNestedNonLowercaseColumnsCompatibility`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `TestDeltaLakeInsertCompatibility.testInsertNestedNonLowercaseColumnsCompatibility`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `VALUES`, `struct`, `onTrino`, `ROW`, `ImmutableList.of`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `VALUES`, `struct`, `env.executeTrinoUpdate`, `ROW`, `ImmutableList.of`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, VALUES, struct, onTrino, ROW, ImmutableList.of, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, VALUES, struct, env.executeTrinoUpdate, ROW, ImmutableList.of, env.executeSpark, env.executeTrino]; SQL verbs differ: legacy [CREATE, INSERT, SELECT] vs current [CREATE, INSERT, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, VALUES, struct, onTrino, ROW, ImmutableList.of, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, VALUES, struct, env.executeTrinoUpdate, ROW, ImmutableList.of, env.executeSpark, env.executeTrino], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testPartitionedInsertNonLowercaseColumnsCompatibility`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `testPartitionedInsertNonLowercaseColumnsCompatibility`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `TestDeltaLakeInsertCompatibility.testPartitionedInsertNonLowercaseColumnsCompatibility`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `BY`, `VALUES`, `onTrino`, `ImmutableList.of`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `BY`, `env.getBucketName`, `VALUES`, `env.executeTrinoUpdate`, `ImmutableList.of`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, BY, VALUES, onTrino, ImmutableList.of, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, VALUES, env.executeTrinoUpdate, ImmutableList.of, env.executeSpark, env.executeTrino]; SQL verbs differ: legacy [CREATE, INSERT, SELECT] vs current [CREATE, INSERT, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, BY, VALUES, onTrino, ImmutableList.of, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, VALUES, env.executeTrinoUpdate, ImmutableList.of, env.executeSpark, env.executeTrino], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `verifyCompressionCodecsDataProvider`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `verifyCompressionCodecsDataProvider`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `TestDeltaLakeInsertCompatibility.verifyCompressionCodecsDataProvider`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `USE`. Current setup shape: `USE`.
- Action parity: Legacy action shape: `SHOW`, `onTrino`, `executeQuery`, `Stream.of`, `compressionCodecs`, `map`, `getOnlyElement`, `asList`, `collect`, `toImmutableList`. Current action shape: `SHOW`, `env.executeTrino`, `Stream.of`, `compressionCodecs`, `map`, `getOnlyElement`, `asList`, `collect`, `toImmutableList`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, Stream.of, compressionCodecs, map, getOnlyElement, asList, collect, toImmutableList] vs current [env.executeTrino, Stream.of, compressionCodecs, map, getOnlyElement, asList, collect, toImmutableList]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, Stream.of, compressionCodecs, map, getOnlyElement, asList, collect, toImmutableList], verbs [SHOW, USE]. Current flow summary -> helpers [env.executeTrino, Stream.of, compressionCodecs, map, getOnlyElement, asList, collect, toImmutableList], verbs [SHOW, USE].
- Audit status: `verified`

### `TestDeltaLakeJmx`


- Owning migration commit: `Migrate TestDeltaLakeJmx to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeJmx.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeJmx.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `2`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly. Cross-class or merged legacy
  coverage accounted for: `testJmxTablesExposedByDeltaLakeConnectorBackedByGlueMetastore` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeJmxDatabricks.java` ->
  `testJmxTablesExposedByDeltaLakeConnectorBackedByGlueMetastore`.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testJmxTablesExposedByDeltaLakeConnectorBackedByThriftMetastore`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeJmx.java` ->
  `testJmxTablesExposedByDeltaLakeConnectorBackedByThriftMetastore`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeJmx.java` ->
  `TestDeltaLakeJmx.testJmxTablesExposedByDeltaLakeConnectorBackedByThriftMetastore`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `SHOW`, `onTrino`, `executeQuery`. Current action shape: `SHOW`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery] vs current [env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery], verbs [SHOW]. Current flow summary -> helpers [env.executeTrino], verbs [SHOW].
- Audit status: `verified`

### `TestDeltaLakePartitioningCompatibility`


- Owning migration commit: `Migrate TestDeltaLakePartitioningCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakePartitioningCompatibility.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakePartitioningCompatibility.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `8`. Current methods: `8`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testSparkCanReadFromCtasTableCreatedByTrinoWithSpecialCharactersInPartitioningColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakePartitioningCompatibility.java` ->
  `testSparkCanReadFromCtasTableCreatedByTrinoWithSpecialCharactersInPartitioningColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakePartitioningCompatibility.java` ->
  `TestDeltaLakePartitioningCompatibility.testSparkCanReadFromCtasTableCreatedByTrinoWithSpecialCharactersInPartitioningColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `testSparkCanReadFromCtasTableCreatedByTrinoWithSpecialCharactersInPartitioningColumnWithCpIntervalSet`, `format`, `randomNameSuffix`, `onTrino`, `onDelta`. Current action shape: `SELECT`, `testSparkCanReadFromCtasTableCreatedByTrinoWithSpecialCharactersInPartitioningColumnWithCpIntervalSet`, `format`, `randomNameSuffix`, `env.executeTrinoUpdate`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `contains`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the helper is semantically unchanged; it still runs interval `1` and `20`, creates a Trino CTAS table partitioned by special-character values, and verifies both engines read the expected rows. The JUnit port tightens the Trino-side assertion from `contains` to `containsOnly`.
- Known intentional difference: None.
- Reviewer note: The top-level method invokes the shared helper for checkpoint intervals 1 and 20. That helper creates the partitioned CTAS table with special-character partition values, verifies both Spark and Trino read the full expected row set, and drops the table.
- Audit status: `verified`

##### `testTrinoCanReadFromCtasTableCreatedBySparkWithSpecialCharactersInPartitioningColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakePartitioningCompatibility.java` ->
  `testTrinoCanReadFromCtasTableCreatedBySparkWithSpecialCharactersInPartitioningColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakePartitioningCompatibility.java` ->
  `TestDeltaLakePartitioningCompatibility.testTrinoCanReadFromCtasTableCreatedBySparkWithSpecialCharactersInPartitioningColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `testTrinoCanReadFromCtasTableCreatedBySparkWithSpecialCharactersInPartitioningColumnWithCpIntervalSet`, `format`, `randomNameSuffix`, `onDelta`, `onTrino`. Current action shape: `SELECT`, `testTrinoCanReadFromCtasTableCreatedBySparkWithSpecialCharactersInPartitioningColumnWithCpIntervalSet`, `format`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `contains`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the helper is semantically unchanged; it still runs interval `1` and `20`, creates a Spark CTAS table partitioned by special-character values, and verifies both engines read the expected rows. The JUnit port tightens the Trino-side assertion from `contains` to `containsOnly`.
- Known intentional difference: None.
- Reviewer note: The top-level method invokes the shared helper for checkpoint intervals 1 and 20. That helper creates the partitioned CTAS table from Spark with special-character partition values, verifies both Trino and Spark read the full expected row set, and drops the table.
- Audit status: `verified`

##### `testSparkCanReadTableCreatedByTrinoWithSpecialCharactersInPartitioningColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakePartitioningCompatibility.java` ->
  `testSparkCanReadTableCreatedByTrinoWithSpecialCharactersInPartitioningColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakePartitioningCompatibility.java` ->
  `TestDeltaLakePartitioningCompatibility.testSparkCanReadTableCreatedByTrinoWithSpecialCharactersInPartitioningColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `testSparkCanReadTableCreatedByTrinoWithSpecialCharactersInPartitioningColumnWithCpIntervalSet`, `format`, `randomNameSuffix`, `onTrino`, `onDelta`. Current action shape: `SELECT`, `testSparkCanReadTableCreatedByTrinoWithSpecialCharactersInPartitioningColumnWithCpIntervalSet`, `format`, `randomNameSuffix`, `env.executeTrinoUpdate`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `contains`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the helper is semantically unchanged; it still runs interval `1` and `20`, creates an empty Trino table partitioned by special-character values, inserts the ten rows, and verifies both engines read them. The JUnit port tightens the Trino-side assertion from `contains` to `containsOnly`.
- Known intentional difference: None.
- Reviewer note: The top-level method invokes the shared helper for checkpoint intervals 1 and 20. That helper creates the partitioned table in Trino, inserts the special-character partition rows, verifies both Spark and Trino read the expected dataset, and drops the table.
- Audit status: `verified`

##### `testTrinoCanReadTableCreatedBySparkWithSpecialCharactersInPartitioningColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakePartitioningCompatibility.java` ->
  `testTrinoCanReadTableCreatedBySaprkWithSpecialCharactersInPartitioningColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakePartitioningCompatibility.java` ->
  `TestDeltaLakePartitioningCompatibility.testTrinoCanReadTableCreatedBySparkWithSpecialCharactersInPartitioningColumn`
- Mapping type: `renamed`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `testTrinoCanReadTableCreatedBySparkWithSpecialCharactersInPartitioningColumnWithCpIntervalSet`, `format`, `randomNameSuffix`, `onDelta`, `onTrino`. Current action shape: `SELECT`, `testTrinoCanReadTableCreatedBySparkWithSpecialCharactersInPartitioningColumnWithCpIntervalSet`, `format`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `contains`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the helper is semantically unchanged; it still runs interval `1` and `20`, creates an empty Spark table partitioned by special-character values, inserts the ten rows, and verifies both engines read them. The JUnit port tightens the Trino-side assertion from `contains` to `containsOnly`.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: The top-level method invokes the shared helper for checkpoint intervals 1 and 20. That helper creates the partitioned table in Spark, inserts the special-character partition rows, verifies both Trino and Spark read the expected dataset, and drops the table.
- Audit status: `verified`

##### `testSparkCanReadFromTableUpdatedByTrino`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakePartitioningCompatibility.java` ->
  `testSparkCanReadFromTableUpdatedByTrino`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakePartitioningCompatibility.java` ->
  `TestDeltaLakePartitioningCompatibility.testSparkCanReadFromTableUpdatedByTrino`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `UPDATE`, `SELECT`, `testSparkCanReadFromTableUpdatedByTrinoWithCpIntervalSet`, `format`, `randomNameSuffix`, `onTrino`, `onDelta`. Current action shape: `UPDATE`, `SELECT`, `testSparkCanReadFromTableUpdatedByTrinoWithCpIntervalSet`, `format`, `randomNameSuffix`, `env.executeTrinoUpdate`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `contains`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the helper is semantically unchanged; it still runs interval `1` and `20`, creates the partitioned Trino table with special-character values, updates every `id` by `+100` from Trino, and verifies both engines read the updated rows. The JUnit port tightens the Trino-side assertion from `contains` to `containsOnly`.
- Known intentional difference: None.
- Reviewer note: The top-level method invokes the shared helper for checkpoint intervals 1 and 20. That helper creates the partitioned table, updates all ids from Trino, verifies both Spark and Trino read the updated dataset, and drops the table.
- Audit status: `verified`

##### `testTrinoCanReadFromTableUpdatedBySpark`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakePartitioningCompatibility.java` ->
  `testTrinoCanReadFromTableUpdatedBySpark`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakePartitioningCompatibility.java` ->
  `TestDeltaLakePartitioningCompatibility.testTrinoCanReadFromTableUpdatedBySpark`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `UPDATE`, `SELECT`, `testTrinoCanReadFromTableUpdatedBySparkWithCpIntervalSet`, `format`, `randomNameSuffix`, `onDelta`, `onTrino`. Current action shape: `UPDATE`, `SELECT`, `testTrinoCanReadFromTableUpdatedBySparkWithCpIntervalSet`, `format`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `contains`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the helper is semantically unchanged; it still runs interval `1` and `20`, creates the partitioned Spark table with special-character values, updates every `id` by `+100` from Spark, and verifies both engines read the updated rows. The JUnit port tightens the Trino-side assertion from `contains` to `containsOnly`.
- Known intentional difference: None.
- Reviewer note: The top-level method invokes the shared helper for checkpoint intervals 1 and 20. That helper creates the partitioned table, updates all ids from Spark, verifies both Trino and Spark read the updated dataset, and drops the table.
- Audit status: `verified`

##### `testTrinoCanReadFromTablePartitionChangedBySpark`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakePartitioningCompatibility.java` ->
  `testTrinoCanReadFromTablePartitionChangedBySpark`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakePartitioningCompatibility.java` ->
  `TestDeltaLakePartitioningCompatibility.testTrinoCanReadFromTablePartitionChangedBySpark`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `ImmutableList.of`, `onDelta`, `executeQuery`, `format`, `BY`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `ImmutableList.of`, `env.executeSparkUpdate`, `format`, `BY`, `env.getBucketName`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `contains`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, ImmutableList.of, onDelta, executeQuery, format, BY, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, ImmutableList.of, env.executeSparkUpdate, format, BY, env.getBucketName, env.executeTrino]; SQL verbs differ: legacy [CREATE, SELECT] vs current [CREATE, SELECT, DROP]; assertion helpers differ: legacy [row, assertThat, contains] vs current [row, assertThat, containsOnly]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, ImmutableList.of, onDelta, executeQuery, format, BY, onTrino, dropDeltaTableWithRetry], verbs [CREATE, SELECT]. Current flow summary -> helpers [randomNameSuffix, ImmutableList.of, env.executeSparkUpdate, format, BY, env.getBucketName, env.executeTrino], verbs [CREATE, SELECT, DROP].
- Audit status: `verified`

##### `testPartitionedByNonLowercaseColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakePartitioningCompatibility.java` ->
  `testPartitionedByNonLowercaseColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakePartitioningCompatibility.java` ->
  `TestDeltaLakePartitioningCompatibility.testPartitionedByNonLowercaseColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `SELECT`, `UPDATE`, `MERGE`, `randomNameSuffix`, `onDelta`, `executeQuery`, `format`, `BY`, `onTrino`, `VALUES`, `USING`, `hasNoRows`. Current action shape: `SELECT`, `UPDATE`, `MERGE`, `randomNameSuffix`, `env.executeSparkUpdate`, `format`, `BY`, `env.getBucketName`, `env.executeTrino`, `env.executeTrinoUpdate`, `VALUES`, `USING`, `hasNoRows`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, format, BY, onTrino, VALUES, USING, hasNoRows, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, format, BY, env.getBucketName, env.executeTrino, env.executeTrinoUpdate, VALUES, USING, hasNoRows]; SQL verbs differ: legacy [CREATE, SELECT, INSERT, DELETE, UPDATE, SET, MERGE] vs current [CREATE, SELECT, INSERT, DELETE, UPDATE, SET, MERGE, DROP]; assertion helpers differ: legacy [assertThat, contains, row] vs current [assertThat, containsOnly, row]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, format, BY, onTrino, VALUES, USING, hasNoRows, dropDeltaTableWithRetry], verbs [CREATE, SELECT, INSERT, DELETE, UPDATE, SET, MERGE]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, format, BY, env.getBucketName, env.executeTrino, env.executeTrinoUpdate, VALUES, USING, hasNoRows], verbs [CREATE, SELECT, INSERT, DELETE, UPDATE, SET, MERGE, DROP].
- Audit status: `verified`

### `TestDeltaLakeProceduresCompatibility`


- Owning migration commit: `Migrate TestDeltaLakeProceduresCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeProceduresCompatibility.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeProceduresCompatibility.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testUnregisterTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeProceduresCompatibility.java` ->
  `testUnregisterTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeProceduresCompatibility.java` ->
  `TestDeltaLakeProceduresCompatibility.testUnregisterTable`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `CALL`, `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `WITH`, `onDelta`, `system.unregister_table`. Current action shape: `SELECT`, `CALL`, `env.getBucketName`, `randomNameSuffix`, `env.executeTrinoUpdate`, `format`, `WITH`, `env.executeTrino`, `env.executeSpark`, `system.unregister_table`, `hasStackTraceContaining`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, format, WITH, onDelta, system.unregister_table] vs current [env.getBucketName, randomNameSuffix, env.executeTrinoUpdate, format, WITH, env.executeTrino, env.executeSpark, system.unregister_table, hasStackTraceContaining]; assertion helpers differ: legacy [assertThat, containsOnly, row, assertQueryFailure, hasMessageMatching] vs current [assertThat, containsOnly, row, assertThatThrownBy, hasMessageContaining]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, format, WITH, onDelta, system.unregister_table], verbs [CREATE, SELECT, CALL, DROP]. Current flow summary -> helpers [env.getBucketName, randomNameSuffix, env.executeTrinoUpdate, format, WITH, env.executeTrino, env.executeSpark, system.unregister_table, hasStackTraceContaining], verbs [CREATE, SELECT, CALL, DROP].
- Audit status: `verified`

### `TestDeltaLakeSelectCompatibility`


- Owning migration commit: `Migrate TestDeltaLakeSelectCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeSelectCompatibility.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeSelectCompatibility.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testPartitionedSelectSpecialCharacters`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeSelectCompatibility.java` ->
  `testPartitionedSelectSpecialCharacters`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeSelectCompatibility.java` ->
  `TestDeltaLakeSelectCompatibility.testPartitionedSelectSpecialCharacters`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `URI.create`. Current setup shape: `CREATE`, `INSERT`, `URI.create`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `BY`, `onTrino`, `ImmutableList.of`, `input_file_name`, `getOnlyValue`, `isNotEqualTo`, `format`, `getPath`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `BY`, `env.getBucketName`, `env.executeTrinoUpdate`, `List.of`, `env.executeSpark`, `env.executeTrino`, `input_file_name`, `getOnlyValue`, `isNotEqualTo`, `format`, `getPath`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`, `isEqualTo`. Current assertion helpers: `row`, `assertThat`, `containsOnly`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, BY, onTrino, ImmutableList.of, input_file_name, getOnlyValue, isNotEqualTo, format, URI.create, getPath, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, env.executeTrinoUpdate, List.of, env.executeSpark, env.executeTrino, input_file_name, getOnlyValue, isNotEqualTo, format, URI.create, getPath]; SQL verbs differ: legacy [CREATE, INSERT, SELECT] vs current [CREATE, INSERT, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, BY, onTrino, ImmutableList.of, input_file_name, getOnlyValue, isNotEqualTo, format, URI.create, getPath, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, env.executeTrinoUpdate, List.of, env.executeSpark, env.executeTrino, input_file_name, getOnlyValue, isNotEqualTo, format, URI.create, getPath], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

### `TestDeltaLakeSystemTableCompatibility`


- Owning migration commit: `Migrate TestDeltaLakeSystemTableCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeSystemTableCompatibility.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeSystemTableCompatibility.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `4`. Current methods: `4`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testTablePropertiesCaseSensitivity`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeSystemTableCompatibility.java` ->
  `testTablePropertiesCaseSensitivity`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeSystemTableCompatibility.java` ->
  `TestDeltaLakeSystemTableCompatibility.testTablePropertiesCaseSensitivity`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `format`, `s`, `TBLPROPERTIES`, `ImmutableList.of`, `onTrino`, `trinoResult.rows`, `containsExactlyInAnyOrderElementsOf`, `deltaResult.rows`. Current action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `format`, `s`, `TBLPROPERTIES`, `env.getBucketName`, `List.of`, `env.executeSpark`, `env.executeTrino`, `trinoResult.rows`, `containsExactlyInAnyOrderElementsOf`, `deltaResult.rows`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `contains`. Current assertion helpers: `row`, `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, format, s, TBLPROPERTIES, ImmutableList.of, onTrino, trinoResult.rows, containsExactlyInAnyOrderElementsOf, deltaResult.rows, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, format, s, TBLPROPERTIES, env.getBucketName, List.of, env.executeSpark, env.executeTrino, trinoResult.rows, containsExactlyInAnyOrderElementsOf, deltaResult.rows]; SQL verbs differ: legacy [CREATE, SHOW, SELECT] vs current [CREATE, SHOW, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, format, s, TBLPROPERTIES, ImmutableList.of, onTrino, trinoResult.rows, containsExactlyInAnyOrderElementsOf, deltaResult.rows, dropDeltaTableWithRetry], verbs [CREATE, SHOW, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, format, s, TBLPROPERTIES, env.getBucketName, List.of, env.executeSpark, env.executeTrino, trinoResult.rows, containsExactlyInAnyOrderElementsOf, deltaResult.rows], verbs [CREATE, SHOW, SELECT, DROP].
- Audit status: `verified`

##### `testTablePropertiesWithTableFeatures`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeSystemTableCompatibility.java` ->
  `testTablePropertiesWithTableFeatures`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeSystemTableCompatibility.java` ->
  `TestDeltaLakeSystemTableCompatibility.testTablePropertiesWithTableFeatures`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `format`, `s`, `TBLPROPERTIES`, `ImmutableList.of`, `onTrino`, `trinoResult.rows`, `containsExactlyInAnyOrderElementsOf`, `deltaResult.rows`. Current action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `format`, `s`, `TBLPROPERTIES`, `env.getBucketName`, `List.of`, `env.executeSpark`, `env.executeTrino`, `trinoResult.rows`, `containsExactlyInAnyOrderElementsOf`, `deltaResult.rows`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `contains`. Current assertion helpers: `row`, `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, format, s, TBLPROPERTIES, ImmutableList.of, onTrino, trinoResult.rows, containsExactlyInAnyOrderElementsOf, deltaResult.rows, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, format, s, TBLPROPERTIES, env.getBucketName, List.of, env.executeSpark, env.executeTrino, trinoResult.rows, containsExactlyInAnyOrderElementsOf, deltaResult.rows]; SQL verbs differ: legacy [CREATE, SHOW, SELECT] vs current [CREATE, SHOW, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, format, s, TBLPROPERTIES, ImmutableList.of, onTrino, trinoResult.rows, containsExactlyInAnyOrderElementsOf, deltaResult.rows, dropDeltaTableWithRetry], verbs [CREATE, SHOW, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, format, s, TBLPROPERTIES, env.getBucketName, List.of, env.executeSpark, env.executeTrino, trinoResult.rows, containsExactlyInAnyOrderElementsOf, deltaResult.rows], verbs [CREATE, SHOW, SELECT, DROP].
- Audit status: `verified`

##### `testTablePartitionsWithDeletionVectors`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeSystemTableCompatibility.java` ->
  `testTablePartitionsWithDeletionVectors`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeSystemTableCompatibility.java` ->
  `TestDeltaLakeSystemTableCompatibility.testTablePartitionsWithDeletionVectors`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `VARCHAR`, `BY`, `TBLPROPERTIES`, `ImmutableList.of`, `VALUES`, `onTrino`, `format`, `cast`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `VARCHAR`, `BY`, `env.getBucketName`, `TBLPROPERTIES`, `List.of`, `VALUES`, `env.executeTrino`, `format`, `cast`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `DROP`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, VARCHAR, BY, TBLPROPERTIES, ImmutableList.of, VALUES, onTrino, format, cast] vs current [randomNameSuffix, env.executeSparkUpdate, VARCHAR, BY, env.getBucketName, TBLPROPERTIES, List.of, VALUES, env.executeTrino, format, cast]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, VARCHAR, BY, TBLPROPERTIES, ImmutableList.of, VALUES, onTrino, format, cast], verbs [CREATE, INSERT, SELECT, DELETE, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, VARCHAR, BY, env.getBucketName, TBLPROPERTIES, List.of, VALUES, env.executeTrino, format, cast], verbs [CREATE, INSERT, SELECT, DELETE, DROP].
- Audit status: `verified`

##### `testTablePartitionsWithNoColumnStats`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeSystemTableCompatibility.java` ->
  `testTablePartitionsWithNoColumnStats`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeSystemTableCompatibility.java` ->
  `TestDeltaLakeSystemTableCompatibility.testTablePartitionsWithNoColumnStats`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `VARCHAR`, `BY`, `TBLPROPERTIES`, `ImmutableList.of`, `VALUES`, `onTrino`, `format`, `cast`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `VARCHAR`, `BY`, `env.getBucketName`, `TBLPROPERTIES`, `List.of`, `VALUES`, `env.executeTrino`, `format`, `cast`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, VARCHAR, BY, TBLPROPERTIES, ImmutableList.of, VALUES, onTrino, format, cast] vs current [randomNameSuffix, env.executeSparkUpdate, VARCHAR, BY, env.getBucketName, TBLPROPERTIES, List.of, VALUES, env.executeTrino, format, cast]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, VARCHAR, BY, TBLPROPERTIES, ImmutableList.of, VALUES, onTrino, format, cast], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, VARCHAR, BY, env.getBucketName, TBLPROPERTIES, List.of, VALUES, env.executeTrino, format, cast], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

### `TestDeltaLakeTimeTravelCompatibility`


- Owning migration commit: `Migrate TestDeltaLakeTimeTravelCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeTimeTravelCompatibility.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeTimeTravelCompatibility.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `3`. Current methods: `3`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testReadFromTableRestoredToPreviousVersion`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeTimeTravelCompatibility.java` ->
  `testReadFromTableRestoredToPreviousVersion`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeTimeTravelCompatibility.java` ->
  `TestDeltaLakeTimeTravelCompatibility.testReadFromTableRestoredToPreviousVersion`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `SET`, `ALTER`.
- Action parity: Legacy action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `s`, `WITH`, `ImmutableList.of`, `onDelta`. Current action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `env.getBucketName`, `List.of`, `env.executeTrino`, `env.executeSparkUpdate`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, format, s, WITH, ImmutableList.of, onDelta, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, List.of, env.executeTrino, env.executeSparkUpdate, env.executeSpark]; SQL verbs differ: legacy [CREATE, INSERT, UPDATE, SET, DELETE, ALTER, SELECT] vs current [CREATE, INSERT, UPDATE, SET, DELETE, ALTER, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, format, s, WITH, ImmutableList.of, onDelta, dropDeltaTableWithRetry], verbs [CREATE, INSERT, UPDATE, SET, DELETE, ALTER, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, env.getBucketName, List.of, env.executeTrino, env.executeSparkUpdate, env.executeSpark], verbs [CREATE, INSERT, UPDATE, SET, DELETE, ALTER, SELECT, DROP].
- Audit status: `verified`

##### `testSelectForVersionAsOf`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeTimeTravelCompatibility.java` ->
  `testSelectForVersionAsOf`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeTimeTravelCompatibility.java` ->
  `TestDeltaLakeTimeTravelCompatibility.testSelectForVersionAsOf`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `BY`, `VALUES`, `ImmutableList.of`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `BY`, `env.getBucketName`, `VALUES`, `List.of`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, BY, VALUES, ImmutableList.of, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, VALUES, List.of, env.executeSpark, env.executeTrino]; SQL verbs differ: legacy [CREATE, INSERT, ALTER, SELECT] vs current [CREATE, INSERT, ALTER, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, BY, VALUES, ImmutableList.of, onTrino, dropDeltaTableWithRetry], verbs [CREATE, INSERT, ALTER, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, BY, env.getBucketName, VALUES, List.of, env.executeSpark, env.executeTrino], verbs [CREATE, INSERT, ALTER, SELECT, DROP].
- Audit status: `verified`

##### `testSelectForTemporalAsOf`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeTimeTravelCompatibility.java` ->
  `testSelectForTemporalAsOf`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeTimeTravelCompatibility.java` ->
  `TestDeltaLakeTimeTravelCompatibility.testSelectForTemporalAsOf`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`, `timeAfterInsert.format`. Current setup shape: `CREATE`, `INSERT`, `ALTER`, `timeAfterInsert.format`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `DateTimeFormatter.ofPattern`, `onDelta`, `executeQuery`, `BY`, `VALUES`, `ZonedDateTime.now`, `ZoneId.of`, `ImmutableList.of`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `DateTimeFormatter.ofPattern`, `env.executeSparkUpdate`, `BY`, `env.getBucketName`, `VALUES`, `ZonedDateTime.now`, `ZoneId.of`, `List.of`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, DateTimeFormatter.ofPattern, onDelta, executeQuery, BY, VALUES, ZonedDateTime.now, ZoneId.of, ImmutableList.of, timeAfterInsert.format, onTrino] vs current [randomNameSuffix, DateTimeFormatter.ofPattern, env.executeSparkUpdate, BY, env.getBucketName, VALUES, ZonedDateTime.now, ZoneId.of, List.of, env.executeSpark, timeAfterInsert.format, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, DateTimeFormatter.ofPattern, onDelta, executeQuery, BY, VALUES, ZonedDateTime.now, ZoneId.of, ImmutableList.of, timeAfterInsert.format, onTrino], verbs [CREATE, INSERT, ALTER, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, DateTimeFormatter.ofPattern, env.executeSparkUpdate, BY, env.getBucketName, VALUES, ZonedDateTime.now, ZoneId.of, List.of, env.executeSpark, timeAfterInsert.format, env.executeTrino], verbs [CREATE, INSERT, ALTER, SELECT, DROP].
- Audit status: `verified`

### `TestDeltaLakeTransactionLogCache`


- Owning migration commit: `Migrate TestDeltaLakeTransactionLogCache to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeTransactionLogCache.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeTransactionLogCache.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testAllDataFilesAreLoadedWhenTransactionLogFileAfterTheCachedTableVersionIsMissing`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeTransactionLogCache.java` ->
  `testAllDataFilesAreLoadedWhenTransactionLogFileAfterTheCachedTableVersionIsMissing`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeTransactionLogCache.java` ->
  `TestDeltaLakeTransactionLogCache.testAllDataFilesAreLoadedWhenTransactionLogFileAfterTheCachedTableVersionIsMissing`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`, `env.createMinioClient`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `s`, `WITH`, `onDelta`, `IntStream.range`, `forEach`, `List.of`, `Stream.of`, `map`, `ObjectIdentifier.builder`, `key`, `build`, `toList`, `request.bucket`, `stream`, `containsExactlyInAnyOrder`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `s`, `WITH`, `formatted`, `env.getBucketName`, `env.executeTrino`, `env.executeSparkUpdate`, `IntStream.range`, `forEach`, `List.of`, `minioClient.removeObject`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `DROP`, `s3.deleteObjects`, `delete`, `delete.objects`, `response.deleted`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, format, s, WITH, onDelta, IntStream.range, forEach, List.of, Stream.of, map, ObjectIdentifier.builder, key, build, toList, s3.deleteObjects, request.bucket, delete, delete.objects, response.deleted, stream, containsExactlyInAnyOrder] vs current [randomNameSuffix, env.executeTrinoUpdate, s, WITH, formatted, env.getBucketName, env.executeTrino, env.executeSparkUpdate, IntStream.range, forEach, List.of, env.createMinioClient, minioClient.removeObject, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, format, s, WITH, onDelta, IntStream.range, forEach, List.of, Stream.of, map, ObjectIdentifier.builder, key, build, toList, s3.deleteObjects, request.bucket, delete, delete.objects, response.deleted, stream, containsExactlyInAnyOrder], verbs [CREATE, INSERT, SELECT, DELETE, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, s, WITH, formatted, env.getBucketName, env.executeTrino, env.executeSparkUpdate, IntStream.range, forEach, List.of, env.createMinioClient, minioClient.removeObject, env.executeSpark], verbs [CREATE, INSERT, SELECT, DELETE, DROP].
- Audit status: `verified`

### `TestDeltaLakeWriteDatabricksCompatibility`


- Owning migration commit: `Migrate TestDeltaLakeWriteDatabricksCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeMinio`.
- Method inventory complete: Yes. Legacy methods: `15`. Current methods: `2`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly. Cross-class or merged legacy
  coverage accounted for: `testCaseDeleteEntirePartition` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `testCaseDeleteEntirePartition`; `testCaseDeletePartialPartition` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `testCaseDeletePartialPartition`; `testCaseUpdateInPartition` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `testCaseUpdateInPartition`; `testCaseUpdatePartitionColumnFails` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `testCaseUpdatePartitionColumnFails`; `testDatabricksRespectsTrinoSettingNonNullableColumn` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `testDatabricksRespectsTrinoSettingNonNullableColumn`; `testDatabricksVacuumRemoveChangeDataFeedFiles` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `testDatabricksVacuumRemoveChangeDataFeedFiles`; `testDeleteCompatibility` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibilityDatabricks.java` ->
  `testTruncateTable`; `testDeleteOnPartitionKeyCompatibility` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `testDeleteOnPartitionKeyCompatibility`; `testDeleteOnPartitionedTableCompatibility` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `testDeleteOnPartitionedTableCompatibility`; `testInsertingIntoDatabricksTableWithAddedNotNullConstraint` -> covered
  in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `testInsertingIntoDatabricksTableWithAddedNotNullConstraint`; `testTrinoRespectsDatabricksSettingNonNullableColumn` ->
  covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `testTrinoRespectsDatabricksSettingNonNullableColumn`; `testTrinoVacuumRemoveChangeDataFeedFiles` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `testTrinoVacuumRemoveChangeDataFeedFiles`; `testUpdateCompatibility` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `testUpdateCompatibility`.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testVacuumProtocolCheck`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java` ->
  `testVacuumProtocolCheck`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java` ->
  `TestDeltaLakeWriteDatabricksCompatibility.testVacuumProtocolCheck`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `UPDATE`, `CALL`, `VACUUM`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `onTrino`, `system.vacuum`. Current action shape: `UPDATE`, `CALL`, `VACUUM`, `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.getBucketName`, `TBLPROPERTIES`, `VALUES`, `env.executeTrinoInSession`, `session.executeUpdate`, `system.vacuum`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, system.vacuum] vs current [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrinoInSession, session.executeUpdate, system.vacuum, env.executeSpark, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, VALUES, onTrino, system.vacuum], verbs [CREATE, INSERT, UPDATE, SET, CALL, VACUUM, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, env.getBucketName, TBLPROPERTIES, VALUES, env.executeTrinoInSession, session.executeUpdate, system.vacuum, env.executeSpark, env.executeTrino], verbs [CREATE, INSERT, UPDATE, SET, CALL, VACUUM, SELECT, DROP].
- Audit status: `verified`

##### `testUnsupportedWriterVersion`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java` ->
  `testUnsupportedWriterVersion`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java` ->
  `TestDeltaLakeWriteDatabricksCompatibility.testUnsupportedWriterVersion`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `be`, `kipException`. Current action shape: `randomNameSuffix`, `env.executeSparkUpdate`, `TBLPROPERTIES`, `hasStackTraceContaining`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, dropDeltaTableWithRetry, be, kipException] vs current [randomNameSuffix, env.executeSparkUpdate, TBLPROPERTIES, hasStackTraceContaining]; SQL verbs differ: legacy [CREATE] vs current [CREATE, DROP]; assertion helpers differ: legacy [assertThat, hasMessageMatching] vs current [assertThatThrownBy]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, dropDeltaTableWithRetry, be, kipException], verbs [CREATE]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, TBLPROPERTIES, hasStackTraceContaining], verbs [CREATE, DROP].
- Audit status: `verified`

### `TestDeltaLakeWriteDatabricksCompatibilityDatabricks`


- Owning migration commit: `Migrate TestDeltaLakeWriteDatabricksCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java`
- Class-level environment requirement: `DeltaLakeDatabricksEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `15`. Current methods: `13`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly. Cross-class or merged legacy
  coverage accounted for: `testUnsupportedWriterVersion` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java` ->
  `testUnsupportedWriterVersion`; `testVacuumProtocolCheck` -> covered in
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java` ->
  `testVacuumProtocolCheck`.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testUpdateCompatibility`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java` ->
  `testUpdateCompatibility`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `TestDeltaLakeWriteDatabricksCompatibilityDatabricks.testUpdateCompatibility`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing matches the shared Databricks suite selection in the 133 and 173 lanes.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `format`, `s`, `getBaseLocation`, `VALUES`, `onTrino`, `List.of`. Current action shape: `UPDATE`, `SELECT`, `randomNameSuffix`, `env.executeDatabricksSql`, `format`, `s`, `getBaseLocation`, `VALUES`, `env.executeTrinoSql`, `List.of`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, format, s, getBaseLocation, VALUES, onTrino, List.of, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeDatabricksSql, format, s, getBaseLocation, VALUES, env.executeTrinoSql, List.of, dropDeltaTableWithRetry]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, format, s, getBaseLocation, VALUES, onTrino, List.of, dropDeltaTableWithRetry], verbs [CREATE, INSERT, UPDATE, SET, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeDatabricksSql, format, s, getBaseLocation, VALUES, env.executeTrinoSql, List.of, dropDeltaTableWithRetry], verbs [CREATE, INSERT, UPDATE, SET, SELECT].
- Audit status: `verified`

##### `testDeleteCompatibility`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java` ->
  `testDeleteCompatibility`
- Additional legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `testDeleteCompatibility`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `TestDeltaLakeWriteDatabricksCompatibilityDatabricks.testDeleteCompatibility`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `format`, `s`, `getBaseLocation`, `VALUES`, `onTrino`, `List.of`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeDatabricksSql`, `format`, `s`, `getBaseLocation`, `VALUES`, `env.executeTrinoSql`, `List.of`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, format, s, getBaseLocation, VALUES, onTrino, List.of, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeDatabricksSql, format, s, getBaseLocation, VALUES, env.executeTrinoSql, List.of, dropDeltaTableWithRetry]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, format, s, getBaseLocation, VALUES, onTrino, List.of, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeDatabricksSql, format, s, getBaseLocation, VALUES, env.executeTrinoSql, List.of, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SELECT].
- Audit status: `verified`

##### `testDeleteOnPartitionedTableCompatibility`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java` ->
  `testDeleteOnPartitionedTableCompatibility`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `TestDeltaLakeWriteDatabricksCompatibilityDatabricks.testDeleteOnPartitionedTableCompatibility`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `format`, `s`, `BY`, `getBaseLocation`, `VALUES`, `onTrino`, `List.of`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeDatabricksSql`, `format`, `s`, `BY`, `getBaseLocation`, `VALUES`, `env.executeTrinoSql`, `List.of`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, format, s, BY, getBaseLocation, VALUES, onTrino, List.of, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeDatabricksSql, format, s, BY, getBaseLocation, VALUES, env.executeTrinoSql, List.of, dropDeltaTableWithRetry]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, format, s, BY, getBaseLocation, VALUES, onTrino, List.of, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeDatabricksSql, format, s, BY, getBaseLocation, VALUES, env.executeTrinoSql, List.of, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SELECT].
- Audit status: `verified`

##### `testDeleteOnPartitionKeyCompatibility`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java` ->
  `testDeleteOnPartitionKeyCompatibility`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `TestDeltaLakeWriteDatabricksCompatibilityDatabricks.testDeleteOnPartitionKeyCompatibility`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `format`, `s`, `BY`, `getBaseLocation`, `VALUES`, `onTrino`, `List.of`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeDatabricksSql`, `format`, `s`, `BY`, `getBaseLocation`, `VALUES`, `env.executeTrinoSql`, `List.of`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, format, s, BY, getBaseLocation, VALUES, onTrino, List.of, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeDatabricksSql, format, s, BY, getBaseLocation, VALUES, env.executeTrinoSql, List.of, dropDeltaTableWithRetry]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, format, s, BY, getBaseLocation, VALUES, onTrino, List.of, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeDatabricksSql, format, s, BY, getBaseLocation, VALUES, env.executeTrinoSql, List.of, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SELECT].
- Audit status: `verified`

##### `testCaseUpdateInPartition`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java` ->
  `testCaseUpdateInPartition`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `TestDeltaLakeWriteDatabricksCompatibilityDatabricks.testCaseUpdateInPartition`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `SET`, `aseTestTable`. Current setup shape: `SET`, `aseTestTable`.
- Action parity: Legacy action shape: `UPDATE`, `List.of`, `onTrino`, `executeQuery`, `format`, `table.name`, `table.rows`, `map`, `row.lower`, `row.withUpper`. Current action shape: `UPDATE`, `List.of`, `testRow`, `env.executeTrinoSql`, `format`, `table.name`, `table.rows`, `map`, `testRow.lower`, `testRow.withUpper`.
- Assertion parity: Legacy assertion helpers: `row`, `assertTable`. Current assertion helpers: `assertTable`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [aseTestTable, List.of, onTrino, executeQuery, format, table.name, table.rows, map, row.lower, row.withUpper] vs current [aseTestTable, List.of, testRow, env.executeTrinoSql, format, table.name, table.rows, map, testRow.lower, testRow.withUpper]; assertion helpers differ: legacy [row, assertTable] vs current [assertTable]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [aseTestTable, List.of, onTrino, executeQuery, format, table.name, table.rows, map, row.lower, row.withUpper], verbs [UPDATE, SET]. Current flow summary -> helpers [aseTestTable, List.of, testRow, env.executeTrinoSql, format, table.name, table.rows, map, testRow.lower, testRow.withUpper], verbs [UPDATE, SET].
- Audit status: `verified`

##### `testCaseUpdatePartitionColumnFails`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java` ->
  `testCaseUpdatePartitionColumnFails`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `TestDeltaLakeWriteDatabricksCompatibilityDatabricks.testCaseUpdatePartitionColumnFails`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `SET`, `aseTestTable`. Current setup shape: `SET`, `aseTestTable`.
- Action parity: Legacy action shape: `UPDATE`, `List.of`, `onTrino`, `executeQuery`, `format`, `table.name`, `table.rows`, `map`, `row.withPartition`. Current action shape: `UPDATE`, `List.of`, `testRow`, `env.executeTrinoSql`, `format`, `table.name`, `table.rows`, `map`, `testRow.withPartition`.
- Assertion parity: Legacy assertion helpers: `row`, `assertTable`. Current assertion helpers: `assertTable`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [aseTestTable, List.of, onTrino, executeQuery, format, table.name, table.rows, map, row.withPartition] vs current [aseTestTable, List.of, testRow, env.executeTrinoSql, format, table.name, table.rows, map, testRow.withPartition]; assertion helpers differ: legacy [row, assertTable] vs current [assertTable]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [aseTestTable, List.of, onTrino, executeQuery, format, table.name, table.rows, map, row.withPartition], verbs [UPDATE, SET]. Current flow summary -> helpers [aseTestTable, List.of, testRow, env.executeTrinoSql, format, table.name, table.rows, map, testRow.withPartition], verbs [UPDATE, SET].
- Audit status: `verified`

##### `testCaseDeletePartialPartition`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java` ->
  `testCaseDeletePartialPartition`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `TestDeltaLakeWriteDatabricksCompatibilityDatabricks.testCaseDeletePartialPartition`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `aseTestTable`. Current setup shape: `aseTestTable`.
- Action parity: Legacy action shape: `List.of`, `onTrino`, `executeQuery`, `format`, `table.name`, `table.rows`, `filter`, `not`, `row.lower`. Current action shape: `List.of`, `testRow`, `env.executeTrinoSql`, `format`, `table.name`, `table.rows`, `filter`, `not`, `testRow.lower`.
- Assertion parity: Legacy assertion helpers: `row`, `assertTable`. Current assertion helpers: `assertTable`.
- Cleanup parity: Legacy cleanup shape: `DELETE`. Current cleanup shape: `DELETE`.
- Any observed difference, however small: helper calls differ: legacy [aseTestTable, List.of, onTrino, executeQuery, format, table.name, table.rows, filter, not, row.lower] vs current [aseTestTable, List.of, testRow, env.executeTrinoSql, format, table.name, table.rows, filter, not, testRow.lower]; assertion helpers differ: legacy [row, assertTable] vs current [assertTable]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [aseTestTable, List.of, onTrino, executeQuery, format, table.name, table.rows, filter, not, row.lower], verbs [DELETE]. Current flow summary -> helpers [aseTestTable, List.of, testRow, env.executeTrinoSql, format, table.name, table.rows, filter, not, testRow.lower], verbs [DELETE].
- Audit status: `verified`

##### `testCaseDeleteEntirePartition`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java` ->
  `testCaseDeleteEntirePartition`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `TestDeltaLakeWriteDatabricksCompatibilityDatabricks.testCaseDeleteEntirePartition`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `aseTestTable`. Current setup shape: `aseTestTable`.
- Action parity: Legacy action shape: `List.of`, `onTrino`, `executeQuery`, `format`, `table.name`, `table.rows`, `filter`, `not`, `row.partition`. Current action shape: `List.of`, `testRow`, `env.executeTrinoSql`, `format`, `table.name`, `table.rows`, `filter`, `not`, `testRow.partition`.
- Assertion parity: Legacy assertion helpers: `row`, `assertTable`. Current assertion helpers: `assertTable`.
- Cleanup parity: Legacy cleanup shape: `DELETE`. Current cleanup shape: `DELETE`.
- Any observed difference, however small: helper calls differ: legacy [aseTestTable, List.of, onTrino, executeQuery, format, table.name, table.rows, filter, not, row.partition] vs current [aseTestTable, List.of, testRow, env.executeTrinoSql, format, table.name, table.rows, filter, not, testRow.partition]; assertion helpers differ: legacy [row, assertTable] vs current [assertTable]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [aseTestTable, List.of, onTrino, executeQuery, format, table.name, table.rows, filter, not, row.partition], verbs [DELETE]. Current flow summary -> helpers [aseTestTable, List.of, testRow, env.executeTrinoSql, format, table.name, table.rows, filter, not, testRow.partition], verbs [DELETE].
- Audit status: `verified`

##### `testTrinoRespectsDatabricksSettingNonNullableColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java` ->
  `testTrinoRespectsDatabricksSettingNonNullableColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `TestDeltaLakeWriteDatabricksCompatibilityDatabricks.testTrinoRespectsDatabricksSettingNonNullableColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `format`, `s`, `getBaseLocation`, `VALUES`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeDatabricksSql`, `format`, `s`, `getBaseLocation`, `VALUES`, `env.executeTrinoSql`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageContaining`, `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, format, s, getBaseLocation, VALUES, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeDatabricksSql, format, s, getBaseLocation, VALUES, env.executeTrinoSql, dropDeltaTableWithRetry]; assertion helpers differ: legacy [assertQueryFailure, hasMessageContaining, assertThat, containsOnly, row] vs current [assertThatThrownBy, hasMessageContaining, assertThat, containsOnly, row]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, format, s, getBaseLocation, VALUES, onTrino, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeDatabricksSql, format, s, getBaseLocation, VALUES, env.executeTrinoSql, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT].
- Audit status: `verified`

##### `testDatabricksRespectsTrinoSettingNonNullableColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java` ->
  `testDatabricksRespectsTrinoSettingNonNullableColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `TestDeltaLakeWriteDatabricksCompatibilityDatabricks.testDatabricksRespectsTrinoSettingNonNullableColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `VALUES`, `onDelta`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoSql`, `WITH`, `env.getBucketName`, `VALUES`, `env.executeDatabricksSql`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageContaining`, `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, VALUES, onDelta] vs current [randomNameSuffix, env.executeTrinoSql, WITH, env.getBucketName, VALUES, env.executeDatabricksSql]; assertion helpers differ: legacy [assertQueryFailure, hasMessageContaining, assertThat, containsOnly, row] vs current [assertThatThrownBy, hasMessageContaining, assertThat, containsOnly, row]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, VALUES, onDelta], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoSql, WITH, env.getBucketName, VALUES, env.executeDatabricksSql], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testInsertingIntoDatabricksTableWithAddedNotNullConstraint`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java` ->
  `testInsertingIntoDatabricksTableWithAddedNotNullConstraint`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `TestDeltaLakeWriteDatabricksCompatibilityDatabricks.testInsertingIntoDatabricksTableWithAddedNotNullConstraint`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`. Current setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `format`, `s`, `getBaseLocation`, `onTrino`, `VALUES`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeDatabricksSql`, `format`, `s`, `getBaseLocation`, `env.executeTrinoSql`, `VALUES`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageContaining`, `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, format, s, getBaseLocation, onTrino, VALUES, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeDatabricksSql, format, s, getBaseLocation, env.executeTrinoSql, VALUES, dropDeltaTableWithRetry]; assertion helpers differ: legacy [assertQueryFailure, hasMessageContaining, assertThat, containsOnly, row] vs current [assertThatThrownBy, hasMessageContaining, assertThat, containsOnly, row]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, format, s, getBaseLocation, onTrino, VALUES, dropDeltaTableWithRetry], verbs [CREATE, INSERT, ALTER, SET, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeDatabricksSql, format, s, getBaseLocation, env.executeTrinoSql, VALUES, dropDeltaTableWithRetry], verbs [CREATE, INSERT, ALTER, SET, SELECT].
- Audit status: `verified`

##### `testTrinoVacuumRemoveChangeDataFeedFiles`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java` ->
  `testTrinoVacuumRemoveChangeDataFeedFiles`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `TestDeltaLakeWriteDatabricksCompatibilityDatabricks.testTrinoVacuumRemoveChangeDataFeedFiles`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133`; `SuiteDeltaLakeDatabricks173` excludes this method via `DeltaLakeExclude173`.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `DeltaLakeExclude173`, `ProfileSpecificTests`.
  Tag routing matches the shared Databricks suite selection in the 133 lane, while the 173 lane excludes this method via `DeltaLakeExclude173`.
- Setup parity: Legacy setup shape: `SET`. Current setup shape: `SET`.
- Action parity: Legacy action shape: `INSERT`, `UPDATE`, `CALL`, `VACUUM`, `testVacuumRemoveChangeDataFeedFiles`, `listObjects`, `onTrino`, `onDelta`. Current action shape: `INSERT`, `UPDATE`, `CALL`, `VACUUM`, `testVacuumRemoveChangeDataFeedFiles`, `listObjects`, `env.executeTrinoSqlStatements`, `env.executeDatabricksSql`, `listObjects`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `hasSize`, `hasSizeBetween`, `endsWith`, `isEqualTo`, `isTrue`. Current assertion helpers: `assertThat`, `hasSize`, `hasSizeBetween`, `endsWith`, `isEqualTo`, `isTrue`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the shared helper is semantically unchanged; it still creates a CDF-enabled Databricks table, inserts and updates one row to produce change-data files, executes the caller-supplied vacuum path, and verifies `_change_data` is empty or reduced to the runtime-specific zero-byte directory marker. The JUnit port wraps the Trino vacuum path in `env.executeTrinoSqlStatements`.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: This method supplies a Trino-side vacuum executor to the shared helper. The helper creates the CDF table, verifies one change-data file exists after the update, runs vacuum, checks the remaining `_change_data` objects, and drops the table.
- Audit status: `verified`

##### `testDatabricksVacuumRemoveChangeDataFeedFiles`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibility.java` ->
  `testDatabricksVacuumRemoveChangeDataFeedFiles`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeWriteDatabricksCompatibilityDatabricks.java` ->
  `TestDeltaLakeWriteDatabricksCompatibilityDatabricks.testDatabricksVacuumRemoveChangeDataFeedFiles`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `SET`. Current setup shape: `SET`.
- Action parity: Legacy action shape: `INSERT`, `UPDATE`, `VACUUM`, `testVacuumRemoveChangeDataFeedFiles`, `listObjects`, `onDelta`. Current action shape: `INSERT`, `UPDATE`, `VACUUM`, `testVacuumRemoveChangeDataFeedFiles`, `listObjects`, `env.executeDatabricksSqlStatements`, `env.executeDatabricksSql`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `hasSize`, `hasSizeBetween`, `endsWith`, `isEqualTo`, `isTrue`. Current assertion helpers: `assertThat`, `hasSize`, `hasSizeBetween`, `endsWith`, `isEqualTo`, `isTrue`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the shared helper is semantically unchanged; it still creates a CDF-enabled Databricks table, inserts and updates one row to produce change-data files, executes Databricks vacuum, and verifies `_change_data` is empty or reduced to the runtime-specific zero-byte directory marker. The JUnit port routes the Databricks SQL batch through `env.executeDatabricksSqlStatements`.
- Known intentional difference: None.
- Reviewer note: This method supplies a Databricks-side vacuum executor to the shared helper. The helper creates the CDF table, verifies one change-data file exists after the update, runs vacuum, checks the remaining `_change_data` objects, and drops the table.
- Audit status: `verified`

### `TestHiveAndDeltaLakeRedirect`


- Owning migration commit: `Migrate TestHiveAndDeltaLakeRedirect to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java`
- Class-level environment requirement: `HiveDeltaLakeMinioEnvironment`.
- Class-level tags: `DeltaLakeOss`.
- Method inventory complete: Yes. Legacy methods: `30`. Current methods: `30`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testHiveToDeltaRedirect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testHiveToDeltaRedirect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testHiveToDeltaRedirect`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createTableOnDelta`. Current setup shape: `createTableOnDelta`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `onTrino`, `format`, `hiveResult.rows`, `stream`, `map`, `collect`, `toImmutableList`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.executeSpark`, `env.executeTrino`, `format`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`. Current assertion helpers: `assertResultsEqual`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, onTrino, format, hiveResult.rows, stream, map, collect, toImmutableList, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, env.executeSpark, env.executeTrino, format]; SQL verbs differ: legacy [SELECT] vs current [SELECT, DROP]; assertion helpers differ: legacy [assertThat, containsOnly] vs current [assertResultsEqual]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, onTrino, format, hiveResult.rows, stream, map, collect, toImmutableList, dropDeltaTableWithRetry], verbs [SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, env.executeSpark, env.executeTrino, format], verbs [SELECT, DROP].
- Audit status: `verified`

##### `testHiveToDeltaNonDefaultSchemaRedirect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testHiveToDeltaNonDefaultSchemaRedirect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testHiveToDeltaNonDefaultSchemaRedirect`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `createTableOnDelta`. Current setup shape: `CREATE`, `createTableOnDelta`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `format`, `onDelta`, `executeQuery`, `onTrino`, `hiveResult.rows`, `stream`, `map`, `collect`, `toImmutableList`. Current action shape: `SELECT`, `randomNameSuffix`, `format`, `env.getBucketName`, `env.executeSparkUpdate`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`. Current assertion helpers: `assertResultsEqual`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, format, onDelta, executeQuery, createTableOnDelta, onTrino, hiveResult.rows, stream, map, collect, toImmutableList, dropDeltaTableWithRetry] vs current [randomNameSuffix, format, env.getBucketName, env.executeSparkUpdate, createTableOnDelta, env.executeSpark, env.executeTrino]; assertion helpers differ: legacy [assertThat, containsOnly] vs current [assertResultsEqual]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, format, onDelta, executeQuery, createTableOnDelta, onTrino, hiveResult.rows, stream, map, collect, toImmutableList, dropDeltaTableWithRetry], verbs [CREATE, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, format, env.getBucketName, env.executeSparkUpdate, createTableOnDelta, env.executeSpark, env.executeTrino], verbs [CREATE, SELECT, DROP].
- Audit status: `verified`

##### `testHiveToNonexistentDeltaCatalogRedirectFailure`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testHiveToNonexistentDeltaCatalogRedirectFailure`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testHiveToNonexistentDeltaCatalogRedirectFailure`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `SET`, `createTableOnDelta`. Current setup shape: `SET`, `createTableOnDelta`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `onTrino`, `format`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.executeTrinoInSession`, `session.executeUpdate`, `session.executeQuery`, `format`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`, `hasMessageMatching`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, onTrino, format, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, env.executeTrinoInSession, session.executeUpdate, session.executeQuery, format]; SQL verbs differ: legacy [SET, SELECT] vs current [SET, SELECT, DROP]; assertion helpers differ: legacy [assertQueryFailure, hasMessageMatching] vs current [assertThatThrownBy, hasMessageMatching]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, onTrino, format, dropDeltaTableWithRetry], verbs [SET, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, env.executeTrinoInSession, session.executeUpdate, session.executeQuery, format], verbs [SET, SELECT, DROP].
- Audit status: `verified`

##### `testHiveToDeltaRedirectWithDefaultSchemaInSession`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testHiveToDeltaRedirectWithDefaultSchemaInSession`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testHiveToDeltaRedirectWithDefaultSchemaInSession`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `USE`, `createTableOnDelta`. Current setup shape: `USE`, `createTableOnDelta`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `onTrino`, `format`, `hiveResult.rows`, `stream`, `map`, `collect`, `toImmutableList`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.executeTrinoInSession`, `session.executeUpdate`, `env.executeSpark`, `session.executeQuery`, `format`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`. Current assertion helpers: `assertResultsEqual`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, onTrino, format, hiveResult.rows, stream, map, collect, toImmutableList, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, env.executeTrinoInSession, session.executeUpdate, env.executeSpark, session.executeQuery, format]; SQL verbs differ: legacy [USE, SELECT] vs current [USE, SELECT, DROP]; assertion helpers differ: legacy [assertThat, containsOnly] vs current [assertResultsEqual]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, onTrino, format, hiveResult.rows, stream, map, collect, toImmutableList, dropDeltaTableWithRetry], verbs [USE, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, env.executeTrinoInSession, session.executeUpdate, env.executeSpark, session.executeQuery, format], verbs [USE, SELECT, DROP].
- Audit status: `verified`

##### `testHiveToUnpartitionedDeltaPartitionsRedirect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testHiveToUnpartitionedDeltaPartitionsRedirect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testHiveToUnpartitionedDeltaPartitionsRedirect`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createTableOnDelta`. Current setup shape: `createTableOnDelta`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `onTrino`, `format`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.executeTrino`, `format`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `hasRowsCount`. Current assertion helpers: `assertThat`, `hasRowsCount`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, onTrino, format, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, env.executeTrino, format]; SQL verbs differ: legacy [SELECT] vs current [SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, onTrino, format, dropDeltaTableWithRetry], verbs [SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, env.executeTrino, format], verbs [SELECT, DROP].
- Audit status: `verified`

##### `testDeltaToHiveRedirect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testDeltaToHiveRedirect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testDeltaToHiveRedirect`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createTableInHiveConnector`. Current setup shape: `createTableInHiveConnector`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `ImmutableList.of`, `format`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `ImmutableList.of`, `env.executeTrino`, `format`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, ImmutableList.of, format] vs current [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, ImmutableList.of, env.executeTrino, format]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, ImmutableList.of, format], verbs [SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, ImmutableList.of, env.executeTrino, format], verbs [SELECT, DROP].
- Audit status: `verified`

##### `testDeltaToHiveNonDefaultSchemaRedirect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testDeltaToHiveNonDefaultSchemaRedirect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testDeltaToHiveNonDefaultSchemaRedirect`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `createTableInHiveConnector`. Current setup shape: `CREATE`, `createTableInHiveConnector`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `format`, `onTrino`, `executeQuery`, `WITH`, `ImmutableList.of`. Current action shape: `SELECT`, `randomNameSuffix`, `format`, `env.getBucketName`, `env.executeTrinoUpdate`, `WITH`, `ImmutableList.of`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, format, onTrino, executeQuery, WITH, createTableInHiveConnector, ImmutableList.of] vs current [randomNameSuffix, format, env.getBucketName, env.executeTrinoUpdate, WITH, createTableInHiveConnector, ImmutableList.of, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, format, onTrino, executeQuery, WITH, createTableInHiveConnector, ImmutableList.of], verbs [CREATE, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, format, env.getBucketName, env.executeTrinoUpdate, WITH, createTableInHiveConnector, ImmutableList.of, env.executeTrino], verbs [CREATE, SELECT, DROP].
- Audit status: `verified`

##### `testDeltaToNonexistentHiveCatalogRedirectFailure`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testDeltaToNonexistentHiveCatalogRedirectFailure`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testDeltaToNonexistentHiveCatalogRedirectFailure`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `SET`, `createTableInHiveConnector`. Current setup shape: `SET`, `createTableInHiveConnector`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `format`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `env.executeTrinoInSession`, `session.executeUpdate`, `session.executeQuery`, `format`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`, `hasMessageMatching`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, format] vs current [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, env.executeTrinoInSession, session.executeUpdate, session.executeQuery, format]; assertion helpers differ: legacy [assertQueryFailure, hasMessageMatching] vs current [assertThatThrownBy, hasMessageMatching]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, format], verbs [SET, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, env.executeTrinoInSession, session.executeUpdate, session.executeQuery, format], verbs [SET, SELECT, DROP].
- Audit status: `verified`

##### `testDeltaToHiveRedirectWithDefaultSchemaInSession`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testDeltaToHiveRedirectWithDefaultSchemaInSession`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testDeltaToHiveRedirectWithDefaultSchemaInSession`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `USE`, `createTableInHiveConnector`. Current setup shape: `USE`, `createTableInHiveConnector`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `ImmutableList.of`, `format`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `env.executeTrinoInSession`, `session.executeUpdate`, `ImmutableList.of`, `session.executeQuery`, `format`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, ImmutableList.of, format] vs current [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, env.executeTrinoInSession, session.executeUpdate, ImmutableList.of, session.executeQuery, format]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, ImmutableList.of, format], verbs [USE, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, env.executeTrinoInSession, session.executeUpdate, ImmutableList.of, session.executeQuery, format], verbs [USE, SELECT, DROP].
- Audit status: `verified`

##### `testDeltaToPartitionedHivePartitionsRedirect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testDeltaToPartitionedHivePartitionsRedirect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testDeltaToPartitionedHivePartitionsRedirect`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createTableInHiveConnector`. Current setup shape: `createTableInHiveConnector`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `ImmutableList.of`, `format`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `ImmutableList.of`, `env.executeTrino`, `format`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, ImmutableList.of, format] vs current [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, ImmutableList.of, env.executeTrino, format]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, ImmutableList.of, format], verbs [SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, ImmutableList.of, env.executeTrino, format], verbs [SELECT, DROP].
- Audit status: `verified`

##### `testDeltaToUnpartitionedHivePartitionsRedirectFailure`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testDeltaToUnpartitionedHivePartitionsRedirectFailure`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testDeltaToUnpartitionedHivePartitionsRedirectFailure`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createTableInHiveConnector`. Current setup shape: `createTableInHiveConnector`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `format`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `env.executeTrino`, `format`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, format] vs current [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, env.executeTrino, format]; assertion helpers differ: legacy [assertQueryFailure, hasMessageMatching] vs current [assertThatThrownBy, hasMessageContaining]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, format], verbs [SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, env.executeTrino, format], verbs [SELECT, DROP].
- Audit status: `verified`

##### `testDeltaToHiveInsert`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testDeltaToHiveInsert`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testDeltaToHiveInsert`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `INSERT`, `createTableInHiveConnector`. Current setup shape: `INSERT`, `createTableInHiveConnector`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `VALUES`, `ImmutableList.of`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `format`, `VALUES`, `ImmutableList.of`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, format, VALUES, ImmutableList.of] vs current [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, format, VALUES, ImmutableList.of, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, format, VALUES, ImmutableList.of], verbs [INSERT, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, format, VALUES, ImmutableList.of, env.executeTrino], verbs [INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testHiveToDeltaInsert`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testHiveToDeltaInsert`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testHiveToDeltaInsert`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `INSERT`, `createTableOnDelta`. Current setup shape: `INSERT`, `createTableOnDelta`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `onTrino`, `format`, `VALUES`, `count`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.executeTrinoUpdate`, `format`, `VALUES`, `env.executeTrino`, `count`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, onTrino, format, VALUES, count, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, env.executeTrinoUpdate, format, VALUES, env.executeTrino, count]; SQL verbs differ: legacy [INSERT, SELECT] vs current [INSERT, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, onTrino, format, VALUES, count, dropDeltaTableWithRetry], verbs [INSERT, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, env.executeTrinoUpdate, format, VALUES, env.executeTrino, count], verbs [INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testDeltaToHiveDescribe`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testDeltaToHiveDescribe`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testDeltaToHiveDescribe`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createTableInHiveConnector`. Current setup shape: `createTableInHiveConnector`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `ImmutableList.of`, `format`. Current action shape: `randomNameSuffix`, `env.executeTrinoUpdate`, `ImmutableList.of`, `env.executeTrino`, `format`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, ImmutableList.of, format] vs current [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, ImmutableList.of, env.executeTrino, format]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, ImmutableList.of, format], verbs [DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, ImmutableList.of, env.executeTrino, format], verbs [DROP].
- Audit status: `verified`

##### `testHiveToDeltaDescribe`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testHiveToDeltaDescribe`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testHiveToDeltaDescribe`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `COMMENT`, `createTableOnDelta`. Current setup shape: `COMMENT`, `createTableOnDelta`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onDelta`, `executeQuery`, `ImmutableList.of`, `onTrino`, `format`. Current action shape: `randomNameSuffix`, `env.executeSparkUpdate`, `ImmutableList.of`, `env.executeTrino`, `format`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, ImmutableList.of, onTrino, format, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, ImmutableList.of, env.executeTrino, format]; SQL verbs differ: legacy [COMMENT] vs current [COMMENT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, ImmutableList.of, onTrino, format, dropDeltaTableWithRetry], verbs [COMMENT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, ImmutableList.of, env.executeTrino, format], verbs [COMMENT, DROP].
- Audit status: `verified`

##### `testDeltaToHiveShowCreateTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testDeltaToHiveShowCreateTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testDeltaToHiveShowCreateTable`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `createTableInHiveConnector`. Current setup shape: `CREATE`, `createTableInHiveConnector`.
- Action parity: Legacy action shape: `SHOW`, `randomNameSuffix`, `onTrino`, `executeQuery`, `format`. Current action shape: `SHOW`, `randomNameSuffix`, `env.executeTrinoUpdate`, `env.executeTrino`, `format`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `hasRowsCount`. Current assertion helpers: `assertThat`, `hasRowsCount`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, format] vs current [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, env.executeTrino, format]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, format], verbs [CREATE, SHOW, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, env.executeTrino, format], verbs [CREATE, SHOW, DROP].
- Audit status: `verified`

##### `testHiveToDeltaShowCreateTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testHiveToDeltaShowCreateTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testHiveToDeltaShowCreateTable`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `createTableOnDelta`. Current setup shape: `CREATE`, `createTableOnDelta`.
- Action parity: Legacy action shape: `SHOW`, `randomNameSuffix`, `onDelta`, `executeQuery`, `onTrino`, `format`. Current action shape: `SHOW`, `randomNameSuffix`, `env.executeSparkUpdate`, `env.executeTrino`, `format`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `hasRowsCount`. Current assertion helpers: `assertThat`, `hasRowsCount`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, onTrino, format, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, env.executeTrino, format]; SQL verbs differ: legacy [CREATE, SHOW] vs current [CREATE, SHOW, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, onTrino, format, dropDeltaTableWithRetry], verbs [CREATE, SHOW]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, env.executeTrino, format], verbs [CREATE, SHOW, DROP].
- Audit status: `verified`

##### `testDeltaToHiveAlterTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testDeltaToHiveAlterTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testDeltaToHiveAlterTable`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `ALTER`, `createTableInHiveConnector`. Current setup shape: `ALTER`, `createTableInHiveConnector`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`. Current action shape: `randomNameSuffix`, `env.executeTrinoUpdate`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertResultsEqual`. Current assertion helpers: `assertResultsEqual`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector] vs current [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector], verbs [ALTER, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, env.executeTrino], verbs [ALTER, DROP].
- Audit status: `verified`

##### `testHiveToDeltaAlterTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testHiveToDeltaAlterTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testHiveToDeltaAlterTable`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `ALTER`, `createTableOnDelta`. Current setup shape: `ALTER`, `createTableOnDelta`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onDelta`, `executeQuery`, `onTrino`. Current action shape: `randomNameSuffix`, `env.executeSparkUpdate`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, env.executeTrinoUpdate]; SQL verbs differ: legacy [ALTER] vs current [ALTER, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, onTrino, dropDeltaTableWithRetry], verbs [ALTER]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, env.executeTrinoUpdate], verbs [ALTER, DROP].
- Audit status: `verified`

##### `testDeltaToHiveCommentTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testDeltaToHiveCommentTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testDeltaToHiveCommentTable`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `COMMENT`, `createTableInHiveConnector`. Current setup shape: `COMMENT`, `createTableInHiveConnector`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `is`, `queryResult.getOnlyValue`, `format`. Current action shape: `randomNameSuffix`, `env.executeTrinoUpdate`, `format`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `assertTableComment`, `isEqualTo`. Current assertion helpers: `assertTableComment`, `isNull`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, is, queryResult.getOnlyValue, format] vs current [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, format]; SQL verbs differ: legacy [SELECT, COMMENT, DROP] vs current [COMMENT, DROP]; assertion helpers differ: legacy [assertThat, assertTableComment, isEqualTo] vs current [assertTableComment, isNull, isEqualTo]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, is, queryResult.getOnlyValue, format], verbs [SELECT, COMMENT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, format], verbs [COMMENT, DROP].
- Audit status: `verified`

##### `testHiveToDeltaCommentTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testHiveToDeltaCommentTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testHiveToDeltaCommentTable`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `COMMENT`, `createTableOnDelta`. Current setup shape: `COMMENT`, `createTableOnDelta`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `onTrino`, `is`, `queryResult.getOnlyValue`, `format`. Current action shape: `randomNameSuffix`, `env.executeSparkUpdate`, `env.executeTrinoUpdate`, `format`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `assertTableComment`, `isEqualTo`. Current assertion helpers: `assertTableComment`, `isNull`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, onTrino, is, queryResult.getOnlyValue, format, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, env.executeTrinoUpdate, format]; SQL verbs differ: legacy [SELECT, COMMENT] vs current [COMMENT, DROP]; assertion helpers differ: legacy [assertThat, assertTableComment, isEqualTo] vs current [assertTableComment, isNull, isEqualTo]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, onTrino, is, queryResult.getOnlyValue, format, dropDeltaTableWithRetry], verbs [SELECT, COMMENT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, env.executeTrinoUpdate, format], verbs [COMMENT, DROP].
- Audit status: `verified`

##### `testDeltaToHiveCommentColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testDeltaToHiveCommentColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testDeltaToHiveCommentColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `COMMENT`, `createTableInHiveConnector`. Current setup shape: `COMMENT`, `createTableInHiveConnector`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `format`. Current action shape: `randomNameSuffix`, `env.executeTrinoUpdate`, `format`.
- Assertion parity: Legacy assertion helpers: `assertColumnComment`, `isNull`, `isEqualTo`. Current assertion helpers: `assertColumnComment`, `isNull`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, format] vs current [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, format]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, createTableInHiveConnector, format], verbs [COMMENT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, createTableInHiveConnector, format], verbs [COMMENT, DROP].
- Audit status: `verified`

##### `testHiveToDeltaCommentColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testHiveToDeltaCommentColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testHiveToDeltaCommentColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `COMMENT`, `createTableOnDelta`. Current setup shape: `COMMENT`, `createTableOnDelta`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onDelta`, `executeQuery`, `onTrino`, `format`. Current action shape: `randomNameSuffix`, `env.executeSparkUpdate`, `env.executeTrinoUpdate`, `format`.
- Assertion parity: Legacy assertion helpers: `assertColumnComment`, `isNull`, `isEqualTo`. Current assertion helpers: `assertColumnComment`, `isNull`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, onTrino, format, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, env.executeTrinoUpdate, format]; SQL verbs differ: legacy [COMMENT] vs current [COMMENT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, createTableOnDelta, onTrino, format, dropDeltaTableWithRetry], verbs [COMMENT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, createTableOnDelta, env.executeTrinoUpdate, format], verbs [COMMENT, DROP].
- Audit status: `verified`

##### `testInsertIntoDeltaTableFromHiveNonDefaultSchemaRedirect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testInsertIntoDeltaTableFromHiveNonDefaultSchemaRedirect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testInsertIntoDeltaTableFromHiveNonDefaultSchemaRedirect`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `createTableOnDelta`. Current setup shape: `CREATE`, `INSERT`, `createTableOnDelta`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `format`, `onDelta`, `executeQuery`, `onTrino`, `VALUES`, `builder`, `add`, `ow`, `build`. Current action shape: `SELECT`, `randomNameSuffix`, `format`, `env.getBucketName`, `env.executeSparkUpdate`, `env.executeTrinoUpdate`, `VALUES`, `env.executeTrino`, `builder`, `add`, `build`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, format, onDelta, executeQuery, createTableOnDelta, onTrino, VALUES, builder, add, ow, build, dropDeltaTableWithRetry] vs current [randomNameSuffix, format, env.getBucketName, env.executeSparkUpdate, createTableOnDelta, env.executeTrinoUpdate, VALUES, env.executeTrino, builder, add, build]; assertion helpers differ: legacy [assertThat, containsOnly] vs current [row, assertThat, containsOnly]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, format, onDelta, executeQuery, createTableOnDelta, onTrino, VALUES, builder, add, ow, build, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, format, env.getBucketName, env.executeSparkUpdate, createTableOnDelta, env.executeTrinoUpdate, VALUES, env.executeTrino, builder, add, build], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testInformationSchemaColumnsHiveToDeltaRedirect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testInformationSchemaColumnsHiveToDeltaRedirect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testInformationSchemaColumnsHiveToDeltaRedirect`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`, `createTableOnDelta`. Current setup shape: `CREATE`, `COMMENT`, `createTableOnDelta`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `format`, `onTrino`, `executeQuery`, `WITH`, `onDelta`. Current action shape: `SELECT`, `randomNameSuffix`, `format`, `env.getBucketName`, `env.executeTrinoUpdate`, `WITH`, `env.executeSparkUpdate`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, format, onTrino, executeQuery, WITH, onDelta, createTableOnDelta, dropDeltaTableWithRetry] vs current [randomNameSuffix, format, env.getBucketName, env.executeTrinoUpdate, WITH, env.executeSparkUpdate, createTableOnDelta, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, format, onTrino, executeQuery, WITH, onDelta, createTableOnDelta, dropDeltaTableWithRetry], verbs [CREATE, SELECT, COMMENT, DROP]. Current flow summary -> helpers [randomNameSuffix, format, env.getBucketName, env.executeTrinoUpdate, WITH, env.executeSparkUpdate, createTableOnDelta, env.executeTrino], verbs [CREATE, SELECT, COMMENT, DROP].
- Audit status: `verified`

##### `testInformationSchemaColumnsDeltaToHiveRedirect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testInformationSchemaColumnsDeltaToHiveRedirect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testInformationSchemaColumnsDeltaToHiveRedirect`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `createTableInHiveConnector`. Current setup shape: `CREATE`, `createTableInHiveConnector`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `format`, `onTrino`, `executeQuery`, `WITH`. Current action shape: `SELECT`, `randomNameSuffix`, `format`, `env.getBucketName`, `env.executeTrinoUpdate`, `WITH`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, format, onTrino, executeQuery, WITH, createTableInHiveConnector] vs current [randomNameSuffix, format, env.getBucketName, env.executeTrinoUpdate, WITH, createTableInHiveConnector, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, format, onTrino, executeQuery, WITH, createTableInHiveConnector], verbs [CREATE, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, format, env.getBucketName, env.executeTrinoUpdate, WITH, createTableInHiveConnector, env.executeTrino], verbs [CREATE, SELECT, DROP].
- Audit status: `verified`

##### `testSystemJdbcColumnsHiveToDeltaRedirect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testSystemJdbcColumnsHiveToDeltaRedirect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testSystemJdbcColumnsHiveToDeltaRedirect`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`, `createTableOnDelta`. Current setup shape: `CREATE`, `COMMENT`, `createTableOnDelta`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `format`, `onTrino`, `executeQuery`, `WITH`, `onDelta`. Current action shape: `SELECT`, `randomNameSuffix`, `format`, `env.getBucketName`, `env.executeTrinoUpdate`, `WITH`, `env.executeSparkUpdate`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, format, onTrino, executeQuery, WITH, onDelta, createTableOnDelta, dropDeltaTableWithRetry] vs current [randomNameSuffix, format, env.getBucketName, env.executeTrinoUpdate, WITH, env.executeSparkUpdate, createTableOnDelta, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, format, onTrino, executeQuery, WITH, onDelta, createTableOnDelta, dropDeltaTableWithRetry], verbs [CREATE, SELECT, COMMENT, DROP]. Current flow summary -> helpers [randomNameSuffix, format, env.getBucketName, env.executeTrinoUpdate, WITH, env.executeSparkUpdate, createTableOnDelta, env.executeTrino], verbs [CREATE, SELECT, COMMENT, DROP].
- Audit status: `verified`

##### `testSystemJdbcColumnsDeltaToHiveRedirect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testSystemJdbcColumnsDeltaToHiveRedirect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testSystemJdbcColumnsDeltaToHiveRedirect`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `createTableInHiveConnector`. Current setup shape: `CREATE`, `createTableInHiveConnector`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `format`, `onTrino`, `executeQuery`, `WITH`. Current action shape: `SELECT`, `randomNameSuffix`, `format`, `env.getBucketName`, `env.executeTrinoUpdate`, `WITH`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, format, onTrino, executeQuery, WITH, createTableInHiveConnector] vs current [randomNameSuffix, format, env.getBucketName, env.executeTrinoUpdate, WITH, createTableInHiveConnector, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, format, onTrino, executeQuery, WITH, createTableInHiveConnector], verbs [CREATE, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, format, env.getBucketName, env.executeTrinoUpdate, WITH, createTableInHiveConnector, env.executeTrino], verbs [CREATE, SELECT, DROP].
- Audit status: `verified`

##### `testViewReferencingHiveAndDeltaTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testViewReferencingHiveAndDeltaTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testViewReferencingHiveAndDeltaTable`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `SET`. Current setup shape: `CREATE`, `SET`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `Language`, `CAST`, `decimal`, `timestamp`, `onTrino`, `executeQuery`, `WITH`, `locationForTable`, `onDelta`, `data`, `List.of`, `igDecimal`, `Date.valueOf`, `LocalDate.of`, `failed`. Current action shape: `SELECT`, `randomNameSuffix`, `CAST`, `decimal`, `timestamp`, `env.executeTrinoUpdate`, `WITH`, `locationForTable`, `env.executeSparkUpdate`, `data`, `List.of`, `igDecimal`, `Date.valueOf`, `LocalDate.of`, `env.executeSpark`, `env.executeTrinoInSession`, `session.executeUpdate`, `session.executeQuery`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`, `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `row`, `assertThat`, `containsOnly`, `assertThatThrownBy`, `hasMessageMatching`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, Language, CAST, decimal, timestamp, onTrino, executeQuery, WITH, locationForTable, onDelta, data, List.of, igDecimal, Date.valueOf, LocalDate.of, failed, dropDeltaTableWithRetry] vs current [randomNameSuffix, CAST, decimal, timestamp, env.executeTrinoUpdate, WITH, locationForTable, env.executeSparkUpdate, data, List.of, igDecimal, Date.valueOf, LocalDate.of, env.executeSpark, env.executeTrinoInSession, session.executeUpdate, session.executeQuery, env.executeTrino]; assertion helpers differ: legacy [row, assertThat, containsOnly, assertQueryFailure, hasMessageMatching] vs current [row, assertThat, containsOnly, assertThatThrownBy, hasMessageMatching]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, Language, CAST, decimal, timestamp, onTrino, executeQuery, WITH, locationForTable, onDelta, data, List.of, igDecimal, Date.valueOf, LocalDate.of, failed, dropDeltaTableWithRetry], verbs [SELECT, CREATE, SET, DROP]. Current flow summary -> helpers [randomNameSuffix, CAST, decimal, timestamp, env.executeTrinoUpdate, WITH, locationForTable, env.executeSparkUpdate, data, List.of, igDecimal, Date.valueOf, LocalDate.of, env.executeSpark, env.executeTrinoInSession, session.executeUpdate, session.executeQuery, env.executeTrino], verbs [SELECT, CREATE, SET, DROP].
- Audit status: `verified`

##### `testHiveToDeltaPropertiesRedirect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `testHiveToDeltaPropertiesRedirect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestHiveAndDeltaLakeRedirect.java` ->
  `TestHiveAndDeltaLakeRedirect.testHiveToDeltaPropertiesRedirect`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveDeltaLakeMinioEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 3.
- Tag parity: Current tags: `DeltaLakeOss`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `locationForTable`, `List.of`, `onTrino`, `format`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `locationForTable`, `List.of`, `env.executeTrino`, `format`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, locationForTable, List.of, onTrino, format, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeSparkUpdate, locationForTable, List.of, env.executeTrino, format]; SQL verbs differ: legacy [CREATE, SELECT] vs current [CREATE, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, locationForTable, List.of, onTrino, format, dropDeltaTableWithRetry], verbs [CREATE, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, locationForTable, List.of, env.executeTrino, format], verbs [CREATE, SELECT, DROP].
- Audit status: `verified`

### `BaseTestDeltaLakeMinioReadsJunit`


- Owning migration commit: `Migrate TestDeltaLakeDatabricksMinioReads to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/BaseTestDeltaLakeMinioReadsJunit.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksMinioReads.java`
- Class-level environment requirement: none.
- Class-level tags: none.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testReadRegionTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksMinioReads.java` ->
  `testReadRegionTable`
- Additional legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/BaseTestDeltaLakeMinioReads.java` ->
  `testReadRegionTable`
- Additional legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeOssDeltaLakeMinioReads.java` ->
  `testReadRegionTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/BaseTestDeltaLakeMinioReadsJunit.java` ->
  `BaseTestDeltaLakeMinioReadsJunit.testReadRegionTable`
- Mapping type: `direct`
- Environment parity: Current class does not declare a concrete environment requirement. Routed by source review into `SuiteDeltaLakeOss` run 1.
- Tag parity: Current tags: `DeltaLakeMinio`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `CALL`, `SELECT`, `assertNotificationsCount`, `system.register_table`, `count`, `onTrino`. Current action shape: `CALL`, `SELECT`, `assertEventCountEventually`, `env.executeTrinoUpdate`, `format`, `tableName`, `system.register_table`, `env.getBucketName`, `env.executeTrino`, `count`, `expectedRegionParquetObjectName`.
- Assertion parity: Legacy assertion helpers: `assertNotificationsCount`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `assertEventCountEventually`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: `DROP`.
- Any observed difference, however small: the current base helper adds a defensive `DROP TABLE IF EXISTS` before registration, explicitly drops the table in `finally`, and broadens notification checks to include a GET on the expected parquet object in addition to the legacy log-file notifications. The legacy base relied on Tempto notification-table assertions instead of the current in-memory Minio event capture.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: The legacy base registered the copied region table, counted five rows through Trino, and asserted Minio notification-table entries for the transaction log before dropping the table. The current base performs the same registration and count, then waits for in-memory Minio notifications for both the log objects and the expected parquet object, and drops the table in `finally`.
- Audit status: `verified`

### `TestDeltaLakeDatabricksMinioReads`


- Owning migration commit: `Migrate TestDeltaLakeDatabricksMinioReads to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksMinioReads.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksMinioReads.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: none.
- Method inventory complete: Yes. Legacy/local test methods: `1`. Current/local test methods: `0`. Current class supplies resource/object-name overrides and inherits the audited `testReadRegionTable` implementation from `BaseTestDeltaLakeMinioReadsJunit`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly. Cross-class or merged legacy
  coverage accounted for: `testReadRegionTable` -> covered in shared base
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/BaseTestDeltaLakeMinioReadsJunit.java` ->
  `testReadRegionTable`.
- Intentional differences summary: None identified at class scope beyond the inherited base-class notes.
- Method statuses present: inherited `verified`.

#### Methods

### `TestDeltaLakeOssDeltaLakeMinioReads`


- Owning migration commit: `Migrate TestDeltaLakeOssDeltaLakeMinioReads to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeOssDeltaLakeMinioReads.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeOssDeltaLakeMinioReads.java`
- Class-level environment requirement: `DeltaLakeMinioEnvironment`.
- Class-level tags: none.
- Method inventory complete: Yes. Legacy/local test methods: `1`. Current/local test methods: `0`. Current class supplies resource/object-name overrides and inherits the audited `testReadRegionTable` implementation from `BaseTestDeltaLakeMinioReadsJunit`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly. Cross-class or merged legacy
  coverage accounted for: `testReadRegionTable` -> covered in shared base
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/BaseTestDeltaLakeMinioReadsJunit.java` ->
  `testReadRegionTable`.
- Intentional differences summary: None identified at class scope beyond the inherited base-class notes.
- Method statuses present: inherited `verified`.

#### Methods

### `BaseTestDeltaLakeHdfsReadsJunit`


- Owning migration commit: `Migrate TestDeltaLakeDatabricksHdfsReads to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/BaseTestDeltaLakeHdfsReadsJunit.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/BaseTestDeltaLakeHdfsReads.java`
- Class-level environment requirement: none.
- Class-level tags: none.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testReads`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/BaseTestDeltaLakeHdfsReads.java` ->
  `testReads`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/BaseTestDeltaLakeHdfsReadsJunit.java` ->
  `BaseTestDeltaLakeHdfsReadsJunit.testReads`
- Mapping type: `direct`
- Environment parity: Current class does not declare a concrete environment requirement. Routed by source review into `SuiteDeltaLakeOss` run 2.
- Tag parity: Current tags: `DeltaLakeHdfs`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `CALL`, `SELECT`, `onTrino`, `executeQuery`, `system.register_table`, `count`. Current action shape: `CALL`, `SELECT`, `env.executeTrinoUpdate`, `system.register_table`, `env.executeTrino`, `count`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, system.register_table, count] vs current [env.executeTrinoUpdate, system.register_table, env.executeTrino, count]; SQL verbs differ: legacy [CALL, SELECT, DROP] vs current [DROP, CALL, SELECT]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, system.register_table, count], verbs [CALL, SELECT, DROP]. Current flow summary -> helpers [env.executeTrinoUpdate, system.register_table, env.executeTrino, count], verbs [DROP, CALL, SELECT].
- Audit status: `verified`

### `TestDeltaLakeDatabricksHdfsReads`


- Owning migration commit: `Migrate TestDeltaLakeDatabricksHdfsReads to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksHdfsReads.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksHdfsReads.java`
- Class-level environment requirement: `DeltaLakeOssEnvironment`.
- Class-level tags: none.
- Method inventory complete: Yes. Legacy/local methods: `0`. Current/local methods: `0`. Both legacy and current classes inherit the audited `testReads` implementation from their shared HDFS base class.
- Legacy helper/resource dependencies accounted for: Legacy subclass source reviewed directly; inherited method coverage is audited under `BaseTestDeltaLakeHdfsReadsJunit`.
- Intentional differences summary: None identified at class scope beyond the inherited base-class notes.
- Method statuses present: inherited `verified`.

#### Methods

### `TestDeltaLakeOssDeltaLakeHdfsReads`


- Owning migration commit: `Migrate TestDeltaLakeOssDeltaLakeHdfsReads to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeOssDeltaLakeHdfsReads.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeOssDeltaLakeHdfsReads.java`
- Class-level environment requirement: `DeltaLakeOssEnvironment`.
- Class-level tags: none.
- Method inventory complete: Yes. Legacy/local methods: `0`. Current/local methods: `0`. Both legacy and current classes inherit the audited `testReads` implementation from their shared HDFS base class.
- Legacy helper/resource dependencies accounted for: Legacy subclass source reviewed directly; inherited method coverage is audited under `BaseTestDeltaLakeHdfsReadsJunit`.
- Intentional differences summary: None identified at class scope beyond the inherited base-class notes.
- Method statuses present: inherited `verified`.

#### Methods

### `TestDeltaLakeAlluxioCaching`


- Owning migration commit: `Migrate TestDeltaLakeAlluxioCaching to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeAlluxioCaching.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeAlluxioCaching.java`
- Class-level environment requirement: `DeltaLakeMinioCachingEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `DeltaLakeAlluxioCaching`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testReadFromCache`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeAlluxioCaching.java` ->
  `testReadFromCache`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeAlluxioCaching.java` ->
  `TestDeltaLakeAlluxioCaching.testReadFromCache`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeMinioCachingEnvironment`. Routed by source review into `SuiteDeltaLakeOss` run 4.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeAlluxioCaching`, `ProfileSpecificTests`. Tag routing matches
  the current suite selection.
- Setup parity: Legacy setup shape: `CREATE SCHEMA`, `CREATE TABLE`. Current setup shape: `CREATE SCHEMA`, `CREATE TABLE`.
- Action parity: Legacy action shape: `SELECT`, `testReadFromTable`, `getCacheStats`, `assertEventually`, `onTrino`. Current action shape: `SELECT`, `testReadFromTable`, `createTestTable`, `getCacheStats`, `assertEventually`, `env.executeTrino`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `hasAnyRows`, cache-stat assertions via `assertEventually`. Current assertion helpers: `assertThat`, lambda row-count check, cache-stat assertions via `assertEventually`.
- Cleanup parity: Legacy cleanup shape: `DROP TABLE`, `DROP SCHEMA`. Current cleanup shape: `DROP TABLE`, `DROP SCHEMA`.
- Any observed difference, however small: the current helper broadens the setup and cleanup by creating paired cached and non-cached schemas/tables per invocation instead of using the old fixed schema names, and it validates the cache hit through cache-read/external-read deltas rather than the old cache-space-used comparison against computed table size.
- Known intentional difference: None.
- Reviewer note: The top-level method invokes the shared helper twice for `table1` and `table2`. That helper creates cached and non-cached Delta tables, executes a first read that must hit external storage, executes a second read that must hit only cache, and then drops the tables and schemas.
- Audit status: `verified`
