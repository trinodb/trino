# Lane Audit: Iceberg

## Lane Summary

- Audit status: `manual-pass-complete`
- Section start commit: `Add Iceberg environment runtime support`
- Section end commit: `Remove legacy SuiteIceberg`
- Introduced JUnit suites: `SuiteIceberg`.
- Extended existing suites: none.
- Retired legacy suites: `SuiteIceberg`.
- Environment classes introduced: `MultiNodeIcebergMinioCachingEnvironment`, `SparkIcebergEnvironment`,
  `SparkIcebergJdbcCatalogEnvironment`, `SparkIcebergNessieEnvironment`, `SparkIcebergRestEnvironment`.
- Method status counts: verified `139`, intentional difference `47`, needs follow-up `0`.

## Semantic Audit Status

- Manual review note: the Iceberg lane was freshly re-read end to end in the final pass, including the remaining
  helper-backed delegate methods plus the current/legacy suite and environment sources.

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: `HDP to Hive 3.1 migration`.
- Needs-follow-up methods: none.

## Environment Semantic Audit
### `SparkIcebergEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of legacy `EnvSinglenodeSparkIceberg` against current
  `SparkIcebergEnvironment`.
- Container/service inventory parity: preserved in intent. Both provide Hadoop-backed warehouse storage, Spark Thrift
  Server, and Trino with an `iceberg` catalog for the main Iceberg lane.
- Config/resource wiring parity: preserved in intent. Legacy launcher mounted prepared `iceberg.properties`,
  Spark defaults, and Hive bootstrap scripts; current environment writes the same roles directly through
  Testcontainers-based service setup.
- Recorded differences: `HDP to Hive 3.1 migration`, outside-Docker execution, JUnit/Testcontainers framework
  replacement, and code-driven configuration instead of launcher-time file mounting.
- Reviewer note: no additional Iceberg-environment fidelity gap is currently identified.

### `HiveIcebergRedirectionsEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of legacy `EnvSinglenodeHiveIcebergRedirections` against current
  `HiveIcebergRedirectionsEnvironment`.
- Catalog/redirection parity: preserved in intent. Both expose paired Hive and Iceberg catalogs with bidirectional
  redirection enabled for the redirection and mixed-view coverage in this lane.
- Config/resource wiring parity: preserved in intent. Legacy launcher mounted connector files; current environment
  builds the same Hive-metastore-backed catalog pair directly in code and adds helper methods for Hive and HDFS access.
- Recorded differences: `HDP to Hive 3.1 migration`, outside-Docker execution, JUnit/Testcontainers framework
  replacement, and code-driven configuration instead of launcher-time file mounting.
- Reviewer note: no additional environment-specific fidelity gap is currently identified.

### `SparkIcebergRestEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of legacy `EnvSinglenodeSparkIcebergRest` against current
  `SparkIcebergRestEnvironment`.
- Service-topology parity: preserved in intent. Both add a dedicated Iceberg REST service alongside Hadoop, Spark, and
  Trino for the REST-catalog slice of the suite.
- Config/resource wiring parity: preserved in intent. Legacy launcher mounted REST and Spark config files; current
  environment creates the REST container and Trino catalog properties directly in code.
- Recorded differences: outside-Docker execution, JUnit/Testcontainers framework replacement, and code-driven
  configuration instead of launcher-time file mounting.
- Reviewer note: no additional environment-specific fidelity gap is currently identified.

### `SparkIcebergJdbcCatalogEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of legacy `EnvSinglenodeSparkIcebergJdbcCatalog` against current
  `SparkIcebergJdbcCatalogEnvironment`.
- Service-topology parity: preserved in intent. Both run Hadoop, PostgreSQL-backed JDBC catalog metadata, Spark Thrift
  Server, and Trino with an Iceberg JDBC catalog.
- Config/resource wiring parity: preserved in intent. Legacy launcher mounted PostgreSQL init SQL, Spark defaults, and
  catalog files; current environment creates the PostgreSQL service and matching Iceberg JDBC catalog properties
  directly in code.
- Recorded differences: outside-Docker execution, JUnit/Testcontainers framework replacement, and code-driven
  configuration instead of launcher-time file mounting.
- Reviewer note: no additional environment-specific fidelity gap is currently identified.

### `SparkIcebergNessieEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of legacy `EnvSinglenodeSparkIcebergNessie` against current
  `SparkIcebergNessieEnvironment`.
- Service-topology parity: preserved in intent. Both run Hadoop, Nessie, Spark, and Trino with a Nessie-backed
  Iceberg catalog for the versioned-catalog slice.
- Config/resource wiring parity: preserved in intent. Legacy launcher mounted Nessie and Spark config files; current
  environment builds the same Nessie URI and catalog shape directly in code.
- Recorded differences: outside-Docker execution, JUnit/Testcontainers framework replacement, and code-driven
  configuration instead of launcher-time file mounting.
- Reviewer note: no additional environment-specific fidelity gap is currently identified.

### `MultiNodeIcebergMinioCachingEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of legacy `EnvMultinodeIcebergMinioCaching` against current
  `MultiNodeIcebergMinioCachingEnvironment`.
- Storage/cache parity: preserved in intent. Both expose Minio-backed Iceberg storage with explicit filesystem cache
  directories and tmpfs-backed cache mounts for the Alluxio-caching lane.
- Recorded differences:
  - Legacy launcher ran this coverage on `StandardMultinode`; current environment documents and implements the lane as a
    single Trino container because the current Testcontainers harness does not provide the old multinode abstraction.
  - Current environment creates the Minio bucket and Iceberg catalog properties directly in code instead of mounting the
    legacy launcher property files.
- Reviewer note: the topology is narrower, but no additional unresolved fidelity gap is currently identified for the
  caching lane.

## Suite Semantic Audit
### `SuiteIceberg`
- Suite semantic audit status: `complete`
- CI bucket: `iceberg`
- Relationship to lane: `owned by this lane`.
- Reviewer note: compared directly against legacy `SuiteIceberg`; the current suite preserves the six-run launcher
  structure, with the caching lane now routed through the extracted current caching environment.

## Ported Test Classes

### `TestCreateDropSchema`


- Owning migration commit: `Migrate TestCreateDropSchema to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestCreateDropSchema.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestCreateDropSchema.java`
- Class-level environment requirement: `SparkIcebergEnvironment`.
- Class-level tags: `Iceberg`.
- Method inventory complete: Yes. Legacy methods: `4`. Current methods: `4`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testDropSchemaFiles`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestCreateDropSchema.java` ->
  `testDropSchemaFiles`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestCreateDropSchema.java` ->
  `TestCreateDropSchema.testDropSchemaFiles`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`, `env.createHdfsClient`.
- Action parity: Legacy action shape: `randomNameSuffix`, `format`, `onTrino`, `executeQuery`. Current action shape: `env.getWarehouseDirectory`, `randomNameSuffix`, `format`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertFileExistence`. Current assertion helpers: `assertFileExistence`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, format, onTrino, executeQuery] vs current [env.createHdfsClient, env.getWarehouseDirectory, randomNameSuffix, format, env.executeTrinoUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, format, onTrino, executeQuery], verbs [CREATE, DROP]. Current flow summary -> helpers [env.createHdfsClient, env.getWarehouseDirectory, randomNameSuffix, format, env.executeTrinoUpdate], verbs [CREATE, DROP].
- Audit status: `verified`

##### `testDropSchemaFilesWithLocation`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestCreateDropSchema.java` ->
  `testDropSchemaFilesWithLocation`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestCreateDropSchema.java` ->
  `TestCreateDropSchema.testDropSchemaFilesWithLocation`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`, `env.createHdfsClient`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `WITH`. Current action shape: `env.getWarehouseDirectory`, `randomNameSuffix`, `env.executeTrinoUpdate`, `format`, `WITH`.
- Assertion parity: Legacy assertion helpers: `assertFileExistence`. Current assertion helpers: `assertFileExistence`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, format, WITH] vs current [env.createHdfsClient, env.getWarehouseDirectory, randomNameSuffix, env.executeTrinoUpdate, format, WITH]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, format, WITH], verbs [CREATE, DROP]. Current flow summary -> helpers [env.createHdfsClient, env.getWarehouseDirectory, randomNameSuffix, env.executeTrinoUpdate, format, WITH], verbs [CREATE, DROP].
- Audit status: `verified`

##### `testDropWithExternalFilesInSubdirectory`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestCreateDropSchema.java` ->
  `testDropWithExternalFilesInSubdirectory`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestCreateDropSchema.java` ->
  `TestCreateDropSchema.testDropWithExternalFilesInSubdirectory`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `hdfsClient.createDirectory`. Current setup shape: `CREATE`, `env.createHdfsClient`, `hdfsClient.createDirectory`.
- Action parity: Legacy action shape: `randomNameSuffix`, `hdfsClient.saveFile`, `onTrino`, `executeQuery`, `format`, `WITH`. Current action shape: `env.getWarehouseDirectory`, `randomNameSuffix`, `hdfsClient.saveFile`, `env.executeTrinoUpdate`, `format`, `WITH`.
- Assertion parity: Legacy assertion helpers: `assertFileExistence`. Current assertion helpers: `assertFileExistence`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `hdfsClient.delete`. Current cleanup shape: `DROP`, `hdfsClient.delete`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, hdfsClient.createDirectory, hdfsClient.saveFile, onTrino, executeQuery, format, WITH, hdfsClient.delete] vs current [env.createHdfsClient, env.getWarehouseDirectory, randomNameSuffix, hdfsClient.createDirectory, hdfsClient.saveFile, env.executeTrinoUpdate, format, WITH, hdfsClient.delete]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, hdfsClient.createDirectory, hdfsClient.saveFile, onTrino, executeQuery, format, WITH, hdfsClient.delete], verbs [CREATE, DROP]. Current flow summary -> helpers [env.createHdfsClient, env.getWarehouseDirectory, randomNameSuffix, hdfsClient.createDirectory, hdfsClient.saveFile, env.executeTrinoUpdate, format, WITH, hdfsClient.delete], verbs [CREATE, DROP].
- Audit status: `verified`

##### `testDropWithExternalFiles`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestCreateDropSchema.java` ->
  `testDropWithExternalFiles`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestCreateDropSchema.java` ->
  `TestCreateDropSchema.testDropWithExternalFiles`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `hdfsClient.createDirectory`. Current setup shape: `CREATE`, `env.createHdfsClient`, `hdfsClient.createDirectory`.
- Action parity: Legacy action shape: `randomNameSuffix`, `format`, `hdfsClient.saveFile`, `onTrino`, `executeQuery`. Current action shape: `env.getWarehouseDirectory`, `randomNameSuffix`, `format`, `hdfsClient.saveFile`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertFileExistence`, `assertQuerySucceeds`. Current assertion helpers: `assertFileExistence`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `hdfsClient.delete`. Current cleanup shape: `DROP`, `hdfsClient.delete`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, format, hdfsClient.createDirectory, hdfsClient.saveFile, onTrino, executeQuery, hdfsClient.delete] vs current [env.createHdfsClient, env.getWarehouseDirectory, randomNameSuffix, format, hdfsClient.createDirectory, hdfsClient.saveFile, env.executeTrinoUpdate, hdfsClient.delete]; assertion helpers differ: legacy [assertFileExistence, assertQuerySucceeds] vs current [assertFileExistence]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, format, hdfsClient.createDirectory, hdfsClient.saveFile, onTrino, executeQuery, hdfsClient.delete], verbs [CREATE, DROP]. Current flow summary -> helpers [env.createHdfsClient, env.getWarehouseDirectory, randomNameSuffix, format, hdfsClient.createDirectory, hdfsClient.saveFile, env.executeTrinoUpdate, hdfsClient.delete], verbs [CREATE, DROP].
- Audit status: `verified`

### `TestIcebergAlluxioCaching`


- Owning migration commit: `Migrate TestIcebergAlluxioCaching to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergAlluxioCaching.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergAlluxioCaching.java`
- Class-level environment requirement: `MultiNodeIcebergMinioCachingEnvironment`.
- Class-level tags: `IcebergAlluxioCaching`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `JUnit/AssertJ/Testcontainers framework replacement`
- Method statuses present: `intentional difference`.

#### Methods

##### `testReadFromCache`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergAlluxioCaching.java` ->
  `testReadFromCache`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergAlluxioCaching.java` ->
  `TestIcebergAlluxioCaching.testReadFromCache`
- Mapping type: `direct`
- Environment parity: Current class requires `MultiNodeIcebergMinioCachingEnvironment`. Routed by source review into `SuiteIceberg` run 6.
- Tag parity: Current tags: `IcebergAlluxioCaching`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `testReadFromTable`, `getCacheStats`, `onTrino`, `assertEventually`. Current action shape: `env.getBucketName`, `testReadFromTable`, `createTestTable`, `getCacheStats`, `assertEventually`, `env.executeTrino`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `hasAnyRows`, cache-stat assertions via shared helper. Current assertion helpers: `assertThat`, lambda row-count check, cache-stat assertions via shared helper.
- Cleanup parity: Legacy cleanup shape: `DROP TABLE`. Current cleanup shape: `DROP TABLE`, `DROP SCHEMA`.
- Any observed difference, however small: the JUnit port no longer relies on the old shared Alluxio cache helper alone; it recreates the schema/table inline, queries the native filesystem-cache JMX path first with Alluxio fallback, and drops both table and schema in cleanup.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Legacy flow summary -> helpers [testReadFromTable, getCacheStats, onTrino, assertEventually], verbs [DROP, CREATE, SELECT]. Current flow summary -> helpers [env.getBucketName, testReadFromTable, createTestTable, getCacheStats, assertEventually, env.executeTrino, env.executeTrinoUpdate], verbs [DROP, CREATE, SELECT]. The caching claim remains the same, but the instrumentation and cleanup shape are materially rewritten.
- Audit status: `intentional difference`

### `TestIcebergFormatVersionCompatibility`


- Owning migration commit: `Migrate TestIcebergFormatVersionCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergFormatVersionCompatibility.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergFormatVersionCompatibility.java`
- Class-level environment requirement: `SparkIcebergCompatibilityEnvironment`.
- Class-level tags: `Iceberg`, `IcebergFormatVersionCompatibility`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testTrinoTimeTravelReadTableCreatedByEarlyVersionTrino`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergFormatVersionCompatibility.java` ->
  `testTrinoTimeTravelReadTableCreatedByEarlyVersionTrino`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergFormatVersionCompatibility.java` ->
  `TestIcebergFormatVersionCompatibility.testTrinoTimeTravelReadTableCreatedByEarlyVersionTrino`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergCompatibilityEnvironment`. Routed by source review into `SuiteCompatibility` run 2.
- Tag parity: Current tags: `Iceberg`, `IcebergFormatVersionCompatibility`. Tag routing matches the current suite
  selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `format`, `onCompatibilityTestServer`, `executeQuery`, `s`, `getOnlyValue`, `onTrino`, `rows`, `stream`, `map`, `row.toArray`, `collect`, `toImmutableList`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeCompatibilityTrinoUpdate`, `env.executeCompatibilityTrino`, `getOnlyValue`, `env.executeTrino`, `result.getRows`, `stream`, `map`, `r.getValues`, `toArray`, `collect`, `toImmutableList`, `expected.toArray`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `hasSize`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `hasSize`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, format, onCompatibilityTestServer, executeQuery, s, getOnlyValue, onTrino, rows, stream, map, row.toArray, collect, toImmutableList] vs current [randomNameSuffix, env.executeCompatibilityTrinoUpdate, env.executeCompatibilityTrino, getOnlyValue, env.executeTrino, result.getRows, stream, map, r.getValues, toArray, collect, toImmutableList, expected.toArray, env.executeTrinoUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, format, onCompatibilityTestServer, executeQuery, s, getOnlyValue, onTrino, rows, stream, map, row.toArray, collect, toImmutableList], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeCompatibilityTrinoUpdate, env.executeCompatibilityTrino, getOnlyValue, env.executeTrino, result.getRows, stream, map, r.getValues, toArray, collect, toImmutableList, expected.toArray, env.executeTrinoUpdate], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

### `TestIcebergHiveMetadataListing`


- Owning migration commit: `Migrate TestIcebergHiveMetadataListing to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergHiveMetadataListing.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergHiveMetadataListing.java`
- Class-level environment requirement: `HiveIcebergRedirectionsEnvironment`.
- Class-level tags: `HiveIcebergRedirections`.
- Method inventory complete: Yes. Legacy methods: `2`. Current methods: `2`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`, `intentional difference`.

#### Methods

##### `testTableListing`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergHiveMetadataListing.java` ->
  `testTableListing`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergHiveMetadataListing.java` ->
  `TestIcebergHiveMetadataListing.testTableListing`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SHOW`, `onTrino`, `executeQuery`, `builder`, `addAll`, `add`, `build`. Current action shape: `SHOW`, `SELECT`, `env.executeTrino`, `getRows`, `env.executeTrinoUpdate`, `default.iceberg_table1`, `default.hive_table`, `expectedTables.add`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: `cleanup`.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, builder, addAll, add, build] vs current [cleanup, env.executeTrino, getRows, env.executeTrinoUpdate, default.iceberg_table1, default.hive_table, expectedTables.add]; SQL verbs differ: legacy [SHOW] vs current [SHOW, CREATE, SELECT]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [row, assertThat, containsOnly]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, builder, addAll, add, build], verbs [SHOW]. Current flow summary -> helpers [cleanup, env.executeTrino, getRows, env.executeTrinoUpdate, default.iceberg_table1, default.hive_table, expectedTables.add], verbs [SHOW, CREATE, SELECT].
- Audit status: `verified`

##### `testColumnListing`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergHiveMetadataListing.java` ->
  `testColumnListing`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergHiveMetadataListing.java` ->
  `TestIcebergHiveMetadataListing.testColumnListing`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `builder`, `addAll`, `add`, `build`. Current action shape: `SELECT`, `env.executeTrino`, `getRows`, `env.executeTrinoUpdate`, `default.iceberg_table1`, `default.hive_table`, `expectedColumns.add`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: `cleanup`.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, builder, addAll, add, build] vs current [cleanup, env.executeTrino, getRows, env.executeTrinoUpdate, default.iceberg_table1, default.hive_table, expectedColumns.add]; SQL verbs differ: legacy [SELECT] vs current [SELECT, CREATE]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [row, assertThat, containsOnly]; current expected output now includes `hive_table` columns in Iceberg `information_schema.columns`, whereas the legacy assertion only included the redirected Hive view columns.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, builder, addAll, add, build], verbs [SELECT]. Current flow summary -> helpers [cleanup, env.executeTrino, getRows, env.executeTrinoUpdate, default.iceberg_table1, default.hive_table, expectedColumns.add], verbs [SELECT, CREATE]. The JUnit port widens Iceberg-column visibility to include the redirected Hive table itself, not only the Hive view.
- Audit status: `intentional difference`

### `TestIcebergHiveViewsCompatibility`


- Owning migration commit: `Migrate TestIcebergHiveViewsCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergHiveViewsCompatibility.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergHiveViewsCompatibility.java`
- Class-level environment requirement: `HiveIcebergRedirectionsEnvironment`.
- Class-level tags: `HiveIcebergRedirections`.
- Method inventory complete: Yes. Legacy methods in current class: `1`. Additional moved legacy method from
  `AbstractTestHiveViews`: `1`. Current methods: `2`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly. Additional legacy source reviewed directly: `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/AbstractTestHiveViews.java`.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `intentional difference`, `verified`.

#### Methods

##### `testIcebergHiveViewsCompatibility`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergHiveViewsCompatibility.java` ->
  `testIcebergHiveViewsCompatibility`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergHiveViewsCompatibility.java` ->
  `TestIcebergHiveViewsCompatibility.testIcebergHiveViewsCompatibility`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `USE`. Current setup shape: `CREATE`, `USE`.
- Action parity: Legacy action shape: `SHOW`, `SELECT`, `onTrino`, `executeQuery`, `rows`, `stream`, `map`, `list.toArray`, `collect`, `toList`, `builder`, `add`, `build`, `addAll`. Current action shape: `SHOW`, `SELECT`, `env.executeTrino`, `getRows`, `env.executeTrinoUpdate`, `enabled`, `env.executeTrinoInSession`, `session.executeUpdate`, `succeeds`, `List.of`, `expectedHiveTables.addAll`, `expectedIcebergTables.addAll`.
- Assertion parity: Legacy assertion helpers: `row`, `assertQueryFailure`, `hasMessageMatching`, `assertThatThrownBy`, `hasMessageContaining`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `cleanup`. Current cleanup shape: `cleanup`.
- Any observed difference, however small: helper calls differ: legacy [cleanup, onTrino, executeQuery, rows, stream, map, list.toArray, collect, toList, builder, add, build, addAll] vs current [cleanup, env.executeTrino, getRows, env.executeTrinoUpdate, enabled, env.executeTrinoInSession, session.executeUpdate, succeeds, List.of, expectedHiveTables.addAll, expectedIcebergTables.addAll]; assertion helpers differ: legacy [row, assertQueryFailure, hasMessageMatching, assertThatThrownBy, hasMessageContaining, assertThat, containsOnly] vs current [row, assertThat, containsOnly]; the two legacy failure cases for unqualified cross-catalog references now succeed under bi-directional redirections and are asserted as successful view creations plus successful reads.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [cleanup, onTrino, executeQuery, rows, stream, map, list.toArray, collect, toList, builder, add, build, addAll], verbs [SHOW, CREATE, SELECT, USE]. Current flow summary -> helpers [cleanup, env.executeTrino, getRows, env.executeTrinoUpdate, enabled, env.executeTrinoInSession, session.executeUpdate, succeeds, List.of, expectedHiveTables.addAll, expectedIcebergTables.addAll], verbs [SHOW, CREATE, SELECT, USE]. This is not just a helper rewrite: the JUnit port widens behavior by allowing the formerly failing unqualified Hive-to-Iceberg and Iceberg-to-Hive view definitions when redirections are enabled in both directions.
- Audit status: `intentional difference`

##### `testViewReferencingHiveAndIcebergTables`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hive/AbstractTestHiveViews.java` ->
  `testViewReferencingHiveAndIcebergTables`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergHiveViewsCompatibility.java` ->
  `TestIcebergHiveViewsCompatibility.testViewReferencingHiveAndIcebergTables`
- Mapping type: `moved`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into
  `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `DROP`, `SHOW`. Current setup shape: `CREATE`, `DROP`, `SHOW`.
- Action parity: Legacy action shape: `CREATE`, `SELECT`, `DROP`, `SHOW`, `extractMatch`, `collect`, `joining`,
  `onTrino`, `onHive`. Current action shape: `CREATE`, `SELECT`, `DROP`, `SHOW`, `env.executeTrino`,
  `env.executeTrinoUpdate`, `env.executeHiveUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `assertQueryFailure`,
  `hasMessageContaining`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: Current JUnit recreates the same mixed Hive/Iceberg view scenario with direct
  environment helpers and explicit cleanup around the temporary Iceberg tables.
- Known intentional difference: none recorded.
- Reviewer note: The previously missing mixed Hive/Iceberg view coverage is now represented directly in the current
  JUnit class.
- Audit status: `verified`

### `TestIcebergInsert`


- Owning migration commit: `Migrate TestIcebergInsert to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergInsert.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergInsert.java`
- Class-level environment requirement: `SparkIcebergEnvironment`.
- Class-level tags: `Iceberg`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testIcebergConcurrentInsert`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergInsert.java` ->
  `testIcebergConcurrentInsert`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergInsert.java` ->
  `TestIcebergInsert.testIcebergConcurrentInsert`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `inserted.add`, `allInserted.stream`. Current setup shape: `CREATE`, `INSERT`, `env.createTrinoConnection`, `conn.createStatement`, `inserted.add`, `allInserted.stream`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `Executors.newFixedThreadPool`, `yclicBarrier`, `executor.invokeAll`, `IntStream.range`, `mapToObj`, `barrier.await`, `onTrino.executeQuery`, `collect`, `toImmutableList`, `stream`, `map`, `flatMap`, `hasSizeBetween`, `toArray`, `executor.shutdownNow`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `Executors.newFixedThreadPool`, `yclicBarrier`, `executor.invokeAll`, `IntStream.range`, `mapToObj`, `barrier.await`, `stmt.executeUpdate`, `collect`, `toImmutableList`, `stream`, `map`, `flatMap`, `hasSizeBetween`, `env.executeTrino`, `toArray`, `executor.shutdownNow`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, Executors.newFixedThreadPool, yclicBarrier, executor.invokeAll, IntStream.range, mapToObj, barrier.await, onTrino.executeQuery, inserted.add, collect, toImmutableList, stream, map, flatMap, hasSizeBetween, allInserted.stream, toArray, executor.shutdownNow] vs current [randomNameSuffix, env.executeTrinoUpdate, Executors.newFixedThreadPool, yclicBarrier, executor.invokeAll, IntStream.range, mapToObj, barrier.await, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, inserted.add, collect, toImmutableList, stream, map, flatMap, hasSizeBetween, env.executeTrino, allInserted.stream, toArray, executor.shutdownNow]; assertion helpers differ: legacy [assertThat, containsOnly] vs current [assertThat, containsOnly, row]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, Executors.newFixedThreadPool, yclicBarrier, executor.invokeAll, IntStream.range, mapToObj, barrier.await, onTrino.executeQuery, inserted.add, collect, toImmutableList, stream, map, flatMap, hasSizeBetween, allInserted.stream, toArray, executor.shutdownNow], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, Executors.newFixedThreadPool, yclicBarrier, executor.invokeAll, IntStream.range, mapToObj, barrier.await, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, inserted.add, collect, toImmutableList, stream, map, flatMap, hasSizeBetween, env.executeTrino, allInserted.stream, toArray, executor.shutdownNow], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

### `TestIcebergOptimize`


- Owning migration commit: `Migrate TestIcebergOptimize to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergOptimize.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergOptimize.java`
- Class-level environment requirement: `SparkIcebergEnvironment`.
- Class-level tags: `Iceberg`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testOptimizeTableAfterDelete`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergOptimize.java` ->
  `testOptimizeTableAfterDelete`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergOptimize.java` ->
  `TestIcebergOptimize.testOptimizeTableAfterDelete`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`. Current setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `getActiveFiles`, `format`, `getCurrentSnapshotId`, `onSpark`, `TBLPROPERTIES`, `isNotEqualTo`. Current action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `getActiveFiles`, `env.executeTrino`, `format`, `getCurrentSnapshotId`, `env.executeSparkUpdate`, `TBLPROPERTIES`, `env.executeTrinoInSession`, `session.executeUpdate`, `isNotEqualTo`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `hasSize`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `hasSize`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `DELETE`. Current cleanup shape: `DROP`, `DELETE`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, getActiveFiles, format, getCurrentSnapshotId, onSpark, TBLPROPERTIES, isNotEqualTo] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, getActiveFiles, env.executeTrino, format, getCurrentSnapshotId, env.executeSparkUpdate, TBLPROPERTIES, env.executeTrinoInSession, session.executeUpdate, isNotEqualTo]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, getActiveFiles, format, getCurrentSnapshotId, onSpark, TBLPROPERTIES, isNotEqualTo], verbs [DROP, CREATE, INSERT, DELETE, SELECT, ALTER, SET]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, getActiveFiles, env.executeTrino, format, getCurrentSnapshotId, env.executeSparkUpdate, TBLPROPERTIES, env.executeTrinoInSession, session.executeUpdate, isNotEqualTo], verbs [DROP, CREATE, INSERT, DELETE, SELECT, ALTER, SET].
- Audit status: `verified`

### `TestIcebergPartitionEvolution`


- Owning migration commit: `Migrate TestIcebergPartitionEvolution to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergPartitionEvolution.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergPartitionEvolution.java`
- Class-level environment requirement: `SparkIcebergEnvironment`.
- Class-level tags: `Iceberg`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testDroppedPartitionField`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergPartitionEvolution.java` ->
  `testDroppedPartitionField`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergPartitionEvolution.java` ->
  `TestIcebergPartitionEvolution.testDroppedPartitionField`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `USE`, `CREATE`, `INSERT`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `ALTER`.
- Action parity: Legacy action shape: `SHOW`, `SELECT`, `onTrino`, `executeQuery`, `WITH`, `onSpark`, `getOnlyValue`, `matches`, `void`, `ne`, `rowBuilder`, `addField`, `build`, `singletonMetrics`, `dataMetrics`, `VALUES`. Current action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `env.executeSparkUpdate`, `partitioning`, `env.executeTrino`, `getOnlyValue`, `matches`, `void`, `schema`, `rowBuilder`, `addField`, `build`, `singletonMetrics`, `dataMetrics`, `VALUES`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `test_dropped_partition_field`, `default.test_dropped_partition_field`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, test_dropped_partition_field, WITH, onSpark, getOnlyValue, matches, default.test_dropped_partition_field, void, ne, rowBuilder, addField, build, singletonMetrics, dataMetrics, VALUES] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, env.executeSparkUpdate, partitioning, env.executeTrino, getOnlyValue, matches, void, schema, rowBuilder, addField, build, singletonMetrics, dataMetrics, VALUES]; SQL verbs differ: legacy [USE, DROP, CREATE, INSERT, ALTER, SHOW, SELECT] vs current [CREATE, INSERT, ALTER, DROP, SHOW, SELECT]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, test_dropped_partition_field, WITH, onSpark, getOnlyValue, matches, default.test_dropped_partition_field, void, ne, rowBuilder, addField, build, singletonMetrics, dataMetrics, VALUES], verbs [USE, DROP, CREATE, INSERT, ALTER, SHOW, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, env.executeSparkUpdate, partitioning, env.executeTrino, getOnlyValue, matches, void, schema, rowBuilder, addField, build, singletonMetrics, dataMetrics, VALUES], verbs [CREATE, INSERT, ALTER, DROP, SHOW, SELECT].
- Audit status: `verified`

### `TestIcebergProcedureCalls`


- Owning migration commit: `Migrate TestIcebergProcedureCalls to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java`
- Class-level environment requirement: `SparkIcebergEnvironment`.
- Class-level tags: `Iceberg`.
- Method inventory complete: Yes. Legacy methods: `13`. Current methods: `13`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`, `intentional difference`.

#### Methods

##### `testMigrateHiveTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `testMigrateHiveTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `TestIcebergProcedureCalls.testMigrateHiveTable`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `CALL`, `randomNameSuffix`, `onTrino`, `executeQuery`, `system.migrate`, `onSpark`. Current action shape: `SELECT`, `CALL`, `randomNameSuffix`, `env.executeTrinoUpdate`, `system.migrate`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, system.migrate, onSpark] vs current [randomNameSuffix, env.executeTrinoUpdate, system.migrate, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, system.migrate, onSpark], verbs [DROP, CREATE, SELECT, CALL]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, system.migrate, env.executeTrino, env.executeSpark], verbs [DROP, CREATE, SELECT, CALL].
- Audit status: `verified`

##### `testMigrateTimestampHiveTableWithOrc`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `testMigrateTimestampHiveTableWithOrc`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `TestIcebergProcedureCalls.testMigrateTimestampHiveTableWithOrc`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `DROP`, `SET`, `CREATE`. Current setup shape: `DROP`, `SET`, `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `CALL`, `CAST`, `randomNameSuffix`, `testMigrateTimestampHiveTable`, `hive.timestamp_precision`, `iceberg.system.migrate`, `onHive`, `onTrino`, `onSpark`. Current action shape: `SELECT`, `CALL`, `CAST`, `randomNameSuffix`, `testMigrateTimestampHiveTable`, `hive.timestamp_precision`, `iceberg.system.migrate`, `env.executeHive`, `env.executeTrinoInSession`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the current helper maps the precision name to an explicit `TIMESTAMP(3|6|9)` type and keeps the `SET SESSION` plus CTAS/read sequence inside `env.executeTrinoInSession`, while the legacy helper relied on the shared Tempto Trino session and a bare `TIMESTAMP` literal; the migrate/read assertions remain the same.
- Known intentional difference: None.
- Reviewer note: Both implementations drop any preexisting Hive table, set the requested Hive timestamp precision, create a Hive-backed table with a single timestamp value, verify the pre-migration Hive/Trino reads, call `iceberg.system.migrate`, then verify the migrated table from Trino and Spark before dropping it.
- Audit status: `verified`

##### `testMigrateTimestampHiveTableWithParquet`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `testMigrateTimestampHiveTableWithParquet`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `TestIcebergProcedureCalls.testMigrateTimestampHiveTableWithParquet`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `DROP`, `SET`, `CREATE`. Current setup shape: `DROP`, `SET`, `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `CALL`, `CAST`, `randomNameSuffix`, `testMigrateTimestampHiveTable`, `hive.timestamp_precision`, `iceberg.system.migrate`, `onHive`, `onTrino`, `onSpark`. Current action shape: `SELECT`, `CALL`, `CAST`, `randomNameSuffix`, `testMigrateTimestampHiveTable`, `hive.timestamp_precision`, `iceberg.system.migrate`, `env.executeHive`, `env.executeTrinoInSession`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the current helper maps the precision name to an explicit `TIMESTAMP(3|6|9)` type and keeps the `SET SESSION` plus CTAS/read sequence inside `env.executeTrinoInSession`, while the legacy helper relied on the shared Tempto Trino session and a bare `TIMESTAMP` literal; the migrate/read assertions remain the same.
- Known intentional difference: None.
- Reviewer note: Both implementations drop any preexisting Hive table, set the requested Hive timestamp precision, create a Hive-backed table with a single timestamp value, verify the pre-migration Hive/Trino reads, call `iceberg.system.migrate`, then verify the migrated table from Trino and Spark before dropping it.
- Audit status: `verified`

##### `testMigrateHiveTableWithTinyintType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `testMigrateHiveTableWithTinyintType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `TestIcebergProcedureCalls.testMigrateHiveTableWithTinyintType`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `USE`, `INSERT`. Current setup shape: `CREATE`, `USE`, `INSERT`.
- Action parity: Legacy action shape: `CALL`, `SELECT`, `randomNameSuffix`, `WITH`, `fileFormat.equals`, `onTrino`, `executeQuery`, `system.migrate`, `ImmutableList.of`, `onSpark`. Current action shape: `CALL`, `SELECT`, `randomNameSuffix`, `WITH`, `fileFormat.equals`, `env.executeTrinoUpdate`, `system.migrate`, `ImmutableList.of`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageContaining`, `row`, `assertThat`, `containsOnly`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, WITH, fileFormat.equals, onTrino, executeQuery, system.migrate, ImmutableList.of, onSpark] vs current [randomNameSuffix, WITH, fileFormat.equals, env.executeTrinoUpdate, system.migrate, ImmutableList.of, env.executeTrino, env.executeSpark]; assertion helpers differ: legacy [assertQueryFailure, hasMessageContaining, row, assertThat, containsOnly] vs current [assertThatThrownBy, hasMessageContaining, row, assertThat, containsOnly]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, WITH, fileFormat.equals, onTrino, executeQuery, system.migrate, ImmutableList.of, onSpark], verbs [CREATE, USE, INSERT, CALL, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, WITH, fileFormat.equals, env.executeTrinoUpdate, system.migrate, ImmutableList.of, env.executeTrino, env.executeSpark], verbs [CREATE, USE, INSERT, CALL, SELECT, DROP].
- Audit status: `verified`

##### `testMigrateHiveTableWithSmallintType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `testMigrateHiveTableWithSmallintType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `TestIcebergProcedureCalls.testMigrateHiveTableWithSmallintType`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `USE`, `INSERT`. Current setup shape: `CREATE`, `USE`, `INSERT`.
- Action parity: Legacy action shape: `CALL`, `SELECT`, `randomNameSuffix`, `WITH`, `fileFormat.equals`, `onTrino`, `executeQuery`, `system.migrate`, `ImmutableList.of`, `onSpark`. Current action shape: `CALL`, `SELECT`, `randomNameSuffix`, `WITH`, `fileFormat.equals`, `env.executeTrinoUpdate`, `system.migrate`, `ImmutableList.of`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageContaining`, `row`, `assertThat`, `containsOnly`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, WITH, fileFormat.equals, onTrino, executeQuery, system.migrate, ImmutableList.of, onSpark] vs current [randomNameSuffix, WITH, fileFormat.equals, env.executeTrinoUpdate, system.migrate, ImmutableList.of, env.executeTrino, env.executeSpark]; assertion helpers differ: legacy [assertQueryFailure, hasMessageContaining, row, assertThat, containsOnly] vs current [assertThatThrownBy, hasMessageContaining, row, assertThat, containsOnly]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, WITH, fileFormat.equals, onTrino, executeQuery, system.migrate, ImmutableList.of, onSpark], verbs [CREATE, USE, INSERT, CALL, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, WITH, fileFormat.equals, env.executeTrinoUpdate, system.migrate, ImmutableList.of, env.executeTrino, env.executeSpark], verbs [CREATE, USE, INSERT, CALL, SELECT, DROP].
- Audit status: `verified`

##### `testMigrateHiveTableWithComplexType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `testMigrateHiveTableWithComplexType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `TestIcebergProcedureCalls.testMigrateHiveTableWithComplexType`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `CALL`, `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `CAST`, `map`, `system.migrate`, `onSpark`, `element_at`. Current action shape: `SELECT`, `CALL`, `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `CAST`, `map`, `system.migrate`, `env.executeTrino`, `env.executeSpark`, `element_at`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, CAST, map, system.migrate, onSpark, element_at] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, CAST, map, system.migrate, env.executeTrino, env.executeSpark, element_at]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, CAST, map, system.migrate, onSpark, element_at], verbs [DROP, CREATE, SELECT, CALL]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, CAST, map, system.migrate, env.executeTrino, env.executeSpark, element_at], verbs [DROP, CREATE, SELECT, CALL].
- Audit status: `verified`

##### `testMigrateHivePartitionedTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `testMigrateHivePartitionedTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `TestIcebergProcedureCalls.testMigrateHivePartitionedTable`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `CALL`, `SHOW`, `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `system.migrate`, `onSpark`, `getOnlyValue`. Current action shape: `SELECT`, `CALL`, `SHOW`, `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `system.migrate`, `env.executeTrino`, `env.executeSpark`, `getOnlyValue`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `contains`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `contains`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, system.migrate, onSpark, getOnlyValue] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, system.migrate, env.executeTrino, env.executeSpark, getOnlyValue]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, system.migrate, onSpark, getOnlyValue], verbs [DROP, CREATE, SELECT, CALL, SHOW]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, system.migrate, env.executeTrino, env.executeSpark, getOnlyValue], verbs [DROP, CREATE, SELECT, CALL, SHOW].
- Audit status: `verified`

##### `testMigrateHiveBucketedTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `testMigrateHiveBucketedTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `TestIcebergProcedureCalls.testMigrateHiveBucketedTable`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `CALL`, `SHOW`, `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `system.migrate`, `onSpark`, `getOnlyValue`. Current action shape: `SELECT`, `CALL`, `SHOW`, `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `system.migrate`, `env.executeTrino`, `env.executeSpark`, `getOnlyValue`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `contains`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `contains`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, system.migrate, onSpark, getOnlyValue] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, system.migrate, env.executeTrino, env.executeSpark, getOnlyValue]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, system.migrate, onSpark, getOnlyValue], verbs [DROP, CREATE, SELECT, CALL, SHOW]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, system.migrate, env.executeTrino, env.executeSpark, getOnlyValue], verbs [DROP, CREATE, SELECT, CALL, SHOW].
- Audit status: `verified`

##### `testMigrateHiveBucketedOnMultipleColumns`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `testMigrateHiveBucketedOnMultipleColumns`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `TestIcebergProcedureCalls.testMigrateHiveBucketedOnMultipleColumns`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `CALL`, `SHOW`, `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `system.migrate`, `onSpark`, `getOnlyValue`. Current action shape: `SELECT`, `CALL`, `SHOW`, `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `system.migrate`, `env.executeTrino`, `env.executeSpark`, `getOnlyValue`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `contains`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `contains`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, system.migrate, onSpark, getOnlyValue] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, system.migrate, env.executeTrino, env.executeSpark, getOnlyValue]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, system.migrate, onSpark, getOnlyValue], verbs [DROP, CREATE, SELECT, CALL, SHOW]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, system.migrate, env.executeTrino, env.executeSpark, getOnlyValue], verbs [DROP, CREATE, SELECT, CALL, SHOW].
- Audit status: `verified`

##### `testTrinoMigrateExternalTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `testTrinoMigrateExternalTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `TestIcebergProcedureCalls.testTrinoMigrateExternalTable`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `CALL`, `migrateExternalTable`, `onTrino`, `executeQuery`, `system.migrate`. Current action shape: `CALL`, `migrateExternalTable`, `env.executeTrinoUpdate`, `system.migrate`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [migrateExternalTable, onTrino, executeQuery, system.migrate] vs current [migrateExternalTable, env.executeTrinoUpdate, system.migrate]; the shared JUnit helper now recreates the warehouse root explicitly, creates the managed/external source tables via Hive DDL instead of `getTableLocation`, and passes the new Trino `recursive_directory = true` argument to `iceberg.system.migrate`.
- Known intentional difference: none recorded.
- Reviewer note: Legacy flow summary -> helpers [migrateExternalTable, onTrino, executeQuery, system.migrate], verbs [CALL]. Current flow summary -> helpers [migrateExternalTable, env.executeTrinoUpdate, system.migrate], verbs [CALL]. The migration claim is still the same, but the current procedure signature and helper setup are materially rewritten.
- Audit status: `intentional difference`

##### `testSparkMigrateExternalTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `testSparkMigrateExternalTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `TestIcebergProcedureCalls.testSparkMigrateExternalTable`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `CALL`, `migrateExternalTable`, `onSpark`, `executeQuery`, `system.migrate`. Current action shape: `CALL`, `migrateExternalTable`, `env.executeSparkUpdate`, `system.migrate`, `map`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [migrateExternalTable, onSpark, executeQuery, system.migrate] vs current [migrateExternalTable, env.executeSparkUpdate, system.migrate, map]; the shared JUnit helper now recreates the warehouse root explicitly, creates the managed/external source tables via Hive DDL instead of `getTableLocation`, and passes Spark the new `map('recursive_directory', 'true')` migrate options.
- Known intentional difference: none recorded.
- Reviewer note: Legacy flow summary -> helpers [migrateExternalTable, onSpark, executeQuery, system.migrate], verbs [CALL]. Current flow summary -> helpers [migrateExternalTable, env.executeSparkUpdate, system.migrate, map], verbs [CALL]. The migration claim is still the same, but the current Spark procedure invocation and helper setup are materially rewritten.
- Audit status: `intentional difference`

##### `testMigrateUnsupportedTransactionalTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `testMigrateUnsupportedTransactionalTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `TestIcebergProcedureCalls.testMigrateUnsupportedTransactionalTable`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `CALL`, `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `system.migrate`. Current action shape: `SELECT`, `CALL`, `randomNameSuffix`, `env.executeTrinoUpdate`, `WITH`, `system.migrate`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, system.migrate] vs current [randomNameSuffix, env.executeTrinoUpdate, WITH, system.migrate, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, system.migrate], verbs [CREATE, SELECT, CALL, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, WITH, system.migrate, env.executeTrino], verbs [CREATE, SELECT, CALL, DROP].
- Audit status: `verified`

##### `testRollbackToSnapshot`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `testRollbackToSnapshot`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergProcedureCalls.java` ->
  `TestIcebergProcedureCalls.testRollbackToSnapshot`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `USE`, `CREATE`, `INSERT`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `s`, `Thread.sleep`, `getSecondOldestTableSnapshot`, `rollback_to_snapshot`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `format`, `s`, `Thread.sleep`, `getSecondOldestTableSnapshot`, `rollback_to_snapshot`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, format, s, Thread.sleep, getSecondOldestTableSnapshot, rollback_to_snapshot] vs current [randomNameSuffix, env.executeTrinoUpdate, format, s, Thread.sleep, getSecondOldestTableSnapshot, rollback_to_snapshot, env.executeTrino]; SQL verbs differ: legacy [USE, DROP, CREATE, INSERT, ALTER, SELECT] vs current [DROP, CREATE, INSERT, ALTER, SELECT]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, format, s, Thread.sleep, getSecondOldestTableSnapshot, rollback_to_snapshot], verbs [USE, DROP, CREATE, INSERT, ALTER, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, format, s, Thread.sleep, getSecondOldestTableSnapshot, rollback_to_snapshot, env.executeTrino], verbs [DROP, CREATE, INSERT, ALTER, SELECT].
- Audit status: `verified`

### `TestIcebergRedirectionToHive`


- Owning migration commit: `Migrate TestIcebergRedirectionToHive to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java`
- Class-level environment requirement: `HiveIcebergRedirectionsEnvironment`.
- Class-level tags: `HiveIcebergRedirections`.
- Method inventory complete: Yes. Legacy methods: `27`. Current methods: `27`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testRedirect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testRedirect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testRedirect`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createHiveTable`. Current setup shape: `createHiveTable`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`. Current action shape: `randomNameSuffix`, `env.executeTrino`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertResultsEqual`. Current assertion helpers: `assertResultsEqual`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery] vs current [randomNameSuffix, createHiveTable, env.executeTrino, env.executeTrinoUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery], verbs [DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrino, env.executeTrinoUpdate], verbs [DROP].
- Audit status: `verified`

##### `testRedirectWithNonDefaultSchema`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testRedirectWithNonDefaultSchema`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testRedirectWithNonDefaultSchema`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createHiveTable`. Current setup shape: `createHiveTable`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`. Current action shape: `randomNameSuffix`, `env.executeTrino`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertResultsEqual`. Current assertion helpers: `assertResultsEqual`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery] vs current [randomNameSuffix, createHiveTable, env.executeTrino, env.executeTrinoUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery], verbs [DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrino, env.executeTrinoUpdate], verbs [DROP].
- Audit status: `verified`

##### `testRedirectToNonexistentCatalog`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testRedirectToNonexistentCatalog`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testRedirectToNonexistentCatalog`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `SET`, `createHiveTable`. Current setup shape: `SET`, `createHiveTable`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`. Current action shape: `randomNameSuffix`, `env.executeTrino`, `env.executeTrinoInSession`, `session.executeUpdate`, `session.executeQuery`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertResultsEqual`, `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertResultsEqual`, `assertThatThrownBy`, `hasMessageMatching`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery] vs current [randomNameSuffix, createHiveTable, env.executeTrino, env.executeTrinoInSession, session.executeUpdate, session.executeQuery, env.executeTrinoUpdate]; assertion helpers differ: legacy [assertResultsEqual, assertQueryFailure, hasMessageMatching] vs current [assertResultsEqual, assertThatThrownBy, hasMessageMatching]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery], verbs [SET, DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrino, env.executeTrinoInSession, session.executeUpdate, session.executeQuery, env.executeTrinoUpdate], verbs [SET, DROP].
- Audit status: `verified`

##### `testRedirectWithDefaultSchemaInSession`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testRedirectWithDefaultSchemaInSession`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testRedirectWithDefaultSchemaInSession`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `USE`, `createHiveTable`. Current setup shape: `USE`, `createHiveTable`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`. Current action shape: `randomNameSuffix`, `env.executeTrinoInSession`, `session.executeUpdate`, `session.executeQuery`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertResultsEqual`. Current assertion helpers: `assertResultsEqual`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery] vs current [randomNameSuffix, createHiveTable, env.executeTrinoInSession, session.executeUpdate, session.executeQuery, env.executeTrinoUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery], verbs [USE, DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrinoInSession, session.executeUpdate, session.executeQuery, env.executeTrinoUpdate], verbs [USE, DROP].
- Audit status: `verified`

##### `testRedirectPartitionsToUnpartitioned`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testRedirectPartitionsToUnpartitioned`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testRedirectPartitionsToUnpartitioned`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createHiveTable`. Current setup shape: `createHiveTable`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `failed`. Current action shape: `randomNameSuffix`, `env.executeTrino`, `cause`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`, `hasMessageMatching`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed] vs current [randomNameSuffix, createHiveTable, env.executeTrino, cause, env.executeTrinoUpdate]; assertion helpers differ: legacy [assertQueryFailure, hasMessageMatching] vs current [assertThatThrownBy, hasMessageMatching]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed], verbs [DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrino, cause, env.executeTrinoUpdate], verbs [DROP].
- Audit status: `verified`

##### `testRedirectInvalidSystemTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testRedirectInvalidSystemTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testRedirectInvalidSystemTable`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createHiveTable`. Current setup shape: `createHiveTable`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `failed`. Current action shape: `randomNameSuffix`, `env.executeTrino`, `cause`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`, `hasMessageMatching`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed] vs current [randomNameSuffix, createHiveTable, env.executeTrino, cause, env.executeTrinoUpdate]; assertion helpers differ: legacy [assertQueryFailure, hasMessageMatching] vs current [assertThatThrownBy, hasMessageMatching]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed], verbs [DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrino, cause, env.executeTrinoUpdate], verbs [DROP].
- Audit status: `verified`

##### `testRedirectPartitionsToPartitioned`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testRedirectPartitionsToPartitioned`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testRedirectPartitionsToPartitioned`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createHiveTable`. Current setup shape: `createHiveTable`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`. Current action shape: `randomNameSuffix`, `env.executeTrino`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery] vs current [randomNameSuffix, createHiveTable, env.executeTrino, env.executeTrinoUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery], verbs [DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrino, env.executeTrinoUpdate], verbs [DROP].
- Audit status: `verified`

##### `testInsert`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testInsert`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testInsert`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `INSERT`, `COMMENT`, `createHiveTable`. Current setup shape: `INSERT`, `COMMENT`, `createHiveTable`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `VALUES`. Current action shape: `randomNameSuffix`, `env.executeTrinoUpdate`, `VALUES`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery, VALUES] vs current [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, VALUES, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery, VALUES], verbs [INSERT, COMMENT, DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, VALUES, env.executeTrino], verbs [INSERT, COMMENT, DROP].
- Audit status: `verified`

##### `testDelete`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testDelete`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testDelete`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `COMMENT`, `createHiveTable`. Current setup shape: `COMMENT`, `createHiveTable`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertResultsEqual`. Current assertion helpers: `assertResultsEqual`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `DROP`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery] vs current [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery], verbs [DELETE, SELECT, COMMENT, DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, env.executeTrino], verbs [DELETE, SELECT, COMMENT, DROP].
- Audit status: `verified`

##### `testUpdate`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testUpdate`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testUpdate`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `SET`, `createHiveTable`. Current setup shape: `SET`, `createHiveTable`.
- Action parity: Legacy action shape: `UPDATE`, `randomNameSuffix`, `onTrino`, `executeQuery`, `failed`. Current action shape: `UPDATE`, `randomNameSuffix`, `env.executeTrinoUpdate`, `cause`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed] vs current [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, cause]; assertion helpers differ: legacy [assertQueryFailure, hasMessageMatching] vs current [assertThatThrownBy, hasMessageContaining]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed], verbs [UPDATE, SET, DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, cause], verbs [UPDATE, SET, DROP].
- Audit status: `verified`

##### `testCreateOrReplaceTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testCreateOrReplaceTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testCreateOrReplaceTable`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `createHiveTable`. Current setup shape: `CREATE`, `createHiveTable`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `failed`. Current action shape: `randomNameSuffix`, `env.executeTrinoUpdate`, `cause`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`, `hasMessageMatching`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed] vs current [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, cause]; assertion helpers differ: legacy [assertQueryFailure, hasMessageMatching] vs current [assertThatThrownBy, hasMessageMatching]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed], verbs [CREATE]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, cause], verbs [CREATE].
- Audit status: `verified`

##### `testDropTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testDropTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testDropTable`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createHiveTable`. Current setup shape: `createHiveTable`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `failed`. Current action shape: `randomNameSuffix`, `env.executeTrinoUpdate`, `env.executeTrino`, `cause`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`, `hasMessageMatching`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed] vs current [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, env.executeTrino, cause]; assertion helpers differ: legacy [assertQueryFailure, hasMessageMatching] vs current [assertThatThrownBy, hasMessageMatching]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed], verbs [DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, env.executeTrino, cause], verbs [DROP].
- Audit status: `verified`

##### `testDescribe`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testDescribe`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testDescribe`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createHiveTable`. Current setup shape: `createHiveTable`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`. Current action shape: `randomNameSuffix`, `env.executeTrino`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertResultsEqual`. Current assertion helpers: `assertResultsEqual`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery] vs current [randomNameSuffix, createHiveTable, env.executeTrino, env.executeTrinoUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery], verbs [DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrino, env.executeTrinoUpdate], verbs [DROP].
- Audit status: `verified`

##### `testShowCreateTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testShowCreateTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testShowCreateTable`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`, `createHiveTable`. Current setup shape: `CREATE`, `COMMENT`, `createHiveTable`.
- Action parity: Legacy action shape: `SHOW`, `randomNameSuffix`, `onTrino`, `executeQuery`, `varchar`, `WITH`. Current action shape: `SHOW`, `randomNameSuffix`, `env.executeTrino`, `varchar`, `WITH`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery, varchar, WITH] vs current [randomNameSuffix, createHiveTable, env.executeTrino, varchar, WITH, env.executeTrinoUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery, varchar, WITH], verbs [CREATE, SHOW, COMMENT, DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrino, varchar, WITH, env.executeTrinoUpdate], verbs [CREATE, SHOW, COMMENT, DROP].
- Audit status: `verified`

##### `testShowStats`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testShowStats`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testShowStats`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `COMMENT`, `createHiveTable`. Current setup shape: `COMMENT`, `createHiveTable`.
- Action parity: Legacy action shape: `SHOW`, `randomNameSuffix`, `onTrino`, `executeQuery`. Current action shape: `SHOW`, `randomNameSuffix`, `env.executeTrino`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery] vs current [randomNameSuffix, createHiveTable, env.executeTrino, env.executeTrinoUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery], verbs [SHOW, COMMENT, DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrino, env.executeTrinoUpdate], verbs [SHOW, COMMENT, DROP].
- Audit status: `verified`

##### `testAlterTableRename`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testAlterTableRename`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testAlterTableRename`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `ALTER`, `createHiveTable`. Current setup shape: `ALTER`, `createHiveTable`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `failed`. Current action shape: `randomNameSuffix`, `env.executeTrinoUpdate`, `cause`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageMatching`, `assertResultsEqual`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `assertResultsEqual`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed] vs current [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, cause, env.executeTrino]; assertion helpers differ: legacy [assertQueryFailure, hasMessageMatching, assertResultsEqual] vs current [assertThatThrownBy, hasMessageContaining, assertResultsEqual]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed], verbs [ALTER, DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, cause, env.executeTrino], verbs [ALTER, DROP].
- Audit status: `verified`

##### `testAlterTableAddColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testAlterTableAddColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testAlterTableAddColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `ALTER`, `COMMENT`, `createHiveTable`. Current setup shape: `ALTER`, `COMMENT`, `createHiveTable`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `column`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `env.executeTrino`, `column`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `assertResultsEqual`. Current assertion helpers: `assertThat`, `containsOnly`, `assertResultsEqual`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery, column] vs current [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, env.executeTrino, column]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery, column], verbs [ALTER, COMMENT, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, env.executeTrino, column], verbs [ALTER, COMMENT, SELECT, DROP].
- Audit status: `verified`

##### `testAlterTableRenameColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testAlterTableRenameColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testAlterTableRenameColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `ALTER`, `COMMENT`, `createHiveTable`. Current setup shape: `ALTER`, `COMMENT`, `createHiveTable`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `column`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `env.executeTrino`, `column`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `assertResultsEqual`. Current assertion helpers: `assertThat`, `containsOnly`, `assertResultsEqual`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery, column] vs current [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, env.executeTrino, column]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery, column], verbs [ALTER, COMMENT, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, env.executeTrino, column], verbs [ALTER, COMMENT, SELECT, DROP].
- Audit status: `verified`

##### `testAlterTableDropColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testAlterTableDropColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testAlterTableDropColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `ALTER`, `COMMENT`, `SET`, `createHiveTable`. Current setup shape: `ALTER`, `COMMENT`, `SET`, `createHiveTable`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `column`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `env.executeTrino`, `column`, `env.executeTrinoInSession`, `session.executeUpdate`, `session.executeQuery`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `assertResultsEqual`. Current assertion helpers: `assertThat`, `containsOnly`, `assertResultsEqual`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery, column] vs current [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, env.executeTrino, column, env.executeTrinoInSession, session.executeUpdate, session.executeQuery]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery, column], verbs [ALTER, DROP, COMMENT, SET, SELECT]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, env.executeTrino, column, env.executeTrinoInSession, session.executeUpdate, session.executeQuery], verbs [ALTER, DROP, COMMENT, SET, SELECT].
- Audit status: `verified`

##### `testCommentTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testCommentTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testCommentTable`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `COMMENT`, `createHiveTable`. Current setup shape: `COMMENT`, `createHiveTable`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `format`. Current action shape: `randomNameSuffix`, `env.executeTrinoUpdate`, `String.format`.
- Assertion parity: Legacy assertion helpers: `assertTableComment`, `isNull`, `isEqualTo`. Current assertion helpers: `assertTableComment`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery, format] vs current [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, String.format]; assertion helpers differ: legacy [assertTableComment, isNull, isEqualTo] vs current [assertTableComment]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery, format], verbs [COMMENT, DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, String.format], verbs [COMMENT, DROP].
- Audit status: `verified`

##### `testShowGrants`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testShowGrants`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testShowGrants`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createHiveTable`. Current setup shape: `createHiveTable`.
- Action parity: Legacy action shape: `SHOW`, `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `failed`. Current action shape: `SHOW`, `randomNameSuffix`, `env.executeTrino`, `String.format`, `cause`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`, `hasMessageMatching`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery, format, failed] vs current [randomNameSuffix, createHiveTable, env.executeTrino, String.format, cause, env.executeTrinoUpdate]; assertion helpers differ: legacy [assertQueryFailure, hasMessageMatching] vs current [assertThatThrownBy, hasMessageMatching]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery, format, failed], verbs [SHOW, DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrino, String.format, cause, env.executeTrinoUpdate], verbs [SHOW, DROP].
- Audit status: `verified`

##### `testInformationSchemaColumns`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testInformationSchemaColumns`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testInformationSchemaColumns`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`, `createHiveTable`. Current setup shape: `CREATE`, `COMMENT`, `createHiveTable`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `varchar`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `env.executeTrino`, `String.format`, `varchar`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, createHiveTable, format, varchar] vs current [randomNameSuffix, env.executeTrinoUpdate, createHiveTable, env.executeTrino, String.format, varchar]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, createHiveTable, format, varchar], verbs [CREATE, SELECT, COMMENT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, createHiveTable, env.executeTrino, String.format, varchar], verbs [CREATE, SELECT, COMMENT, DROP].
- Audit status: `verified`

##### `testSystemJdbcColumns`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testSystemJdbcColumns`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testSystemJdbcColumns`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`, `createHiveTable`. Current setup shape: `CREATE`, `COMMENT`, `createHiveTable`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `format`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `env.executeTrino`, `String.format`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, createHiveTable, format] vs current [randomNameSuffix, env.executeTrinoUpdate, createHiveTable, env.executeTrino, String.format]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, createHiveTable, format], verbs [CREATE, SELECT, COMMENT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, createHiveTable, env.executeTrino, String.format], verbs [CREATE, SELECT, COMMENT, DROP].
- Audit status: `verified`

##### `testGrant`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testGrant`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testGrant`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `GRANT`, `createHiveTable`. Current setup shape: `GRANT`, `createHiveTable`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `failed`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `cause`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`, `hasMessageMatching`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed] vs current [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, cause]; assertion helpers differ: legacy [assertQueryFailure, hasMessageMatching] vs current [assertThatThrownBy, hasMessageMatching]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed], verbs [SELECT, GRANT, DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, cause], verbs [SELECT, GRANT, DROP].
- Audit status: `verified`

##### `testRevoke`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testRevoke`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testRevoke`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `REVOKE`, `createHiveTable`. Current setup shape: `REVOKE`, `createHiveTable`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `failed`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoUpdate`, `cause`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`, `hasMessageMatching`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed] vs current [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, cause]; assertion helpers differ: legacy [assertQueryFailure, hasMessageMatching] vs current [assertThatThrownBy, hasMessageMatching]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed], verbs [SELECT, REVOKE, DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, cause], verbs [SELECT, REVOKE, DROP].
- Audit status: `verified`

##### `testSetTableAuthorization`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testSetTableAuthorization`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testSetTableAuthorization`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `ALTER`, `SET`, `createHiveTable`. Current setup shape: `ALTER`, `SET`, `createHiveTable`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `failed`. Current action shape: `randomNameSuffix`, `env.executeTrinoUpdate`, `cause`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`, `hasMessageMatching`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed] vs current [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, cause]; assertion helpers differ: legacy [assertQueryFailure, hasMessageMatching] vs current [assertThatThrownBy, hasMessageMatching]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed], verbs [ALTER, SET, DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, cause], verbs [ALTER, SET, DROP].
- Audit status: `verified`

##### `testDeny`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `testDeny`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergRedirectionToHive.java` ->
  `TestIcebergRedirectionToHive.testDeny`
- Mapping type: `direct`
- Environment parity: Current class requires `HiveIcebergRedirectionsEnvironment`. Routed by source review into `SuiteIceberg` run 2.
- Tag parity: Current tags: `HiveIcebergRedirections`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createHiveTable`. Current setup shape: `createHiveTable`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `failed`. Current action shape: `randomNameSuffix`, `env.executeTrinoUpdate`, `cause`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`, `hasMessageMatching`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `DROP`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed] vs current [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, cause]; assertion helpers differ: legacy [assertQueryFailure, hasMessageMatching] vs current [assertThatThrownBy, hasMessageMatching]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createHiveTable, onTrino, executeQuery, failed], verbs [DELETE, DROP]. Current flow summary -> helpers [randomNameSuffix, createHiveTable, env.executeTrinoUpdate, cause], verbs [DELETE, DROP].
- Audit status: `verified`

### `TestIcebergSparkCompatibility`


- Owning migration commit: `Migrate TestIcebergSparkCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java`
- Class-level environment requirement: `SparkIcebergEnvironment`.
- Class-level tags: `Iceberg`.
- Method inventory complete: Yes. Legacy methods: `90`. Current methods: `90`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testTrinoReadingSparkData`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoReadingSparkData`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoReadingSparkData`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `format`, `s`, `decimal`, `TBLPROPERTIES`, `onTrino`, `hasNoRows`, `VALUES`, `CAST`, `igDecimal`, `Timestamp.valueOf`, `Date.valueOf`. Current action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `format`, `s`, `decimal`, `TBLPROPERTIES`, `env.executeTrino`, `hasNoRows`, `VALUES`, `CAST`, `igDecimal`, `Timestamp.valueOf`, `Date.valueOf`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `row`, `containsOnly`. Current assertion helpers: `assertThat`, `row`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, decimal, TBLPROPERTIES, onTrino, hasNoRows, VALUES, CAST, igDecimal, Timestamp.valueOf, Date.valueOf] vs current [toLowerCase, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, decimal, TBLPROPERTIES, env.executeTrino, hasNoRows, VALUES, CAST, igDecimal, Timestamp.valueOf, Date.valueOf, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, decimal, TBLPROPERTIES, onTrino, hasNoRows, VALUES, CAST, igDecimal, Timestamp.valueOf, Date.valueOf], verbs [DROP, CREATE, SELECT, INSERT]. Current flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, decimal, TBLPROPERTIES, env.executeTrino, hasNoRows, VALUES, CAST, igDecimal, Timestamp.valueOf, Date.valueOf, env.executeSpark], verbs [DROP, CREATE, SELECT, INSERT].
- Audit status: `verified`

##### `testSparkReadingTrinoData`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkReadingTrinoData`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkReadingTrinoData`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `s`, `decimal`, `timestamp`, `WITH`, `formatted`, `format`, `nsupportedOperationException`, `igDecimal`, `CAST`, `onSpark`. Current action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `s`, `decimal`, `timestamp`, `WITH`, `formatted`, `format`, `nsupportedOperationException`, `igDecimal`, `env.executeTrino`, `CAST`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, trinoTableName, sparkTableName, onTrino, executeQuery, s, decimal, timestamp, WITH, formatted, format, nsupportedOperationException, igDecimal, CAST, onSpark] vs current [toLowerCase, trinoTableName, sparkTableName, env.executeTrinoUpdate, s, decimal, timestamp, WITH, formatted, format, nsupportedOperationException, igDecimal, env.executeTrino, CAST, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, onTrino, executeQuery, s, decimal, timestamp, WITH, formatted, format, nsupportedOperationException, igDecimal, CAST, onSpark], verbs [SELECT, CREATE, DROP, INSERT]. Current flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, env.executeTrinoUpdate, s, decimal, timestamp, WITH, formatted, format, nsupportedOperationException, igDecimal, env.executeTrino, CAST, env.executeSpark], verbs [SELECT, CREATE, DROP, INSERT].
- Audit status: `verified`

##### `testSparkReadingTrinoDataWithVersions`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkReadingTrinoDataWithVersions`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkReadingTrinoDataWithVersions`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `format`, `s`, `decimal`, `timestamp`, `time`, `WITH`, `nsupportedOperationException`, `igDecimal`, `CAST`, `onSpark`. Current action shape: `SELECT`, `assumeTrinoSupportsFormatVersion3`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `format`, `s`, `decimal`, `timestamp`, `WITH`, `nsupportedOperationException`, `igDecimal`, `env.executeTrino`, `CAST`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, decimal, timestamp, time, WITH, nsupportedOperationException, igDecimal, CAST, onSpark] vs current [assumeTrinoSupportsFormatVersion3, toLowerCase, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, decimal, timestamp, WITH, nsupportedOperationException, igDecimal, env.executeTrino, CAST, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, decimal, timestamp, time, WITH, nsupportedOperationException, igDecimal, CAST, onSpark], verbs [DROP, SELECT, CREATE, INSERT]. Current flow summary -> helpers [assumeTrinoSupportsFormatVersion3, toLowerCase, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, decimal, timestamp, WITH, nsupportedOperationException, igDecimal, env.executeTrino, CAST, env.executeSpark], verbs [DROP, SELECT, CREATE, INSERT].
- Audit status: `verified`

##### `testSparkCreatesTrinoDrops`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkCreatesTrinoDrops`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkCreatesTrinoDrops`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `onSpark`, `executeQuery`, `format`, `s`, `TBLPROPERTIES`, `sparkTableName`, `onTrino`, `trinoTableName`. Current action shape: `env.executeSparkUpdate`, `format`, `s`, `TBLPROPERTIES`, `sparkTableName`, `env.executeTrinoUpdate`, `trinoTableName`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [onSpark, executeQuery, format, s, TBLPROPERTIES, sparkTableName, onTrino, trinoTableName] vs current [env.executeSparkUpdate, format, s, TBLPROPERTIES, sparkTableName, env.executeTrinoUpdate, trinoTableName]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [onSpark, executeQuery, format, s, TBLPROPERTIES, sparkTableName, onTrino, trinoTableName], verbs [CREATE, DROP]. Current flow summary -> helpers [env.executeSparkUpdate, format, s, TBLPROPERTIES, sparkTableName, env.executeTrinoUpdate, trinoTableName], verbs [CREATE, DROP].
- Audit status: `verified`

##### `testTrinoCreatesSparkDrops`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoCreatesSparkDrops`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoCreatesSparkDrops`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `onTrino`, `executeQuery`, `format`, `s`, `trinoTableName`, `onSpark`, `sparkTableName`. Current action shape: `env.executeTrinoUpdate`, `format`, `s`, `trinoTableName`, `env.executeSparkUpdate`, `sparkTableName`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, format, s, trinoTableName, onSpark, sparkTableName] vs current [env.executeTrinoUpdate, format, s, trinoTableName, env.executeSparkUpdate, sparkTableName]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, format, s, trinoTableName, onSpark, sparkTableName], verbs [CREATE, DROP]. Current flow summary -> helpers [env.executeTrinoUpdate, format, s, trinoTableName, env.executeSparkUpdate, sparkTableName], verbs [CREATE, DROP].
- Audit status: `verified`

##### `testSparkReadsTrinoPartitionedTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkReadsTrinoPartitionedTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkReadsTrinoPartitionedTable`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `format`, `s`, `WITH`, `VALUES`, `onSpark`. Current action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `format`, `s`, `WITH`, `VALUES`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, WITH, VALUES, onSpark] vs current [toLowerCase, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, WITH, VALUES, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, WITH, VALUES, onSpark], verbs [DROP, CREATE, INSERT, SELECT]. Current flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, WITH, VALUES, env.executeTrino, env.executeSpark], verbs [DROP, CREATE, INSERT, SELECT].
- Audit status: `verified`

##### `testTrinoReadsSparkPartitionedTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoReadsSparkPartitionedTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoReadsSparkPartitionedTable`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `format`, `s`, `BY`, `TBLPROPERTIES`, `VALUES`, `onTrino`. Current action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `format`, `s`, `BY`, `TBLPROPERTIES`, `VALUES`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, BY, TBLPROPERTIES, VALUES, onTrino] vs current [toLowerCase, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, BY, TBLPROPERTIES, VALUES, env.executeSpark, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, BY, TBLPROPERTIES, VALUES, onTrino], verbs [DROP, CREATE, INSERT, SELECT]. Current flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, BY, TBLPROPERTIES, VALUES, env.executeSpark, env.executeTrino], verbs [DROP, CREATE, INSERT, SELECT].
- Audit status: `verified`

##### `testSparkReadsTrinoNestedPartitionedTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkReadsTrinoNestedPartitionedTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkReadsTrinoNestedPartitionedTable`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `SET`, `ALTER`.
- Action parity: Legacy action shape: `UPDATE`, `SELECT`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `format`, `s`, `ROW`, `WITH`, `ImmutableList.of`, `onSpark`. Current action shape: `UPDATE`, `SELECT`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `format`, `s`, `ROW`, `WITH`, `ImmutableList.of`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageContaining`, `row`, `assertThat`, `containsOnly`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `DROP`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, ROW, WITH, ImmutableList.of, onSpark] vs current [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, ROW, WITH, ImmutableList.of, env.executeTrino, env.executeSpark]; assertion helpers differ: legacy [assertQueryFailure, hasMessageContaining, row, assertThat, containsOnly] vs current [assertThatThrownBy, hasMessageContaining, row, assertThat, containsOnly]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, ROW, WITH, ImmutableList.of, onSpark], verbs [CREATE, INSERT, UPDATE, SET, DELETE, ALTER, DROP, SELECT]. Current flow summary -> helpers [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, ROW, WITH, ImmutableList.of, env.executeTrino, env.executeSpark], verbs [CREATE, INSERT, UPDATE, SET, DELETE, ALTER, DROP, SELECT].
- Audit status: `verified`

##### `testTrinoReadsSparkNestedPartitionedTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoReadsSparkNestedPartitionedTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoReadsSparkNestedPartitionedTable`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `SET`, `ALTER`.
- Action parity: Legacy action shape: `UPDATE`, `SELECT`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `format`, `s`, `BY`, `TBLPROPERTIES`, `named_struct`, `identity`, `onTrino`. Current action shape: `UPDATE`, `SELECT`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `format`, `s`, `BY`, `TBLPROPERTIES`, `named_struct`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageContaining`, `row`, `assertThat`, `containsOnly`. Current assertion helpers: `assertThatThrownBy`, `isInstanceOf`, `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `DROP`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, BY, TBLPROPERTIES, named_struct, identity, onTrino] vs current [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, BY, TBLPROPERTIES, named_struct, env.executeTrino, env.executeSpark]; assertion helpers differ: legacy [assertQueryFailure, hasMessageContaining, row, assertThat, containsOnly] vs current [assertThatThrownBy, isInstanceOf, row, assertThat, containsOnly]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, BY, TBLPROPERTIES, named_struct, identity, onTrino], verbs [CREATE, INSERT, UPDATE, SET, DELETE, ALTER, DROP, SELECT]. Current flow summary -> helpers [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, BY, TBLPROPERTIES, named_struct, env.executeTrino, env.executeSpark], verbs [CREATE, INSERT, UPDATE, SET, DELETE, ALTER, DROP, SELECT].
- Audit status: `verified`

##### `testSparkReadsTrinoNestedPartitionedTableWithOneFieldStruct`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkReadsTrinoNestedPartitionedTableWithOneFieldStruct`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkReadsTrinoNestedPartitionedTableWithOneFieldStruct`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `format`, `s`, `ROW`, `WITH`, `onSpark`. Current action shape: `SELECT`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `format`, `s`, `ROW`, `WITH`, `env.executeTrino`, `env.executeSpark`, `hasStackTraceContaining`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`, `assertThatThrownBy`, `hasMessageContaining`. Current assertion helpers: `row`, `assertThat`, `containsOnly`, `assertThatThrownBy`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, ROW, WITH, onSpark] vs current [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, ROW, WITH, env.executeTrino, env.executeSpark, hasStackTraceContaining]; assertion helpers differ: legacy [row, assertThat, containsOnly, assertThatThrownBy, hasMessageContaining] vs current [row, assertThat, containsOnly, assertThatThrownBy]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, ROW, WITH, onSpark], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, ROW, WITH, env.executeTrino, env.executeSpark, hasStackTraceContaining], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testTrinoReadsSparkNestedPartitionedTableWithOneFieldStruct`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoReadsSparkNestedPartitionedTableWithOneFieldStruct`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoReadsSparkNestedPartitionedTableWithOneFieldStruct`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `format`, `s`, `BY`, `TBLPROPERTIES`, `named_struct`, `onTrino`. Current action shape: `SELECT`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `format`, `s`, `BY`, `TBLPROPERTIES`, `named_struct`, `env.executeTrino`, `env.executeSpark`, `hasStackTraceContaining`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`, `assertThatThrownBy`, `hasMessageContaining`. Current assertion helpers: `row`, `assertThat`, `containsOnly`, `assertThatThrownBy`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, BY, TBLPROPERTIES, named_struct, onTrino] vs current [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, BY, TBLPROPERTIES, named_struct, env.executeTrino, env.executeSpark, hasStackTraceContaining]; assertion helpers differ: legacy [row, assertThat, containsOnly, assertThatThrownBy, hasMessageContaining] vs current [row, assertThat, containsOnly, assertThatThrownBy]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, BY, TBLPROPERTIES, named_struct, onTrino], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, BY, TBLPROPERTIES, named_struct, env.executeTrino, env.executeSpark, hasStackTraceContaining], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testTrinoPartitionedByRealWithNaN`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoPartitionedByRealWithNaN`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoPartitionedByRealWithNaN`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `testTrinoPartitionedByNaN`, `trinoTableName`, `sparkTableName`, `onTrino`, `onSpark`. Current action shape: `SELECT`, `randomNameSuffix`, `testTrinoPartitionedByNaN`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper structure is unchanged; the JUnit port swaps the executor plumbing to `env.executeTrino*` and `env.executeSpark`.
- Known intentional difference: None.
- Reviewer note: The shared helper creates a Trino Iceberg table partitioned on `col` with a NaN literal of the requested floating type, verifies both Trino and Spark read the NaN row back, and drops the table.
- Audit status: `verified`

##### `testTrinoPartitionedByDoubleWithNaN`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoPartitionedByDoubleWithNaN`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoPartitionedByDoubleWithNaN`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `testTrinoPartitionedByNaN`, `trinoTableName`, `sparkTableName`, `onTrino`, `onSpark`. Current action shape: `SELECT`, `randomNameSuffix`, `testTrinoPartitionedByNaN`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper structure is unchanged; the JUnit port swaps the executor plumbing to `env.executeTrino*` and `env.executeSpark`.
- Known intentional difference: None.
- Reviewer note: The shared helper creates a Trino Iceberg table partitioned on `col` with a NaN literal of the requested floating type, verifies both Trino and Spark read the NaN row back, and drops the table.
- Audit status: `verified`

##### `testSparkPartitionedByRealWithNaN`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkPartitionedByRealWithNaN`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkPartitionedByRealWithNaN`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `testSparkPartitionedByNaN`, `trinoTableName`, `sparkTableName`, `onSpark`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `testSparkPartitionedByNaN`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper structure is unchanged; the JUnit port swaps the executor plumbing to `env.executeSpark*` and `env.executeTrino`.
- Known intentional difference: None.
- Reviewer note: The shared helper creates a Spark Iceberg table partitioned on `col` with a NaN literal of the requested floating type, verifies both Spark and Trino read the NaN row back, and drops the table.
- Audit status: `verified`

##### `testSparkPartitionedByDoubleWithNaN`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkPartitionedByDoubleWithNaN`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkPartitionedByDoubleWithNaN`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `testSparkPartitionedByNaN`, `trinoTableName`, `sparkTableName`, `onSpark`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `testSparkPartitionedByNaN`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper structure is unchanged; the JUnit port swaps the executor plumbing to `env.executeSpark*` and `env.executeTrino`.
- Known intentional difference: None.
- Reviewer note: The shared helper creates a Spark Iceberg table partitioned on `col` with a NaN literal of the requested floating type, verifies both Spark and Trino read the NaN row back, and drops the table.
- Audit status: `verified`

##### `testTrinoReadingCompositeSparkData`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoReadingCompositeSparkData`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoReadingCompositeSparkData`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `format`, `s`, `TBLPROPERTIES`, `map`, `array`, `named_struct`, `onTrino`. Current action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `format`, `s`, `TBLPROPERTIES`, `map`, `array`, `named_struct`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, map, array, named_struct, onTrino] vs current [toLowerCase, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, map, array, named_struct, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, map, array, named_struct, onTrino], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, map, array, named_struct, env.executeTrino], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testSparkReadingCompositeTrinoData`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkReadingCompositeTrinoData`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkReadingCompositeTrinoData`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `format`, `s`, `MAP`, `ARRAY`, `ROW`, `WITH`, `VALUES`, `onSpark`. Current action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `format`, `s`, `MAP`, `ARRAY`, `ROW`, `WITH`, `VALUES`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, MAP, ARRAY, ROW, WITH, VALUES, onSpark] vs current [toLowerCase, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, MAP, ARRAY, ROW, WITH, VALUES, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, MAP, ARRAY, ROW, WITH, VALUES, onSpark], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, MAP, ARRAY, ROW, WITH, VALUES, env.executeSpark], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testTrinoReadingSparkIcebergTablePropertiesData`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoReadingSparkIcebergTablePropertiesData`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoReadingSparkIcebergTablePropertiesData`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `sparkTableName`, `trinoTableName`, `onSpark`, `executeQuery`, `format`, `s`, `TBLPROPERTIES`, `storageFormat.toString`, `onTrino`, `storageFormat.name`. Current action shape: `SELECT`, `toLowerCase`, `sparkTableName`, `trinoTableName`, `env.executeSparkUpdate`, `format`, `s`, `TBLPROPERTIES`, `storageFormat.toString`, `env.executeTrino`, `storageFormat.name`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `row`. Current assertion helpers: `assertThat`, `contains`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, sparkTableName, trinoTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, storageFormat.toString, onTrino, storageFormat.name] vs current [toLowerCase, sparkTableName, trinoTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, storageFormat.toString, env.executeTrino, storageFormat.name]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, sparkTableName, trinoTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, storageFormat.toString, onTrino, storageFormat.name], verbs [DROP, CREATE, SELECT]. Current flow summary -> helpers [toLowerCase, sparkTableName, trinoTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, storageFormat.toString, env.executeTrino, storageFormat.name], verbs [DROP, CREATE, SELECT].
- Audit status: `verified`

##### `testSparkReadingTrinoIcebergTablePropertiesData`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkReadingTrinoIcebergTablePropertiesData`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkReadingTrinoIcebergTablePropertiesData`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SHOW`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `WITH`, `MAP`, `onSpark`. Current action shape: `SHOW`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `WITH`, `MAP`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `row`. Current assertion helpers: `assertThat`, `contains`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, WITH, MAP, onSpark] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, WITH, MAP, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, WITH, MAP, onSpark], verbs [CREATE, SHOW, DROP]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, WITH, MAP, env.executeSpark], verbs [CREATE, SHOW, DROP].
- Audit status: `verified`

##### `testTrinoReadingNestedSparkData`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoReadingNestedSparkData`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoReadingNestedSparkData`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `format`, `s`, `TBLPROPERTIES`, `map`, `array`, `named_struct`, `onTrino`. Current action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `format`, `s`, `TBLPROPERTIES`, `map`, `array`, `named_struct`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, map, array, named_struct, onTrino] vs current [toLowerCase, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, map, array, named_struct, env.executeSpark, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, map, array, named_struct, onTrino], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, map, array, named_struct, env.executeSpark, env.executeTrino], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testSparkReadingNestedTrinoData`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkReadingNestedTrinoData`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkReadingNestedTrinoData`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `format`, `s`, `MAP`, `ARRAY`, `ROW`, `WITH`, `map`, `onSpark`. Current action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `format`, `s`, `MAP`, `ARRAY`, `ROW`, `WITH`, `map`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, MAP, ARRAY, ROW, WITH, map, onSpark] vs current [toLowerCase, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, MAP, ARRAY, ROW, WITH, map, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, MAP, ARRAY, ROW, WITH, map, onSpark], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, MAP, ARRAY, ROW, WITH, map, env.executeTrino, env.executeSpark], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testTrinoWritingDataAfterSpark`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoWritingDataAfterSpark`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoWritingDataAfterSpark`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `sparkTableName`, `trinoTableName`, `onSpark`, `executeQuery`, `onTrino`, `ImmutableList.of`. Current action shape: `SELECT`, `toLowerCase`, `sparkTableName`, `trinoTableName`, `env.executeSparkUpdate`, `env.executeTrinoUpdate`, `ImmutableList.of`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, sparkTableName, trinoTableName, onSpark, executeQuery, onTrino, ImmutableList.of] vs current [toLowerCase, sparkTableName, trinoTableName, env.executeSparkUpdate, env.executeTrinoUpdate, ImmutableList.of, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, sparkTableName, trinoTableName, onSpark, executeQuery, onTrino, ImmutableList.of], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [toLowerCase, sparkTableName, trinoTableName, env.executeSparkUpdate, env.executeTrinoUpdate, ImmutableList.of, env.executeTrino, env.executeSpark], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testTrinoSparkConcurrentInsert`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoSparkConcurrentInsert`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoSparkConcurrentInsert`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `inserted.add`. Current setup shape: `CREATE`, `INSERT`, `inserted.add`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `Executors.newFixedThreadPool`, `yclicBarrier`, `onSpark`, `executor.invokeAll`, `Stream.of`, `map`, `barrier.await`, `engine.name`, `toLowerCase`, `onTrino.executeQuery`, `format`, `VALUES`, `onSpark.executeQuery`, `nsupportedOperationException`, `collect`, `toImmutableList`, `stream`, `flatMap`, `hasSizeBetween`, `succeed`, `count`, `executor.shutdownNow`. Current action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `Executors.newFixedThreadPool`, `yclicBarrier`, `executor.invokeAll`, `Stream.of`, `map`, `barrier.await`, `engine.name`, `toLowerCase`, `format`, `VALUES`, `env.executeSparkUpdate`, `nsupportedOperationException`, `collect`, `toImmutableList`, `stream`, `flatMap`, `hasSizeBetween`, `succeed`, `env.executeTrino`, `count`, `executor.shutdownNow`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, Executors.newFixedThreadPool, yclicBarrier, onSpark, executor.invokeAll, Stream.of, map, barrier.await, engine.name, toLowerCase, onTrino.executeQuery, format, VALUES, onSpark.executeQuery, nsupportedOperationException, inserted.add, collect, toImmutableList, stream, flatMap, hasSizeBetween, succeed, count, executor.shutdownNow] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, Executors.newFixedThreadPool, yclicBarrier, executor.invokeAll, Stream.of, map, barrier.await, engine.name, toLowerCase, format, VALUES, env.executeSparkUpdate, nsupportedOperationException, inserted.add, collect, toImmutableList, stream, flatMap, hasSizeBetween, succeed, env.executeTrino, count, executor.shutdownNow]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, Executors.newFixedThreadPool, yclicBarrier, onSpark, executor.invokeAll, Stream.of, map, barrier.await, engine.name, toLowerCase, onTrino.executeQuery, format, VALUES, onSpark.executeQuery, nsupportedOperationException, inserted.add, collect, toImmutableList, stream, flatMap, hasSizeBetween, succeed, count, executor.shutdownNow], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, Executors.newFixedThreadPool, yclicBarrier, executor.invokeAll, Stream.of, map, barrier.await, engine.name, toLowerCase, format, VALUES, env.executeSparkUpdate, nsupportedOperationException, inserted.add, collect, toImmutableList, stream, flatMap, hasSizeBetween, succeed, env.executeTrino, count, executor.shutdownNow], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testTrinoShowingSparkCreatedTables`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoShowingSparkCreatedTables`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoShowingSparkCreatedTables`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SHOW`, `onSpark`, `executeQuery`, `onTrino`, `format`, `s`, `TBLPROPERTIES`, `sparkTableName`, `trinoTableName`. Current action shape: `SHOW`, `env.executeSparkUpdate`, `sparkTableName`, `env.executeTrinoUpdate`, `trinoTableName`, `format`, `s`, `TBLPROPERTIES`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [onSpark, executeQuery, onTrino, format, s, TBLPROPERTIES, sparkTableName, trinoTableName] vs current [env.executeSparkUpdate, sparkTableName, env.executeTrinoUpdate, trinoTableName, format, s, TBLPROPERTIES, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [onSpark, executeQuery, onTrino, format, s, TBLPROPERTIES, sparkTableName, trinoTableName], verbs [DROP, CREATE, SHOW]. Current flow summary -> helpers [env.executeSparkUpdate, sparkTableName, env.executeTrinoUpdate, trinoTableName, format, s, TBLPROPERTIES, env.executeTrino], verbs [DROP, CREATE, SHOW].
- Audit status: `verified`

##### `testSparkReadingTrinoParquetBloomFilters`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkReadingTrinoParquetBloomFilters`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkReadingTrinoParquetBloomFilters`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `String.format`, `s`, `WITH`, `format`, `FROM`, `DATA`, `onSpark`. Current action shape: `SELECT`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `String.format`, `s`, `WITH`, `format`, `FROM`, `DATA`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `assertTrinoBloomFilterTableSelectResult`, `assertSparkBloomFilterTableSelectResult`. Current assertion helpers: `assertTrinoBloomFilterTableSelectResult`, `assertSparkBloomFilterTableSelectResult`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [trinoTableName, sparkTableName, onTrino, executeQuery, String.format, s, WITH, format, FROM, DATA, onSpark] vs current [trinoTableName, sparkTableName, env.executeTrinoUpdate, String.format, s, WITH, format, FROM, DATA, env.executeSparkUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [trinoTableName, sparkTableName, onTrino, executeQuery, String.format, s, WITH, format, FROM, DATA, onSpark], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [trinoTableName, sparkTableName, env.executeTrinoUpdate, String.format, s, WITH, format, FROM, DATA, env.executeSparkUpdate], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testTrinoReadingSparkParquetBloomFilters`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoReadingSparkParquetBloomFilters`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoReadingSparkParquetBloomFilters`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `String.format`, `s`, `TBLPROPERTIES`, `format`, `FROM`, `DATA`, `onTrino`. Current action shape: `SELECT`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `String.format`, `s`, `TBLPROPERTIES`, `format`, `FROM`, `DATA`, `env.executeTrinoUpdate`.
- Assertion parity: Legacy assertion helpers: `assertTrinoBloomFilterTableSelectResult`, `assertSparkBloomFilterTableSelectResult`. Current assertion helpers: `assertTrinoBloomFilterTableSelectResult`, `assertSparkBloomFilterTableSelectResult`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [trinoTableName, sparkTableName, onSpark, executeQuery, String.format, s, TBLPROPERTIES, format, FROM, DATA, onTrino] vs current [trinoTableName, sparkTableName, env.executeSparkUpdate, String.format, s, TBLPROPERTIES, format, FROM, DATA, env.executeTrinoUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [trinoTableName, sparkTableName, onSpark, executeQuery, String.format, s, TBLPROPERTIES, format, FROM, DATA, onTrino], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [trinoTableName, sparkTableName, env.executeSparkUpdate, String.format, s, TBLPROPERTIES, format, FROM, DATA, env.executeTrinoUpdate], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testTrinoAnalyze`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoAnalyze`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoAnalyze`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `ANALYZE`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `List.of`, `onSpark`. Current action shape: `SELECT`, `ANALYZE`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `List.of`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, ANALYZE, List.of, onSpark] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, ANALYZE, List.of, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, ANALYZE, List.of, onSpark], verbs [DROP, CREATE, SELECT, ANALYZE]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, ANALYZE, List.of, env.executeTrino, env.executeSpark], verbs [DROP, CREATE, SELECT, ANALYZE].
- Audit status: `verified`

##### `testOptimizeManifests`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testOptimizeManifests`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testOptimizeManifests`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `onSpark`, `executeQuery`, `VALUES`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `env.executeSparkUpdate`, `VALUES`, `env.executeTrinoUpdate`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, sparkTableName, trinoTableName, onSpark, executeQuery, VALUES, onTrino] vs current [randomNameSuffix, sparkTableName, trinoTableName, env.executeSparkUpdate, VALUES, env.executeTrinoUpdate, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, sparkTableName, trinoTableName, onSpark, executeQuery, VALUES, onTrino], verbs [CREATE, INSERT, ALTER, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, sparkTableName, trinoTableName, env.executeSparkUpdate, VALUES, env.executeTrinoUpdate, env.executeTrino, env.executeSpark], verbs [CREATE, INSERT, ALTER, SELECT, DROP].
- Audit status: `verified`

##### `testAlterTableExecuteProceduresOnEmptyTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testAlterTableExecuteProceduresOnEmptyTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testAlterTableExecuteProceduresOnEmptyTable`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`. Current setup shape: `CREATE`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `format`, `s`, `onTrino`, `expire_snapshots`, `remove_orphan_files`, `hasNoRows`. Current action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `format`, `s`, `env.executeTrinoUpdate`, `expire_snapshots`, `remove_orphan_files`, `env.executeTrino`, `hasNoRows`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, onTrino, expire_snapshots, remove_orphan_files, hasNoRows] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, env.executeTrinoUpdate, expire_snapshots, remove_orphan_files, env.executeTrino, hasNoRows]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, onTrino, expire_snapshots, remove_orphan_files, hasNoRows], verbs [CREATE, ALTER, SELECT]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, env.executeTrinoUpdate, expire_snapshots, remove_orphan_files, env.executeTrino, hasNoRows], verbs [CREATE, ALTER, SELECT].
- Audit status: `verified`

##### `testAddNotNullColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testAddNotNullColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testAddNotNullColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`. Current setup shape: `CREATE`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `onSpark`. Current action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `env.executeTrino`, `env.executeSparkUpdate`, `hasStackTraceContaining`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, onSpark] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, env.executeTrino, env.executeSparkUpdate, hasStackTraceContaining]; assertion helpers differ: legacy [assertThat, containsOnly, row, assertQueryFailure, hasMessageMatching] vs current [assertThat, containsOnly, row, assertThatThrownBy, hasMessageContaining]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, onSpark], verbs [CREATE, SELECT, ALTER, DROP]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, env.executeTrino, env.executeSparkUpdate, hasStackTraceContaining], verbs [CREATE, SELECT, ALTER, DROP].
- Audit status: `verified`

##### `testAddNestedField`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testAddNestedField`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testAddNestedField`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`. Current setup shape: `CREATE`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `CAST`, `onSpark`. Current action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `CAST`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, CAST, onSpark] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, CAST, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, CAST, onSpark], verbs [CREATE, SELECT, ALTER, DROP]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, CAST, env.executeTrino, env.executeSpark], verbs [CREATE, SELECT, ALTER, DROP].
- Audit status: `verified`

##### `testDropNestedField`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testDropNestedField`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testDropNestedField`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`. Current setup shape: `CREATE`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `CAST`, `onSpark`. Current action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `CAST`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, CAST, onSpark] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, CAST, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, CAST, onSpark], verbs [CREATE, SELECT, ALTER, DROP]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, CAST, env.executeTrino, env.executeSpark], verbs [CREATE, SELECT, ALTER, DROP].
- Audit status: `verified`

##### `testTrinoReadsSparkRowLevelDeletes`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoReadsSparkRowLevelDeletes`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoReadsSparkRowLevelDeletes`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `MERGE`, `CALL`, `SELECT`, `toLowerCase`, `format`, `tableStorageFormat.name`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `onSpark`, `executeQuery`, `BY`, `TBLPROPERTIES`, `VALUES`, `system.rewrite_data_files`, `map`, `ImmutableList.of`, `onTrino`. Current action shape: `MERGE`, `CALL`, `SELECT`, `toLowerCase`, `format`, `tableStorageFormat.name`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `env.executeSparkUpdate`, `BY`, `TBLPROPERTIES`, `VALUES`, `system.rewrite_data_files`, `map`, `ImmutableList.of`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `DROP`, `deleteFileStorageFormat.name`. Current cleanup shape: `DELETE`, `DROP`, `deleteFileStorageFormat.name`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, format, tableStorageFormat.name, deleteFileStorageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, onSpark, executeQuery, BY, TBLPROPERTIES, VALUES, system.rewrite_data_files, map, ImmutableList.of, onTrino] vs current [toLowerCase, format, tableStorageFormat.name, deleteFileStorageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, env.executeSparkUpdate, BY, TBLPROPERTIES, VALUES, system.rewrite_data_files, map, ImmutableList.of, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, format, tableStorageFormat.name, deleteFileStorageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, onSpark, executeQuery, BY, TBLPROPERTIES, VALUES, system.rewrite_data_files, map, ImmutableList.of, onTrino], verbs [CREATE, DELETE, MERGE, INSERT, CALL, SELECT, DROP]. Current flow summary -> helpers [toLowerCase, format, tableStorageFormat.name, deleteFileStorageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, env.executeSparkUpdate, BY, TBLPROPERTIES, VALUES, system.rewrite_data_files, map, ImmutableList.of, env.executeTrino, env.executeSpark], verbs [CREATE, DELETE, MERGE, INSERT, CALL, SELECT, DROP].
- Audit status: `verified`

##### `testSparkReadsTrinoRowLevelDeletes`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkReadsTrinoRowLevelDeletes`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkReadsTrinoRowLevelDeletes`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `format`, `storageFormat.name`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `onTrino`, `executeQuery`, `WITH`, `VALUES`, `ImmutableList.of`, `onSpark`. Current action shape: `SELECT`, `toLowerCase`, `format`, `storageFormat.name`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `env.executeTrinoUpdate`, `WITH`, `VALUES`, `ImmutableList.of`, `env.executeTrino`, `env.executeSpark`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `DROP`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, format, storageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, onTrino, executeQuery, WITH, VALUES, ImmutableList.of, onSpark] vs current [toLowerCase, format, storageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, env.executeTrinoUpdate, WITH, VALUES, ImmutableList.of, env.executeTrino, env.executeSpark, env.executeSparkUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, format, storageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, onTrino, executeQuery, WITH, VALUES, ImmutableList.of, onSpark], verbs [CREATE, INSERT, DELETE, SELECT, DROP]. Current flow summary -> helpers [toLowerCase, format, storageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, env.executeTrinoUpdate, WITH, VALUES, ImmutableList.of, env.executeTrino, env.executeSpark, env.executeSparkUpdate], verbs [CREATE, INSERT, DELETE, SELECT, DROP].
- Audit status: `verified`

##### `testSparkReadsTrinoV3DeletionVectors`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkReadsTrinoV3DeletionVectors`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkReadsTrinoV3DeletionVectors`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `format`, `storageFormat.name`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `onTrino`, `executeQuery`, `WITH`, `VALUES`, `IN`, `ImmutableList.of`, `onSpark`. Current action shape: `SELECT`, `assumeTrinoSupportsFormatVersion3`, `toLowerCase`, `format`, `storageFormat.name`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `env.executeTrinoUpdate`, `WITH`, `VALUES`, `IN`, `ImmutableList.of`, `env.executeTrino`, `env.executeSpark`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `DROP`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, format, storageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, onTrino, executeQuery, WITH, VALUES, IN, ImmutableList.of, onSpark] vs current [assumeTrinoSupportsFormatVersion3, toLowerCase, format, storageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, env.executeTrinoUpdate, WITH, VALUES, IN, ImmutableList.of, env.executeTrino, env.executeSpark, env.executeSparkUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, format, storageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, onTrino, executeQuery, WITH, VALUES, IN, ImmutableList.of, onSpark], verbs [CREATE, INSERT, DELETE, SELECT, DROP]. Current flow summary -> helpers [assumeTrinoSupportsFormatVersion3, toLowerCase, format, storageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, env.executeTrinoUpdate, WITH, VALUES, IN, ImmutableList.of, env.executeTrino, env.executeSpark, env.executeSparkUpdate], verbs [CREATE, INSERT, DELETE, SELECT, DROP].
- Audit status: `verified`

##### `testTrinoReadsSparkV3DeletionVectors`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoReadsSparkV3DeletionVectors`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoReadsSparkV3DeletionVectors`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `MERGE`, `SELECT`, `toLowerCase`, `format`, `storageFormat.name`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `onSpark`, `executeQuery`, `BY`, `TBLPROPERTIES`, `VALUES`, `ImmutableList.of`, `onTrino`. Current action shape: `MERGE`, `SELECT`, `assumeTrinoSupportsFormatVersion3`, `toLowerCase`, `format`, `storageFormat.name`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `env.executeSparkUpdate`, `BY`, `TBLPROPERTIES`, `VALUES`, `ImmutableList.of`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `DROP`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, format, storageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, onSpark, executeQuery, BY, TBLPROPERTIES, VALUES, ImmutableList.of, onTrino] vs current [assumeTrinoSupportsFormatVersion3, toLowerCase, format, storageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, env.executeSparkUpdate, BY, TBLPROPERTIES, VALUES, ImmutableList.of, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, format, storageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, onSpark, executeQuery, BY, TBLPROPERTIES, VALUES, ImmutableList.of, onTrino], verbs [CREATE, DELETE, MERGE, INSERT, SELECT, DROP]. Current flow summary -> helpers [assumeTrinoSupportsFormatVersion3, toLowerCase, format, storageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, env.executeSparkUpdate, BY, TBLPROPERTIES, VALUES, ImmutableList.of, env.executeTrino, env.executeSpark], verbs [CREATE, DELETE, MERGE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testIdBasedFieldMapping`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testIdBasedFieldMapping`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testIdBasedFieldMapping`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `format`, `s`, `BY`, `TBLPROPERTIES`, `named_struct`, `onTrino`, `project`, `rowBuilder`, `addField`, `build`. Current action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `format`, `s`, `BY`, `TBLPROPERTIES`, `named_struct`, `env.executeTrino`, `project`, `rowBuilder`, `addField`, `build`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, BY, TBLPROPERTIES, named_struct, onTrino, project, rowBuilder, addField, build] vs current [toLowerCase, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, BY, TBLPROPERTIES, named_struct, env.executeTrino, project, rowBuilder, addField, build]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, BY, TBLPROPERTIES, named_struct, onTrino, project, rowBuilder, addField, build], verbs [DROP, CREATE, INSERT, SELECT, ALTER]. Current flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, BY, TBLPROPERTIES, named_struct, env.executeTrino, project, rowBuilder, addField, build], verbs [DROP, CREATE, INSERT, SELECT, ALTER].
- Audit status: `verified`

##### `testReadAfterPartitionEvolution`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testReadAfterPartitionEvolution`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testReadAfterPartitionEvolution`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `format`, `s`, `TBLPROPERTIES`, `VALUES`, `named_struct`, `bucket`, `days`, `hours`, `rowBuilder`, `addField`, `build`, `onTrino`, `CAST`, `buildStructColValue.apply`, `Timestamp.valueOf`, `count`, `year`. Current action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `format`, `s`, `TBLPROPERTIES`, `VALUES`, `named_struct`, `bucket`, `days`, `hours`, `env.executeTrino`, `CAST`, `Timestamp.valueOf`, `count`, `year`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, VALUES, named_struct, bucket, days, hours, rowBuilder, addField, build, onTrino, CAST, buildStructColValue.apply, Timestamp.valueOf, count, year] vs current [toLowerCase, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, VALUES, named_struct, bucket, days, hours, env.executeTrino, CAST, Timestamp.valueOf, count, year]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, VALUES, named_struct, bucket, days, hours, rowBuilder, addField, build, onTrino, CAST, buildStructColValue.apply, Timestamp.valueOf, count, year], verbs [DROP, CREATE, INSERT, ALTER, SELECT]. Current flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, VALUES, named_struct, bucket, days, hours, env.executeTrino, CAST, Timestamp.valueOf, count, year], verbs [DROP, CREATE, INSERT, ALTER, SELECT].
- Audit status: `verified`

##### `testUpdateAfterSchemaEvolution`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testUpdateAfterSchemaEvolution`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testUpdateAfterSchemaEvolution`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`. Current setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`.
- Action parity: Legacy action shape: `MERGE`, `SELECT`, `UPDATE`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `onSpark`, `BY`, `TBLPROPERTIES`, `VALUES`, `ImmutableList.of`, `column`, `filePaths.stream`, `map`, `filter`, `count`. Current action shape: `MERGE`, `SELECT`, `UPDATE`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `env.executeSparkUpdate`, `BY`, `TBLPROPERTIES`, `VALUES`, `ImmutableList.of`, `env.executeTrino`, `env.executeSpark`, `column`, `filePaths.stream`, `map`, `filter`, `count`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`, `path.contains`, `isEqualTo`. Current assertion helpers: `row`, `assertThat`, `containsOnly`, `path.contains`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `DELETE`. Current cleanup shape: `DROP`, `DELETE`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, onSpark, BY, TBLPROPERTIES, VALUES, ImmutableList.of, column, filePaths.stream, map, filter, count] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, env.executeSparkUpdate, BY, TBLPROPERTIES, VALUES, ImmutableList.of, env.executeTrino, env.executeSpark, column, filePaths.stream, map, filter, count]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, onSpark, BY, TBLPROPERTIES, VALUES, ImmutableList.of, column, filePaths.stream, map, filter, count], verbs [DROP, CREATE, DELETE, MERGE, INSERT, ALTER, SELECT, UPDATE, SET]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, env.executeSparkUpdate, BY, TBLPROPERTIES, VALUES, ImmutableList.of, env.executeTrino, env.executeSpark, column, filePaths.stream, map, filter, count], verbs [DROP, CREATE, DELETE, MERGE, INSERT, ALTER, SELECT, UPDATE, SET].
- Audit status: `verified`

##### `testUpdateOnPartitionColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testUpdateOnPartitionColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testUpdateOnPartitionColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `MERGE`, `UPDATE`, `SELECT`, `CALL`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `onSpark`, `BY`, `TBLPROPERTIES`, `VALUES`, `ImmutableList.of`, `system.rewrite_data_files`, `map`. Current action shape: `MERGE`, `UPDATE`, `SELECT`, `CALL`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `env.executeSparkUpdate`, `BY`, `TBLPROPERTIES`, `VALUES`, `ImmutableList.of`, `env.executeTrino`, `env.executeSpark`, `system.rewrite_data_files`, `map`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `DELETE`. Current cleanup shape: `DROP`, `DELETE`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, onSpark, BY, TBLPROPERTIES, VALUES, ImmutableList.of, system.rewrite_data_files, map] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, env.executeSparkUpdate, BY, TBLPROPERTIES, VALUES, ImmutableList.of, env.executeTrino, env.executeSpark, system.rewrite_data_files, map]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, onSpark, BY, TBLPROPERTIES, VALUES, ImmutableList.of, system.rewrite_data_files, map], verbs [DROP, CREATE, DELETE, MERGE, INSERT, UPDATE, SET, SELECT, CALL]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, env.executeSparkUpdate, BY, TBLPROPERTIES, VALUES, ImmutableList.of, env.executeTrino, env.executeSpark, system.rewrite_data_files, map], verbs [DROP, CREATE, DELETE, MERGE, INSERT, UPDATE, SET, SELECT, CALL].
- Audit status: `verified`

##### `testPartitionColumnNameConflict`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testPartitionColumnNameConflict`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testPartitionColumnNameConflict`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `WITH`, `day`, `VALUES`, `onSpark`, `Timestamp.valueOf`, `Date.valueOf`. Current action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `WITH`, `day`, `VALUES`, `env.executeSpark`, `Timestamp.valueOf`, `Date.valueOf`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, WITH, day, VALUES, onSpark, Timestamp.valueOf, Date.valueOf] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, WITH, day, VALUES, env.executeSpark, Timestamp.valueOf, Date.valueOf]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, WITH, day, VALUES, onSpark, Timestamp.valueOf, Date.valueOf], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, WITH, day, VALUES, env.executeSpark, Timestamp.valueOf, Date.valueOf], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testTrinoReadsSparkSortOrder`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoReadsSparkSortOrder`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoReadsSparkSortOrder`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`, `INSERT`. Current setup shape: `CREATE`, `ALTER`, `INSERT`.
- Action parity: Legacy action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `onTrino`, `getOnlyValue`, `VALUES`. Current action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `env.executeTrino`, `getOnlyValue`, `env.executeTrinoUpdate`, `VALUES`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `row`. Current assertion helpers: `assertThat`, `contains`, `row`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, onTrino, getOnlyValue, VALUES] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, env.executeTrino, getOnlyValue, env.executeTrinoUpdate, VALUES, env.executeSpark]; SQL verbs differ: legacy [CREATE, ALTER, SHOW, INSERT, SELECT] vs current [CREATE, ALTER, SHOW, INSERT, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, onTrino, getOnlyValue, VALUES], verbs [CREATE, ALTER, SHOW, INSERT, SELECT]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, env.executeTrino, getOnlyValue, env.executeTrinoUpdate, VALUES, env.executeSpark], verbs [CREATE, ALTER, SHOW, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testMetadataCompressionCodecGzip`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testMetadataCompressionCodecGzip`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testMetadataCompressionCodecGzip`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`. Current setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`, `env.createHdfsClient`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `onTrino`, `stripNamenodeURI`, `getTableLocation`, `hdfsClient.listDirectory`, `stream`, `filter`, `file.endsWith`, `collect`, `toImmutableList`, `isNotEmpty`, `filteredOn`. Current action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `TBLPROPERTIES`, `VALUES`, `env.executeTrinoUpdate`, `env.executeTrino`, `SparkIcebergEnvironment.stripNamenodeURI`, `env.getTableLocation`, `Arrays.stream`, `hdfsClient.listDirectory`, `filter`, `file.endsWith`, `collect`, `toImmutableList`, `isNotEmpty`, `filteredOn`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `isEqualTo`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, TBLPROPERTIES, VALUES, onTrino, stripNamenodeURI, getTableLocation, hdfsClient.listDirectory, stream, filter, file.endsWith, collect, toImmutableList, isNotEmpty, filteredOn] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, TBLPROPERTIES, VALUES, env.executeTrinoUpdate, env.executeTrino, env.createHdfsClient, SparkIcebergEnvironment.stripNamenodeURI, env.getTableLocation, Arrays.stream, hdfsClient.listDirectory, filter, file.endsWith, collect, toImmutableList, isNotEmpty, filteredOn]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, TBLPROPERTIES, VALUES, onTrino, stripNamenodeURI, getTableLocation, hdfsClient.listDirectory, stream, filter, file.endsWith, collect, toImmutableList, isNotEmpty, filteredOn], verbs [CREATE, INSERT, SELECT, ALTER, SET, DROP]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, TBLPROPERTIES, VALUES, env.executeTrinoUpdate, env.executeTrino, env.createHdfsClient, SparkIcebergEnvironment.stripNamenodeURI, env.getTableLocation, Arrays.stream, hdfsClient.listDirectory, filter, file.endsWith, collect, toImmutableList, isNotEmpty, filteredOn], verbs [CREATE, INSERT, SELECT, ALTER, SET, DROP].
- Audit status: `verified`

##### `testCreateAndDropTableWithSameLocationWorksOnSpark`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testCreateAndDropTableWithSameLocationWorksOnSpark`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testCreateAndDropTableWithSameLocationWorksOnSpark`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onSpark`, `executeQuery`, `format`, `s`, `TBLPROPERTIES`, `sparkTableName`, `onTrino`, `trinoTableName`, `hasNoRows`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `format`, `s`, `TBLPROPERTIES`, `sparkTableName`, `env.executeTrino`, `trinoTableName`, `hasNoRows`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onSpark, executeQuery, format, s, TBLPROPERTIES, sparkTableName, onTrino, trinoTableName, hasNoRows] vs current [randomNameSuffix, env.executeSparkUpdate, format, s, TBLPROPERTIES, sparkTableName, env.executeTrino, trinoTableName, hasNoRows]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onSpark, executeQuery, format, s, TBLPROPERTIES, sparkTableName, onTrino, trinoTableName, hasNoRows], verbs [CREATE, DROP, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, format, s, TBLPROPERTIES, sparkTableName, env.executeTrino, trinoTableName, hasNoRows], verbs [CREATE, DROP, SELECT].
- Audit status: `verified`

##### `testCreateAndDropTableWithSameLocationFailsOnTrino`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testCreateAndDropTableWithSameLocationFailsOnTrino`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testCreateAndDropTableWithSameLocationFailsOnTrino`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onSpark`, `executeQuery`, `format`, `s`, `TBLPROPERTIES`, `sparkTableName`, `onTrino`, `trinoTableName`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeSparkUpdate`, `format`, `s`, `TBLPROPERTIES`, `sparkTableName`, `env.executeTrinoUpdate`, `trinoTableName`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onSpark, executeQuery, format, s, TBLPROPERTIES, sparkTableName, onTrino, trinoTableName] vs current [randomNameSuffix, env.executeSparkUpdate, format, s, TBLPROPERTIES, sparkTableName, env.executeTrinoUpdate, trinoTableName, env.executeTrino]; assertion helpers differ: legacy [assertQueryFailure, hasMessageMatching] vs current [assertThatThrownBy, hasMessageContaining]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onSpark, executeQuery, format, s, TBLPROPERTIES, sparkTableName, onTrino, trinoTableName], verbs [CREATE, DROP, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeSparkUpdate, format, s, TBLPROPERTIES, sparkTableName, env.executeTrinoUpdate, trinoTableName, env.executeTrino], verbs [CREATE, DROP, SELECT].
- Audit status: `verified`

##### `testTrinoWritingDataWithObjectStorageLocationProvider`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoWritingDataWithObjectStorageLocationProvider`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoWritingDataWithObjectStorageLocationProvider`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `sparkTableName`, `trinoTableName`, `onSpark`, `executeQuery`, `format`, `s`, `TBLPROPERTIES`, `onTrino`, `VALUES`, `hasColumnsCount`, `queryResult.getOnlyValue`. Current action shape: `SELECT`, `toLowerCase`, `sparkTableName`, `trinoTableName`, `env.executeSparkUpdate`, `format`, `s`, `TBLPROPERTIES`, `env.executeTrinoUpdate`, `VALUES`, `env.executeSpark`, `env.executeTrino`, `queryResult.getRowsCount`, `queryResult.getColumnCount`, `queryResult.getOnlyValue`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`, `hasRowsCount`, `contains`. Current assertion helpers: `row`, `assertThat`, `containsOnly`, `isEqualTo`, `contains`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, sparkTableName, trinoTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, onTrino, VALUES, hasColumnsCount, queryResult.getOnlyValue] vs current [toLowerCase, sparkTableName, trinoTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, env.executeTrinoUpdate, VALUES, env.executeSpark, env.executeTrino, queryResult.getRowsCount, queryResult.getColumnCount, queryResult.getOnlyValue]; assertion helpers differ: legacy [row, assertThat, containsOnly, hasRowsCount, contains] vs current [row, assertThat, containsOnly, isEqualTo, contains]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, sparkTableName, trinoTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, onTrino, VALUES, hasColumnsCount, queryResult.getOnlyValue], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [toLowerCase, sparkTableName, trinoTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, env.executeTrinoUpdate, VALUES, env.executeSpark, env.executeTrino, queryResult.getRowsCount, queryResult.getColumnCount, queryResult.getOnlyValue], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testSparkReadingTrinoObjectStorage`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkReadingTrinoObjectStorage`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkReadingTrinoObjectStorage`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `sparkTableName`, `trinoTableName`, `onTrino`, `executeQuery`, `format`, `s`, `WITH`, `VALUES`, `onSpark`, `hasColumnsCount`, `queryResult.getOnlyValue`. Current action shape: `SELECT`, `toLowerCase`, `sparkTableName`, `trinoTableName`, `env.executeTrinoUpdate`, `format`, `s`, `WITH`, `VALUES`, `env.executeSpark`, `env.executeTrino`, `queryResult.getRowsCount`, `queryResult.getColumnCount`, `queryResult.getOnlyValue`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`, `hasRowsCount`, `contains`. Current assertion helpers: `row`, `assertThat`, `containsOnly`, `isEqualTo`, `contains`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, sparkTableName, trinoTableName, onTrino, executeQuery, format, s, WITH, VALUES, onSpark, hasColumnsCount, queryResult.getOnlyValue] vs current [toLowerCase, sparkTableName, trinoTableName, env.executeTrinoUpdate, format, s, WITH, VALUES, env.executeSpark, env.executeTrino, queryResult.getRowsCount, queryResult.getColumnCount, queryResult.getOnlyValue]; assertion helpers differ: legacy [row, assertThat, containsOnly, hasRowsCount, contains] vs current [row, assertThat, containsOnly, isEqualTo, contains]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, sparkTableName, trinoTableName, onTrino, executeQuery, format, s, WITH, VALUES, onSpark, hasColumnsCount, queryResult.getOnlyValue], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [toLowerCase, sparkTableName, trinoTableName, env.executeTrinoUpdate, format, s, WITH, VALUES, env.executeSpark, env.executeTrino, queryResult.getRowsCount, queryResult.getColumnCount, queryResult.getOnlyValue], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testOptimizeOnV2IcebergTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testOptimizeOnV2IcebergTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testOptimizeOnV2IcebergTable`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `ALTER`.
- Action parity: Legacy action shape: `MERGE`, `SELECT`, `format`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `onSpark`, `executeQuery`, `BY`, `TBLPROPERTIES`, `VALUES`, `onTrino`. Current action shape: `MERGE`, `SELECT`, `format`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `env.executeSparkUpdate`, `BY`, `TBLPROPERTIES`, `VALUES`, `env.executeTrinoUpdate`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DELETE`. Current cleanup shape: `DELETE`.
- Any observed difference, however small: helper calls differ: legacy [format, randomNameSuffix, sparkTableName, trinoTableName, onSpark, executeQuery, BY, TBLPROPERTIES, VALUES, onTrino] vs current [format, randomNameSuffix, sparkTableName, trinoTableName, env.executeSparkUpdate, BY, TBLPROPERTIES, VALUES, env.executeTrinoUpdate, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [format, randomNameSuffix, sparkTableName, trinoTableName, onSpark, executeQuery, BY, TBLPROPERTIES, VALUES, onTrino], verbs [CREATE, DELETE, MERGE, INSERT, ALTER, SELECT]. Current flow summary -> helpers [format, randomNameSuffix, sparkTableName, trinoTableName, env.executeSparkUpdate, BY, TBLPROPERTIES, VALUES, env.executeTrinoUpdate, env.executeSpark], verbs [CREATE, DELETE, MERGE, INSERT, ALTER, SELECT].
- Audit status: `verified`

##### `testPartialStats`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testPartialStats`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testPartialStats`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`. Current setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`.
- Action parity: Legacy action shape: `SHOW`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `onSpark`, `executeQuery`, `VALUES`, `onTrino`, `TBLPROPERTIES`. Current action shape: `SHOW`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `env.executeSparkUpdate`, `VALUES`, `env.executeTrino`, `TBLPROPERTIES`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, sparkTableName, trinoTableName, onSpark, executeQuery, VALUES, onTrino, TBLPROPERTIES] vs current [randomNameSuffix, sparkTableName, trinoTableName, env.executeSparkUpdate, VALUES, env.executeTrino, TBLPROPERTIES]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, sparkTableName, trinoTableName, onSpark, executeQuery, VALUES, onTrino, TBLPROPERTIES], verbs [CREATE, INSERT, SHOW, ALTER, SET, DROP]. Current flow summary -> helpers [randomNameSuffix, sparkTableName, trinoTableName, env.executeSparkUpdate, VALUES, env.executeTrino, TBLPROPERTIES], verbs [CREATE, INSERT, SHOW, ALTER, SET, DROP].
- Audit status: `verified`

##### `testStatsAfterAddingPartitionField`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testStatsAfterAddingPartitionField`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testStatsAfterAddingPartitionField`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `ALTER`.
- Action parity: Legacy action shape: `SHOW`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `onSpark`, `executeQuery`, `VALUES`, `onTrino`, `bucket`. Current action shape: `SHOW`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `env.executeSparkUpdate`, `VALUES`, `env.executeTrino`, `bucket`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, sparkTableName, trinoTableName, onSpark, executeQuery, VALUES, onTrino, bucket] vs current [randomNameSuffix, sparkTableName, trinoTableName, env.executeSparkUpdate, VALUES, env.executeTrino, bucket]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, sparkTableName, trinoTableName, onSpark, executeQuery, VALUES, onTrino, bucket], verbs [CREATE, INSERT, SHOW, ALTER, DROP]. Current flow summary -> helpers [randomNameSuffix, sparkTableName, trinoTableName, env.executeSparkUpdate, VALUES, env.executeTrino, bucket], verbs [CREATE, INSERT, SHOW, ALTER, DROP].
- Audit status: `verified`

##### `testMissingMetrics`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testMissingMetrics`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testMissingMetrics`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `sparkTableName`, `onSpark`, `executeQuery`, `BY`, `TBLPROPERTIES`, `VALUES`, `onTrino`, `format`, `count`. Current action shape: `SELECT`, `randomNameSuffix`, `sparkTableName`, `env.executeSparkUpdate`, `BY`, `TBLPROPERTIES`, `VALUES`, `env.executeTrino`, `format`, `count`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, sparkTableName, onSpark, executeQuery, BY, TBLPROPERTIES, VALUES, onTrino, format, count] vs current [randomNameSuffix, sparkTableName, env.executeSparkUpdate, BY, TBLPROPERTIES, VALUES, env.executeTrino, format, count]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, sparkTableName, onSpark, executeQuery, BY, TBLPROPERTIES, VALUES, onTrino, format, count], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, sparkTableName, env.executeSparkUpdate, BY, TBLPROPERTIES, VALUES, env.executeTrino, format, count], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testTrinoAnalyzeWithNonLowercaseColumnName`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoAnalyzeWithNonLowercaseColumnName`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoAnalyzeWithNonLowercaseColumnName`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `ANALYZE`, `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `VALUES`, `onTrino`. Current action shape: `ANALYZE`, `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `VALUES`, `env.executeTrinoUpdate`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, VALUES, onTrino, ANALYZE] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, VALUES, env.executeTrinoUpdate, ANALYZE, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, VALUES, onTrino, ANALYZE], verbs [CREATE, INSERT, ANALYZE, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, VALUES, env.executeTrinoUpdate, ANALYZE, env.executeTrino, env.executeSpark], verbs [CREATE, INSERT, ANALYZE, SELECT, DROP].
- Audit status: `verified`

##### `testSparkReadsTrinoTableAfterCleaningUp`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkReadsTrinoTableAfterCleaningUp`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkReadsTrinoTableAfterCleaningUp`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `SET`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `format`, `s`, `WITH`, `VALUES`, `calculateMetadataFilesForPartitionedTable`, `EXPIRE_SNAPSHOTS`, `REMOVE_ORPHAN_FILES`, `isLessThan`, `SUM`, `onSpark`. Current action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `format`, `s`, `WITH`, `VALUES`, `env.executeTrinoInSession`, `session.executeUpdate`, `EXPIRE_SNAPSHOTS`, `REMOVE_ORPHAN_FILES`, `SUM`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `row`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `DELETE`. Current cleanup shape: `DROP`, `DELETE`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, WITH, VALUES, calculateMetadataFilesForPartitionedTable, EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES, isLessThan, SUM, onSpark] vs current [toLowerCase, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, WITH, VALUES, env.executeTrinoInSession, session.executeUpdate, EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES, SUM, env.executeTrino, env.executeSpark]; assertion helpers differ: legacy [assertThat, row, containsOnly] vs current [row, assertThat, containsOnly]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, WITH, VALUES, calculateMetadataFilesForPartitionedTable, EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES, isLessThan, SUM, onSpark], verbs [DROP, CREATE, INSERT, DELETE, SET, ALTER, SELECT]. Current flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, WITH, VALUES, env.executeTrinoInSession, session.executeUpdate, EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES, SUM, env.executeTrino, env.executeSpark], verbs [DROP, CREATE, INSERT, DELETE, SET, ALTER, SELECT].
- Audit status: `verified`

##### `testDeleteAfterPartitionEvolution`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testDeleteAfterPartitionEvolution`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testDeleteAfterPartitionEvolution`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `ALTER`.
- Action parity: Legacy action shape: `MERGE`, `SELECT`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `format`, `s`, `TBLPROPERTIES`, `VALUES`, `bucket`, `expected.add`, `onTrino`, `expected.remove`. Current action shape: `MERGE`, `SELECT`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `format`, `s`, `TBLPROPERTIES`, `VALUES`, `bucket`, `expected.add`, `env.executeTrino`, `env.executeTrinoUpdate`, `expected.remove`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `DELETE`. Current cleanup shape: `DROP`, `DELETE`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, VALUES, bucket, expected.add, onTrino, expected.remove] vs current [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, VALUES, bucket, expected.add, env.executeTrino, env.executeTrinoUpdate, expected.remove, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, VALUES, bucket, expected.add, onTrino, expected.remove], verbs [DROP, CREATE, DELETE, MERGE, INSERT, ALTER, SELECT]. Current flow summary -> helpers [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, VALUES, bucket, expected.add, env.executeTrino, env.executeTrinoUpdate, expected.remove, env.executeSpark], verbs [DROP, CREATE, DELETE, MERGE, INSERT, ALTER, SELECT].
- Audit status: `verified`

##### `testTrinoAlterStructColumnType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoAlterStructColumnType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoAlterStructColumnType`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`, `SET`. Current setup shape: `CREATE`, `ALTER`, `SET`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `WITH`, `CAST`, `getColumnType`, `onSpark`. Current action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `WITH`, `CAST`, `getColumnType`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `isEqualTo`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `isEqualTo`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, WITH, CAST, getColumnType, onSpark] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, WITH, CAST, getColumnType, env.executeSpark, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, WITH, CAST, getColumnType, onSpark], verbs [CREATE, SELECT, ALTER, SET, DROP]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, WITH, CAST, getColumnType, env.executeSpark, env.executeTrino], verbs [CREATE, SELECT, ALTER, SET, DROP].
- Audit status: `verified`

##### `testTrinoSetColumnType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoSetColumnType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoSetColumnType`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`. Current setup shape: `CREATE`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `testTrinoSetColumnType`, `trinoTableName`, `sparkTableName`, `getColumnType`, `onTrino`, `onSpark`. Current action shape: `SELECT`, `randomNameSuffix`, `testTrinoSetColumnType`, `trinoTableName`, `sparkTableName`, `getColumnType`, `env.executeTrinoUpdate`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isEqualTo`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the helper flow is unchanged; the JUnit port threads `SparkIcebergEnvironment` through the shared helper and routes the reads and DDL through `env.executeTrino*` / `env.executeSpark`.
- Known intentional difference: None.
- Reviewer note: The shared helper creates a Trino Iceberg table with the source type, alters `col` to the requested target type, verifies the Iceberg schema change via `getColumnType`, checks both Trino and Spark read back the converted value, and drops the table.
- Audit status: `verified`

##### `testTrinoSetPartitionedColumnType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoSetPartitionedColumnType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoSetPartitionedColumnType`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`. Current setup shape: `CREATE`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `testTrinoSetColumnType`, `trinoTableName`, `sparkTableName`, `getColumnType`, `onTrino`, `onSpark`. Current action shape: `SELECT`, `randomNameSuffix`, `testTrinoSetColumnType`, `trinoTableName`, `sparkTableName`, `getColumnType`, `env.executeTrinoUpdate`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isEqualTo`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the helper flow is unchanged; the JUnit port threads `SparkIcebergEnvironment` through the shared helper and routes the reads and DDL through `env.executeTrino*` / `env.executeSpark`.
- Known intentional difference: None.
- Reviewer note: The shared helper creates a Trino Iceberg table with the source type, alters `col` to the requested target type while enabling partitioning on `col` for this split case, verifies the Iceberg schema change via `getColumnType`, checks both Trino and Spark read back the converted value, and drops the table.
- Audit status: `verified`

##### `testTrinoSetFieldType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoSetFieldType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoSetFieldType`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`, `SET`. Current setup shape: `CREATE`, `ALTER`, `SET`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `WITH`, `CAST`, `getColumnType`, `onSpark`. Current action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `WITH`, `CAST`, `getColumnType`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `isEqualTo`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `isEqualTo`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, WITH, CAST, getColumnType, onSpark] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, WITH, CAST, getColumnType, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, WITH, CAST, getColumnType, onSpark], verbs [CREATE, SELECT, ALTER, SET, DROP]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, WITH, CAST, getColumnType, env.executeTrino, env.executeSpark], verbs [CREATE, SELECT, ALTER, SET, DROP].
- Audit status: `verified`

##### `testSparkAlterColumnType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkAlterColumnType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkAlterColumnType`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`. Current setup shape: `CREATE`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `testSparkAlterColumnType`, `trinoTableName`, `sparkTableName`, `getColumnType`, `onSpark`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `testSparkAlterColumnType`, `trinoTableName`, `sparkTableName`, `getColumnType`, `env.executeSparkUpdate`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isEqualTo`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the helper flow is unchanged; the JUnit port threads `SparkIcebergEnvironment` through the shared helper and routes the reads and DDL through `env.executeSpark*` / `env.executeTrino`.
- Known intentional difference: None.
- Reviewer note: The shared helper creates a Spark Iceberg table with the source type, alters `col` to the requested target type from Spark, verifies the Iceberg schema change via `getColumnType`, checks both Spark and Trino read back the converted value, and drops the table.
- Audit status: `verified`

##### `testSparkAlterPartitionedColumnType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkAlterPartitionedColumnType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkAlterPartitionedColumnType`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`. Current setup shape: `CREATE`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `testSparkAlterColumnType`, `trinoTableName`, `sparkTableName`, `getColumnType`, `onSpark`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `testSparkAlterColumnType`, `trinoTableName`, `sparkTableName`, `getColumnType`, `env.executeSparkUpdate`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isEqualTo`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: the helper flow is unchanged; the JUnit port threads `SparkIcebergEnvironment` through the shared helper and routes the reads and DDL through `env.executeSpark*` / `env.executeTrino`.
- Known intentional difference: None.
- Reviewer note: The shared helper creates a Spark Iceberg table with the source type, alters `col` to the requested target type while enabling partitioning on `col` for this split case, verifies the Iceberg schema change via `getColumnType`, checks both Spark and Trino read back the converted value, and drops the table.
- Audit status: `verified`

##### `testSparkAlterStructColumnType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkAlterStructColumnType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkAlterStructColumnType`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`. Current setup shape: `CREATE`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `TBLPROPERTIES`, `named_struct`, `getColumnType`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `TBLPROPERTIES`, `named_struct`, `getColumnType`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`, `row`, `containsOnly`. Current assertion helpers: `assertThat`, `isEqualTo`, `row`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, TBLPROPERTIES, named_struct, getColumnType, onTrino] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, TBLPROPERTIES, named_struct, getColumnType, env.executeSpark, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, TBLPROPERTIES, named_struct, getColumnType, onTrino], verbs [CREATE, SELECT, ALTER, DROP]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, TBLPROPERTIES, named_struct, getColumnType, env.executeSpark, env.executeTrino], verbs [CREATE, SELECT, ALTER, DROP].
- Audit status: `verified`

##### `testTrinoReadingSparkCompressedData`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkConfiguredCompressionCodecOnSparkAndTrino`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoReadingSparkCompressedData`
- Mapping type: `renamed`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `SET`, `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `format`, `s`, `TBLPROPERTIES`, `storageFormat.name`, `getIcebergCompressionCodecTableProperty`, `getIcebergCompressionCodecName`, `onSpark`, `executeQuery`, `IntStream.range`, `mapToObj`, `collect`, `toImmutableList`, `sparkRows.stream`, `map`, `row.getValues`, `get`, `Collectors.joining`, `onTrino`, `trinoRows.stream`, `failed`, `formatted`. Current action shape: `SELECT`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `IntStream.range`, `mapToObj`, `collect`, `toImmutableList`, `env.executeSparkUpdate`, `equals`, `hasStackTraceContaining`, `Assumptions.abort`, `nsupportedOperationException`, `TBLPROPERTIES`, `rows.stream`, `map`, `format`, `row.getValues`, `get`, `Collectors.joining`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`, `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `row`, `assertThatThrownBy`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, format, s, TBLPROPERTIES, storageFormat.name, getIcebergCompressionCodecTableProperty, getIcebergCompressionCodecName, onSpark, executeQuery, IntStream.range, mapToObj, collect, toImmutableList, sparkRows.stream, map, row.getValues, get, Collectors.joining, onTrino, trinoRows.stream, failed, formatted] vs current [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, IntStream.range, mapToObj, collect, toImmutableList, env.executeSparkUpdate, equals, hasStackTraceContaining, Assumptions.abort, nsupportedOperationException, TBLPROPERTIES, rows.stream, map, format, row.getValues, get, Collectors.joining, env.executeSpark, env.executeTrino]; SQL verbs differ: legacy [CREATE, INSERT, SELECT, DROP] vs current [SET, CREATE, INSERT, SELECT, DROP]; assertion helpers differ: legacy [row, assertThat, containsOnly, assertQueryFailure, hasMessageMatching] vs current [row, assertThatThrownBy, assertThat, containsOnly]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, format, s, TBLPROPERTIES, storageFormat.name, getIcebergCompressionCodecTableProperty, getIcebergCompressionCodecName, onSpark, executeQuery, IntStream.range, mapToObj, collect, toImmutableList, sparkRows.stream, map, row.getValues, get, Collectors.joining, onTrino, trinoRows.stream, failed, formatted], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, IntStream.range, mapToObj, collect, toImmutableList, env.executeSparkUpdate, equals, hasStackTraceContaining, Assumptions.abort, nsupportedOperationException, TBLPROPERTIES, rows.stream, map, format, row.getValues, get, Collectors.joining, env.executeSpark, env.executeTrino], verbs [SET, CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testSparkReadingTrinoCompressedData`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoConfiguredCompressionCodecOnSparkAndTrino`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkReadingTrinoCompressedData`
- Mapping type: `renamed`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `format`, `s`, `WITH`, `storageFormat.name`, `compressionCodec.name`, `TODO`, `onTrino`, `executeQuery`, `failed`, `IntStream.range`, `mapToObj`, `collect`, `toImmutableList`, `trinoRows.stream`, `map`, `row.getValues`, `get`, `Collectors.joining`, `onSpark`, `sparkRows.stream`, `formatted`. Current action shape: `SELECT`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `WITH`, `equals`, `TODO`, `env.executeTrinoUpdate`, `compressionCodec.equals`, `env.executeTrino`, `rows`, `stream`, `map`, `row.toArray`, `collect`, `toImmutableList`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageMatching`, `row`, `assertThat`, `containsOnly`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, format, s, WITH, storageFormat.name, compressionCodec.name, TODO, onTrino, executeQuery, failed, IntStream.range, mapToObj, collect, toImmutableList, trinoRows.stream, map, row.getValues, get, Collectors.joining, onSpark, sparkRows.stream, formatted] vs current [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, WITH, equals, TODO, env.executeTrinoUpdate, compressionCodec.equals, env.executeTrino, rows, stream, map, row.toArray, collect, toImmutableList, env.executeSpark]; SQL verbs differ: legacy [CREATE, INSERT, SELECT, DROP] vs current [CREATE, SELECT, DROP]; assertion helpers differ: legacy [assertQueryFailure, hasMessageMatching, row, assertThat, containsOnly] vs current [assertThatThrownBy, hasMessageContaining, row, assertThat, containsOnly]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, format, s, WITH, storageFormat.name, compressionCodec.name, TODO, onTrino, executeQuery, failed, IntStream.range, mapToObj, collect, toImmutableList, trinoRows.stream, map, row.getValues, get, Collectors.joining, onSpark, sparkRows.stream, formatted], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, WITH, equals, TODO, env.executeTrinoUpdate, compressionCodec.equals, env.executeTrino, rows, stream, map, row.toArray, collect, toImmutableList, env.executeSpark], verbs [CREATE, SELECT, DROP].
- Audit status: `verified`

##### `testTrinoWritingDataWithWriterDataPathSet`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoWritingDataWithWriterDataPathSet`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoWritingDataWithWriterDataPathSet`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `sparkTableName`, `trinoTableName`, `onSpark`, `executeQuery`, `format`, `s`, `TBLPROPERTIES`, `onTrino`, `VALUES`, `hasColumnsCount`, `queryResult.getOnlyValue`. Current action shape: `SELECT`, `toLowerCase`, `sparkTableName`, `trinoTableName`, `env.executeSparkUpdate`, `format`, `s`, `TBLPROPERTIES`, `env.executeTrinoUpdate`, `VALUES`, `env.executeSpark`, `env.executeTrino`, `queryResult.getRowsCount`, `queryResult.getColumnCount`, `queryResult.getOnlyValue`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`, `hasRowsCount`, `contains`. Current assertion helpers: `row`, `assertThat`, `containsOnly`, `isEqualTo`, `contains`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, sparkTableName, trinoTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, onTrino, VALUES, hasColumnsCount, queryResult.getOnlyValue] vs current [toLowerCase, sparkTableName, trinoTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, env.executeTrinoUpdate, VALUES, env.executeSpark, env.executeTrino, queryResult.getRowsCount, queryResult.getColumnCount, queryResult.getOnlyValue]; assertion helpers differ: legacy [row, assertThat, containsOnly, hasRowsCount, contains] vs current [row, assertThat, containsOnly, isEqualTo, contains]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, sparkTableName, trinoTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, onTrino, VALUES, hasColumnsCount, queryResult.getOnlyValue], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [toLowerCase, sparkTableName, trinoTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, env.executeTrinoUpdate, VALUES, env.executeSpark, env.executeTrino, queryResult.getRowsCount, queryResult.getColumnCount, queryResult.getOnlyValue], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testTrinoIgnoresUnsupportedSparkSortOrder`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoIgnoresUnsupportedSparkSortOrder`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoIgnoresUnsupportedSparkSortOrder`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`, `INSERT`. Current setup shape: `CREATE`, `ALTER`, `INSERT`.
- Action parity: Legacy action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `onTrino`, `getOnlyValue`, `doesNotContain`, `VALUES`. Current action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `env.executeTrino`, `getOnlyValue`, `doesNotContain`, `env.executeTrinoUpdate`, `VALUES`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `row`. Current assertion helpers: `assertThat`, `contains`, `row`.
- Cleanup parity: Legacy cleanup shape: `truncate`. Current cleanup shape: `DROP`, `truncate`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, truncate, onTrino, getOnlyValue, doesNotContain, VALUES] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, truncate, env.executeTrino, getOnlyValue, doesNotContain, env.executeTrinoUpdate, VALUES, env.executeSpark]; SQL verbs differ: legacy [CREATE, ALTER, SHOW, INSERT, SELECT] vs current [CREATE, ALTER, SHOW, INSERT, SELECT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, truncate, onTrino, getOnlyValue, doesNotContain, VALUES], verbs [CREATE, ALTER, SHOW, INSERT, SELECT]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, truncate, env.executeTrino, getOnlyValue, doesNotContain, env.executeTrinoUpdate, VALUES, env.executeSpark], verbs [CREATE, ALTER, SHOW, INSERT, SELECT, DROP].
- Audit status: `verified`

##### `testRenameNestedField`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testRenameNestedField`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testRenameNestedField`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`. Current setup shape: `CREATE`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `CAST`, `onSpark`. Current action shape: `SELECT`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `CAST`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, CAST, onSpark] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, CAST, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, CAST, onSpark], verbs [CREATE, SELECT, ALTER, DROP]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, CAST, env.executeTrino, env.executeSpark], verbs [CREATE, SELECT, ALTER, DROP].
- Audit status: `verified`

##### `testDropPastPartitionedField`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testDropPastPartitionedField`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testDropPastPartitionedField`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`, `SET`. Current setup shape: `CREATE`, `ALTER`, `SET`.
- Action parity: Legacy action shape: `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `ROW`, `onSpark`. Current action shape: `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `ROW`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageContaining`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, ROW, onSpark] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, ROW, env.executeSparkUpdate]; assertion helpers differ: legacy [assertQueryFailure, hasMessageContaining] vs current [assertThatThrownBy, hasMessageContaining]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, ROW, onSpark], verbs [CREATE, ALTER, SET, DROP]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, ROW, env.executeSparkUpdate], verbs [CREATE, ALTER, SET, DROP].
- Audit status: `verified`

##### `testTrinoReadsSparkRowLevelDeletesWithRowTypes`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoReadsSparkRowLevelDeletesWithRowTypes`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoReadsSparkRowLevelDeletesWithRowTypes`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `MERGE`, `CALL`, `SELECT`, `toLowerCase`, `format`, `tableStorageFormat.name`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `onSpark`, `executeQuery`, `BY`, `TBLPROPERTIES`, `named_struct`, `system.rewrite_data_files`, `map`, `ImmutableList.of`, `onTrino`. Current action shape: `MERGE`, `CALL`, `SELECT`, `toLowerCase`, `format`, `tableStorageFormat.name`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `env.executeSparkUpdate`, `BY`, `TBLPROPERTIES`, `named_struct`, `system.rewrite_data_files`, `map`, `ImmutableList.of`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `DROP`, `deleteFileStorageFormat.name`. Current cleanup shape: `DELETE`, `DROP`, `deleteFileStorageFormat.name`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, format, tableStorageFormat.name, deleteFileStorageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, onSpark, executeQuery, BY, TBLPROPERTIES, named_struct, system.rewrite_data_files, map, ImmutableList.of, onTrino] vs current [toLowerCase, format, tableStorageFormat.name, deleteFileStorageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, env.executeSparkUpdate, BY, TBLPROPERTIES, named_struct, system.rewrite_data_files, map, ImmutableList.of, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, format, tableStorageFormat.name, deleteFileStorageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, onSpark, executeQuery, BY, TBLPROPERTIES, named_struct, system.rewrite_data_files, map, ImmutableList.of, onTrino], verbs [CREATE, DELETE, MERGE, INSERT, CALL, SELECT, DROP]. Current flow summary -> helpers [toLowerCase, format, tableStorageFormat.name, deleteFileStorageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, env.executeSparkUpdate, BY, TBLPROPERTIES, named_struct, system.rewrite_data_files, map, ImmutableList.of, env.executeTrino, env.executeSpark], verbs [CREATE, DELETE, MERGE, INSERT, CALL, SELECT, DROP].
- Audit status: `verified`

##### `testSparkReadsTrinoRowLevelDeletesWithRowTypes`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkReadsTrinoRowLevelDeletesWithRowTypes`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkReadsTrinoRowLevelDeletesWithRowTypes`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `format`, `storageFormat.name`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `onTrino`, `executeQuery`, `ROW`, `WITH`, `VALUES`, `ImmutableList.of`, `onSpark`. Current action shape: `SELECT`, `toLowerCase`, `format`, `storageFormat.name`, `randomNameSuffix`, `sparkTableName`, `trinoTableName`, `env.executeTrinoUpdate`, `ROW`, `WITH`, `VALUES`, `ImmutableList.of`, `env.executeTrino`, `env.executeSpark`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `DROP`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, format, storageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, onTrino, executeQuery, ROW, WITH, VALUES, ImmutableList.of, onSpark] vs current [toLowerCase, format, storageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, env.executeTrinoUpdate, ROW, WITH, VALUES, ImmutableList.of, env.executeTrino, env.executeSpark, env.executeSparkUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, format, storageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, onTrino, executeQuery, ROW, WITH, VALUES, ImmutableList.of, onSpark], verbs [CREATE, INSERT, DELETE, SELECT, DROP]. Current flow summary -> helpers [toLowerCase, format, storageFormat.name, randomNameSuffix, sparkTableName, trinoTableName, env.executeTrinoUpdate, ROW, WITH, VALUES, ImmutableList.of, env.executeTrino, env.executeSpark, env.executeSparkUpdate], verbs [CREATE, INSERT, DELETE, SELECT, DROP].
- Audit status: `verified`

##### `testSparkReadsTrinoTableAfterOptimizeAndCleaningUp`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testSparkReadsTrinoTableAfterOptimizeAndCleaningUp`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testSparkReadsTrinoTableAfterOptimizeAndCleaningUp`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`. Current setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `format`, `s`, `WITH`, `VALUES`, `getRowsCount`, `calculateMetadataFilesForPartitionedTable`, `EXPIRE_SNAPSHOTS`, `REMOVE_ORPHAN_FILES`, `isLessThan`, `SUM`, `onSpark`. Current action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `format`, `s`, `WITH`, `VALUES`, `env.executeTrinoInSession`, `session.executeUpdate`, `EXPIRE_SNAPSHOTS`, `REMOVE_ORPHAN_FILES`, `SUM`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `row`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `DELETE`. Current cleanup shape: `DROP`, `DELETE`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, WITH, VALUES, getRowsCount, calculateMetadataFilesForPartitionedTable, EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES, isLessThan, SUM, onSpark] vs current [toLowerCase, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, WITH, VALUES, env.executeTrinoInSession, session.executeUpdate, EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES, SUM, env.executeTrino, env.executeSpark]; SQL verbs differ: legacy [DROP, CREATE, INSERT, DELETE, SELECT, ALTER, SET] vs current [DROP, CREATE, INSERT, DELETE, ALTER, SET, SELECT]; assertion helpers differ: legacy [assertThat, row, containsOnly] vs current [row, assertThat, containsOnly]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, WITH, VALUES, getRowsCount, calculateMetadataFilesForPartitionedTable, EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES, isLessThan, SUM, onSpark], verbs [DROP, CREATE, INSERT, DELETE, SELECT, ALTER, SET]. Current flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, WITH, VALUES, env.executeTrinoInSession, session.executeUpdate, EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES, SUM, env.executeTrino, env.executeSpark], verbs [DROP, CREATE, INSERT, DELETE, ALTER, SET, SELECT].
- Audit status: `verified`

##### `testTrinoReadsTrinoTableWithSparkDeletesAfterOptimizeAndCleanUp`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoReadsTrinoTableWithSparkDeletesAfterOptimizeAndCleanUp`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoReadsTrinoTableWithSparkDeletesAfterOptimizeAndCleanUp`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`. Current setup shape: `CREATE`, `INSERT`, `ALTER`, `SET`.
- Action parity: Legacy action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `format`, `s`, `WITH`, `VALUES`, `onSpark`, `EXPIRE_SNAPSHOTS`, `REMOVE_ORPHAN_FILES`, `SUM`. Current action shape: `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `format`, `s`, `WITH`, `VALUES`, `env.executeSparkUpdate`, `env.executeTrinoInSession`, `session.executeUpdate`, `EXPIRE_SNAPSHOTS`, `REMOVE_ORPHAN_FILES`, `SUM`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `DELETE`. Current cleanup shape: `DROP`, `DELETE`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, WITH, VALUES, onSpark, EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES, SUM] vs current [toLowerCase, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, WITH, VALUES, env.executeSparkUpdate, env.executeTrinoInSession, session.executeUpdate, EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES, SUM, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, WITH, VALUES, onSpark, EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES, SUM], verbs [DROP, CREATE, INSERT, DELETE, ALTER, SET, SELECT]. Current flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, WITH, VALUES, env.executeSparkUpdate, env.executeTrinoInSession, session.executeUpdate, EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES, SUM, env.executeTrino, env.executeSpark], verbs [DROP, CREATE, INSERT, DELETE, ALTER, SET, SELECT].
- Audit status: `verified`

##### `testCleaningUpIcebergTableWithRowLevelDeletes`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testCleaningUpIcebergTableWithRowLevelDeletes`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testCleaningUpIcebergTableWithRowLevelDeletes`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `SET`, `ALTER`.
- Action parity: Legacy action shape: `MERGE`, `CALL`, `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `onSpark`, `BY`, `TBLPROPERTIES`, `tableStorageFormat.name`, `named_struct`, `system.rewrite_data_files`, `map`, `SUM`, `format`, `EXPIRE_SNAPSHOTS`, `REMOVE_ORPHAN_FILES`. Current action shape: `MERGE`, `CALL`, `SELECT`, `toLowerCase`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `env.executeSparkUpdate`, `BY`, `TBLPROPERTIES`, `tableStorageFormat.name`, `named_struct`, `system.rewrite_data_files`, `map`, `SUM`, `env.executeTrino`, `format`, `env.executeSpark`, `env.executeTrinoInSession`, `session.executeUpdate`, `EXPIRE_SNAPSHOTS`, `REMOVE_ORPHAN_FILES`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `DELETE`, `deleteFileStorageFormat.name`. Current cleanup shape: `DROP`, `DELETE`, `deleteFileStorageFormat.name`.
- Any observed difference, however small: helper calls differ: legacy [toLowerCase, trinoTableName, sparkTableName, onTrino, executeQuery, onSpark, BY, TBLPROPERTIES, tableStorageFormat.name, deleteFileStorageFormat.name, named_struct, system.rewrite_data_files, map, SUM, format, EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES] vs current [toLowerCase, trinoTableName, sparkTableName, env.executeTrinoUpdate, env.executeSparkUpdate, BY, TBLPROPERTIES, tableStorageFormat.name, deleteFileStorageFormat.name, named_struct, system.rewrite_data_files, map, SUM, env.executeTrino, format, env.executeSpark, env.executeTrinoInSession, session.executeUpdate, EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, onTrino, executeQuery, onSpark, BY, TBLPROPERTIES, tableStorageFormat.name, deleteFileStorageFormat.name, named_struct, system.rewrite_data_files, map, SUM, format, EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES], verbs [DROP, CREATE, DELETE, MERGE, INSERT, CALL, SELECT, SET, ALTER]. Current flow summary -> helpers [toLowerCase, trinoTableName, sparkTableName, env.executeTrinoUpdate, env.executeSparkUpdate, BY, TBLPROPERTIES, tableStorageFormat.name, deleteFileStorageFormat.name, named_struct, system.rewrite_data_files, map, SUM, env.executeTrino, format, env.executeSpark, env.executeTrinoInSession, session.executeUpdate, EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES], verbs [DROP, CREATE, DELETE, MERGE, INSERT, CALL, SELECT, SET, ALTER].
- Audit status: `verified`

##### `testRegisterTableWithTableLocation`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testRegisterTableWithTableLocation`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testRegisterTableWithTableLocation`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `CALL`, `SELECT`, `storageFormat.name`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `format`, `s`, `TBLPROPERTIES`, `values`, `List.of`, `getTableLocation`, `onTrino`, `system.register_table`. Current action shape: `CALL`, `SELECT`, `storageFormat.name`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `format`, `s`, `TBLPROPERTIES`, `values`, `List.of`, `env.getTableLocation`, `env.executeTrinoUpdate`, `system.register_table`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `dropTableFromMetastore`. Current cleanup shape: `DROP`, `env.dropTableFromMetastore`.
- Any observed difference, however small: helper calls differ: legacy [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, values, List.of, getTableLocation, dropTableFromMetastore, onTrino, system.register_table] vs current [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, values, List.of, env.getTableLocation, env.dropTableFromMetastore, env.executeTrinoUpdate, system.register_table, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, values, List.of, getTableLocation, dropTableFromMetastore, onTrino, system.register_table], verbs [CREATE, INSERT, CALL, SELECT, DROP]. Current flow summary -> helpers [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, values, List.of, env.getTableLocation, env.dropTableFromMetastore, env.executeTrinoUpdate, system.register_table, env.executeTrino, env.executeSpark], verbs [CREATE, INSERT, CALL, SELECT, DROP].
- Audit status: `verified`

##### `testRegisterTableWithComments`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testRegisterTableWithComments`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testRegisterTableWithComments`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `COMMENT`. Current setup shape: `CREATE`, `INSERT`, `COMMENT`.
- Action parity: Legacy action shape: `CALL`, `storageFormat.name`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `format`, `s`, `with`, `onSpark`, `values`, `getTableLocation`, `getLatestMetadataFilename`, `system.register_table`, `getTableComment`, `getColumnComment`. Current action shape: `CALL`, `storageFormat.name`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `format`, `s`, `with`, `env.executeSparkUpdate`, `values`, `env.getTableLocation`, `env.getLatestMetadataFilename`, `system.register_table`, `getTableComment`, `getColumnComment`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `dropTableFromMetastore`. Current cleanup shape: `DROP`, `env.dropTableFromMetastore`.
- Any observed difference, however small: helper calls differ: legacy [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, with, onSpark, values, getTableLocation, getLatestMetadataFilename, dropTableFromMetastore, system.register_table, getTableComment, getColumnComment] vs current [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, with, env.executeSparkUpdate, values, env.getTableLocation, env.getLatestMetadataFilename, env.dropTableFromMetastore, system.register_table, getTableComment, getColumnComment]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, with, onSpark, values, getTableLocation, getLatestMetadataFilename, dropTableFromMetastore, system.register_table, getTableComment, getColumnComment], verbs [CREATE, INSERT, COMMENT, CALL, DROP]. Current flow summary -> helpers [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, with, env.executeSparkUpdate, values, env.getTableLocation, env.getLatestMetadataFilename, env.dropTableFromMetastore, system.register_table, getTableComment, getColumnComment], verbs [CREATE, INSERT, COMMENT, CALL, DROP].
- Audit status: `verified`

##### `testRegisterTableWithShowCreateTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testRegisterTableWithShowCreateTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testRegisterTableWithShowCreateTable`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `expectedShowCreateTable.rows`, `expectedShowCreateTable.getColumnTypes`. Current setup shape: `CREATE`, `INSERT`, `expectedShowCreateTable.rows`, `expectedShowCreateTable.getColumnTypes`.
- Action parity: Legacy action shape: `SHOW`, `CALL`, `storageFormat.name`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `format`, `s`, `TBLPROPERTIES`, `values`, `onTrino`, `expectedDescribeTable.rows`, `stream`, `map`, `columns.toArray`, `collect`, `toImmutableList`, `getTableLocation`, `getLatestMetadataFilename`, `system.register_table`, `hasColumns`, `expectedDescribeTable.getColumnTypes`. Current action shape: `SHOW`, `CALL`, `storageFormat.name`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `format`, `s`, `TBLPROPERTIES`, `values`, `env.executeTrinoUpdate`, `env.executeSpark`, `expectedDescribeTable.rows`, `stream`, `map`, `columns.toArray`, `collect`, `toImmutableList`, `env.executeTrino`, `env.getTableLocation`, `env.getLatestMetadataFilename`, `system.register_table`, `hasColumns`, `expectedDescribeTable.getColumnTypes`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsExactlyInOrder`. Current assertion helpers: `row`, `assertThat`, `containsExactlyInOrder`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `dropTableFromMetastore`. Current cleanup shape: `DROP`, `env.dropTableFromMetastore`.
- Any observed difference, however small: helper calls differ: legacy [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, values, onTrino, expectedDescribeTable.rows, stream, map, columns.toArray, collect, toImmutableList, expectedShowCreateTable.rows, getTableLocation, getLatestMetadataFilename, dropTableFromMetastore, system.register_table, hasColumns, expectedDescribeTable.getColumnTypes, expectedShowCreateTable.getColumnTypes] vs current [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, values, env.executeTrinoUpdate, env.executeSpark, expectedDescribeTable.rows, stream, map, columns.toArray, collect, toImmutableList, env.executeTrino, expectedShowCreateTable.rows, env.getTableLocation, env.getLatestMetadataFilename, env.dropTableFromMetastore, system.register_table, hasColumns, expectedDescribeTable.getColumnTypes, expectedShowCreateTable.getColumnTypes]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, values, onTrino, expectedDescribeTable.rows, stream, map, columns.toArray, collect, toImmutableList, expectedShowCreateTable.rows, getTableLocation, getLatestMetadataFilename, dropTableFromMetastore, system.register_table, hasColumns, expectedDescribeTable.getColumnTypes, expectedShowCreateTable.getColumnTypes], verbs [CREATE, INSERT, SHOW, CALL, DROP]. Current flow summary -> helpers [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, values, env.executeTrinoUpdate, env.executeSpark, expectedDescribeTable.rows, stream, map, columns.toArray, collect, toImmutableList, env.executeTrino, expectedShowCreateTable.rows, env.getTableLocation, env.getLatestMetadataFilename, env.dropTableFromMetastore, system.register_table, hasColumns, expectedDescribeTable.getColumnTypes, expectedShowCreateTable.getColumnTypes], verbs [CREATE, INSERT, SHOW, CALL, DROP].
- Audit status: `verified`

##### `testRegisterTableWithReInsert`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testRegisterTableWithReInsert`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testRegisterTableWithReInsert`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `CALL`, `SELECT`, `storageFormat.name`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `format`, `s`, `with`, `onSpark`, `values`, `getTableLocation`, `getLatestMetadataFilename`, `system.register_table`, `List.of`. Current action shape: `CALL`, `SELECT`, `storageFormat.name`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `format`, `s`, `with`, `env.executeSparkUpdate`, `values`, `env.getTableLocation`, `env.getLatestMetadataFilename`, `system.register_table`, `List.of`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `dropTableFromMetastore`. Current cleanup shape: `DROP`, `env.dropTableFromMetastore`.
- Any observed difference, however small: helper calls differ: legacy [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, with, onSpark, values, getTableLocation, getLatestMetadataFilename, dropTableFromMetastore, system.register_table, List.of] vs current [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, with, env.executeSparkUpdate, values, env.getTableLocation, env.getLatestMetadataFilename, env.dropTableFromMetastore, system.register_table, List.of, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, with, onSpark, values, getTableLocation, getLatestMetadataFilename, dropTableFromMetastore, system.register_table, List.of], verbs [CREATE, INSERT, CALL, SELECT, DROP]. Current flow summary -> helpers [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, with, env.executeSparkUpdate, values, env.getTableLocation, env.getLatestMetadataFilename, env.dropTableFromMetastore, system.register_table, List.of, env.executeTrino, env.executeSpark], verbs [CREATE, INSERT, CALL, SELECT, DROP].
- Audit status: `verified`

##### `testRegisterTableWithDroppedTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testRegisterTableWithDroppedTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testRegisterTableWithDroppedTable`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `CALL`, `storageFormat.name`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `format`, `s`, `TBLPROPERTIES`, `values`, `onTrino`, `getTableLocation`, `found`, `system.register_table`. Current action shape: `CALL`, `storageFormat.name`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `format`, `s`, `TBLPROPERTIES`, `values`, `env.executeTrinoUpdate`, `env.getTableLocation`, `found`, `system.register_table`, `hasStackTraceContaining`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, values, onTrino, getTableLocation, found, system.register_table] vs current [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, values, env.executeTrinoUpdate, env.getTableLocation, found, system.register_table, hasStackTraceContaining]; assertion helpers differ: legacy [assertQueryFailure, hasMessageMatching] vs current [assertThatThrownBy]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, values, onTrino, getTableLocation, found, system.register_table], verbs [CREATE, INSERT, DROP, CALL]. Current flow summary -> helpers [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, values, env.executeTrinoUpdate, env.getTableLocation, found, system.register_table, hasStackTraceContaining], verbs [CREATE, INSERT, DROP, CALL].
- Audit status: `verified`

##### `testRegisterTableWithDifferentTableName`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testRegisterTableWithDifferentTableName`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testRegisterTableWithDifferentTableName`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `CALL`, `SELECT`, `storageFormat.name`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `format`, `s`, `TBLPROPERTIES`, `values`, `onTrino`, `getTableLocation`, `getLatestMetadataFilename`, `system.register_table`, `List.of`. Current action shape: `CALL`, `SELECT`, `storageFormat.name`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `format`, `s`, `TBLPROPERTIES`, `values`, `env.executeTrinoUpdate`, `env.getTableLocation`, `env.getLatestMetadataFilename`, `system.register_table`, `List.of`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `dropTableFromMetastore`. Current cleanup shape: `DROP`, `env.dropTableFromMetastore`.
- Any observed difference, however small: helper calls differ: legacy [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, values, onTrino, getTableLocation, getLatestMetadataFilename, dropTableFromMetastore, system.register_table, List.of] vs current [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, values, env.executeTrinoUpdate, env.getTableLocation, env.getLatestMetadataFilename, env.dropTableFromMetastore, system.register_table, List.of, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, values, onTrino, getTableLocation, getLatestMetadataFilename, dropTableFromMetastore, system.register_table, List.of], verbs [CREATE, INSERT, CALL, SELECT, DROP]. Current flow summary -> helpers [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, values, env.executeTrinoUpdate, env.getTableLocation, env.getLatestMetadataFilename, env.dropTableFromMetastore, system.register_table, List.of, env.executeTrino, env.executeSpark], verbs [CREATE, INSERT, CALL, SELECT, DROP].
- Audit status: `verified`

##### `testRegisterTableWithMetadataFile`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testRegisterTableWithMetadataFile`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testRegisterTableWithMetadataFile`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `CALL`, `SELECT`, `storageFormat.name`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `format`, `s`, `TBLPROPERTIES`, `values`, `onTrino`, `getTableLocation`, `metastoreClient.getTable`, `getParameters`, `get`, `metadataLocation.substring`, `metadataLocation.lastIndexOf`, `system.register_table`, `List.of`. Current action shape: `CALL`, `SELECT`, `storageFormat.name`, `toLowerCase`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `format`, `s`, `TBLPROPERTIES`, `values`, `env.executeTrinoUpdate`, `env.getTableLocation`, `env.getLatestMetadataFilename`, `system.register_table`, `List.of`, `env.executeTrino`, `env.executeSpark`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `dropTableFromMetastore`. Current cleanup shape: `DROP`, `env.dropTableFromMetastore`.
- Any observed difference, however small: helper calls differ: legacy [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, values, onTrino, getTableLocation, metastoreClient.getTable, getParameters, get, metadataLocation.substring, metadataLocation.lastIndexOf, dropTableFromMetastore, system.register_table, List.of] vs current [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, values, env.executeTrinoUpdate, env.getTableLocation, env.getLatestMetadataFilename, env.dropTableFromMetastore, system.register_table, List.of, env.executeTrino, env.executeSpark]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, format, s, TBLPROPERTIES, values, onTrino, getTableLocation, metastoreClient.getTable, getParameters, get, metadataLocation.substring, metadataLocation.lastIndexOf, dropTableFromMetastore, system.register_table, List.of], verbs [CREATE, INSERT, CALL, SELECT, DROP]. Current flow summary -> helpers [storageFormat.name, toLowerCase, randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, TBLPROPERTIES, values, env.executeTrinoUpdate, env.getTableLocation, env.getLatestMetadataFilename, env.dropTableFromMetastore, system.register_table, List.of, env.executeTrino, env.executeSpark], verbs [CREATE, INSERT, CALL, SELECT, DROP].
- Audit status: `verified`

##### `testUnregisterNotIcebergTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testUnregisterNotIcebergTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testUnregisterNotIcebergTable`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `CALL`, `randomNameSuffix`, `trinoTableName`, `onHive`, `executeQuery`, `onTrino`, `system.unregister_table`, `onSpark`. Current action shape: `SELECT`, `CALL`, `randomNameSuffix`, `trinoTableName`, `env.executeHiveUpdate`, `env.executeTrinoUpdate`, `system.unregister_table`, `hasStackTraceContaining`, `env.executeSpark`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThatThrownBy`, `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, onHive, executeQuery, onTrino, system.unregister_table, onSpark] vs current [randomNameSuffix, trinoTableName, env.executeHiveUpdate, env.executeTrinoUpdate, system.unregister_table, hasStackTraceContaining, env.executeSpark, env.executeTrino]; assertion helpers differ: legacy [assertThatThrownBy, hasMessageContaining, assertThat, containsOnly, row] vs current [assertThatThrownBy, assertThat, containsOnly, row]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, onHive, executeQuery, onTrino, system.unregister_table, onSpark], verbs [CREATE, SELECT, CALL, DROP]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, env.executeHiveUpdate, env.executeTrinoUpdate, system.unregister_table, hasStackTraceContaining, env.executeSpark, env.executeTrino], verbs [CREATE, SELECT, CALL, DROP].
- Audit status: `verified`

##### `testPartitionedByNonLowercaseColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testPartitionedByNonLowercaseColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testPartitionedByNonLowercaseColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `SELECT`, `UPDATE`, `MERGE`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `BY`, `TBLPROPERTIES`, `onTrino`, `VALUES`, `USING`, `hasNoRows`. Current action shape: `SELECT`, `UPDATE`, `MERGE`, `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `BY`, `TBLPROPERTIES`, `env.executeTrino`, `env.executeTrinoUpdate`, `VALUES`, `USING`, `hasNoRows`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `row`. Current assertion helpers: `assertThat`, `contains`, `row`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `DROP`. Current cleanup shape: `DELETE`, `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, BY, TBLPROPERTIES, onTrino, VALUES, USING, hasNoRows] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, BY, TBLPROPERTIES, env.executeTrino, env.executeTrinoUpdate, VALUES, USING, hasNoRows]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onSpark, executeQuery, BY, TBLPROPERTIES, onTrino, VALUES, USING, hasNoRows], verbs [CREATE, SELECT, INSERT, DELETE, UPDATE, SET, MERGE, DROP]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeSparkUpdate, BY, TBLPROPERTIES, env.executeTrino, env.executeTrinoUpdate, VALUES, USING, hasNoRows], verbs [CREATE, SELECT, INSERT, DELETE, UPDATE, SET, MERGE, DROP].
- Audit status: `verified`

##### `testPartitioningWithMixedCaseColumnInTrino`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testPartitioningWithMixedCaseColumnInTrino`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testPartitioningWithMixedCaseColumnInTrino`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`, `SET`, `INSERT`. Current setup shape: `CREATE`, `ALTER`, `SET`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `SHOW`, `trinoTableName`, `sparkTableName`, `onSpark`, `executeQuery`, `format`, `s`, `onTrino`, `VALUES`, `ImmutableList.of`, `getOnlyValue`, `BY`. Current action shape: `SELECT`, `SHOW`, `trinoTableName`, `sparkTableName`, `env.executeSparkUpdate`, `format`, `s`, `env.executeTrinoUpdate`, `hasStackTraceContaining`, `VALUES`, `ImmutableList.of`, `env.executeTrino`, `env.executeSpark`, `getOnlyValue`, `BY`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageContaining`, `row`, `assertThat`, `contains`. Current assertion helpers: `assertThatThrownBy`, `row`, `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [trinoTableName, sparkTableName, onSpark, executeQuery, format, s, onTrino, VALUES, ImmutableList.of, getOnlyValue, BY] vs current [trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, env.executeTrinoUpdate, hasStackTraceContaining, VALUES, ImmutableList.of, env.executeTrino, env.executeSpark, getOnlyValue, BY]; assertion helpers differ: legacy [assertQueryFailure, hasMessageContaining, row, assertThat, contains] vs current [assertThatThrownBy, row, assertThat, contains]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [trinoTableName, sparkTableName, onSpark, executeQuery, format, s, onTrino, VALUES, ImmutableList.of, getOnlyValue, BY], verbs [DROP, CREATE, ALTER, SET, INSERT, SELECT, SHOW]. Current flow summary -> helpers [trinoTableName, sparkTableName, env.executeSparkUpdate, format, s, env.executeTrinoUpdate, hasStackTraceContaining, VALUES, ImmutableList.of, env.executeTrino, env.executeSpark, getOnlyValue, BY], verbs [DROP, CREATE, ALTER, SET, INSERT, SELECT, SHOW].
- Audit status: `verified`

##### `testStringPartitioningWithSpecialCharactersCtasInTrino`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testStringPartitioningWithSpecialCharactersCtasInTrino`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testStringPartitioningWithSpecialCharactersCtasInTrino`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `format`, `s`, `WITH`. Current action shape: `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `format`, `s`, `WITH`.
- Assertion parity: Legacy assertion helpers: `assertSelectsOnSpecialCharacters`. Current assertion helpers: `assertSelectsOnSpecialCharacters`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [trinoTableName, sparkTableName, onTrino, executeQuery, format, s, WITH] vs current [trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, WITH]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [trinoTableName, sparkTableName, onTrino, executeQuery, format, s, WITH], verbs [DROP, CREATE]. Current flow summary -> helpers [trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, WITH], verbs [DROP, CREATE].
- Audit status: `verified`

##### `testStringPartitioningWithSpecialCharactersInsertInTrino`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testStringPartitioningWithSpecialCharactersInsertInTrino`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testStringPartitioningWithSpecialCharactersInsertInTrino`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `format`, `s`, `WITH`. Current action shape: `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `format`, `s`, `WITH`.
- Assertion parity: Legacy assertion helpers: `assertSelectsOnSpecialCharacters`. Current assertion helpers: `assertSelectsOnSpecialCharacters`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [trinoTableName, sparkTableName, onTrino, executeQuery, format, s, WITH] vs current [trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, WITH]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [trinoTableName, sparkTableName, onTrino, executeQuery, format, s, WITH], verbs [DROP, CREATE, INSERT]. Current flow summary -> helpers [trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, WITH], verbs [DROP, CREATE, INSERT].
- Audit status: `verified`

##### `testStringPartitioningWithSpecialCharactersInsertInSpark`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testStringPartitioningWithSpecialCharactersInsertInSpark`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testStringPartitioningWithSpecialCharactersInsertInSpark`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `format`, `s`, `WITH`, `onSpark`. Current action shape: `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `format`, `s`, `WITH`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `assertSelectsOnSpecialCharacters`. Current assertion helpers: `assertSelectsOnSpecialCharacters`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [trinoTableName, sparkTableName, onTrino, executeQuery, format, s, WITH, onSpark] vs current [trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, WITH, env.executeSparkUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [trinoTableName, sparkTableName, onTrino, executeQuery, format, s, WITH, onSpark], verbs [DROP, CREATE, INSERT]. Current flow summary -> helpers [trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, WITH, env.executeSparkUpdate], verbs [DROP, CREATE, INSERT].
- Audit status: `verified`

##### `testTrinoReadingMigratedNestedData`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testTrinoReadingMigratedNestedData`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testTrinoReadingMigratedNestedData`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `USE`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `USE`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `CALL`, `randomNameSuffix`, `sparkDefaultCatalogTableName`, `s`, `onSpark`, `executeQuery`, `format`, `storageFormat.name`, `toLowerCase`, `map`, `array`, `named_struct`, `system.migrate`, `e.getMessage`, `kipException`, `sparkTableName`, `trinoTableName`, `onTrino`, `TBLPROPERTIES`. Current action shape: `SELECT`, `CALL`, `randomNameSuffix`, `sparkDefaultCatalogTableName`, `s`, `env.executeSparkUpdate`, `format`, `storageFormat.name`, `toLowerCase`, `map`, `array`, `named_struct`, `system.migrate`, `e.getMessage`, `Assumptions.abort`, `sparkTableName`, `env.executeSpark`, `trinoTableName`, `env.executeTrino`, `TBLPROPERTIES`.
- Assertion parity: Legacy assertion helpers: `contains`, `row`, `assertThat`, `containsOnly`. Current assertion helpers: `contains`, `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, sparkDefaultCatalogTableName, s, onSpark, executeQuery, format, storageFormat.name, toLowerCase, map, array, named_struct, system.migrate, e.getMessage, kipException, sparkTableName, trinoTableName, onTrino, TBLPROPERTIES] vs current [randomNameSuffix, sparkDefaultCatalogTableName, s, env.executeSparkUpdate, format, storageFormat.name, toLowerCase, map, array, named_struct, system.migrate, e.getMessage, Assumptions.abort, sparkTableName, env.executeSpark, trinoTableName, env.executeTrino, TBLPROPERTIES]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, sparkDefaultCatalogTableName, s, onSpark, executeQuery, format, storageFormat.name, toLowerCase, map, array, named_struct, system.migrate, e.getMessage, kipException, sparkTableName, trinoTableName, onTrino, TBLPROPERTIES], verbs [CREATE, INSERT, SELECT, CALL, USE, ALTER]. Current flow summary -> helpers [randomNameSuffix, sparkDefaultCatalogTableName, s, env.executeSparkUpdate, format, storageFormat.name, toLowerCase, map, array, named_struct, system.migrate, e.getMessage, Assumptions.abort, sparkTableName, env.executeSpark, trinoTableName, env.executeTrino, TBLPROPERTIES], verbs [CREATE, INSERT, SELECT, CALL, USE, ALTER].
- Audit status: `verified`

##### `testMigratedDataWithAlteredSchema`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testMigratedDataWithAlteredSchema`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testMigratedDataWithAlteredSchema`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `USE`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `USE`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `CALL`, `randomNameSuffix`, `sparkDefaultCatalogTableName`, `s`, `onSpark`, `executeQuery`, `format`, `named_struct`, `system.migrate`, `e.getMessage`, `kipException`, `sparkTableName`, `ImmutableList.of`, `trinoTableName`, `onTrino`, `TBLPROPERTIES`. Current action shape: `SELECT`, `CALL`, `randomNameSuffix`, `sparkDefaultCatalogTableName`, `s`, `env.executeSparkUpdate`, `format`, `named_struct`, `system.migrate`, `e.getMessage`, `Assumptions.abort`, `sparkTableName`, `env.executeSpark`, `ImmutableList.of`, `trinoTableName`, `env.executeTrino`, `TBLPROPERTIES`.
- Assertion parity: Legacy assertion helpers: `contains`, `row`, `assertThat`, `containsOnly`. Current assertion helpers: `contains`, `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, sparkDefaultCatalogTableName, s, onSpark, executeQuery, format, named_struct, system.migrate, e.getMessage, kipException, sparkTableName, ImmutableList.of, trinoTableName, onTrino, TBLPROPERTIES] vs current [randomNameSuffix, sparkDefaultCatalogTableName, s, env.executeSparkUpdate, format, named_struct, system.migrate, e.getMessage, Assumptions.abort, sparkTableName, env.executeSpark, ImmutableList.of, trinoTableName, env.executeTrino, TBLPROPERTIES]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, sparkDefaultCatalogTableName, s, onSpark, executeQuery, format, named_struct, system.migrate, e.getMessage, kipException, sparkTableName, ImmutableList.of, trinoTableName, onTrino, TBLPROPERTIES], verbs [CREATE, INSERT, SELECT, CALL, USE, ALTER]. Current flow summary -> helpers [randomNameSuffix, sparkDefaultCatalogTableName, s, env.executeSparkUpdate, format, named_struct, system.migrate, e.getMessage, Assumptions.abort, sparkTableName, env.executeSpark, ImmutableList.of, trinoTableName, env.executeTrino, TBLPROPERTIES], verbs [CREATE, INSERT, SELECT, CALL, USE, ALTER].
- Audit status: `verified`

##### `testMigratedDataWithPartialNameMapping`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testMigratedDataWithPartialNameMapping`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testMigratedDataWithPartialNameMapping`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `USE`, `ALTER`, `SET`. Current setup shape: `CREATE`, `INSERT`, `USE`, `ALTER`, `SET`.
- Action parity: Legacy action shape: `SELECT`, `CALL`, `randomNameSuffix`, `sparkDefaultCatalogTableName`, `s`, `storageFormat.name`, `toLowerCase`, `onSpark`, `executeQuery`, `format`, `system.migrate`, `e.getMessage`, `kipException`, `sparkTableName`, `trinoTableName`, `TBLPROPERTIES`, `onTrino`. Current action shape: `SELECT`, `CALL`, `randomNameSuffix`, `sparkDefaultCatalogTableName`, `s`, `storageFormat.name`, `toLowerCase`, `env.executeSparkUpdate`, `format`, `system.migrate`, `e.getMessage`, `Assumptions.abort`, `sparkTableName`, `trinoTableName`, `TBLPROPERTIES`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `contains`, `assertThat`, `containsOnly`, `row`. Current assertion helpers: `contains`, `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, sparkDefaultCatalogTableName, s, storageFormat.name, toLowerCase, onSpark, executeQuery, format, system.migrate, e.getMessage, kipException, sparkTableName, trinoTableName, TBLPROPERTIES, onTrino] vs current [randomNameSuffix, sparkDefaultCatalogTableName, s, storageFormat.name, toLowerCase, env.executeSparkUpdate, format, system.migrate, e.getMessage, Assumptions.abort, sparkTableName, trinoTableName, TBLPROPERTIES, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, sparkDefaultCatalogTableName, s, storageFormat.name, toLowerCase, onSpark, executeQuery, format, system.migrate, e.getMessage, kipException, sparkTableName, trinoTableName, TBLPROPERTIES, onTrino], verbs [CREATE, INSERT, SELECT, CALL, USE, ALTER, SET]. Current flow summary -> helpers [randomNameSuffix, sparkDefaultCatalogTableName, s, storageFormat.name, toLowerCase, env.executeSparkUpdate, format, system.migrate, e.getMessage, Assumptions.abort, sparkTableName, trinoTableName, TBLPROPERTIES, env.executeTrino], verbs [CREATE, INSERT, SELECT, CALL, USE, ALTER, SET].
- Audit status: `verified`

##### `testHandlingPartitionSchemaEvolutionInPartitionMetadata`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testHandlingPartitionSchemaEvolutionInPartitionMetadata`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testHandlingPartitionSchemaEvolutionInPartitionMetadata`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `ALTER`.
- Action parity: Legacy action shape: `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `format`, `s`, `WITH`, `VALUES`, `validatePartitioning`, `ImmutableList.of`, `ImmutableMap.of`, `onSpark`, `days`, `months`. Current action shape: `randomNameSuffix`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `format`, `s`, `WITH`, `VALUES`, `validatePartitioning`, `ImmutableList.of`, `ImmutableMap.of`, `env.executeSparkUpdate`, `days`, `months`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, WITH, VALUES, validatePartitioning, ImmutableList.of, ImmutableMap.of, onSpark, days, months] vs current [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, WITH, VALUES, validatePartitioning, ImmutableList.of, ImmutableMap.of, env.executeSparkUpdate, days, months]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, onTrino, executeQuery, format, s, WITH, VALUES, validatePartitioning, ImmutableList.of, ImmutableMap.of, onSpark, days, months], verbs [CREATE, INSERT, ALTER, DROP]. Current flow summary -> helpers [randomNameSuffix, trinoTableName, sparkTableName, env.executeTrinoUpdate, format, s, WITH, VALUES, validatePartitioning, ImmutableList.of, ImmutableMap.of, env.executeSparkUpdate, days, months], verbs [CREATE, INSERT, ALTER, DROP].
- Audit status: `verified`

##### `testInsertReadingFromParquetTableWithNestedRowFieldNotPresentInDataFile`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `testInsertReadingFromParquetTableWithNestedRowFieldNotPresentInDataFile`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkCompatibility.java` ->
  `TestIcebergSparkCompatibility.testInsertReadingFromParquetTableWithNestedRowFieldNotPresentInDataFile`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`, `INSERT`. Current setup shape: `CREATE`, `ALTER`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `trinoTableName`, `sparkTableName`, `onTrino`, `executeQuery`, `WITH`, `CAST`, `ROW`, `onSpark`, `rowBuilder`, `addField`, `build`. Current action shape: `SELECT`, `trinoTableName`, `sparkTableName`, `env.executeTrinoUpdate`, `WITH`, `CAST`, `ROW`, `env.executeSparkUpdate`, `env.executeTrino`, `rowBuilder`, `addField`, `build`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [trinoTableName, sparkTableName, onTrino, executeQuery, WITH, CAST, ROW, onSpark, rowBuilder, addField, build] vs current [trinoTableName, sparkTableName, env.executeTrinoUpdate, WITH, CAST, ROW, env.executeSparkUpdate, env.executeTrino, rowBuilder, addField, build]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [trinoTableName, sparkTableName, onTrino, executeQuery, WITH, CAST, ROW, onSpark, rowBuilder, addField, build], verbs [DROP, CREATE, SELECT, ALTER, INSERT]. Current flow summary -> helpers [trinoTableName, sparkTableName, env.executeTrinoUpdate, WITH, CAST, ROW, env.executeSparkUpdate, env.executeTrino, rowBuilder, addField, build], verbs [DROP, CREATE, SELECT, ALTER, INSERT].
- Audit status: `verified`

### `TestIcebergSparkDropTableCompatibility`


- Owning migration commit: `Migrate TestIcebergSparkDropTableCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkDropTableCompatibility.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkDropTableCompatibility.java`
- Class-level environment requirement: `SparkIcebergEnvironment`.
- Class-level tags: `Iceberg`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testCleanupOnDropTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/iceberg/TestIcebergSparkDropTableCompatibility.java` ->
  `testCleanupOnDropTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestIcebergSparkDropTableCompatibility.java` ->
  `TestIcebergSparkDropTableCompatibility.testCleanupOnDropTable`
- Mapping type: `direct`
- Environment parity: Current class requires `SparkIcebergEnvironment`. Routed by source review into `SuiteIceberg` run 1.
- Tag parity: Current tags: `Iceberg`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `USE`, `CREATE`, `INSERT`, `env.createHdfsClient`.
- Action parity: Legacy action shape: `randomNameSuffix`, `tableCreatorEngine.queryExecutor`, `executeQuery`, `onTrino`, `VALUES`, `stripNamenodeURI`, `getTableLocation`, `getDataFilePaths`, `format`, `dataFilePaths.forEach`. Current action shape: `catalogs`, `env.executeTrinoUpdate`, `env.executeSparkUpdate`, `randomNameSuffix`, `executeUpdate`, `Trino`, `VALUES`, `SparkIcebergEnvironment.stripNamenodeURI`, `getTableLocation`, `getDataFilePaths`.
- Assertion parity: Legacy assertion helpers: `assertFileExistence`. Current assertion helpers: `assertFileExistence`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `tableDropperEngine.queryExecutor`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, tableCreatorEngine.queryExecutor, executeQuery, onTrino, VALUES, stripNamenodeURI, getTableLocation, getDataFilePaths, tableDropperEngine.queryExecutor, format, dataFilePaths.forEach] vs current [catalogs, env.executeTrinoUpdate, env.executeSparkUpdate, env.createHdfsClient, randomNameSuffix, executeUpdate, Trino, VALUES, SparkIcebergEnvironment.stripNamenodeURI, getTableLocation, getDataFilePaths]; SQL verbs differ: legacy [CREATE, INSERT, DROP] vs current [USE, CREATE, INSERT, DROP]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, tableCreatorEngine.queryExecutor, executeQuery, onTrino, VALUES, stripNamenodeURI, getTableLocation, getDataFilePaths, tableDropperEngine.queryExecutor, format, dataFilePaths.forEach], verbs [CREATE, INSERT, DROP]. Current flow summary -> helpers [catalogs, env.executeTrinoUpdate, env.executeSparkUpdate, env.createHdfsClient, randomNameSuffix, executeUpdate, Trino, VALUES, SparkIcebergEnvironment.stripNamenodeURI, getTableLocation, getDataFilePaths], verbs [USE, CREATE, INSERT, DROP].
- Audit status: `verified`


## Current-only Environment Verification Coverage


### `TestHiveIcebergRedirectionsEnvironment`

- Owning introduction commit: `Add Iceberg environments`
- Current class added in same commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestHiveIcebergRedirectionsEnvironment.java`
- Legacy class removed in same commit:
  none. This is current-only JUnit environment verification coverage added during the framework rewrite.
- Class-level environment requirement: `HiveIcebergRedirectionsEnvironment`.
- Class-level tags: `HiveIcebergRedirections`.
- Method inventory complete: Current-only class. Legacy methods: `0`. Current methods: `5`.
- Legacy helper/resource dependencies accounted for: not applicable; current class source reviewed directly.
- Intentional differences summary: `new JUnit-side environment verification coverage`
- Method statuses present: `intentional difference`.

#### Methods

##### `verifyTrinoConnectivity`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestHiveIcebergRedirectionsEnvironment.java` ->
  `TestHiveIcebergRedirectionsEnvironment.verifyTrinoConnectivity`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `HiveIcebergRedirectionsEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `trino connectivity`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `HiveIcebergRedirectionsEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyHiveCatalog`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestHiveIcebergRedirectionsEnvironment.java` ->
  `TestHiveIcebergRedirectionsEnvironment.verifyHiveCatalog`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `HiveIcebergRedirectionsEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `hive catalog`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `HiveIcebergRedirectionsEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyIcebergCatalog`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestHiveIcebergRedirectionsEnvironment.java` ->
  `TestHiveIcebergRedirectionsEnvironment.verifyIcebergCatalog`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `HiveIcebergRedirectionsEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `iceberg catalog`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `HiveIcebergRedirectionsEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyHiveToIcebergRedirection`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestHiveIcebergRedirectionsEnvironment.java` ->
  `TestHiveIcebergRedirectionsEnvironment.verifyHiveToIcebergRedirection`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `HiveIcebergRedirectionsEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `hive to iceberg redirection`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `HiveIcebergRedirectionsEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyIcebergToHiveRedirection`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestHiveIcebergRedirectionsEnvironment.java` ->
  `TestHiveIcebergRedirectionsEnvironment.verifyIcebergToHiveRedirection`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `HiveIcebergRedirectionsEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `iceberg to hive redirection`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `HiveIcebergRedirectionsEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`


### `TestMultiNodeIcebergMinioCachingEnvironment`

- Owning introduction commit: `Add Iceberg environments`
- Current class added in same commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestMultiNodeIcebergMinioCachingEnvironment.java`
- Legacy class removed in same commit:
  none. This is current-only JUnit environment verification coverage added during the framework rewrite.
- Class-level environment requirement: `MultiNodeIcebergMinioCachingEnvironment`.
- Class-level tags: `IcebergAlluxioCaching`.
- Method inventory complete: Current-only class. Legacy methods: `0`. Current methods: `6`.
- Legacy helper/resource dependencies accounted for: not applicable; current class source reviewed directly.
- Intentional differences summary: `new JUnit-side environment verification coverage`
- Method statuses present: `intentional difference`.

#### Methods

##### `verifyTrinoConnectivity`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestMultiNodeIcebergMinioCachingEnvironment.java` ->
  `TestMultiNodeIcebergMinioCachingEnvironment.verifyTrinoConnectivity`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultiNodeIcebergMinioCachingEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `trino connectivity`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultiNodeIcebergMinioCachingEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyIcebergCatalog`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestMultiNodeIcebergMinioCachingEnvironment.java` ->
  `TestMultiNodeIcebergMinioCachingEnvironment.verifyIcebergCatalog`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultiNodeIcebergMinioCachingEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `iceberg catalog`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultiNodeIcebergMinioCachingEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyS3StorageWorks`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestMultiNodeIcebergMinioCachingEnvironment.java` ->
  `TestMultiNodeIcebergMinioCachingEnvironment.verifyS3StorageWorks`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultiNodeIcebergMinioCachingEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `s3 storage works`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultiNodeIcebergMinioCachingEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyCachingEnabled`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestMultiNodeIcebergMinioCachingEnvironment.java` ->
  `TestMultiNodeIcebergMinioCachingEnvironment.verifyCachingEnabled`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultiNodeIcebergMinioCachingEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `caching enabled`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultiNodeIcebergMinioCachingEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyMultipleTableOperations`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestMultiNodeIcebergMinioCachingEnvironment.java` ->
  `TestMultiNodeIcebergMinioCachingEnvironment.verifyMultipleTableOperations`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultiNodeIcebergMinioCachingEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `multiple table operations`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultiNodeIcebergMinioCachingEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyIcebergMetadataQueries`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestMultiNodeIcebergMinioCachingEnvironment.java` ->
  `TestMultiNodeIcebergMinioCachingEnvironment.verifyIcebergMetadataQueries`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `MultiNodeIcebergMinioCachingEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `iceberg metadata queries`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `MultiNodeIcebergMinioCachingEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`


### `TestSparkIcebergEnvironment`

- Owning introduction commit: `Add Iceberg environments`
- Current class added in same commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergEnvironment.java`
- Legacy class removed in same commit:
  none. This is current-only JUnit environment verification coverage added during the framework rewrite.
- Class-level environment requirement: `SparkIcebergEnvironment`.
- Class-level tags: `Iceberg`.
- Method inventory complete: Current-only class. Legacy methods: `0`. Current methods: `11`.
- Legacy helper/resource dependencies accounted for: not applicable; current class source reviewed directly.
- Intentional differences summary: `new JUnit-side environment verification coverage`
- Method statuses present: `intentional difference`.

#### Methods

##### `verifyTrinoConnectivity`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergEnvironment.java` ->
  `TestSparkIcebergEnvironment.verifyTrinoConnectivity`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `trino connectivity`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifySparkConnectivity`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergEnvironment.java` ->
  `TestSparkIcebergEnvironment.verifySparkConnectivity`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `spark connectivity`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyHiveConnectivity`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergEnvironment.java` ->
  `TestSparkIcebergEnvironment.verifyHiveConnectivity`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `hive connectivity`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyIcebergCatalog`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergEnvironment.java` ->
  `TestSparkIcebergEnvironment.verifyIcebergCatalog`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `iceberg catalog`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifySparkCreatesTableTrinoReads`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergEnvironment.java` ->
  `TestSparkIcebergEnvironment.verifySparkCreatesTableTrinoReads`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `spark creates table trino reads`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyTrinoCreatesTableSparkReads`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergEnvironment.java` ->
  `TestSparkIcebergEnvironment.verifyTrinoCreatesTableSparkReads`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `trino creates table spark reads`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyMetastoreClient`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergEnvironment.java` ->
  `TestSparkIcebergEnvironment.verifyMetastoreClient`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `metastore client`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyIcebergUtilityMethods`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergEnvironment.java` ->
  `TestSparkIcebergEnvironment.verifyIcebergUtilityMethods`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `iceberg utility methods`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `investigateMetadataLocation`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergEnvironment.java` ->
  `TestSparkIcebergEnvironment.investigateMetadataLocation`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `metadata location`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifySparkCanAlterTrinoCreatedTable`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergEnvironment.java` ->
  `TestSparkIcebergEnvironment.verifySparkCanAlterTrinoCreatedTable`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `spark can alter trino created table`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifySparkCanDropPartitionFieldOnTrinoCreatedTable`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergEnvironment.java` ->
  `TestSparkIcebergEnvironment.verifySparkCanDropPartitionFieldOnTrinoCreatedTable`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `spark can drop partition field on trino created table`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`


### `TestSparkIcebergJdbcCatalogEnvironment`

- Owning introduction commit: `Add Iceberg environments`
- Current class added in same commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergJdbcCatalogEnvironment.java`
- Legacy class removed in same commit:
  none. This is current-only JUnit environment verification coverage added during the framework rewrite.
- Class-level environment requirement: `SparkIcebergJdbcCatalogEnvironment`.
- Class-level tags: `IcebergJdbc`.
- Method inventory complete: Current-only class. Legacy methods: `0`. Current methods: `7`.
- Legacy helper/resource dependencies accounted for: not applicable; current class source reviewed directly.
- Intentional differences summary: `new JUnit-side environment verification coverage`
- Method statuses present: `intentional difference`.

#### Methods

##### `verifyTrinoConnectivity`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergJdbcCatalogEnvironment.java` ->
  `TestSparkIcebergJdbcCatalogEnvironment.verifyTrinoConnectivity`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergJdbcCatalogEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `trino connectivity`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergJdbcCatalogEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifySparkConnectivity`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergJdbcCatalogEnvironment.java` ->
  `TestSparkIcebergJdbcCatalogEnvironment.verifySparkConnectivity`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergJdbcCatalogEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `spark connectivity`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergJdbcCatalogEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyPostgresqlConnectivity`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergJdbcCatalogEnvironment.java` ->
  `TestSparkIcebergJdbcCatalogEnvironment.verifyPostgresqlConnectivity`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergJdbcCatalogEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `postgresql connectivity`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergJdbcCatalogEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyJdbcCatalogMetadata`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergJdbcCatalogEnvironment.java` ->
  `TestSparkIcebergJdbcCatalogEnvironment.verifyJdbcCatalogMetadata`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergJdbcCatalogEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `jdbc catalog metadata`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergJdbcCatalogEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifySparkCreatesTableTrinoReads`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergJdbcCatalogEnvironment.java` ->
  `TestSparkIcebergJdbcCatalogEnvironment.verifySparkCreatesTableTrinoReads`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergJdbcCatalogEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `spark creates table trino reads`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergJdbcCatalogEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyTrinoCreatesTableSparkReads`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergJdbcCatalogEnvironment.java` ->
  `TestSparkIcebergJdbcCatalogEnvironment.verifyTrinoCreatesTableSparkReads`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergJdbcCatalogEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `trino creates table spark reads`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergJdbcCatalogEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyCustomTableProperty`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergJdbcCatalogEnvironment.java` ->
  `TestSparkIcebergJdbcCatalogEnvironment.verifyCustomTableProperty`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergJdbcCatalogEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `custom table property`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergJdbcCatalogEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`


### `TestSparkIcebergNessieEnvironment`

- Owning introduction commit: `Add Iceberg environments`
- Current class added in same commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergNessieEnvironment.java`
- Legacy class removed in same commit:
  none. This is current-only JUnit environment verification coverage added during the framework rewrite.
- Class-level environment requirement: `SparkIcebergNessieEnvironment`.
- Class-level tags: `IcebergNessie`.
- Method inventory complete: Current-only class. Legacy methods: `0`. Current methods: `6`.
- Legacy helper/resource dependencies accounted for: not applicable; current class source reviewed directly.
- Intentional differences summary: `new JUnit-side environment verification coverage`
- Method statuses present: `intentional difference`.

#### Methods

##### `verifyTrinoConnectivity`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergNessieEnvironment.java` ->
  `TestSparkIcebergNessieEnvironment.verifyTrinoConnectivity`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergNessieEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `trino connectivity`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergNessieEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifySparkConnectivity`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergNessieEnvironment.java` ->
  `TestSparkIcebergNessieEnvironment.verifySparkConnectivity`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergNessieEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `spark connectivity`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergNessieEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyNessieEndpoint`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergNessieEnvironment.java` ->
  `TestSparkIcebergNessieEnvironment.verifyNessieEndpoint`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergNessieEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `nessie endpoint`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergNessieEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyIcebergCatalog`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergNessieEnvironment.java` ->
  `TestSparkIcebergNessieEnvironment.verifyIcebergCatalog`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergNessieEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `iceberg catalog`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergNessieEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifySparkCreatesTableTrinoReads`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergNessieEnvironment.java` ->
  `TestSparkIcebergNessieEnvironment.verifySparkCreatesTableTrinoReads`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergNessieEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `spark creates table trino reads`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergNessieEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyTrinoCreatesTableSparkReads`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergNessieEnvironment.java` ->
  `TestSparkIcebergNessieEnvironment.verifyTrinoCreatesTableSparkReads`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergNessieEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `trino creates table spark reads`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergNessieEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`


### `TestSparkIcebergRestEnvironment`

- Owning introduction commit: `Add Iceberg environments`
- Current class added in same commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergRestEnvironment.java`
- Legacy class removed in same commit:
  none. This is current-only JUnit environment verification coverage added during the framework rewrite.
- Class-level environment requirement: `SparkIcebergRestEnvironment`.
- Class-level tags: `IcebergRest`.
- Method inventory complete: Current-only class. Legacy methods: `0`. Current methods: `7`.
- Legacy helper/resource dependencies accounted for: not applicable; current class source reviewed directly.
- Intentional differences summary: `new JUnit-side environment verification coverage`
- Method statuses present: `intentional difference`.

#### Methods

##### `verifyTrinoConnectivity`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergRestEnvironment.java` ->
  `TestSparkIcebergRestEnvironment.verifyTrinoConnectivity`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergRestEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `trino connectivity`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergRestEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifySparkConnectivity`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergRestEnvironment.java` ->
  `TestSparkIcebergRestEnvironment.verifySparkConnectivity`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergRestEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `spark connectivity`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergRestEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyRestCatalogEndpoint`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergRestEnvironment.java` ->
  `TestSparkIcebergRestEnvironment.verifyRestCatalogEndpoint`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergRestEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `rest catalog endpoint`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergRestEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyIcebergRestCatalog`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergRestEnvironment.java` ->
  `TestSparkIcebergRestEnvironment.verifyIcebergRestCatalog`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergRestEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `iceberg rest catalog`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergRestEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifySparkCreatesTableTrinoReads`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergRestEnvironment.java` ->
  `TestSparkIcebergRestEnvironment.verifySparkCreatesTableTrinoReads`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergRestEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `spark creates table trino reads`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergRestEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyTrinoCreatesTableSparkReads`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergRestEnvironment.java` ->
  `TestSparkIcebergRestEnvironment.verifyTrinoCreatesTableSparkReads`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergRestEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `trino creates table spark reads`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergRestEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`

##### `verifyCustomTableProperty`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/iceberg/TestSparkIcebergRestEnvironment.java` ->
  `TestSparkIcebergRestEnvironment.verifyCustomTableProperty`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `SparkIcebergRestEnvironment` environment directly; no legacy method existed.
- Tag parity: Routed by current suite selection into `SuiteIceberg`.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `custom table property`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `SparkIcebergRestEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: Routed by current suite selection into `SuiteIceberg`.
- Audit status: `intentional difference`
