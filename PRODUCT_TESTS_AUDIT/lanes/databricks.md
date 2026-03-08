# Lane Audit: Databricks

## Lane Summary

- Audit status: `manual-pass-complete`
- Section start commit: `Add Databricks environment`
- Section end commit: `Remove legacy Databricks launcher suites`
- Introduced JUnit suites: `SuiteDeltaLakeDatabricks133`, `SuiteDeltaLakeDatabricks143`, `SuiteDeltaLakeDatabricks154`,
  `SuiteDeltaLakeDatabricks164`, `SuiteDeltaLakeDatabricks173`.
- Extended existing suites: none.
- Retired legacy suites: `Databricks launcher suites`.
- Environment classes introduced: `DeltaLakeDatabricksEnvironment`.
- Method status counts: verified `25`, intentional difference `0`, needs follow-up `0`.

## Semantic Audit Status

- Final-pass review note: legacy/current source bodies were freshly reread one method at a time for all 25 mapped
  Databricks-lane methods in this pass, including the split Databricks coverage carried out of the shared Delta Lake
  classes. The five versioned launcher suites and six current/legacy environment classes were also re-read directly from
  source and history in this pass.

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: `HDP to Hive 3.1 migration`.
- Needs-follow-up methods: none currently identified in this source/history pass.

## Environment Semantic Audit
### `DeltaLakeDatabricks133Environment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of legacy `EnvSinglenodeDeltaLakeDatabricks133` against current
  `DeltaLakeDatabricks133Environment`.
- JDBC configuration parity: preserved in intent. Both source the Databricks 13.3 JDBC URL from
  `DATABRICKS_133_JDBC_URL` and disable Arrow transport in the URL itself.
- Recorded differences: current subclass also appends `SocketTimeout=120` and inherits the direct
  Testcontainers-based environment wiring from `DeltaLakeDatabricksEnvironment` instead of using the launcher wrapper.
- Reviewer note: no additional environment-specific fidelity gap is currently identified.

### `DeltaLakeDatabricks143Environment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of legacy `EnvSinglenodeDeltaLakeDatabricks143` against current
  `DeltaLakeDatabricks143Environment`.
- JDBC configuration parity: preserved in intent. Both source the Databricks 14.3 JDBC URL from
  `DATABRICKS_143_JDBC_URL` and disable Arrow transport in the URL itself.
- Recorded differences: current subclass also appends `SocketTimeout=120` and inherits the direct
  Testcontainers-based environment wiring from `DeltaLakeDatabricksEnvironment` instead of using the launcher wrapper.
- Reviewer note: no additional environment-specific fidelity gap is currently identified.

### `DeltaLakeDatabricks154Environment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of legacy `EnvSinglenodeDeltaLakeDatabricks154` against current
  `DeltaLakeDatabricks154Environment`.
- JDBC configuration parity: preserved in intent. Both source the Databricks 15.4 JDBC URL from
  `DATABRICKS_154_JDBC_URL` and disable Arrow transport in the URL itself.
- Recorded differences: current subclass also appends `SocketTimeout=120`, and it includes an unscheduled standalone
  `main` helper for direct environment startup that has no legacy launcher counterpart.
- Reviewer note: no additional environment-specific fidelity gap is currently identified.

### `DeltaLakeDatabricks164Environment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of legacy `EnvSinglenodeDeltaLakeDatabricks164` against current
  `DeltaLakeDatabricks164Environment`.
- JDBC configuration parity: preserved in intent. Both source the Databricks 16.4 JDBC URL from
  `DATABRICKS_164_JDBC_URL` without adding a new Arrow override because the CI secret already carries that setting.
- Recorded differences: current subclass inherits the direct Testcontainers-based environment wiring from
  `DeltaLakeDatabricksEnvironment` instead of using the launcher wrapper.
- Reviewer note: no additional environment-specific fidelity gap is currently identified.

### `DeltaLakeDatabricks173Environment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of legacy `EnvSinglenodeDeltaLakeDatabricks173` against current
  `DeltaLakeDatabricks173Environment`.
- JDBC configuration parity: preserved in intent. Both source the Databricks 17.3 JDBC URL from
  `DATABRICKS_173_JDBC_URL` and rely on the externally prepared URL settings for Arrow behavior.
- Recorded differences: current subclass inherits the direct Testcontainers-based environment wiring from
  `DeltaLakeDatabricksEnvironment` instead of using the launcher wrapper.
- Reviewer note: no additional environment-specific fidelity gap is currently identified.

### `DeltaLakeDatabricksEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of legacy `AbstractSinglenodeDeltaLakeDatabricks` against current
  `DeltaLakeDatabricksEnvironment`.
- External-service parity: preserved in intent. Both rely on the same external AWS S3, Glue metastore, and Databricks
  services, and both expose `hive`, `delta`, and `tpch` catalogs to the Databricks interoperability tests.
- Config/resource wiring parity: preserved in intent. Legacy launcher pushed environment variables and mounted connector
  files into the coordinator/tests containers; current environment builds the same catalog set directly in the Trino
  container and provides helper methods for Trino, Databricks JDBC, and Glue interactions.
- Recorded differences: outside-Docker execution, JUnit/Testcontainers framework replacement, and code-driven
  configuration instead of launcher-time file mounting.
- Reviewer note: no additional environment-specific fidelity gap is currently identified.

## Suite Semantic Audit
### `SuiteDeltaLakeDatabricks133`
- Suite semantic audit status: `complete`
- CI bucket: `databricks-133`
- Relationship to lane: `owned by this lane`.
- Reviewer note: compared directly against legacy `SuiteDeltaLakeDatabricks133`; the current suite still selects the
  shared Databricks tag set, covering 44 mapped methods across the `delta-lake` and `databricks` lane classes.

### `SuiteDeltaLakeDatabricks143`
- Suite semantic audit status: `complete`
- CI bucket: `databricks-143`
- Relationship to lane: `owned by this lane`.
- Reviewer note: compared directly against legacy `SuiteDeltaLakeDatabricks143`; the current suite still selects four
  mapped methods across three split classes, despite the stale zero-tests comment in the current suite source.

### `SuiteDeltaLakeDatabricks154`
- Suite semantic audit status: `complete`
- CI bucket: `databricks-154`
- Relationship to lane: `owned by this lane`.
- Reviewer note: compared directly against legacy `SuiteDeltaLakeDatabricks154`; the current suite still selects four
  mapped methods across three split classes, despite the stale zero-tests comment in the current suite source.

### `SuiteDeltaLakeDatabricks164`
- Suite semantic audit status: `complete`
- CI bucket: `databricks-164`
- Relationship to lane: `owned by this lane`.
- Reviewer note: compared directly against legacy `SuiteDeltaLakeDatabricks164`; the current suite still selects four
  mapped methods across three split classes, despite the stale zero-tests comment in the current suite source.

### `SuiteDeltaLakeDatabricks173`
- Suite semantic audit status: `complete`
- CI bucket: `databricks-173`
- Relationship to lane: `owned by this lane`.
- Reviewer note: compared directly against legacy `SuiteDeltaLakeDatabricks173`; the current suite preserves the shared
  Databricks tag set plus the `DELTA_LAKE_EXCLUDE_173` exclusion, covering 38 mapped methods across the `delta-lake`
  and `databricks` lane classes.

## Ported Test Classes

### `TestDatabricksWithGlueMetastoreCleanUp`


- Owning migration commit: `Migrate TestDatabricksWithGlueMetastoreCleanUp to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDatabricksWithGlueMetastoreCleanUp.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDatabricksWithGlueMetastoreCleanUp.java`
- Class-level environment requirement: `DeltaLakeDatabricksEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testCleanUpOldTablesUsingDelta`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDatabricksWithGlueMetastoreCleanUp.java` ->
  `testCleanUpOldTablesUsingDelta`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDatabricksWithGlueMetastoreCleanUp.java` ->
  `TestDatabricksWithGlueMetastoreCleanUp.testCleanUpOldTablesUsingDelta`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing matches the shared Databricks suite selection in the 133 and 173 lanes.
- Setup parity: Legacy setup shape: `SET`, `GlueClient.create`. Current setup shape: `SET`, `env.createGlueClient`.
- Action parity: Legacy action shape: `SELECT`, `currentTimeMillis`, `onTrino`, `executeQuery`, `DISTINCT`, `rows`, `stream`, `map`, `row.get`, `filter`, `schema.toLowerCase`, `startsWith`, `schema.equals`, `collect`, `toUnmodifiableList`, `schemas.forEach`, `cleanSchema`. Current action shape: `SELECT`, `currentTimeMillis`, `env.executeTrinoSql`, `DISTINCT`, `rows`, `stream`, `map`, `row.get`, `filter`, `schema.toLowerCase`, `startsWith`, `schema.equals`, `collect`, `toUnmodifiableList`, `schemas.forEach`, `cleanSchema`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: none.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [GlueClient.create, currentTimeMillis, onTrino, executeQuery, DISTINCT, rows, stream, map, row.get, filter, schema.toLowerCase, startsWith, schema.equals, collect, toUnmodifiableList, schemas.forEach, cleanSchema] vs current [env.createGlueClient, currentTimeMillis, env.executeTrinoSql, DISTINCT, rows, stream, map, row.get, filter, schema.toLowerCase, startsWith, schema.equals, collect, toUnmodifiableList, schemas.forEach, cleanSchema]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [GlueClient.create, currentTimeMillis, onTrino, executeQuery, DISTINCT, rows, stream, map, row.get, filter, schema.toLowerCase, startsWith, schema.equals, collect, toUnmodifiableList, schemas.forEach, cleanSchema], verbs [SELECT, SET]. Current flow summary -> helpers [env.createGlueClient, currentTimeMillis, env.executeTrinoSql, DISTINCT, rows, stream, map, row.get, filter, schema.toLowerCase, startsWith, schema.equals, collect, toUnmodifiableList, schemas.forEach, cleanSchema], verbs [SELECT, SET].
- Audit status: `verified`

### `TestDeltaLakeDatabricksCleanUpGlueMetastore`


- Owning migration commit: `Migrate TestDeltaLakeDatabricksCleanUpGlueMetastore to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCleanUpGlueMetastore.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCleanUpGlueMetastore.java`
- Class-level environment requirement: `DeltaLakeDatabricksEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testCleanupOrphanedDatabases`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCleanUpGlueMetastore.java` ->
  `testCleanupOrphanedDatabases`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCleanUpGlueMetastore.java` ->
  `TestDeltaLakeDatabricksCleanUpGlueMetastore.testCleanupOrphanedDatabases`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `GlueClient.create`. Current setup shape: `env.createGlueClient`.
- Action parity: Legacy action shape: `currentTimeMillis`, `DAYS.toMillis`, `glueClient.getDatabasesPaginator`, `stream`, `map`, `flatMap`, `filter`, `isOrphanedTestDatabase`, `toList`, `log.info`, `orphanedDatabases.size`, `orphanedDatabases.forEach`, `builder.name`, `log.warn`. Current action shape: `currentTimeMillis`, `DAYS.toMillis`, `glueClient.getDatabasesPaginator`, `stream`, `map`, `flatMap`, `filter`, `isOrphanedTestDatabase`, `toList`, `log.info`, `orphanedDatabases.size`, `orphanedDatabases.forEach`, `builder.name`, `log.warn`.
- Assertion parity: Legacy assertion helpers: `orphanedDatabases.isEmpty`. Current assertion helpers: `orphanedDatabases.isEmpty`.
- Cleanup parity: Legacy cleanup shape: `glueClient.deleteDatabase`. Current cleanup shape: `glueClient.deleteDatabase`.
- Any observed difference, however small: helper calls differ: legacy [GlueClient.create, currentTimeMillis, DAYS.toMillis, glueClient.getDatabasesPaginator, stream, map, flatMap, filter, isOrphanedTestDatabase, toList, log.info, orphanedDatabases.size, orphanedDatabases.forEach, glueClient.deleteDatabase, builder.name, log.warn] vs current [env.createGlueClient, currentTimeMillis, DAYS.toMillis, glueClient.getDatabasesPaginator, stream, map, flatMap, filter, isOrphanedTestDatabase, toList, log.info, orphanedDatabases.size, orphanedDatabases.forEach, glueClient.deleteDatabase, builder.name, log.warn]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [GlueClient.create, currentTimeMillis, DAYS.toMillis, glueClient.getDatabasesPaginator, stream, map, flatMap, filter, isOrphanedTestDatabase, toList, log.info, orphanedDatabases.size, orphanedDatabases.forEach, glueClient.deleteDatabase, builder.name, log.warn], verbs [none]. Current flow summary -> helpers [env.createGlueClient, currentTimeMillis, DAYS.toMillis, glueClient.getDatabasesPaginator, stream, map, flatMap, filter, isOrphanedTestDatabase, toList, log.info, orphanedDatabases.size, orphanedDatabases.forEach, glueClient.deleteDatabase, builder.name, log.warn], verbs [none].
- Audit status: `verified`

### `TestDeltaLakeDatabricksCreateTableCompatibility`


- Owning migration commit: `Migrate TestDeltaLakeDatabricksCreateTableCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java`
- Class-level environment requirement: `DeltaLakeDatabricksEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `10`. Current methods: `10`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testDatabricksCanReadInitialCreateTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `testDatabricksCanReadInitialCreateTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `TestDeltaLakeDatabricksCreateTableCompatibility.testDatabricksCanReadInitialCreateTable`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `CREATE`, `testInsert`. Current setup shape: `CREATE`, `testInsert`.
- Action parity: Legacy action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `s`, `with`, `onDelta`, `count`, `getDatabricksDefaultTableProperties`, `ImmutableList.of`. Current action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `env.executeTrinoSql`, `format`, `s`, `with`, `env.getBucketName`, `env.executeDatabricksSql`, `count`, `getDatabricksDefaultTableProperties`, `ImmutableList.of`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `row`, `containsExactlyInOrder`. Current assertion helpers: `assertThat`, `contains`, `row`, `containsExactlyInOrder`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, format, s, with, onDelta, count, getDatabricksDefaultTableProperties, testInsert, ImmutableList.of, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeTrinoSql, format, s, with, env.getBucketName, env.executeDatabricksSql, count, getDatabricksDefaultTableProperties, testInsert, ImmutableList.of, dropDeltaTableWithRetry]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, format, s, with, onDelta, count, getDatabricksDefaultTableProperties, testInsert, ImmutableList.of, dropDeltaTableWithRetry], verbs [CREATE, SHOW, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoSql, format, s, with, env.getBucketName, env.executeDatabricksSql, count, getDatabricksDefaultTableProperties, testInsert, ImmutableList.of, dropDeltaTableWithRetry], verbs [CREATE, SHOW, SELECT].
- Audit status: `verified`

##### `testDatabricksCanReadInitialCreatePartitionedTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `testDatabricksCanReadInitialCreatePartitionedTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `TestDeltaLakeDatabricksCreateTableCompatibility.testDatabricksCanReadInitialCreatePartitionedTable`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `CREATE`, `testInsert`. Current setup shape: `CREATE`, `testInsert`.
- Action parity: Legacy action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `s`, `with`, `onDelta`, `count`, `BY`, `getDatabricksDefaultTableProperties`, `ImmutableList.of`. Current action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `env.executeTrinoSql`, `format`, `s`, `with`, `env.getBucketName`, `env.executeDatabricksSql`, `count`, `BY`, `getDatabricksDefaultTableProperties`, `ImmutableList.of`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `row`, `containsExactlyInOrder`. Current assertion helpers: `assertThat`, `contains`, `row`, `containsExactlyInOrder`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, format, s, with, onDelta, count, BY, getDatabricksDefaultTableProperties, testInsert, ImmutableList.of, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeTrinoSql, format, s, with, env.getBucketName, env.executeDatabricksSql, count, BY, getDatabricksDefaultTableProperties, testInsert, ImmutableList.of, dropDeltaTableWithRetry]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, format, s, with, onDelta, count, BY, getDatabricksDefaultTableProperties, testInsert, ImmutableList.of, dropDeltaTableWithRetry], verbs [CREATE, SHOW, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoSql, format, s, with, env.getBucketName, env.executeDatabricksSql, count, BY, getDatabricksDefaultTableProperties, testInsert, ImmutableList.of, dropDeltaTableWithRetry], verbs [CREATE, SHOW, SELECT].
- Audit status: `verified`

##### `testDatabricksCanReadInitialCreateTableAs`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `testDatabricksCanReadInitialCreateTableAs`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `TestDeltaLakeDatabricksCreateTableCompatibility.testDatabricksCanReadInitialCreateTableAs`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `CREATE`, `testInsert`. Current setup shape: `CREATE`, `testInsert`.
- Action parity: Legacy action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `s`, `with`, `VALUES`, `onDelta`, `count`, `getDatabricksDefaultTableProperties`, `ImmutableList.of`. Current action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `env.executeTrinoSql`, `format`, `s`, `with`, `VALUES`, `env.getBucketName`, `env.executeDatabricksSql`, `count`, `getDatabricksDefaultTableProperties`, `ImmutableList.of`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `row`, `containsExactlyInOrder`. Current assertion helpers: `assertThat`, `contains`, `row`, `containsExactlyInOrder`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, format, s, with, VALUES, onDelta, count, getDatabricksDefaultTableProperties, testInsert, ImmutableList.of, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeTrinoSql, format, s, with, VALUES, env.getBucketName, env.executeDatabricksSql, count, getDatabricksDefaultTableProperties, testInsert, ImmutableList.of, dropDeltaTableWithRetry]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, format, s, with, VALUES, onDelta, count, getDatabricksDefaultTableProperties, testInsert, ImmutableList.of, dropDeltaTableWithRetry], verbs [CREATE, SHOW, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoSql, format, s, with, VALUES, env.getBucketName, env.executeDatabricksSql, count, getDatabricksDefaultTableProperties, testInsert, ImmutableList.of, dropDeltaTableWithRetry], verbs [CREATE, SHOW, SELECT].
- Audit status: `verified`

##### `testDatabricksCanReadInitialCreatePartitionedTableAs`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `testDatabricksCanReadInitialCreatePartitionedTableAs`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `TestDeltaLakeDatabricksCreateTableCompatibility.testDatabricksCanReadInitialCreatePartitionedTableAs`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `CREATE`, `testInsert`. Current setup shape: `CREATE`, `testInsert`.
- Action parity: Legacy action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `s`, `with`, `VALUES`, `onDelta`, `count`, `BY`, `getDatabricksDefaultTableProperties`, `ImmutableList.of`. Current action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `env.executeTrinoSql`, `format`, `s`, `with`, `VALUES`, `env.getBucketName`, `env.executeDatabricksSql`, `count`, `BY`, `getDatabricksDefaultTableProperties`, `ImmutableList.of`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `row`, `containsExactlyInOrder`. Current assertion helpers: `assertThat`, `contains`, `row`, `containsExactlyInOrder`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, format, s, with, VALUES, onDelta, count, BY, getDatabricksDefaultTableProperties, testInsert, ImmutableList.of, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeTrinoSql, format, s, with, VALUES, env.getBucketName, env.executeDatabricksSql, count, BY, getDatabricksDefaultTableProperties, testInsert, ImmutableList.of, dropDeltaTableWithRetry]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, format, s, with, VALUES, onDelta, count, BY, getDatabricksDefaultTableProperties, testInsert, ImmutableList.of, dropDeltaTableWithRetry], verbs [CREATE, SHOW, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoSql, format, s, with, VALUES, env.getBucketName, env.executeDatabricksSql, count, BY, getDatabricksDefaultTableProperties, testInsert, ImmutableList.of, dropDeltaTableWithRetry], verbs [CREATE, SHOW, SELECT].
- Audit status: `verified`

##### `testCreateTableWithTableComment`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `testCreateTableWithTableComment`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `TestDeltaLakeDatabricksCreateTableCompatibility.testCreateTableWithTableComment`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`. Current setup shape: `CREATE`, `COMMENT`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `s`, `WITH`, `getTableCommentOnTrino`, `getTableCommentOnDelta`. Current action shape: `randomNameSuffix`, `env.executeTrinoSql`, `format`, `s`, `WITH`, `env.getBucketName`, `getTableCommentOnTrino`, `getTableCommentOnDatabricks`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, format, s, WITH, getTableCommentOnTrino, getTableCommentOnDelta] vs current [randomNameSuffix, env.executeTrinoSql, format, s, WITH, env.getBucketName, getTableCommentOnTrino, getTableCommentOnDatabricks]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, format, s, WITH, getTableCommentOnTrino, getTableCommentOnDelta], verbs [CREATE, COMMENT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoSql, format, s, WITH, env.getBucketName, getTableCommentOnTrino, getTableCommentOnDatabricks], verbs [CREATE, COMMENT, DROP].
- Audit status: `verified`

##### `testCreateTableWithColumnCommentOnTrino`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `testCreateTableWithColumnCommentOnTrino`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `TestDeltaLakeDatabricksCreateTableCompatibility.testCreateTableWithColumnCommentOnTrino`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`, `ALTER`. Current setup shape: `CREATE`, `COMMENT`, `ALTER`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `s`, `WITH`, `getColumnCommentOnTrino`, `getColumnCommentOnDelta`. Current action shape: `randomNameSuffix`, `env.executeTrinoSql`, `format`, `s`, `WITH`, `env.getBucketName`, `getColumnCommentOnTrino`, `getColumnCommentOnDatabricks`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, format, s, WITH, getColumnCommentOnTrino, getColumnCommentOnDelta] vs current [randomNameSuffix, env.executeTrinoSql, format, s, WITH, env.getBucketName, getColumnCommentOnTrino, getColumnCommentOnDatabricks]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, format, s, WITH, getColumnCommentOnTrino, getColumnCommentOnDelta], verbs [CREATE, COMMENT, ALTER, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoSql, format, s, WITH, env.getBucketName, getColumnCommentOnTrino, getColumnCommentOnDatabricks], verbs [CREATE, COMMENT, ALTER, DROP].
- Audit status: `verified`

##### `testCreateTableWithColumnCommentOnDelta`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `testCreateTableWithColumnCommentOnDelta`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `TestDeltaLakeDatabricksCreateTableCompatibility.testCreateTableWithColumnCommentOnDelta`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`. Current setup shape: `CREATE`, `COMMENT`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onDelta`, `executeQuery`, `format`, `s`, `getColumnCommentOnTrino`. Current action shape: `randomNameSuffix`, `env.executeDatabricksSql`, `format`, `s`, `env.getBucketName`, `getColumnCommentOnTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, format, s, getColumnCommentOnTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeDatabricksSql, format, s, env.getBucketName, getColumnCommentOnTrino, dropDeltaTableWithRetry]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, format, s, getColumnCommentOnTrino, dropDeltaTableWithRetry], verbs [CREATE, COMMENT]. Current flow summary -> helpers [randomNameSuffix, env.executeDatabricksSql, format, s, env.getBucketName, getColumnCommentOnTrino, dropDeltaTableWithRetry], verbs [CREATE, COMMENT].
- Audit status: `verified`

##### `testCreateTableWithDuplicatedColumnNames`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `testCreateTableWithDuplicatedColumnNames`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `TestDeltaLakeDatabricksCreateTableCompatibility.testCreateTableWithDuplicatedColumnNames`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onDelta`, `executeQuery`. Current action shape: `randomNameSuffix`, `env.executeDatabricksSql`, `env.getBucketName`.
- Assertion parity: Legacy assertion helpers: `assertThatThrownBy`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`, `hasMessageMatching`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeDatabricksSql, env.getBucketName, dropDeltaTableWithRetry]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, dropDeltaTableWithRetry], verbs [CREATE]. Current flow summary -> helpers [randomNameSuffix, env.executeDatabricksSql, env.getBucketName, dropDeltaTableWithRetry], verbs [CREATE].
- Audit status: `verified`

##### `testCreateTableWithUnsupportedPartitionType`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `testCreateTableWithUnsupportedPartitionType`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `TestDeltaLakeDatabricksCreateTableCompatibility.testCreateTableWithUnsupportedPartitionType`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `CREATE`, `USE`. Current setup shape: `CREATE`, `USE`.
- Action parity: Legacy action shape: `randomNameSuffix`, `formatted`, `onTrino`, `executeQuery`, `ARRAY`, `WITH`, `MAP`, `ROW`, `onDelta`, `BY`. Current action shape: `randomNameSuffix`, `formatted`, `env.getBucketName`, `env.executeTrinoSql`, `ARRAY`, `WITH`, `MAP`, `ROW`, `env.executeDatabricksSql`, `BY`.
- Assertion parity: Legacy assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `hasMessageMatching`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `hasMessageMatching`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, formatted, onTrino, executeQuery, ARRAY, WITH, MAP, ROW, onDelta, BY, dropDeltaTableWithRetry] vs current [randomNameSuffix, formatted, env.getBucketName, env.executeTrinoSql, ARRAY, WITH, MAP, ROW, env.executeDatabricksSql, BY, dropDeltaTableWithRetry]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, formatted, onTrino, executeQuery, ARRAY, WITH, MAP, ROW, onDelta, BY, dropDeltaTableWithRetry], verbs [CREATE, USE]. Current flow summary -> helpers [randomNameSuffix, formatted, env.getBucketName, env.executeTrinoSql, ARRAY, WITH, MAP, ROW, env.executeDatabricksSql, BY, dropDeltaTableWithRetry], verbs [CREATE, USE].
- Audit status: `verified`

##### `testCreateTableWithAllPartitionColumns`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `testCreateTableWithAllPartitionColumns`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDatabricksCreateTableCompatibility.java` ->
  `TestDeltaLakeDatabricksCreateTableCompatibility.testCreateTableWithAllPartitionColumns`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `CREATE`, `USE`. Current setup shape: `CREATE`, `USE`.
- Action parity: Legacy action shape: `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `onDelta`, `BY`. Current action shape: `randomNameSuffix`, `env.executeTrinoSql`, `WITH`, `env.getBucketName`, `env.executeDatabricksSql`, `BY`.
- Assertion parity: Legacy assertion helpers: `assertThatThrownBy`, `hasMessageContaining`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, onDelta, BY, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeTrinoSql, WITH, env.getBucketName, env.executeDatabricksSql, BY, dropDeltaTableWithRetry]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, onDelta, BY, dropDeltaTableWithRetry], verbs [CREATE, USE]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoSql, WITH, env.getBucketName, env.executeDatabricksSql, BY, dropDeltaTableWithRetry], verbs [CREATE, USE].
- Audit status: `verified`

### `TestDeltaLakeIdentityColumnCompatibility`


- Owning migration commit: `Migrate TestDeltaLakeIdentityColumnCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeIdentityColumnCompatibility.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeIdentityColumnCompatibility.java`
- Class-level environment requirement: `DeltaLakeDatabricksEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `DeltaLakeExclude173`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `5`. Current methods: `5`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testIdentityColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeIdentityColumnCompatibility.java` ->
  `testIdentityColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeIdentityColumnCompatibility.java` ->
  `TestDeltaLakeIdentityColumnCompatibility.testIdentityColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133`; `SuiteDeltaLakeDatabricks173` excludes this class via `DeltaLakeExclude173`.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `DeltaLakeExclude173`, `ProfileSpecificTests`.
  Tag routing matches the shared Databricks suite selection in the 133 lane, while the 173 lane excludes this coverage via `DeltaLakeExclude173`.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`, `ALTER`, `INSERT`. Current setup shape: `CREATE`, `COMMENT`, `ALTER`, `INSERT`.
- Action parity: Legacy action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `format`, `s`, `onTrino`, `getColumnCommentOnDelta`, `getTableCommentOnDelta`, `getColumnNamesOnDelta`, `getOnlyValue`, `VALUES`. Current action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `env.executeDatabricksSql`, `format`, `s`, `env.getBucketName`, `env.executeTrinoSql`, `getColumnCommentOnDatabricks`, `getTableCommentOnDatabricks`, `getColumnNamesOnDatabricks`, `getOnlyValue`, `VALUES`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`, `containsExactly`, `contains`, `row`, `containsOnly`. Current assertion helpers: `assertThat`, `isEqualTo`, `containsExactly`, `contains`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, format, s, onTrino, getColumnCommentOnDelta, getTableCommentOnDelta, getColumnNamesOnDelta, getOnlyValue, VALUES, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeDatabricksSql, format, s, env.getBucketName, env.executeTrinoSql, getColumnCommentOnDatabricks, getTableCommentOnDatabricks, getColumnNamesOnDatabricks, getOnlyValue, VALUES, dropDeltaTableWithRetry]; assertion helpers differ: legacy [assertThat, isEqualTo, containsExactly, contains, row, containsOnly] vs current [assertThat, isEqualTo, containsExactly, contains, containsOnly, row]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, format, s, onTrino, getColumnCommentOnDelta, getTableCommentOnDelta, getColumnNamesOnDelta, getOnlyValue, VALUES, dropDeltaTableWithRetry], verbs [CREATE, COMMENT, ALTER, SHOW, INSERT, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeDatabricksSql, format, s, env.getBucketName, env.executeTrinoSql, getColumnCommentOnDatabricks, getTableCommentOnDatabricks, getColumnNamesOnDatabricks, getOnlyValue, VALUES, dropDeltaTableWithRetry], verbs [CREATE, COMMENT, ALTER, SHOW, INSERT, SELECT].
- Audit status: `verified`

##### `testRenameIdentityColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeIdentityColumnCompatibility.java` ->
  `testRenameIdentityColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeIdentityColumnCompatibility.java` ->
  `TestDeltaLakeIdentityColumnCompatibility.testRenameIdentityColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133`; `SuiteDeltaLakeDatabricks173` excludes this class via `DeltaLakeExclude173`.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `DeltaLakeExclude173`, `ProfileSpecificTests`.
  Tag routing matches the shared Databricks suite selection in the 133 lane, while the 173 lane excludes this coverage via `DeltaLakeExclude173`.
- Setup parity: Legacy setup shape: `CREATE`, `ALTER`, `INSERT`. Current setup shape: `CREATE`, `ALTER`, `INSERT`.
- Action parity: Legacy action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `getOnlyValue`, `onTrino`, `VALUES`. Current action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `env.executeDatabricksSql`, `env.getBucketName`, `TBLPROPERTIES`, `getOnlyValue`, `env.executeTrinoSql`, `VALUES`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `containsOnly`, `row`, `assertQueryFailure`, `hasMessageContaining`. Current assertion helpers: `assertThat`, `contains`, `containsOnly`, `row`, `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, getOnlyValue, onTrino, VALUES, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeDatabricksSql, env.getBucketName, TBLPROPERTIES, getOnlyValue, env.executeTrinoSql, VALUES, dropDeltaTableWithRetry]; assertion helpers differ: legacy [assertThat, contains, containsOnly, row, assertQueryFailure, hasMessageContaining] vs current [assertThat, contains, containsOnly, row, assertThatThrownBy, hasMessageContaining]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, getOnlyValue, onTrino, VALUES, dropDeltaTableWithRetry], verbs [CREATE, ALTER, SHOW, INSERT, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeDatabricksSql, env.getBucketName, TBLPROPERTIES, getOnlyValue, env.executeTrinoSql, VALUES, dropDeltaTableWithRetry], verbs [CREATE, ALTER, SHOW, INSERT, SELECT].
- Audit status: `verified`

##### `testDropIdentityColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeIdentityColumnCompatibility.java` ->
  `testDropIdentityColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeIdentityColumnCompatibility.java` ->
  `TestDeltaLakeIdentityColumnCompatibility.testDropIdentityColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133`; `SuiteDeltaLakeDatabricks173` excludes this class via `DeltaLakeExclude173`.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `DeltaLakeExclude173`, `ProfileSpecificTests`.
  Tag routing matches the shared Databricks suite selection in the 133 lane, while the 173 lane excludes this coverage via `DeltaLakeExclude173`.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `ALTER`. Current setup shape: `CREATE`, `INSERT`, `ALTER`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `onTrino`, `VALUES`, `getColumnNamesOnDelta`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeDatabricksSql`, `env.getBucketName`, `TBLPROPERTIES`, `env.executeTrinoSql`, `VALUES`, `getColumnNamesOnDatabricks`.
- Assertion parity: Legacy assertion helpers: `assertQueryFailure`, `hasMessageContaining`, `assertThat`, `containsExactly`, `containsOnly`, `row`. Current assertion helpers: `assertThatThrownBy`, `hasMessageContaining`, `assertThat`, `containsExactly`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `dropDeltaTableWithRetry`. Current cleanup shape: `DROP`, `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino, VALUES, getColumnNamesOnDelta, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeDatabricksSql, env.getBucketName, TBLPROPERTIES, env.executeTrinoSql, VALUES, getColumnNamesOnDatabricks, dropDeltaTableWithRetry]; assertion helpers differ: legacy [assertQueryFailure, hasMessageContaining, assertThat, containsExactly, containsOnly, row] vs current [assertThatThrownBy, hasMessageContaining, assertThat, containsExactly, containsOnly, row]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino, VALUES, getColumnNamesOnDelta, dropDeltaTableWithRetry], verbs [CREATE, INSERT, ALTER, DROP, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeDatabricksSql, env.getBucketName, TBLPROPERTIES, env.executeTrinoSql, VALUES, getColumnNamesOnDatabricks, dropDeltaTableWithRetry], verbs [CREATE, INSERT, ALTER, DROP, SELECT].
- Audit status: `verified`

##### `testVacuumProcedureWithIdentityColumn`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeIdentityColumnCompatibility.java` ->
  `testVacuumProcedureWithIdentityColumn`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeIdentityColumnCompatibility.java` ->
  `TestDeltaLakeIdentityColumnCompatibility.testVacuumProcedureWithIdentityColumn`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133`; `SuiteDeltaLakeDatabricks173` excludes this class via `DeltaLakeExclude173`.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `DeltaLakeExclude173`, `ProfileSpecificTests`.
  Tag routing matches the shared Databricks suite selection in the 133 lane, while the 173 lane excludes this coverage via `DeltaLakeExclude173`.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SET`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `CALL`, `VACUUM`, `SHOW`, `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `onTrino`, `system.vacuum`, `getOnlyValue`. Current action shape: `SHOW`, `SELECT`, `randomNameSuffix`, `env.executeDatabricksSql`, `env.getBucketName`, `executeTrinoVacuumWithMinRetention`, `getOnlyValue`, `env.executeTrinoSql`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `contains`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DELETE`, `dropDeltaTableWithRetry`. Current cleanup shape: `DELETE`, `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, onTrino, system.vacuum, getOnlyValue, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeDatabricksSql, env.getBucketName, executeTrinoVacuumWithMinRetention, getOnlyValue, env.executeTrinoSql, dropDeltaTableWithRetry]; SQL verbs differ: legacy [CREATE, INSERT, DELETE, SET, CALL, VACUUM, SHOW, SELECT] vs current [CREATE, INSERT, DELETE, SHOW, SELECT]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, onTrino, system.vacuum, getOnlyValue, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SET, CALL, VACUUM, SHOW, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeDatabricksSql, env.getBucketName, executeTrinoVacuumWithMinRetention, getOnlyValue, env.executeTrinoSql, dropDeltaTableWithRetry], verbs [CREATE, INSERT, DELETE, SHOW, SELECT].
- Audit status: `verified`

##### `testIdentityColumnCheckpointInterval`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeIdentityColumnCompatibility.java` ->
  `testIdentityColumnCheckpointInterval`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeIdentityColumnCompatibility.java` ->
  `TestDeltaLakeIdentityColumnCompatibility.testIdentityColumnCheckpointInterval`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133`; `SuiteDeltaLakeDatabricks173` excludes this class via `DeltaLakeExclude173`.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `DeltaLakeExclude173`, `ProfileSpecificTests`.
  Tag routing matches the shared Databricks suite selection in the 133 lane, while the 173 lane excludes this coverage via `DeltaLakeExclude173`.
- Setup parity: Legacy setup shape: `CREATE`, `COMMENT`. Current setup shape: `CREATE`, `COMMENT`.
- Action parity: Legacy action shape: `SHOW`, `randomNameSuffix`, `onDelta`, `executeQuery`, `TBLPROPERTIES`, `onTrino`, `getOnlyValue`. Current action shape: `SHOW`, `randomNameSuffix`, `env.executeDatabricksSql`, `env.getBucketName`, `TBLPROPERTIES`, `env.executeTrinoSql`, `getOnlyValue`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino, getOnlyValue, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeDatabricksSql, env.getBucketName, TBLPROPERTIES, env.executeTrinoSql, getOnlyValue, dropDeltaTableWithRetry]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, TBLPROPERTIES, onTrino, getOnlyValue, dropDeltaTableWithRetry], verbs [CREATE, COMMENT, SHOW]. Current flow summary -> helpers [randomNameSuffix, env.executeDatabricksSql, env.getBucketName, TBLPROPERTIES, env.executeTrinoSql, getOnlyValue, dropDeltaTableWithRetry], verbs [CREATE, COMMENT, SHOW].
- Audit status: `verified`

### `TestDeltaLakeUpdateCompatibility`


- Owning migration commit: `Migrate TestDeltaLakeUpdateCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeUpdateCompatibility.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeUpdateCompatibility.java`
- Class-level environment requirement: `DeltaLakeDatabricksEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testUpdatesFromDatabricks`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeUpdateCompatibility.java` ->
  `testUpdatesFromDatabricks`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeUpdateCompatibility.java` ->
  `TestDeltaLakeUpdateCompatibility.testUpdatesFromDatabricks`
- Mapping type: `direct`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment`. Routed by source review into `SuiteDeltaLakeDatabricks133` and `SuiteDeltaLakeDatabricks173`, the suites that include the shared `DeltaLakeDatabricks` tag.
- Tag parity: Current tags: `ConfiguredFeatures`, `DeltaLakeDatabricks`, `ProfileSpecificTests`. Tag routing needs no
  suite-level product-test claim.
- Setup parity: Legacy setup shape: `CREATE`, `SET`. Current setup shape: `CREATE`, `SET`.
- Action parity: Legacy action shape: `SELECT`, `UPDATE`, `randomNameSuffix`, `onTrino`, `executeQuery`, `WITH`, `onDelta`, `format`, `toRows`. Current action shape: `SELECT`, `UPDATE`, `randomNameSuffix`, `env.executeTrinoSql`, `WITH`, `env.getBucketName`, `env.executeDatabricksSql`, `format`, `toRows`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `containsExactlyInOrder`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `containsExactlyInOrder`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, WITH, onDelta, format, toRows, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeTrinoSql, WITH, env.getBucketName, env.executeDatabricksSql, format, toRows, dropDeltaTableWithRetry]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, WITH, onDelta, format, toRows, dropDeltaTableWithRetry], verbs [CREATE, SELECT, UPDATE, SET]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoSql, WITH, env.getBucketName, env.executeDatabricksSql, format, toRows, dropDeltaTableWithRetry], verbs [CREATE, SELECT, UPDATE, SET].
- Audit status: `verified`


## Additional Split Databricks Coverage Classes


### `TestDeltaLakeCreateTableAsSelectCompatibilityDatabricks`

- Owning migration commit: `Migrate TestDeltaLakeCreateTableAsSelectCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateTableAsSelectCompatibilityDatabricks.java`
- Legacy class removed in same migration commit:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateTableAsSelectCompatibility.java` (shared legacy Delta class split between the Delta Lake and Databricks lanes).
- Class-level environment requirement: `DeltaLakeDatabricksEnvironment`.
- Class-level tags: `ConfiguredFeatures, DeltaLakeDatabricks, ProfileSpecificTests, DeltaLakeDatabricks143, DeltaLakeDatabricks154, DeltaLakeDatabricks164`.
- Method inventory complete: Yes. Legacy methods represented in this split class: `2`. Current methods: `2`.
- Legacy helper/resource dependencies accounted for: legacy shared class source reviewed directly; this class carries the Databricks-specific split coverage.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testTrinoTypesWithDatabricks`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateTableAsSelectCompatibility.java` ->
  `testTrinoTypesWithDatabricks`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateTableAsSelectCompatibilityDatabricks.java` ->
  `TestDeltaLakeCreateTableAsSelectCompatibilityDatabricks.testTrinoTypesWithDatabricks`
- Mapping type: `split from shared legacy Delta class`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment` and preserves the Databricks-backed execution path for this method. Routed by current suite selection into `SuiteDeltaLakeDatabricks133`, `SuiteDeltaLakeDatabricks143`, `SuiteDeltaLakeDatabricks154`, `SuiteDeltaLakeDatabricks164`, `SuiteDeltaLakeDatabricks173`.
- Tag parity: Current tags `ConfiguredFeatures, DeltaLakeDatabricks, ProfileSpecificTests, DeltaLakeDatabricks143, DeltaLakeDatabricks154, DeltaLakeDatabricks164` preserve the Databricks suite routing for this split method set.
- Setup parity: Legacy setup shape: `CREATE`, `VALUES`. Current setup shape: `CREATE`, `VALUES`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `row`, `onDelta`, `format`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoSql`, `row`, `env.getBucketName`, `env.executeDatabricksSql`, `format`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `rows`, `stream`, `map`, `collect`, `toImmutableList`. Current assertion helpers: `assertThat`, `containsOnly`, `getRows`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, row, onDelta, format, rows, stream, map, collect, toImmutableList, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeTrinoSql, row, env.getBucketName, env.executeDatabricksSql, format, getRows, dropDeltaTableWithRetry]; the current JUnit tree also places this legacy method in a Databricks-specific split class rather than keeping it in the shared Delta OSS class.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, row, onDelta, format, rows, stream, map, collect, toImmutableList, dropDeltaTableWithRetry], verbs [CREATE, SELECT, VALUES]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoSql, row, env.getBucketName, env.executeDatabricksSql, format, getRows, dropDeltaTableWithRetry], verbs [CREATE, SELECT, VALUES]. Routed by current suite selection into `SuiteDeltaLakeDatabricks133`, `SuiteDeltaLakeDatabricks143`, `SuiteDeltaLakeDatabricks154`, `SuiteDeltaLakeDatabricks164`, `SuiteDeltaLakeDatabricks173`.
- Audit status: `verified`

##### `testTrinoTimestampsWithDatabricks`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateTableAsSelectCompatibility.java` ->
  `testTrinoTimestampsWithDatabricks`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeCreateTableAsSelectCompatibilityDatabricks.java` ->
  `TestDeltaLakeCreateTableAsSelectCompatibilityDatabricks.testTrinoTimestampsWithDatabricks`
- Mapping type: `split from shared legacy Delta class`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment` and preserves the Databricks-backed execution path for this method. Routed by current suite selection into `SuiteDeltaLakeDatabricks133`, `SuiteDeltaLakeDatabricks173`.
- Tag parity: Current tags `ConfiguredFeatures, DeltaLakeDatabricks, ProfileSpecificTests, DeltaLakeDatabricks143, DeltaLakeDatabricks154, DeltaLakeDatabricks164` preserve the Databricks suite routing for this split method set.
- Setup parity: Legacy setup shape: `CREATE`, `VALUES`. Current setup shape: `CREATE`, `VALUES`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `row`, `onDelta`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrinoSql`, `row`, `env.getBucketName`, `env.executeDatabricksSql`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `rows`, `stream`, `map`, `collect`, `toImmutableList`. Current assertion helpers: `assertThat`, `containsOnly`, `getRows`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, row, onDelta, rows, stream, map, collect, toImmutableList, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeTrinoSql, row, env.getBucketName, env.executeDatabricksSql, getRows, dropDeltaTableWithRetry]; the current JUnit tree also places this legacy method in a Databricks-specific split class rather than keeping it in the shared Delta OSS class.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, row, onDelta, rows, stream, map, collect, toImmutableList, dropDeltaTableWithRetry], verbs [CREATE, SELECT, VALUES]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoSql, row, env.getBucketName, env.executeDatabricksSql, getRows, dropDeltaTableWithRetry], verbs [CREATE, SELECT, VALUES]. Routed by current suite selection into `SuiteDeltaLakeDatabricks133`, `SuiteDeltaLakeDatabricks173`.
- Audit status: `verified`


### `TestDeltaLakeDeleteCompatibilityDatabricks`

- Owning migration commit: `Migrate TestDeltaLakeDeleteCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibilityDatabricks.java`
- Legacy class removed in same migration commit:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` (shared legacy Delta class split between the Delta Lake and Databricks lanes).
- Class-level environment requirement: `DeltaLakeDatabricksEnvironment`.
- Class-level tags: `ConfiguredFeatures, DeltaLakeDatabricks, ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods represented in this split class: `2`. Current methods: `2`.
- Legacy helper/resource dependencies accounted for: legacy shared class source reviewed directly; this class carries the Databricks-specific split coverage.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testTruncateTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `testTruncateTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibilityDatabricks.java` ->
  `TestDeltaLakeDeleteCompatibilityDatabricks.testTruncateTable`
- Mapping type: `split from shared legacy Delta class`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment` and preserves the Databricks-backed execution path for this method. Routed by current suite selection into `SuiteDeltaLakeDatabricks133`, `SuiteDeltaLakeDatabricks173`.
- Tag parity: Current tags `ConfiguredFeatures, DeltaLakeDatabricks, ProfileSpecificTests` preserve the Databricks suite routing for this split method set.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `TRUNCATE`, `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `onDelta`. Current action shape: `TRUNCATE`, `SELECT`, `randomNameSuffix`, `env.executeTrinoSql`, `env.getBucketName`, `env.executeDatabricksSql`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `hasNoRows`. Current assertion helpers: `assertThat`, `hasNoRows`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, onDelta] vs current [randomNameSuffix, env.executeTrinoSql, env.getBucketName, env.executeDatabricksSql]; the current JUnit tree also places this legacy method in a Databricks-specific split class rather than keeping it in the shared Delta OSS class.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, onDelta], verbs [CREATE, INSERT, TRUNCATE, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoSql, env.getBucketName, env.executeDatabricksSql], verbs [CREATE, INSERT, TRUNCATE, SELECT, DROP]. Routed by current suite selection into `SuiteDeltaLakeDatabricks133`, `SuiteDeltaLakeDatabricks173`.
- Audit status: `verified`

##### `testDeletionVectorsTruncateTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibility.java` ->
  `testDeletionVectorsTruncateTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeDeleteCompatibilityDatabricks.java` ->
  `TestDeltaLakeDeleteCompatibilityDatabricks.testDeletionVectorsTruncateTable`
- Mapping type: `split from shared legacy Delta class`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment` and preserves the Databricks-backed execution path for this method. Routed by current suite selection into `SuiteDeltaLakeDatabricks133`, `SuiteDeltaLakeDatabricks173`.
- Tag parity: Current tags `ConfiguredFeatures, DeltaLakeDatabricks, ProfileSpecificTests` preserve the Databricks suite routing for this split method set.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `SELECT`. Current setup shape: `CREATE`, `INSERT`, `SELECT`.
- Action parity: Legacy action shape: `TRUNCATE`, `SELECT`, `randomNameSuffix`, `testDeletionVectorsDeleteAll`, `onDelta`, `executeQuery`, `deleteRow.accept`, `onTrino`. Current action shape: `TRUNCATE`, `SELECT`, `randomNameSuffix`, `testDeletionVectorsDeleteAll`, `env.executeDatabricksSql`, `deleteRow.accept`, `env.executeTrinoSql`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `hasRowsCount`, `hasNoRows`. Current assertion helpers: `assertThat`, `hasRowsCount`, `hasNoRows`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, testDeletionVectorsDeleteAll, onDelta, executeQuery, deleteRow.accept, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, testDeletionVectorsDeleteAll, env.executeDatabricksSql, deleteRow.accept, env.executeTrinoSql, dropDeltaTableWithRetry]; the current JUnit tree also places this legacy method in a Databricks-specific split class rather than keeping it in the shared Delta OSS class.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, testDeletionVectorsDeleteAll, onDelta, executeQuery, deleteRow.accept, onTrino, dropDeltaTableWithRetry], verbs [CREATE, INSERT, TRUNCATE, SELECT]. Current flow summary -> helpers [randomNameSuffix, testDeletionVectorsDeleteAll, env.executeDatabricksSql, deleteRow.accept, env.executeTrinoSql, dropDeltaTableWithRetry], verbs [CREATE, INSERT, TRUNCATE, SELECT]. Routed by current suite selection into `SuiteDeltaLakeDatabricks133`, `SuiteDeltaLakeDatabricks173`.
- Audit status: `verified`


### `TestDeltaLakeInsertCompatibilityDatabricks`

- Owning migration commit: `Migrate TestDeltaLakeInsertCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibilityDatabricks.java`
- Legacy class removed in same migration commit:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` (shared legacy Delta class split between the Delta Lake and Databricks lanes).
- Class-level environment requirement: `DeltaLakeDatabricksEnvironment`.
- Class-level tags: `ConfiguredFeatures, DeltaLakeDatabricks, ProfileSpecificTests, DeltaLakeDatabricks143, DeltaLakeDatabricks154, DeltaLakeDatabricks164`.
- Method inventory complete: Yes. Legacy methods represented in this split class: `3`. Current methods: `3`.
- Legacy helper/resource dependencies accounted for: legacy shared class source reviewed directly; this class carries the Databricks-specific split coverage.
- Intentional differences summary: `HDP to Hive 3.1 migration`
- Method statuses present: `verified`.

#### Methods

##### `testPartitionedInsertCompatibility`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `testPartitionedInsertCompatibility`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibilityDatabricks.java` ->
  `TestDeltaLakeInsertCompatibilityDatabricks.testPartitionedInsertCompatibility`
- Mapping type: `split from shared legacy Delta class`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment` and preserves the Databricks-backed execution path for this method. Routed by current suite selection into `SuiteDeltaLakeDatabricks133`, `SuiteDeltaLakeDatabricks143`, `SuiteDeltaLakeDatabricks154`, `SuiteDeltaLakeDatabricks164`, `SuiteDeltaLakeDatabricks173`.
- Tag parity: Current tags `ConfiguredFeatures, DeltaLakeDatabricks, ProfileSpecificTests, DeltaLakeDatabricks143, DeltaLakeDatabricks154, DeltaLakeDatabricks164` preserve the Databricks suite routing for this split method set.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onDelta`, `executeQuery`, `onTrino`, `column`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeDatabricksSql`, `env.getBucketName`, `env.executeTrinoSql`, `column`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `hasSize`, `allMatch`. Current assertion helpers: `assertThat`, `containsOnly`, `hasSize`, `allMatch`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, onTrino, column, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeDatabricksSql, env.getBucketName, env.executeTrinoSql, column, dropDeltaTableWithRetry]; the current JUnit tree also places this legacy method in a Databricks-specific split class rather than keeping it in the shared Delta OSS class.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, onTrino, column, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT]. Current flow summary -> helpers [randomNameSuffix, env.executeDatabricksSql, env.getBucketName, env.executeTrinoSql, column, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT]. Routed by current suite selection into `SuiteDeltaLakeDatabricks133`, `SuiteDeltaLakeDatabricks143`, `SuiteDeltaLakeDatabricks154`, `SuiteDeltaLakeDatabricks164`, `SuiteDeltaLakeDatabricks173`.
- Audit status: `verified`

##### `testCompression`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `testCompression`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibilityDatabricks.java` ->
  `TestDeltaLakeInsertCompatibilityDatabricks.testCompression`
- Mapping type: `split from shared legacy Delta class`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment` and preserves the Databricks-backed execution path for this method. Routed by current suite selection into `SuiteDeltaLakeDatabricks133`, `SuiteDeltaLakeDatabricks173`.
- Tag parity: Current tags `ConfiguredFeatures, DeltaLakeDatabricks, ProfileSpecificTests, DeltaLakeDatabricks143, DeltaLakeDatabricks154, DeltaLakeDatabricks164` preserve the Databricks suite routing for this split method set.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SET`, `INSERT`, `SELECT`, `TABLE`, `randomNameSuffix`, `onTrino`, `executeQuery`, `rows`, `stream`, `map`, `row`, `collect`, `toImmutableList`, `onDelta`. Current action shape: `SET`, `INSERT`, `SELECT`, `TABLE`, `randomNameSuffix`, `env.executeTrinoSql`, `env.getBucketName`, `getRows`, `env.executeDatabricksSql`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThat`, `containsOnly`, `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, rows, stream, map, row, collect, toImmutableList, onDelta] vs current [randomNameSuffix, env.executeTrinoSql, env.getBucketName, getRows, env.executeDatabricksSql]; assertion helper/message matching also differs because the current JUnit rewrite uses `assertThatThrownBy(...).hasMessageContaining("Unsupported codec: LZ4")` instead of the legacy regex-based `assertQueryFailure(...).hasMessageMatching(...)`; the current JUnit tree also places this legacy method in a Databricks-specific split class rather than keeping it in the shared Delta OSS class.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, rows, stream, map, row, collect, toImmutableList, onDelta], verbs [CREATE, SET, INSERT, SELECT, TABLE, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoSql, env.getBucketName, getRows, env.executeDatabricksSql], verbs [CREATE, SET, INSERT, SELECT, TABLE, DROP]. Routed by current suite selection into `SuiteDeltaLakeDatabricks133`, `SuiteDeltaLakeDatabricks173`.
- Audit status: `verified`

##### `testWritesToTableWithGeneratedColumnFails`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibility.java` ->
  `testWritesToTableWithGeneratedColumnFails`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/deltalake/TestDeltaLakeInsertCompatibilityDatabricks.java` ->
  `TestDeltaLakeInsertCompatibilityDatabricks.testWritesToTableWithGeneratedColumnFails`
- Mapping type: `split from shared legacy Delta class`
- Environment parity: Current class requires `DeltaLakeDatabricksEnvironment` and preserves the Databricks-backed execution path for this method. Routed by current suite selection into `SuiteDeltaLakeDatabricks133`, `SuiteDeltaLakeDatabricks173`.
- Tag parity: Current tags `ConfiguredFeatures, DeltaLakeDatabricks, ProfileSpecificTests, DeltaLakeDatabricks143, DeltaLakeDatabricks154, DeltaLakeDatabricks164` preserve the Databricks suite routing for this split method set.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `randomNameSuffix`, `onDelta`, `executeQuery`, `onTrino`. Current action shape: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `randomNameSuffix`, `env.executeDatabricksSql`, `env.getBucketName`, `env.executeTrinoSql`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `assertQueryFailure`, `hasMessageContaining`. Current assertion helpers: `assertThat`, `containsOnly`, `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `dropDeltaTableWithRetry`. Current cleanup shape: `dropDeltaTableWithRetry`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onDelta, executeQuery, onTrino, dropDeltaTableWithRetry] vs current [randomNameSuffix, env.executeDatabricksSql, env.getBucketName, env.executeTrinoSql, dropDeltaTableWithRetry]; assertion helpers differ because the current JUnit rewrite uses `assertThatThrownBy` instead of legacy `assertQueryFailure`; the current JUnit tree also places this legacy method in a Databricks-specific split class rather than keeping it in the shared Delta OSS class.
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onDelta, executeQuery, onTrino, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT, UPDATE, DELETE, MERGE]. Current flow summary -> helpers [randomNameSuffix, env.executeDatabricksSql, env.getBucketName, env.executeTrinoSql, dropDeltaTableWithRetry], verbs [CREATE, INSERT, SELECT, UPDATE, DELETE, MERGE]. Routed by current suite selection into `SuiteDeltaLakeDatabricks133`, `SuiteDeltaLakeDatabricks173`.
- Audit status: `verified`
