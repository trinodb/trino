# Lane Audit: Hudi

## Lane Summary

- Audit status: `complete`
- Section start commit: `Add Hudi environment`
- Section end commit: `Remove legacy SuiteHudi`
- Introduced JUnit suites: `SuiteHudi`.
- Extended existing suites: none.
- Retired legacy suites: `SuiteHudi`.
- Environment classes introduced: none.
- Method status counts: verified `11`, intentional difference `6`, needs follow-up `0`.

## Semantic Audit Status

- Manual review note: the legacy Hudi lane was re-read from `SuiteHudi`, `EnvSinglenodeHudi`, `EnvSinglenodeHiveHudiRedirections`, `TestHudiSparkCompatibility`, and `TestHiveRedirectionToHudi`, then compared against the current JUnit/Testcontainers sources in this lane.

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: `HDP to Hive 3.1 migration`.
- Needs-follow-up methods: none identified in the manual source-body pass for this lane.

## Environment Semantic Audit
### `HudiEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of legacy `EnvSinglenodeHudi` against current `HudiEnvironment`.
- Container/service inventory parity: preserved in intent. Both provide Hadoop/Hive metastore, Spark with Hudi support, and Trino with `hudi` plus `hive` catalogs against the same metastore.
- Config/resource wiring parity: preserved in intent. Legacy launcher assembled the environment through container extenders and mounted files; current environment programs the same container roles and catalog settings directly in code.
- Startup dependency parity: preserved. Hadoop starts first, Spark depends on Hadoop, and Trino starts after both.
- Catalog/session parity: preserved. Both expose `hudi` as the primary Hudi connector and `hive` for cross-engine visibility/redirection checks.
- Recorded differences: outside-Docker execution, JUnit/Testcontainers framework replacement, and code-driven configuration instead of launcher-time file assembly.
- Reviewer note: no Hudi-environment fidelity gap is currently identified.

### `HiveHudiRedirectionsEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: manual comparison of legacy `EnvSinglenodeHiveHudiRedirections` against current `HiveHudiRedirectionsEnvironment`.
- Container/service inventory parity: preserved in intent. Both provide Hive, Hudi-capable Spark, Trino, and object-storage-backed warehouses for Hive-to-Hudi redirection tests.
- Redirection parity: preserved. The current `hive` catalog still points at the `hudi` catalog via `hive.hudi-catalog-name=hudi`, matching the legacy redirection contract.
- Storage/runtime differences: current environment uses direct Testcontainers-managed MinIO/Hive4/SparkHudi containers instead of the launcher singlenode environment assembly.
- Recorded differences: outside-Docker execution, JUnit/Testcontainers framework replacement, and MinIO/Hive4 container wiring expressed directly in code.
- Reviewer note: no Hudi-redirection environment fidelity gap is currently identified.

## Suite Semantic Audit
### `SuiteHudi`
- Suite semantic audit status: `complete`
- CI bucket: `hive-kerberos`
- Relationship to lane: `owned by this lane`.
- Legacy/current basis: manual comparison of legacy launcher `SuiteHudi` and current `SuiteHudi`.
- Environment-run parity: preserved. Legacy launcher had two runs, one for `HUDI` and one for `HIVE_HUDI_REDIRECTIONS`; current suite keeps the same two runs on the extracted environments.
- Tag parity: preserved. The same `Hudi` and `HiveHudiRedirections` slices are executed.
- Coverage-intent parity: preserved. Current suite still separates Spark/Hudi interoperability from Hive-to-Hudi redirection behavior exactly as the legacy suite did.
- Recorded differences: current execution is direct JUnit suite execution instead of launcher-driven Docker-side execution.
- Reviewer note: no Hudi suite-level fidelity gap is currently identified.

## Ported Test Classes

### `TestHudiSparkCompatibility`


- Owning migration commit: `Migrate TestHudiSparkCompatibility to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java`
- Class-level environment requirement: `HudiEnvironment`.
- Class-level tags: `Hudi`.
- Method inventory complete: Yes. Legacy methods: `11`. Current methods: `11`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `HDP to Hive 3.1 migration`. The current class remains a direct behavioral port of the legacy Hudi/Spark compatibility class, with helper calls routed through `HudiEnvironment` instead of Tempto executors.
- Method statuses present: `verified`.

#### Methods

##### `testCopyOnWriteShowCreateTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `testCopyOnWriteShowCreateTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `TestHudiSparkCompatibility.testCopyOnWriteShowCreateTable`
- Mapping type: `direct`
- Environment parity: Current class requires `HudiEnvironment`. Routed by source review into `SuiteHudi` run 1.
- Tag parity: Current tags: `Hudi`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `createNonPartitionedTable`. Current setup shape: `CREATE`, `createNonPartitionedTable`.
- Action parity: Legacy action shape: `SHOW`, `randomNameSuffix`, `onTrino`, `executeQuery`, `getOnlyValue`, `format`, `s`, `WITH`, `onHudi`, `project`, `TBLPROPERTIES`. Current action shape: `SHOW`, `randomNameSuffix`, `env.getWarehouseDirectory`, `env.executeTrino`, `getOnlyValue`, `env.executeSpark`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `contains`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createNonPartitionedTable, onTrino, executeQuery, getOnlyValue, format, s, WITH, onHudi, project, TBLPROPERTIES] vs current [randomNameSuffix, env.getWarehouseDirectory, createNonPartitionedTable, env.executeTrino, getOnlyValue, env.executeSpark, env.executeSparkUpdate]; assertion helpers differ: legacy [assertThat, isEqualTo] vs current [assertThat, contains]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createNonPartitionedTable, onTrino, executeQuery, getOnlyValue, format, s, WITH, onHudi, project, TBLPROPERTIES], verbs [CREATE, SHOW, DROP]. Current flow summary -> helpers [randomNameSuffix, env.getWarehouseDirectory, createNonPartitionedTable, env.executeTrino, getOnlyValue, env.executeSpark, env.executeSparkUpdate], verbs [CREATE, SHOW, DROP].
- Audit status: `verified`

##### `testCopyOnWriteTableSelect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `testCopyOnWriteTableSelect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `TestHudiSparkCompatibility.testCopyOnWriteTableSelect`
- Mapping type: `direct`
- Environment parity: Current class requires `HudiEnvironment`. Routed by source review into `SuiteHudi` run 1.
- Tag parity: Current tags: `Hudi`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createNonPartitionedTable`. Current setup shape: `createNonPartitionedTable`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `ImmutableList.of`, `onHudi`, `executeQuery`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `List.of`, `env.executeSpark`, `env.executeTrino`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createNonPartitionedTable, ImmutableList.of, onHudi, executeQuery, onTrino] vs current [randomNameSuffix, createNonPartitionedTable, List.of, env.executeSpark, env.executeTrino, env.executeSparkUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createNonPartitionedTable, ImmutableList.of, onHudi, executeQuery, onTrino], verbs [SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, createNonPartitionedTable, List.of, env.executeSpark, env.executeTrino, env.executeSparkUpdate], verbs [SELECT, DROP].
- Audit status: `verified`

##### `testCopyOnWritePartitionedTableSelect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `testCopyOnWritePartitionedTableSelect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `TestHudiSparkCompatibility.testCopyOnWritePartitionedTableSelect`
- Mapping type: `direct`
- Environment parity: Current class requires `HudiEnvironment`. Routed by source review into `SuiteHudi` run 1.
- Tag parity: Current tags: `Hudi`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createPartitionedTable`. Current setup shape: `createPartitionedTable`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `ImmutableList.of`, `onHudi`, `executeQuery`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `List.of`, `env.executeSpark`, `env.executeTrino`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createPartitionedTable, ImmutableList.of, onHudi, executeQuery, onTrino] vs current [randomNameSuffix, createPartitionedTable, List.of, env.executeSpark, env.executeTrino, env.executeSparkUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createPartitionedTable, ImmutableList.of, onHudi, executeQuery, onTrino], verbs [SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, createPartitionedTable, List.of, env.executeSpark, env.executeTrino, env.executeSparkUpdate], verbs [SELECT, DROP].
- Audit status: `verified`

##### `testCopyOnWriteTableSelectAfterUpdate`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `testCopyOnWriteTableSelectAfterUpdate`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `TestHudiSparkCompatibility.testCopyOnWriteTableSelectAfterUpdate`
- Mapping type: `direct`
- Environment parity: Current class requires `HudiEnvironment`. Routed by source review into `SuiteHudi` run 1.
- Tag parity: Current tags: `Hudi`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `SET`, `createPartitionedTable`. Current setup shape: `SET`, `createPartitionedTable`.
- Action parity: Legacy action shape: `SELECT`, `UPDATE`, `randomNameSuffix`, `ImmutableList.of`, `onHudi`, `executeQuery`, `onTrino`. Current action shape: `SELECT`, `UPDATE`, `randomNameSuffix`, `List.of`, `env.executeSpark`, `env.executeTrino`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createPartitionedTable, ImmutableList.of, onHudi, executeQuery, onTrino] vs current [randomNameSuffix, createPartitionedTable, List.of, env.executeSpark, env.executeTrino, env.executeSparkUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createPartitionedTable, ImmutableList.of, onHudi, executeQuery, onTrino], verbs [SELECT, UPDATE, SET, DROP]. Current flow summary -> helpers [randomNameSuffix, createPartitionedTable, List.of, env.executeSpark, env.executeTrino, env.executeSparkUpdate], verbs [SELECT, UPDATE, SET, DROP].
- Audit status: `verified`

##### `testMergeOnReadTableSelect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `testMergeOnReadTableSelect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `TestHudiSparkCompatibility.testMergeOnReadTableSelect`
- Mapping type: `direct`
- Environment parity: Current class requires `HudiEnvironment`. Routed by source review into `SuiteHudi` run 1.
- Tag parity: Current tags: `Hudi`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createNonPartitionedTable`. Current setup shape: `createNonPartitionedTable`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `ImmutableList.of`, `onHudi`, `executeQuery`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `List.of`, `env.executeSpark`, `env.executeTrino`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createNonPartitionedTable, ImmutableList.of, onHudi, executeQuery, onTrino] vs current [randomNameSuffix, createNonPartitionedTable, List.of, env.executeSpark, env.executeTrino, env.executeSparkUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createNonPartitionedTable, ImmutableList.of, onHudi, executeQuery, onTrino], verbs [SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, createNonPartitionedTable, List.of, env.executeSpark, env.executeTrino, env.executeSparkUpdate], verbs [SELECT, DROP].
- Audit status: `verified`

##### `testMergeOnReadTableSelectAfterUpdate`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `testMergeOnReadTableSelectAfterUpdate`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `TestHudiSparkCompatibility.testMergeOnReadTableSelectAfterUpdate`
- Mapping type: `direct`
- Environment parity: Current class requires `HudiEnvironment`. Routed by source review into `SuiteHudi` run 1.
- Tag parity: Current tags: `Hudi`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `SET`, `createNonPartitionedTable`. Current setup shape: `SET`, `createNonPartitionedTable`.
- Action parity: Legacy action shape: `SELECT`, `UPDATE`, `randomNameSuffix`, `ImmutableList.of`, `onHudi`, `executeQuery`, `onTrino`. Current action shape: `SELECT`, `UPDATE`, `randomNameSuffix`, `List.of`, `env.executeSpark`, `env.executeTrino`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createNonPartitionedTable, ImmutableList.of, onHudi, executeQuery, onTrino] vs current [randomNameSuffix, createNonPartitionedTable, List.of, env.executeSpark, env.executeTrino, env.executeSparkUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createNonPartitionedTable, ImmutableList.of, onHudi, executeQuery, onTrino], verbs [SELECT, UPDATE, SET, DROP]. Current flow summary -> helpers [randomNameSuffix, createNonPartitionedTable, List.of, env.executeSpark, env.executeTrino, env.executeSparkUpdate], verbs [SELECT, UPDATE, SET, DROP].
- Audit status: `verified`

##### `testMergeOnReadPartitionedTableSelect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `testMergeOnReadPartitionedTableSelect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `TestHudiSparkCompatibility.testMergeOnReadPartitionedTableSelect`
- Mapping type: `direct`
- Environment parity: Current class requires `HudiEnvironment`. Routed by source review into `SuiteHudi` run 1.
- Tag parity: Current tags: `Hudi`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createPartitionedTable`. Current setup shape: `createPartitionedTable`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `ImmutableList.of`, `onHudi`, `executeQuery`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `List.of`, `env.executeSpark`, `env.executeTrino`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `row`, `assertThat`, `containsOnly`. Current assertion helpers: `row`, `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createPartitionedTable, ImmutableList.of, onHudi, executeQuery, onTrino] vs current [randomNameSuffix, createPartitionedTable, List.of, env.executeSpark, env.executeTrino, env.executeSparkUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createPartitionedTable, ImmutableList.of, onHudi, executeQuery, onTrino], verbs [SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, createPartitionedTable, List.of, env.executeSpark, env.executeTrino, env.executeSparkUpdate], verbs [SELECT, DROP].
- Audit status: `verified`

##### `testCopyOnWriteTableSelectWithSessionProperties`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `testCopyOnWriteTableSelectWithSessionProperties`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `TestHudiSparkCompatibility.testCopyOnWriteTableSelectWithSessionProperties`
- Mapping type: `direct`
- Environment parity: Current class requires `HudiEnvironment`. Routed by source review into `SuiteHudi` run 1.
- Tag parity: Current tags: `Hudi`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `SET`, `createNonPartitionedTable`. Current setup shape: `SET`, `createNonPartitionedTable`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `ImmutableList.of`, `onHudi`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrino`, `List.of`, `env.executeTrinoInSession`, `session.executeUpdate`, `session.executeQuery`, `rows`, `containsExactlyInAnyOrder`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createNonPartitionedTable, onTrino, executeQuery, ImmutableList.of, onHudi] vs current [randomNameSuffix, createNonPartitionedTable, env.executeTrino, List.of, env.executeTrinoInSession, session.executeUpdate, session.executeQuery, rows, containsExactlyInAnyOrder, env.executeSparkUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createNonPartitionedTable, onTrino, executeQuery, ImmutableList.of, onHudi], verbs [SELECT, SET, DROP]. Current flow summary -> helpers [randomNameSuffix, createNonPartitionedTable, env.executeTrino, List.of, env.executeTrinoInSession, session.executeUpdate, session.executeQuery, rows, containsExactlyInAnyOrder, env.executeSparkUpdate], verbs [SELECT, SET, DROP].
- Audit status: `verified`

##### `testTimelineTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `testTimelineTable`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `TestHudiSparkCompatibility.testTimelineTable`
- Mapping type: `direct`
- Environment parity: Current class requires `HudiEnvironment`. Routed by source review into `SuiteHudi` run 1.
- Tag parity: Current tags: `Hudi`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createNonPartitionedTable`. Current setup shape: `createNonPartitionedTable`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `onHudi`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrino`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createNonPartitionedTable, onTrino, executeQuery, format, onHudi] vs current [randomNameSuffix, createNonPartitionedTable, env.executeTrino, env.executeSparkUpdate]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createNonPartitionedTable, onTrino, executeQuery, format, onHudi], verbs [SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, createNonPartitionedTable, env.executeTrino, env.executeSparkUpdate], verbs [SELECT, DROP].
- Audit status: `verified`

##### `testTimelineTableRedirect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `testTimelineTableRedirect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `TestHudiSparkCompatibility.testTimelineTableRedirect`
- Mapping type: `direct`
- Environment parity: Current class requires `HudiEnvironment`. Routed by source review into `SuiteHudi` run 1.
- Tag parity: Current tags: `Hudi`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createNonPartitionedTable`. Current setup shape: `createNonPartitionedTable`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `format`, `onHudi`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeTrino`, `rootCause`, `env.executeSparkUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `assertQueryFailure`, `hasMessageMatching`. Current assertion helpers: `assertThat`, `containsOnly`, `row`, `assertThatThrownBy`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, createNonPartitionedTable, onTrino, executeQuery, format, onHudi] vs current [randomNameSuffix, createNonPartitionedTable, env.executeTrino, rootCause, env.executeSparkUpdate]; assertion helpers differ: legacy [assertThat, containsOnly, row, assertQueryFailure, hasMessageMatching] vs current [assertThat, containsOnly, row, assertThatThrownBy, hasMessageContaining]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, createNonPartitionedTable, onTrino, executeQuery, format, onHudi], verbs [SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, createNonPartitionedTable, env.executeTrino, rootCause, env.executeSparkUpdate], verbs [SELECT, DROP].
- Audit status: `verified`

##### `testReadCopyOnWriteTableWithReplaceCommits`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `testReadCopyOnWriteTableWithReplaceCommits`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hudi/TestHudiSparkCompatibility.java` ->
  `TestHudiSparkCompatibility.testReadCopyOnWriteTableWithReplaceCommits`
- Mapping type: `direct`
- Environment parity: Current class requires `HudiEnvironment`. Routed by source review into `SuiteHudi` run 1.
- Tag parity: Current tags: `Hudi`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onHudi`, `executeQuery`, `TBLPROPERTIES`, `VALUES`, `onTrino`. Current action shape: `SELECT`, `randomNameSuffix`, `env.getWarehouseDirectory`, `env.executeSparkUpdate`, `TBLPROPERTIES`, `VALUES`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onHudi, executeQuery, TBLPROPERTIES, VALUES, onTrino] vs current [randomNameSuffix, env.getWarehouseDirectory, env.executeSparkUpdate, TBLPROPERTIES, VALUES, env.executeTrino]
- Known intentional difference: `HDP to Hive 3.1 migration`
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onHudi, executeQuery, TBLPROPERTIES, VALUES, onTrino], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.getWarehouseDirectory, env.executeSparkUpdate, TBLPROPERTIES, VALUES, env.executeTrino], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`


## Current-only Environment Verification Coverage


### `TestHiveHudiRedirectionsEnvironment`

- Owning introduction commit: `Add Hive environments`
- Current class added in same commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveHudiRedirectionsEnvironment.java`
- Legacy class removed in same commit:
  none. This is current-only JUnit environment verification coverage added during the framework rewrite.
- Class-level environment requirement: `HiveHudiRedirectionsEnvironment`.
- Class-level tags: `none`.
- Method inventory complete: Current-only class. Legacy methods: `0`. Current methods: `6`.
- Legacy helper/resource dependencies accounted for: not applicable; current class source reviewed directly.
- Intentional differences summary: `new JUnit-side environment verification coverage`
- Method statuses present: `intentional difference`.

#### Methods

##### `verifyTrinoConnectivity`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveHudiRedirectionsEnvironment.java` ->
  `TestHiveHudiRedirectionsEnvironment.verifyTrinoConnectivity`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `HiveHudiRedirectionsEnvironment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `trino connectivity`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `HiveHudiRedirectionsEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

##### `verifyHiveCatalogExists`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveHudiRedirectionsEnvironment.java` ->
  `TestHiveHudiRedirectionsEnvironment.verifyHiveCatalogExists`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `HiveHudiRedirectionsEnvironment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `hive catalog exists`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `HiveHudiRedirectionsEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

##### `verifyHudiCatalogExists`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveHudiRedirectionsEnvironment.java` ->
  `TestHiveHudiRedirectionsEnvironment.verifyHudiCatalogExists`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `HiveHudiRedirectionsEnvironment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `hudi catalog exists`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `HiveHudiRedirectionsEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

##### `verifyHiveDefaultSchema`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveHudiRedirectionsEnvironment.java` ->
  `TestHiveHudiRedirectionsEnvironment.verifyHiveDefaultSchema`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `HiveHudiRedirectionsEnvironment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `hive default schema`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `HiveHudiRedirectionsEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

##### `verifyCreateAndReadTable`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveHudiRedirectionsEnvironment.java` ->
  `TestHiveHudiRedirectionsEnvironment.verifyCreateAndReadTable`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `HiveHudiRedirectionsEnvironment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `create and read table`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `HiveHudiRedirectionsEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

##### `verifyS3Storage`

- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/hive/TestHiveHudiRedirectionsEnvironment.java` ->
  `TestHiveHudiRedirectionsEnvironment.verifyS3Storage`
- Mapping type: `current-only environment verification coverage`
- Environment parity: Verifies the current `HiveHudiRedirectionsEnvironment` environment directly; no legacy method existed.
- Tag parity: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Setup parity: No legacy setup existed. Current method body was reviewed directly.
- Action parity: Current-only verification of `s3 storage`.
- Assertion parity: Current-only assertions on environment behavior; no legacy assertion existed.
- Cleanup parity: Current method cleans up any resources it creates inline; no legacy cleanup existed.
- Any observed difference, however small: this is new current-only JUnit environment verification coverage for `HiveHudiRedirectionsEnvironment` with no legacy counterpart.
- Known intentional difference: `new JUnit-side environment verification coverage`
- Reviewer note: This class is not selected by any current suite and currently serves as direct JUnit / IDE environment verification coverage only.
- Audit status: `intentional difference`

