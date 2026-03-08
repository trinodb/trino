# Lane Audit: Ignite

## Lane Summary

- Audit status: `complete`
- Section start commit: `Add Ignite environment`
- Section end commit: `Remove legacy SuiteIgnite`
- Introduced JUnit suites: `SuiteIgnite`.
- Extended existing suites: none.
- Retired legacy suites: `SuiteIgnite`.
- Environment classes introduced: `IgniteEnvironment`.
- Method status counts: verified `1`, intentional difference `0`, needs follow-up `0`.

## Semantic Audit Status

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: `Ignite upgrade`.
- Needs-follow-up methods: none currently identified in this source/history pass.

## Environment Semantic Audit
### `IgniteEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against legacy launcher `EnvMultinodeIgnite` and current `IgniteEnvironment`.
- Recorded differences:
  - Current lane uses the approved Ignite upgrade and ARM-capable image path instead of the legacy `apacheignite/ignite:2.9.0` launcher setup.
  - Legacy environment layered Ignite onto shared `StandardMultinode`; current environment uses two `IgniteContainer` instances plus a dedicated `MultiNodeTrinoCluster`.
  - Current environment applies `JAVA_TOOL_OPTIONS` open-module flags directly in the Trino cluster customizers instead of through copied launcher JVM config.
- Reviewer note: these differences are within the approved Ignite-upgrade envelope and preserve the same CTAS/read/drop behavior that the legacy method exercised.

## Suite Semantic Audit
### `SuiteIgnite`
- Suite semantic audit status: `complete`
- CI bucket: `connector-smoke`
- Relationship to lane: `owned by this lane`.
- Legacy/current basis: compared against legacy launcher `SuiteIgnite` and current `SuiteIgnite`.
- Recorded differences:
  - Legacy suite ran `CONFIGURED_FEATURES` + `IGNITE` on `EnvMultinodeIgnite`.
  - Current suite runs `ConfiguredFeatures` + `Ignite` on `IgniteEnvironment`.
- Reviewer note: suite coverage remains faithful after the approved Ignite runtime update.

## Ported Test Classes

### `TestIgnite`


- Owning migration commit: `Migrate TestIgnite to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ignite/TestIgnite.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/ignite/TestIgnite.java`
- Class-level environment requirement: `IgniteEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `Ignite`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `Ignite upgrade`
- Method statuses present: `verified`.

#### Methods

##### `testCreateTableAsSelect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/ignite/TestIgnite.java` ->
  `testCreateTableAsSelect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ignite/TestIgnite.java` ->
  `TestIgnite.testCreateTableAsSelect`
- Mapping type: `direct`
- Environment parity: Current class requires `IgniteEnvironment`. Routed by source review into `SuiteIgnite` run 1.
- Tag parity: Current tags: `ConfiguredFeatures`, `Ignite`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `updatedRowsCountIsEqualTo`, `COUNT`. Current action shape: `SELECT`, `env.executeTrinoUpdate`, `env.executeTrino`, `COUNT`, `BigDecimal.valueOf`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isEqualTo`, `QueryResultAssert.assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, updatedRowsCountIsEqualTo, COUNT] vs current [env.executeTrinoUpdate, env.executeTrino, COUNT, BigDecimal.valueOf]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat, isEqualTo, QueryResultAssert.assertThat, containsOnly, row]
- Known intentional difference: `Ignite upgrade`
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, updatedRowsCountIsEqualTo, COUNT], verbs [CREATE, SELECT, DROP]. Current flow summary -> helpers [env.executeTrinoUpdate, env.executeTrino, COUNT, BigDecimal.valueOf], verbs [CREATE, SELECT, DROP].
- Audit status: `verified`
