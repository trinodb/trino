# Lane Audit: All Connectors Smoke

## Lane Summary

- Audit status: `complete`
- Section start commit: `Add All Connectors Smoke environment`
- Section end commit: `Remove legacy SuiteAllConnectorsSmoke`
- Introduced JUnit suites: `SuiteAllConnectorsSmoke`.
- Extended existing suites: none.
- Retired legacy suites: `SuiteAllConnectorsSmoke`.
- Environment classes introduced: `AllConnectorsSmokeEnvironment`.
- Method status counts: verified `1`, intentional difference `0`, needs follow-up `0`.

## Semantic Audit Status

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: none beyond the framework baseline documented in the reviewer guide.
- Needs-follow-up methods: none currently identified in this source/history pass.

## Environment Semantic Audit
### `AllConnectorsSmokeEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against legacy launcher `EnvMultinodeAllConnectors` and current `AllConnectorsSmokeEnvironment`.
- Recorded differences:
  - Legacy environment extended the broad shared `StandardMultinode` launcher cluster; current environment uses a single `TrinoProductTestContainer`.
  - Current environment preserves the same catalog/property inventory by copying the same `multinode-all` config files and normalizing only the file paths needed for the Testcontainers layout.
  - Current method no longer issues the legacy `SET` used to skip the exception helper path; instead the configured-connector list is supplied directly by the environment.
- Reviewer note: the current environment preserves the legacy connector-registration intent even though the runtime shape is narrower and the helper path is more explicit.

## Suite Semantic Audit
### `SuiteAllConnectorsSmoke`
- Suite semantic audit status: `complete`
- CI bucket: `connector-smoke`
- Relationship to lane: `owned by this lane`.
- Legacy/current basis: compared against legacy launcher `SuiteAllConnectorsSmoke` and current `SuiteAllConnectorsSmoke`.
- Recorded differences:
  - Legacy suite targeted `TestConfiguredFeatures.selectConfiguredConnectors` explicitly on `EnvMultinodeAllConnectors`.
  - Current suite routes `SuiteTag.AllConnectorsSmoke` on `AllConnectorsSmokeEnvironment`, which still selects the single migrated method.
- Reviewer note: suite intent is preserved; the current JUnit suite simply expresses the single-method routing through tags instead of launcher test-name selection.

## Ported Test Classes

### `TestConfiguredFeaturesJunit`


- Owning migration commit: `Migrate TestConfiguredFeatures to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/TestConfiguredFeaturesJunit.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/TestConfiguredFeatures.java`
- Class-level environment requirement: `AllConnectorsSmokeEnvironment`.
- Class-level tags: `ConfiguredFeatures`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `selectConfiguredConnectors`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/TestConfiguredFeatures.java` ->
  `selectConfiguredConnectors`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/TestConfiguredFeaturesJunit.java` ->
  `TestConfiguredFeaturesJunit.selectConfiguredConnectors`
- Mapping type: `direct`
- Environment parity: Current class requires `AllConnectorsSmokeEnvironment`. Routed by source review into `SuiteAllConnectorsSmoke` run 1.
- Tag parity: Current tags: `ConfiguredFeatures`. Tag routing matches `SuiteAllConnectorsSmoke`.
- Setup parity: Legacy setup shape: `SET`. Current setup shape: none.
- Action parity: Legacy action shape: `SELECT`, `kipException`, `onTrino`, `executeQuery`, `hasColumns`, `configuredConnectors.stream`, `map`, `collect`, `Collectors.toList`. Current action shape: `SELECT`, `env.getConfiguredConnectors`, `isNotEmpty`, `env.executeTrino`, `stream`, `map`, `toList`.
- Assertion parity: Legacy assertion helpers: `configuredConnectors.isEmpty`, `assertThat`, `containsOnly`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [kipException, onTrino, executeQuery, hasColumns, configuredConnectors.stream, map, collect, Collectors.toList] vs current [env.getConfiguredConnectors, isNotEmpty, env.executeTrino, stream, map, toList]; SQL verbs differ: legacy [SET, SELECT] vs current [SELECT]; assertion helpers differ: legacy [configuredConnectors.isEmpty, assertThat, containsOnly] vs current [assertThat, containsOnly, row]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [kipException, onTrino, executeQuery, hasColumns, configuredConnectors.stream, map, collect, Collectors.toList], verbs [SET, SELECT]. Current flow summary -> helpers [env.getConfiguredConnectors, isNotEmpty, env.executeTrino, stream, map, toList], verbs [SELECT].
- Audit status: `verified`
