# Lane Audit: Functions

## Lane Summary

- Audit status: `complete`
- Section start commit: `Add Functions environment`
- Section end commit: `Remove legacy SuiteFunctions`
- Introduced JUnit suites: `SuiteFunctions`.
- Extended existing suites: none.
- Retired legacy suites: `SuiteFunctions`.
- Environment classes introduced: `FunctionsEnvironment`.
- Method status counts: verified `5`, intentional difference `0`, needs follow-up `0`.

## Semantic Audit Status

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: none beyond the framework baseline documented in the reviewer guide.
- Needs-follow-up methods: none currently identified in this source/history pass.

## Environment Semantic Audit
### `FunctionsEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against legacy launcher `EnvMultinode` as used by legacy launcher `SuiteFunctions` and current `FunctionsEnvironment`.
- Recorded differences:
  - Legacy suite reused the broad `EnvMultinode` launcher environment; current lane uses a dedicated `FunctionsEnvironment` with only `tpch` and `tpcds` catalogs.
  - Current environment is a single `TrinoProductTestContainer` instead of the broader launcher-managed cluster.
  - The current environment removes unrelated services/catalogs that were present in the shared legacy environment but not used by the Teradata function methods.
- Reviewer note: this is a behavior-preserving environment extraction that keeps the tested SQL function surface while reducing unrelated runtime complexity.

## Suite Semantic Audit
### `SuiteFunctions`
- Suite semantic audit status: `complete`
- CI bucket: `jdbc-core`
- Relationship to lane: `owned by this lane`.
- Legacy/current basis: compared against legacy launcher `SuiteFunctions` and current `SuiteFunctions`.
- Recorded differences:
  - Legacy suite ran `CONFIGURED_FEATURES` + `FUNCTIONS` on shared `EnvMultinode`; current suite runs `ConfiguredFeatures` + `Functions` on dedicated `FunctionsEnvironment`.
  - Current suite isolates the Teradata function coverage from the unrelated legacy `EnvMultinode` surface.
- Reviewer note: suite selection and effective method coverage remain faithful after environment extraction.

## Ported Test Classes

### `TestTeradataFunctions`


- Owning migration commit: `Migrate TestTeradataFunctions to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/teradata/TestTeradataFunctions.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/teradata/TestTeradataFunctions.java`
- Class-level environment requirement: `FunctionsEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `Functions`.
- Method inventory complete: Yes. Legacy methods: `5`. Current methods: `5`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testIndex`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/teradata/TestTeradataFunctions.java` ->
  `testIndex`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/teradata/TestTeradataFunctions.java` ->
  `TestTeradataFunctions.testIndex`
- Mapping type: `direct`
- Environment parity: Current class requires `FunctionsEnvironment`. Routed by source review into `SuiteFunctions` run 1.
- Tag parity: Current tags: `ConfiguredFeatures`, `Functions`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `index`. Current action shape: `SELECT`, `env.executeTrino`, `index`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `row`. Current assertion helpers: `assertThat`, `contains`, `row`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, index] vs current [env.executeTrino, index]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, index], verbs [SELECT]. Current flow summary -> helpers [env.executeTrino, index], verbs [SELECT].
- Audit status: `verified`

##### `testChar2HexInt`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/teradata/TestTeradataFunctions.java` ->
  `testChar2HexInt`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/teradata/TestTeradataFunctions.java` ->
  `TestTeradataFunctions.testChar2HexInt`
- Mapping type: `direct`
- Environment parity: Current class requires `FunctionsEnvironment`. Routed by source review into `SuiteFunctions` run 1.
- Tag parity: Current tags: `ConfiguredFeatures`, `Functions`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `char2hexint`. Current action shape: `SELECT`, `env.executeTrino`, `char2hexint`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `row`. Current assertion helpers: `assertThat`, `contains`, `row`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, char2hexint] vs current [env.executeTrino, char2hexint]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, char2hexint], verbs [SELECT]. Current flow summary -> helpers [env.executeTrino, char2hexint], verbs [SELECT].
- Audit status: `verified`

##### `testToDate`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/teradata/TestTeradataFunctions.java` ->
  `testToDate`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/teradata/TestTeradataFunctions.java` ->
  `TestTeradataFunctions.testToDate`
- Mapping type: `direct`
- Environment parity: Current class requires `FunctionsEnvironment`. Routed by source review into `SuiteFunctions` run 1.
- Tag parity: Current tags: `ConfiguredFeatures`, `Functions`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `to_date`, `Date.valueOf`. Current action shape: `SELECT`, `env.executeTrino`, `to_date`, `Date.valueOf`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `row`. Current assertion helpers: `assertThat`, `contains`, `row`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, to_date, Date.valueOf] vs current [env.executeTrino, to_date, Date.valueOf]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, to_date, Date.valueOf], verbs [SELECT]. Current flow summary -> helpers [env.executeTrino, to_date, Date.valueOf], verbs [SELECT].
- Audit status: `verified`

##### `testToTimestamp`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/teradata/TestTeradataFunctions.java` ->
  `testToTimestamp`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/teradata/TestTeradataFunctions.java` ->
  `TestTeradataFunctions.testToTimestamp`
- Mapping type: `direct`
- Environment parity: Current class requires `FunctionsEnvironment`. Routed by source review into `SuiteFunctions` run 1.
- Tag parity: Current tags: `ConfiguredFeatures`, `Functions`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `to_timestamp`, `Timestamp.valueOf`, `LocalDateTime.of`. Current action shape: `SELECT`, `env.executeTrino`, `to_timestamp`, `Timestamp.valueOf`, `LocalDateTime.of`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `row`. Current assertion helpers: `assertThat`, `contains`, `row`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, to_timestamp, Timestamp.valueOf, LocalDateTime.of] vs current [env.executeTrino, to_timestamp, Timestamp.valueOf, LocalDateTime.of]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, to_timestamp, Timestamp.valueOf, LocalDateTime.of], verbs [SELECT]. Current flow summary -> helpers [env.executeTrino, to_timestamp, Timestamp.valueOf, LocalDateTime.of], verbs [SELECT].
- Audit status: `verified`

##### `testToChar`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/teradata/TestTeradataFunctions.java` ->
  `testToChar`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/teradata/TestTeradataFunctions.java` ->
  `TestTeradataFunctions.testToChar`
- Mapping type: `direct`
- Environment parity: Current class requires `FunctionsEnvironment`. Routed by source review into `SuiteFunctions` run 1.
- Tag parity: Current tags: `ConfiguredFeatures`, `Functions`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: none.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `to_char`. Current action shape: `SELECT`, `env.executeTrino`, `to_char`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `row`. Current assertion helpers: `assertThat`, `contains`, `row`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, to_char] vs current [env.executeTrino, to_char]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, to_char], verbs [SELECT]. Current flow summary -> helpers [env.executeTrino, to_char], verbs [SELECT].
- Audit status: `verified`
