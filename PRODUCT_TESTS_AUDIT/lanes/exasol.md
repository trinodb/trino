# Lane Audit: Exasol

## Lane Summary

- Audit status: `complete`
- Section start commit: `Add Exasol environment`
- Section end commit: `Remove legacy SuiteExasol`
- Introduced JUnit suites: `SuiteExasol`.
- Extended existing suites: none.
- Retired legacy suites: `SuiteExasol`.
- Environment classes introduced: `ExasolEnvironment`.
- Method status counts: verified `1`, intentional difference `0`, needs follow-up `0`.

## Semantic Audit Status

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: none beyond the framework baseline documented in the reviewer guide.
- Needs-follow-up methods: none currently identified in this source/history pass.

## Environment Semantic Audit
### `ExasolEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against legacy launcher `EnvMultinodeExasol` and current `ExasolEnvironment`.
- Recorded differences:
  - Legacy launcher environment used the shared multinode Trino cluster shape; current environment uses a single `TrinoProductTestContainer` plus an Exasol container on a dedicated network.
  - Current environment configures only the Exasol catalog required by the lane instead of the broader launcher environment surface.
  - Exasol image/runtime details differ because the current environment starts the database directly with Testcontainers rather than through launcher wiring.
- Reviewer note: these are environment-shape simplifications; the test method still exercises the same Exasol write/read/drop flow and no unresolved Exasol-specific fidelity gap was found.

## Suite Semantic Audit
### `SuiteExasol`
- Suite semantic audit status: `complete`
- CI bucket: `jdbc-external`
- Relationship to lane: `owned by this lane`.
- Legacy/current basis: compared against legacy launcher `SuiteExasol` and current `SuiteExasol`.
- Recorded differences:
  - Legacy suite ran `configured_features` + `exasol` on `EnvMultinodeExasol`; current suite runs `ConfiguredFeatures` + `Exasol` on `ExasolEnvironment`.
  - The current suite is a dedicated JUnit entrypoint instead of a launcher suite wrapper, but retains the same effective class/method coverage intent.
- Reviewer note: suite coverage is faithful after the environment extraction.

## Ported Test Classes

### `TestExasol`


- Owning migration commit: `Migrate TestExasol to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/exasol/TestExasol.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/exasol/TestExasol.java`
- Class-level environment requirement: `ExasolEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `Exasol`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testSelect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/exasol/TestExasol.java` ->
  `testSelect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/exasol/TestExasol.java` ->
  `TestExasol.testSelect`
- Mapping type: `direct`
- Environment parity: Current class requires `ExasolEnvironment`. Routed by source review into `SuiteExasol` run 1.
- Tag parity: Current tags: `ConfiguredFeatures`, `Exasol`, `ProfileSpecificTests`. Tag routing matches the current
  suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`. Current setup shape: `CREATE`, `INSERT`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onExasol`, `executeQuery`, `varchar`, `VALUES`, `onTrino`, `BigDecimal.valueOf`. Current action shape: `SELECT`, `randomNameSuffix`, `env.executeExasolUpdate`, `varchar`, `VALUES`, `env.executeTrino`, `BigDecimal.valueOf`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `containsOnly`, `row`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onExasol, executeQuery, varchar, VALUES, onTrino, BigDecimal.valueOf] vs current [randomNameSuffix, env.executeExasolUpdate, varchar, VALUES, env.executeTrino, BigDecimal.valueOf]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onExasol, executeQuery, varchar, VALUES, onTrino, BigDecimal.valueOf], verbs [CREATE, INSERT, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.executeExasolUpdate, varchar, VALUES, env.executeTrino, BigDecimal.valueOf], verbs [CREATE, INSERT, SELECT, DROP].
- Audit status: `verified`
