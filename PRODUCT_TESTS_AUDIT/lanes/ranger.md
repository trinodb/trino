# Lane Audit: Ranger

## Lane Summary

- Audit status: `complete`
- Section start commit: `Add Ranger environment`
- Section end commit: `Remove legacy SuiteRanger`
- Introduced JUnit suites: `SuiteRanger`.
- Extended existing suites: none.
- Retired legacy suites: `SuiteRanger`.
- Environment classes introduced: `RangerEnvironment`.
- Method status counts: verified `1`, intentional difference `0`, needs follow-up `0`.

## Semantic Audit Status

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: none beyond the framework baseline documented in the reviewer guide.
- Needs-follow-up methods: none currently identified in this source/history pass.

## Environment Semantic Audit
### `RangerEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against legacy launcher `EnvMultinodeRanger` and current `RangerEnvironment`.
- Recorded differences:
  - Legacy launcher environment used a broader multinode Ranger topology; current environment uses a single `TrinoProductTestContainer` plus MariaDB with the Ranger policy files copied directly into the Trino container.
  - Current environment retains the effective Ranger access-control config and `hive` superuser default needed by the legacy test flow, but narrows the runtime shape to the services the lane actually uses.
- Reviewer note: no unresolved Ranger-specific environment drift was found; the current environment preserves the privilege model and policy-file inputs the test depends on.

## Suite Semantic Audit
### `SuiteRanger`
- Suite semantic audit status: `complete`
- CI bucket: `auth-and-clients`
- Relationship to lane: `owned by this lane`.
- Legacy/current basis: compared against legacy launcher `SuiteRanger` and current `SuiteRanger`.
- Recorded differences:
  - Legacy suite ran `configured_features` + `ranger` on `EnvMultinodeRanger`.
  - Current suite runs `ConfiguredFeatures` + `Ranger` on `RangerEnvironment`.
- Reviewer note: the suite remains a direct extraction of the legacy Ranger lane.

## Ported Test Classes

### `TestApacheRanger`


- Owning migration commit: `Migrate TestApacheRanger to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ranger/TestApacheRanger.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/ranger/TestApacheRanger.java`
- Class-level environment requirement: `RangerEnvironment`.
- Class-level tags: `ConfiguredFeatures`, `ProfileSpecificTests`, `Ranger`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testCreateTableAsSelect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/ranger/TestApacheRanger.java` ->
  `testCreateTableAsSelect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/ranger/TestApacheRanger.java` ->
  `TestApacheRanger.testCreateTableAsSelect`
- Mapping type: `direct`
- Environment parity: Current class requires `RangerEnvironment`. Routed by source review into `SuiteRanger` run 1.
- Tag parity: Current tags: `ConfiguredFeatures`, `ProfileSpecificTests`, `Ranger`. Tag routing matches the current
  suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `INSERT`, `COMMENT`, `SET`. Current setup shape: `CREATE`, `COMMENT`, `INSERT`, `SET`.
- Action parity: Legacy action shape: `SELECT`, `UPDATE`, `randomNameSuffix`, `onTrino`, `trino.executeQuery`, `updatedRowsCountIsEqualTo`, `COUNT`, `connectToTrino`, `userAlice.executeQuery`. Current action shape: `SELECT`, `UPDATE`, `randomNameSuffix`, `env.executeTrinoUpdate`, `format`, `CAST`, `UNNEST`, `sequence`, `t`, `env.executeTrino`, `COUNT`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`, `assertThatThrownBy`, `isInstanceOf`. Current assertion helpers: `assertThat`, `isEqualTo`, `containsOnly`, `row`, `assertThatThrownBy`, `isInstanceOf`.
- Cleanup parity: Legacy cleanup shape: `DROP`, `DELETE`. Current cleanup shape: `DROP`, `DELETE`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, trino.executeQuery, updatedRowsCountIsEqualTo, COUNT, connectToTrino, userAlice.executeQuery] vs current [randomNameSuffix, env.executeTrinoUpdate, format, CAST, UNNEST, sequence, t, env.executeTrino, COUNT]; SQL verbs differ: legacy [DROP, CREATE, SELECT, INSERT, UPDATE, COMMENT, SET, DELETE] vs current [DROP, CREATE, SELECT, COMMENT, INSERT, UPDATE, SET, DELETE]; assertion helpers differ: legacy [assertThat, containsOnly, row, assertThatThrownBy, isInstanceOf] vs current [assertThat, isEqualTo, containsOnly, row, assertThatThrownBy, isInstanceOf]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, trino.executeQuery, updatedRowsCountIsEqualTo, COUNT, connectToTrino, userAlice.executeQuery], verbs [DROP, CREATE, SELECT, INSERT, UPDATE, COMMENT, SET, DELETE]. Current flow summary -> helpers [randomNameSuffix, env.executeTrinoUpdate, format, CAST, UNNEST, sequence, t, env.executeTrino, COUNT], verbs [DROP, CREATE, SELECT, COMMENT, INSERT, UPDATE, SET, DELETE].
- Audit status: `verified`
