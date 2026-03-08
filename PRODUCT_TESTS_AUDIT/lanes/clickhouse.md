# Lane Audit: Clickhouse

## Lane Summary

- Audit status: `manual-pass-complete`
- Section start commit: `Add ClickHouse environment`
- Section end commit: `Remove legacy SuiteClickhouse`
- Introduced JUnit suites: `SuiteClickhouse`.
- Extended existing suites: none.
- Retired legacy suites: `SuiteClickhouse`.
- Environment classes introduced: `ClickHouseEnvironment`.
- Method status counts: verified `1`, intentional difference `0`, needs follow-up `0`.

## Semantic Audit Status

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: none beyond the framework baseline documented in the reviewer guide.
- Needs-follow-up methods: none currently identified in this source/history pass.

## Environment Semantic Audit
### `ClickHouseEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against legacy launcher `EnvMultinodeClickhouse` and current `ClickHouseEnvironment`.
- Recorded differences: legacy launcher used a multi-node ClickHouse topology; current environment uses a single ClickHouse Testcontainers service plus one Trino container. This difference is recorded explicitly because the migrated coverage is only the single-table CTAS path and does not exercise distributed ClickHouse behavior.
- Reviewer note: No additional unresolved environment-specific fidelity gap is recorded beyond the topology simplification noted above.

## Suite Semantic Audit
### `SuiteClickhouse`
- Suite semantic audit status: `complete`
- CI bucket: `connector-smoke`
- Relationship to lane: `owned by this lane`.
- Reviewer note: Compared directly against legacy `SuiteClickhouse`; current suite preserves the dedicated ClickHouse run while accepting the environment topology simplification recorded above.

## Ported Test Classes

### `TestClickHouse`


- Owning migration commit: `Migrate TestClickHouse to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/clickhouse/TestClickHouse.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/clickhouse/TestClickHouse.java`
- Class-level environment requirement: `ClickHouseEnvironment`.
- Class-level tags: `Clickhouse`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testCreateTableAsSelect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/clickhouse/TestClickHouse.java` ->
  `testCreateTableAsSelect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/clickhouse/TestClickHouse.java` ->
  `TestClickHouse.testCreateTableAsSelect`
- Mapping type: `direct`
- Environment parity: Current class requires `ClickHouseEnvironment`. Routed by source review into `SuiteClickhouse` run 1.
- Tag parity: Current tags: `Clickhouse`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `updatedRowsCountIsEqualTo`, `COUNT`. Current action shape: `SELECT`, `stmt.executeUpdate`, `stmt.executeQuery`, `COUNT`, `rs.next`, `rs.getLong`, `stmt.execute`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isEqualTo`, `isTrue`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, updatedRowsCountIsEqualTo, COUNT] vs current [env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, stmt.executeQuery, COUNT, rs.next, rs.getLong, stmt.execute]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat, isEqualTo, isTrue]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, updatedRowsCountIsEqualTo, COUNT], verbs [CREATE, SELECT, DROP]. Current flow summary -> helpers [env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, stmt.executeQuery, COUNT, rs.next, rs.getLong, stmt.execute], verbs [CREATE, SELECT, DROP].
- Audit status: `verified`
