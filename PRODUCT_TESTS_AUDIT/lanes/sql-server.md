# Lane Audit: Sql Server

## Lane Summary

- Audit status: `manual-pass-complete`
- Section start commit: `Add SQL Server environment`
- Section end commit: `Remove legacy SuiteSqlServer`
- Introduced JUnit suites: `SuiteSqlServer`.
- Extended existing suites: none.
- Retired legacy suites: `SuiteSqlServer`.
- Environment classes introduced: `SqlServerEnvironment`.
- Method status counts: verified `1`, intentional difference `0`, needs follow-up `0`.

## Semantic Audit Status

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: none beyond the framework baseline documented in the reviewer guide.
- Needs-follow-up methods: none currently identified in this source/history pass.

## Environment Semantic Audit
### `SqlServerEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against the effective legacy launcher environment `EnvMultinodeSqlserver` and current `SqlServerEnvironment`.
- Recorded differences: current environment runs outside Docker and uses one Trino container plus one SQL Server Testcontainers service; legacy coverage lived inside aggregate launcher `Suite7NonGeneric`. This falls within the approved framework replacement difference set.
- Reviewer note: Current environment preserves the same single SQL Server catalog path required by the CTAS coverage.

## Suite Semantic Audit
### `SuiteSqlServer`
- Suite semantic audit status: `complete`
- CI bucket: `jdbc-core`
- Relationship to lane: `owned by this lane`.
- Reviewer note: Compared directly against the effective SQL Server run in legacy aggregate `Suite7NonGeneric`; current suite isolates the same coverage into an explicit SQL Server-owned suite.

## Ported Test Classes

### `TestSqlServer`


- Owning migration commit: `Migrate TestSqlServer to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/sqlserver/TestSqlServer.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/sqlserver/TestSqlServer.java`
- Class-level environment requirement: `SqlServerEnvironment`.
- Class-level tags: `ProfileSpecificTests`, `Sqlserver`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `testCreateTableAsSelect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/sqlserver/TestSqlServer.java` ->
  `testCreateTableAsSelect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/sqlserver/TestSqlServer.java` ->
  `TestSqlServer.testCreateTableAsSelect`
- Mapping type: `direct`
- Environment parity: Current class requires `SqlServerEnvironment`. Routed by source review into `SuiteSqlServer` run 1.
- Tag parity: Current tags: `ProfileSpecificTests`, `Sqlserver`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`, `env.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `randomNameSuffix`, `onTrino`, `executeQuery`, `updatedRowsCountIsEqualTo`, `COUNT`. Current action shape: `SELECT`, `randomNameSuffix`, `stmt.executeUpdate`, `stmt.executeQuery`, `COUNT`, `rs.next`, `rs.getLong`, `stmt.execute`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isEqualTo`, `isTrue`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [randomNameSuffix, onTrino, executeQuery, updatedRowsCountIsEqualTo, COUNT] vs current [randomNameSuffix, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, stmt.executeQuery, COUNT, rs.next, rs.getLong, stmt.execute]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat, isEqualTo, isTrue]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [randomNameSuffix, onTrino, executeQuery, updatedRowsCountIsEqualTo, COUNT], verbs [CREATE, SELECT, DROP]. Current flow summary -> helpers [randomNameSuffix, env.createTrinoConnection, conn.createStatement, stmt.executeUpdate, stmt.executeQuery, COUNT, rs.next, rs.getLong, stmt.execute], verbs [CREATE, SELECT, DROP].
- Audit status: `verified`
