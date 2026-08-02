# Lane Audit: Jdbc

## Lane Summary

- Audit status: `manual-pass-complete`
- Section start commit: `Add JDBC environment`
- Section end commit: `Migrate TestPreparedStatements to JUnit`
- Introduced JUnit suites: `SuiteClients`.
- Extended existing suites: none.
- Retired legacy suites: none.
- Environment classes introduced: `JdbcBasicEnvironment`.
- Method status counts: verified `20`, intentional difference `0`, needs follow-up `0`.

## Semantic Audit Status

- Baseline note: detailed method, environment, and suite records below are retained from earlier audit work and must not be treated as final semantic findings until this lane is marked `complete` in `PROGRESS.md`.

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: none beyond the framework baseline documented in the reviewer guide.
- Needs-follow-up methods: none identified in the literal method-body comparison.

## Environment Semantic Audit
### `JdbcBasicEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against the `EnvMultinodeTlsKerberosDelegation` run in legacy `Suite3`, the current `JdbcBasicEnvironment`, and the current `SuiteClients` JDBC run.
- Recorded differences:
  - Legacy JDBC coverage ran inside the mixed multinode TLS/Kerberos-delegation launcher environment.
  - Current JUnit lane isolates plain JDBC coverage in a dedicated lightweight environment with built-in `tpch` plus `memory` catalogs.
- Reviewer note: this is an environment simplification rather than a test-semantic reduction for the `TestJdbc` methods themselves; the JDBC method bodies still exercise query, metadata, locale, timezone, and session-property behavior.

### `CliEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared only as the later `SuiteClients` extension target used by the `clients` lane.
- Recorded differences:
  - `CliEnvironment` is not the primary environment for the `jdbc` lane's `TestJdbc` class.
  - It appears here because current `SuiteClients` is a shared suite that now bundles JDBC and CLI runs.
- Reviewer note: detailed CLI environment fidelity is recorded in the `clients` lane; the `jdbc` lane uses this section only to explain the shared suite shape.

## Suite Semantic Audit
### `SuiteClients`
- Suite semantic audit status: `complete`
- CI bucket: `auth-and-clients`
- Relationship to lane: `extended by this lane and later extended again by the clients lane`.
- Legacy/current basis: compared against legacy launcher `SuiteClients` and current `SuiteClients` source.
- Recorded differences:
  - Legacy launcher `SuiteClients` had one run on `EnvMultinode` with groups `CONFIGURED_FEATURES`, `CLI`, `JDBC`, and `TRINO_JDBC`, excluding `PROFILE_SPECIFIC_TESTS`.
  - Current JUnit suite splits that mixed run into one `JdbcBasicEnvironment` run for `Jdbc` and one `CliEnvironment` run for `Cli`.
  - The current suite therefore preserves the combined logical lane but not the single shared runtime shape.
- Reviewer note: for the JDBC portion, the semantic coverage is preserved; the suite change is a runtime decomposition, not a documented coverage loss.

## Ported Test Classes

### `TestJdbc`


- Owning migration commit: `Migrate TestJdbc to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestJdbc.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestJdbc.java`
- Class-level environment requirement: `JdbcBasicEnvironment`.
- Class-level tags: `Jdbc`.
- Method inventory complete: Yes. Legacy methods: `11`. Current methods: `11`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `shouldExecuteQuery`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `shouldExecuteQuery`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `TestJdbc.shouldExecuteQuery`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createStatement`. Current setup shape: none.
- Action parity: Legacy action shape: `SELECT`, `connection`, `queryResult`, `matches`. Current action shape: `SELECT`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [connection, createStatement, queryResult, matches] vs current [env.executeTrino]; assertion helpers differ: legacy [assertThat] vs current [assertThat, containsOnly]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [connection, createStatement, queryResult, matches], verbs [SELECT]. Current flow summary -> helpers [env.executeTrino], verbs [SELECT].
- Audit status: `verified`

##### `shouldInsertSelectQuery`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `shouldInsertSelectQuery`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `TestJdbc.shouldInsertSelectQuery`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `INSERT`, `createStatement`. Current setup shape: `CREATE`, `env.createTrinoConnection`, `connection.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `mutableTablesState`, `get`, `getNameInDatabase`, `onTrino`, `executeQuery`, `hasNoRows`, `connection`, `statement.executeUpdate`, `matches`. Current action shape: `SELECT`, `statement.execute`, `yet`, `statement.executeUpdate`, `env.executeTrino`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `isEqualTo`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [mutableTablesState, get, getNameInDatabase, onTrino, executeQuery, hasNoRows, connection, createStatement, statement.executeUpdate, matches] vs current [env.createTrinoConnection, connection.createStatement, statement.execute, yet, statement.executeUpdate, env.executeTrino]; SQL verbs differ: legacy [SELECT, INSERT] vs current [CREATE, DROP, SELECT]; assertion helpers differ: legacy [assertThat, isEqualTo] vs current [assertThat, isEqualTo, containsOnly]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [mutableTablesState, get, getNameInDatabase, onTrino, executeQuery, hasNoRows, connection, createStatement, statement.executeUpdate, matches], verbs [SELECT, INSERT]. Current flow summary -> helpers [env.createTrinoConnection, connection.createStatement, statement.execute, yet, statement.executeUpdate, env.executeTrino], verbs [CREATE, DROP, SELECT].
- Audit status: `verified`

##### `shouldExecuteQueryWithSelectedCatalogAndSchema`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `shouldExecuteQueryWithSelectedCatalogAndSchema`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `TestJdbc.shouldExecuteQueryWithSelectedCatalogAndSchema`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `setCatalog`, `setSchema`, `createStatement`. Current setup shape: `env.createTrinoConnection`, `connection.setCatalog`, `connection.setSchema`, `connection.createStatement`, `QueryResult.forResultSet`.
- Action parity: Legacy action shape: `SELECT`, `connection`, `queryResult`, `matches`. Current action shape: `SELECT`, `statement.executeQuery`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [connection, setCatalog, setSchema, createStatement, queryResult, matches] vs current [env.createTrinoConnection, connection.setCatalog, connection.setSchema, connection.createStatement, statement.executeQuery, QueryResult.forResultSet]; assertion helpers differ: legacy [assertThat] vs current [assertThat, containsOnly]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [connection, setCatalog, setSchema, createStatement, queryResult, matches], verbs [SELECT]. Current flow summary -> helpers [env.createTrinoConnection, connection.setCatalog, connection.setSchema, connection.createStatement, statement.executeQuery, QueryResult.forResultSet], verbs [SELECT].
- Audit status: `verified`

##### `shouldSetTimezone`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `shouldSetTimezone`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `TestJdbc.shouldSetTimezone`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `setTimeZoneId`. Current setup shape: `env.createTrinoConnection`, `setTimeZoneId`, `connection.createStatement`.
- Action parity: Legacy action shape: `connection`. Current action shape: `SELECT`, `statement.executeQuery`, `current_timezone`, `rs.next`, `rs.getString`.
- Assertion parity: Legacy assertion helpers: `assertConnectionTimezone`. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [connection, setTimeZoneId] vs current [env.createTrinoConnection, setTimeZoneId, connection.createStatement, statement.executeQuery, current_timezone, rs.next, rs.getString]; SQL verbs differ: legacy [none] vs current [SELECT]; assertion helpers differ: legacy [assertConnectionTimezone] vs current [assertThat, isTrue, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [connection, setTimeZoneId], verbs [none]. Current flow summary -> helpers [env.createTrinoConnection, setTimeZoneId, connection.createStatement, statement.executeQuery, current_timezone, rs.next, rs.getString], verbs [SELECT].
- Audit status: `verified`

##### `shouldSetLocale`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `shouldSetLocale`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `TestJdbc.shouldSetLocale`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `setLocale`, `createStatement`. Current setup shape: `env.createTrinoConnection`, `setLocale`, `connection.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `connection`, `queryResult`, `date_format`. Current action shape: `SELECT`, `statement.executeQuery`, `date_format`, `rs.next`, `rs.getString`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `row`. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [connection, setLocale, createStatement, queryResult, date_format] vs current [env.createTrinoConnection, setLocale, connection.createStatement, statement.executeQuery, date_format, rs.next, rs.getString]; assertion helpers differ: legacy [assertThat, contains, row] vs current [assertThat, isTrue, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [connection, setLocale, createStatement, queryResult, date_format], verbs [SELECT]. Current flow summary -> helpers [env.createTrinoConnection, setLocale, connection.createStatement, statement.executeQuery, date_format, rs.next, rs.getString], verbs [SELECT].
- Audit status: `verified`

##### `shouldGetSchemas`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `shouldGetSchemas`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `TestJdbc.shouldGetSchemas`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `QueryResult.forResultSet`. Current setup shape: `env.createTrinoConnection`.
- Action parity: Legacy action shape: `metaData`, `getSchemas`. Current action shape: `connection.getMetaData`, `getSchemas`, `rs.next`, `equals`, `rs.getString`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `row`. Current assertion helpers: `assertThat`, `isTrue`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [QueryResult.forResultSet, metaData, getSchemas] vs current [env.createTrinoConnection, connection.getMetaData, getSchemas, rs.next, equals, rs.getString]; assertion helpers differ: legacy [assertThat, contains, row] vs current [assertThat, isTrue]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [QueryResult.forResultSet, metaData, getSchemas], verbs [none]. Current flow summary -> helpers [env.createTrinoConnection, connection.getMetaData, getSchemas, rs.next, equals, rs.getString], verbs [none].
- Audit status: `verified`

##### `shouldGetTables`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `shouldGetTables`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `TestJdbc.shouldGetTables`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `QueryResult.forResultSet`. Current setup shape: `env.createTrinoConnection`.
- Action parity: Legacy action shape: `metaData`, `getTables`. Current action shape: `connection.getMetaData`, `getTables`, `rs.next`, `rs.getString`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `row`. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [QueryResult.forResultSet, metaData, getTables] vs current [env.createTrinoConnection, connection.getMetaData, getTables, rs.next, rs.getString]; assertion helpers differ: legacy [assertThat, contains, row] vs current [assertThat, isTrue, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [QueryResult.forResultSet, metaData, getTables], verbs [none]. Current flow summary -> helpers [env.createTrinoConnection, connection.getMetaData, getTables, rs.next, rs.getString], verbs [none].
- Audit status: `verified`

##### `shouldGetColumns`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `shouldGetColumns`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `TestJdbc.shouldGetColumns`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `QueryResult.forResultSet`. Current setup shape: `COMMENT`, `env.createTrinoConnection`.
- Action parity: Legacy action shape: `metaData`, `getColumns`, `matches`, `sqlResultDescriptorForResource`. Current action shape: `connection.getMetaData`, `getColumns`, `rs.next`, `rs.getString`, `isIn`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [QueryResult.forResultSet, metaData, getColumns, matches, sqlResultDescriptorForResource] vs current [env.createTrinoConnection, connection.getMetaData, getColumns, rs.next, rs.getString, isIn]; SQL verbs differ: legacy [none] vs current [COMMENT]; assertion helpers differ: legacy [assertThat] vs current [assertThat, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [QueryResult.forResultSet, metaData, getColumns, matches, sqlResultDescriptorForResource], verbs [none]. Current flow summary -> helpers [env.createTrinoConnection, connection.getMetaData, getColumns, rs.next, rs.getString, isIn], verbs [COMMENT].
- Audit status: `verified`

##### `shouldGetTableTypes`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `shouldGetTableTypes`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `TestJdbc.shouldGetTableTypes`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `QueryResult.forResultSet`. Current setup shape: `env.createTrinoConnection`.
- Action parity: Legacy action shape: `metaData`, `getTableTypes`. Current action shape: `connection.getMetaData`, `metaData.getTableTypes`, `rs.next`, `rs.getString`, `equals`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `contains`, `row`. Current assertion helpers: `assertThat`, `isTrue`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [QueryResult.forResultSet, metaData, getTableTypes] vs current [env.createTrinoConnection, connection.getMetaData, metaData.getTableTypes, rs.next, rs.getString, equals]; assertion helpers differ: legacy [assertThat, contains, row] vs current [assertThat, isTrue]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [QueryResult.forResultSet, metaData, getTableTypes], verbs [none]. Current flow summary -> helpers [env.createTrinoConnection, connection.getMetaData, metaData.getTableTypes, rs.next, rs.getString, equals], verbs [none].
- Audit status: `verified`

##### `testSessionProperties`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `testSessionProperties`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `TestJdbc.testSessionProperties`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `setSessionProperty`, `resetSessionProperty`. Current setup shape: `env.createTrinoConnection`, `setSessionProperty`, `resetSessionProperty`.
- Action parity: Legacy action shape: `getSessionProperty`, `connection`. Current action shape: `getSessionProperty`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [getSessionProperty, connection, setSessionProperty, resetSessionProperty] vs current [env.createTrinoConnection, getSessionProperty, setSessionProperty, resetSessionProperty]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [getSessionProperty, connection, setSessionProperty, resetSessionProperty], verbs [none]. Current flow summary -> helpers [env.createTrinoConnection, getSessionProperty, setSessionProperty, resetSessionProperty], verbs [none].
- Audit status: `verified`

##### `testDeallocate`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `testDeallocate`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestJdbc.java` ->
  `TestJdbc.testDeallocate`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `env.createTrinoConnection`.
- Action parity: Legacy action shape: `SELECT`, `connection`, `connection.prepareStatement`, `repeat`, `preparedStatement.executeQuery`, `untimeException`. Current action shape: `SELECT`, `connection.prepareStatement`, `repeat`, `preparedStatement.executeQuery`, `rs.next`.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: `assertThat`, `isTrue`.
- Cleanup parity: Legacy cleanup shape: `close`. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [connection, connection.prepareStatement, repeat, preparedStatement.executeQuery, close, untimeException] vs current [env.createTrinoConnection, connection.prepareStatement, repeat, preparedStatement.executeQuery, rs.next]; assertion helpers differ: legacy [none] vs current [assertThat, isTrue]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [connection, connection.prepareStatement, repeat, preparedStatement.executeQuery, close, untimeException], verbs [SELECT]. Current flow summary -> helpers [env.createTrinoConnection, connection.prepareStatement, repeat, preparedStatement.executeQuery, rs.next], verbs [SELECT].
- Audit status: `verified`

### `TestPreparedStatements`


- Owning migration commit: `Migrate TestPreparedStatements to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestPreparedStatements.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestPreparedStatements.java`
- Class-level environment requirement: `JdbcBasicEnvironment`.
- Class-level tags: `Jdbc`.
- Method inventory complete: Yes. Legacy methods: `9`. Current methods: `9`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `preparedSelectApi`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestPreparedStatements.java` ->
  `preparedSelectApi`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestPreparedStatements.java` ->
  `TestPreparedStatements.preparedSelectApi`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `env.createTrinoConnection`, `ps.setLong`, `ps.setNull`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `param`, `hasNoRows`. Current action shape: `SELECT`, `connection.prepareStatement`, `value`, `ps.executeQuery`, `rs.next`, `rs.getLong`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`, `isFalse`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, param, hasNoRows] vs current [env.createTrinoConnection, connection.prepareStatement, value, ps.setLong, ps.executeQuery, rs.next, rs.getLong, ps.setNull]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat, isTrue, isEqualTo, isFalse]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, param, hasNoRows], verbs [SELECT]. Current flow summary -> helpers [env.createTrinoConnection, connection.prepareStatement, value, ps.setLong, ps.executeQuery, rs.next, rs.getLong, ps.setNull], verbs [SELECT].
- Audit status: `verified`

##### `preparedSelectSql`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestPreparedStatements.java` ->
  `preparedSelectSql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestPreparedStatements.java` ->
  `TestPreparedStatements.preparedSelectSql`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createStatement`, `QueryResult.forResultSet`. Current setup shape: `env.createTrinoConnection`, `connection.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `connection`, `statement.execute`, `statement.executeQuery`, `hasNoRows`. Current action shape: `SELECT`, `statement.execute`, `statement.executeQuery`, `rs.next`, `rs.getLong`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`, `isFalse`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [connection, createStatement, statement.execute, QueryResult.forResultSet, statement.executeQuery, hasNoRows] vs current [env.createTrinoConnection, connection.createStatement, statement.execute, statement.executeQuery, rs.next, rs.getLong]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat, isTrue, isEqualTo, isFalse]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [connection, createStatement, statement.execute, QueryResult.forResultSet, statement.executeQuery, hasNoRows], verbs [SELECT]. Current flow summary -> helpers [env.createTrinoConnection, connection.createStatement, statement.execute, statement.executeQuery, rs.next, rs.getLong], verbs [SELECT].
- Audit status: `verified`

##### `executeImmediateSelectSql`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestPreparedStatements.java` ->
  `executeImmediateSelectSql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestPreparedStatements.java` ->
  `TestPreparedStatements.executeImmediateSelectSql`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `conn.createStatement`, `QueryResult.forResultSet`. Current setup shape: `env.createTrinoConnection`, `connection.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `connection`, `statement.executeQuery`, `hasNoRows`. Current action shape: `SELECT`, `statement.executeQuery`, `rs.next`, `rs.getLong`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`, `isFalse`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [connection, conn.createStatement, QueryResult.forResultSet, statement.executeQuery, hasNoRows] vs current [env.createTrinoConnection, connection.createStatement, statement.executeQuery, rs.next, rs.getLong]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat, isTrue, isEqualTo, isFalse]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [connection, conn.createStatement, QueryResult.forResultSet, statement.executeQuery, hasNoRows], verbs [SELECT]. Current flow summary -> helpers [env.createTrinoConnection, connection.createStatement, statement.executeQuery, rs.next, rs.getLong], verbs [SELECT].
- Audit status: `verified`

##### `preparedInsertVarbinaryApi`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestPreparedStatements.java` ->
  `preparedInsertVarbinaryApi`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestPreparedStatements.java` ->
  `TestPreparedStatements.preparedInsertVarbinaryApi`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `CREATE`, `INSERT`, `env.createTrinoConnection`, `connection.createStatement`, `ps.setNull`, `ps.setBytes`.
- Action parity: Legacy action shape: `mutableTablesState`, `get`, `getNameInDatabase`, `format`, `onTrino`, `executeQuery`, `param`. Current action shape: `SELECT`, `uniqueTableName`, `statement.execute`, `allTypesDdl`, `connection.prepareStatement`, `VALUES`, `ps.executeUpdate`, `statement.executeQuery`, `rs.next`, `rs.getObject`, `rs.wasNull`, `rs.getBytes`.
- Assertion parity: Legacy assertion helpers: `assertColumnTypes`, `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [mutableTablesState, get, getNameInDatabase, format, onTrino, executeQuery, param] vs current [uniqueTableName, env.createTrinoConnection, connection.createStatement, statement.execute, allTypesDdl, connection.prepareStatement, VALUES, ps.setNull, ps.setBytes, ps.executeUpdate, statement.executeQuery, rs.next, rs.getObject, rs.wasNull, rs.getBytes]; SQL verbs differ: legacy [none] vs current [CREATE, DROP, INSERT, SELECT]; assertion helpers differ: legacy [assertColumnTypes, assertThat, containsOnly, row] vs current [assertThat, isTrue, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [mutableTablesState, get, getNameInDatabase, format, onTrino, executeQuery, param], verbs [none]. Current flow summary -> helpers [uniqueTableName, env.createTrinoConnection, connection.createStatement, statement.execute, allTypesDdl, connection.prepareStatement, VALUES, ps.setNull, ps.setBytes, ps.executeUpdate, statement.executeQuery, rs.next, rs.getObject, rs.wasNull, rs.getBytes], verbs [CREATE, DROP, INSERT, SELECT].
- Audit status: `verified`

##### `preparedInsertApi`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestPreparedStatements.java` ->
  `preparedInsertApi`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestPreparedStatements.java` ->
  `TestPreparedStatements.preparedInsertApi`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: none. Current setup shape: `CREATE`, `INSERT`, `env.createTrinoConnection`, `connection.createStatement`, `ps.setByte`, `ps.setShort`, `ps.setInt`, `ps.setLong`, `ps.setFloat`, `ps.setDouble`, `ps.setBigDecimal`, `ps.setTimestamp`, `ps.setDate`, `ps.setString`, `ps.setBoolean`, `ps.setBytes`, `ps.setNull`.
- Action parity: Legacy action shape: `mutableTablesState`, `get`, `getNameInDatabase`, `format`, `onTrino`, `executeQuery`, `param`, `igInteger`, `Float.valueOf`, `BigDecimal.valueOf`, `Timestamp.valueOf`, `Date.valueOf`, `Long.valueOf`, `igDecimal`. Current action shape: `SELECT`, `uniqueTableName`, `statement.execute`, `allTypesDdl`, `connection.prepareStatement`, `VALUES`, `BigDecimal.valueOf`, `igDecimal`, `Timestamp.valueOf`, `Date.valueOf`, `ps.executeUpdate`, `statement.executeQuery`, `COUNT`, `rs.next`, `rs.getInt`, `values`, `rs.getByte`, `rs.getShort`, `rs.getLong`, `rs.getFloat`, `rs.getDouble`, `rs.getBigDecimal`, `isEqualByComparingTo`, `rs.getTimestamp`, `rs.getDate`, `rs.getString`, `rs.getBoolean`, `rs.getBytes`.
- Assertion parity: Legacy assertion helpers: `assertColumnTypes`, `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [mutableTablesState, get, getNameInDatabase, format, onTrino, executeQuery, param, igInteger, Float.valueOf, BigDecimal.valueOf, Timestamp.valueOf, Date.valueOf, Long.valueOf, igDecimal] vs current [uniqueTableName, env.createTrinoConnection, connection.createStatement, statement.execute, allTypesDdl, connection.prepareStatement, VALUES, ps.setByte, ps.setShort, ps.setInt, ps.setLong, ps.setFloat, ps.setDouble, ps.setBigDecimal, BigDecimal.valueOf, igDecimal, ps.setTimestamp, Timestamp.valueOf, ps.setDate, Date.valueOf, ps.setString, ps.setBoolean, ps.setBytes, ps.executeUpdate, ps.setNull, statement.executeQuery, COUNT, rs.next, rs.getInt, values, rs.getByte, rs.getShort, rs.getLong, rs.getFloat, rs.getDouble, rs.getBigDecimal, isEqualByComparingTo, rs.getTimestamp, rs.getDate, rs.getString, rs.getBoolean, rs.getBytes]; SQL verbs differ: legacy [none] vs current [CREATE, DROP, INSERT, SELECT]; assertion helpers differ: legacy [assertColumnTypes, assertThat, containsOnly, row] vs current [assertThat, isTrue, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [mutableTablesState, get, getNameInDatabase, format, onTrino, executeQuery, param, igInteger, Float.valueOf, BigDecimal.valueOf, Timestamp.valueOf, Date.valueOf, Long.valueOf, igDecimal], verbs [none]. Current flow summary -> helpers [uniqueTableName, env.createTrinoConnection, connection.createStatement, statement.execute, allTypesDdl, connection.prepareStatement, VALUES, ps.setByte, ps.setShort, ps.setInt, ps.setLong, ps.setFloat, ps.setDouble, ps.setBigDecimal, BigDecimal.valueOf, igDecimal, ps.setTimestamp, Timestamp.valueOf, ps.setDate, Date.valueOf, ps.setString, ps.setBoolean, ps.setBytes, ps.executeUpdate, ps.setNull, statement.executeQuery, COUNT, rs.next, rs.getInt, values, rs.getByte, rs.getShort, rs.getLong, rs.getFloat, rs.getDouble, rs.getBigDecimal, isEqualByComparingTo, rs.getTimestamp, rs.getDate, rs.getString, rs.getBoolean, rs.getBytes], verbs [CREATE, DROP, INSERT, SELECT].
- Audit status: `verified`

##### `preparedInsertSql`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestPreparedStatements.java` ->
  `preparedInsertSql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestPreparedStatements.java` ->
  `TestPreparedStatements.preparedInsertSql`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `conn.createStatement`, `insertAndVerify`. Current setup shape: `CREATE`, `INSERT`, `env.createTrinoConnection`, `connection.createStatement`.
- Action parity: Legacy action shape: `mutableTablesState`, `get`, `getNameInDatabase`, `format`, `connection`, `statement.execute`. Current action shape: `SELECT`, `uniqueTableName`, `statement.execute`, `allTypesDdl`, `VALUES`, `CAST`, `DECIMAL`, `CHAR`, `statement.executeQuery`, `COUNT`, `rs.next`, `rs.getInt`, `values`, `rs.getByte`, `rs.getShort`, `rs.getLong`, `rs.getFloat`, `rs.getDouble`, `rs.getBigDecimal`, `isEqualByComparingTo`, `BigDecimal.valueOf`, `igDecimal`, `rs.getTimestamp`, `Timestamp.valueOf`, `rs.getDate`, `Date.valueOf`, `rs.getString`, `rs.getBoolean`, `rs.getBytes`. Legacy delegate calls: `mutableTablesState`, `get`, `getNameInDatabase`, `format`. Current delegate calls: none.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: `DROP`.
- Any observed difference, however small: delegate calls differ: legacy [mutableTablesState, get, getNameInDatabase, format] vs current [none]; helper calls differ: legacy [mutableTablesState, get, getNameInDatabase, format, connection, conn.createStatement, statement.execute, insertAndVerify] vs current [uniqueTableName, env.createTrinoConnection, connection.createStatement, statement.execute, allTypesDdl, VALUES, CAST, DECIMAL, CHAR, statement.executeQuery, COUNT, rs.next, rs.getInt, values, rs.getByte, rs.getShort, rs.getLong, rs.getFloat, rs.getDouble, rs.getBigDecimal, isEqualByComparingTo, BigDecimal.valueOf, igDecimal, rs.getTimestamp, Timestamp.valueOf, rs.getDate, Date.valueOf, rs.getString, rs.getBoolean, rs.getBytes]; SQL verbs differ: legacy [none] vs current [CREATE, DROP, INSERT, SELECT]; assertion helpers differ: legacy [none] vs current [assertThat, isTrue, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [mutableTablesState, get, getNameInDatabase, format, connection, conn.createStatement, statement.execute, insertAndVerify], verbs [none]. Current flow summary -> helpers [uniqueTableName, env.createTrinoConnection, connection.createStatement, statement.execute, allTypesDdl, VALUES, CAST, DECIMAL, CHAR, statement.executeQuery, COUNT, rs.next, rs.getInt, values, rs.getByte, rs.getShort, rs.getLong, rs.getFloat, rs.getDouble, rs.getBigDecimal, isEqualByComparingTo, BigDecimal.valueOf, igDecimal, rs.getTimestamp, Timestamp.valueOf, rs.getDate, Date.valueOf, rs.getString, rs.getBoolean, rs.getBytes], verbs [CREATE, DROP, INSERT, SELECT].
- Audit status: `verified`

##### `executeImmediateInsertSql`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestPreparedStatements.java` ->
  `executeImmediateInsertSql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestPreparedStatements.java` ->
  `TestPreparedStatements.executeImmediateInsertSql`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `conn.createStatement`, `insertAndVerify`. Current setup shape: `CREATE`, `INSERT`, `env.createTrinoConnection`, `connection.createStatement`.
- Action parity: Legacy action shape: `mutableTablesState`, `get`, `getNameInDatabase`, `format`, `connection`. Current action shape: `SELECT`, `uniqueTableName`, `statement.execute`, `allTypesDdl`, `VALUES`, `CAST`, `DECIMAL`, `CHAR`, `statement.executeQuery`, `COUNT`, `rs.next`, `rs.getInt`, `values`, `rs.getByte`, `rs.getShort`, `rs.getLong`, `rs.getFloat`, `rs.getDouble`, `rs.getBigDecimal`, `isEqualByComparingTo`, `BigDecimal.valueOf`, `igDecimal`, `rs.getTimestamp`, `Timestamp.valueOf`, `rs.getDate`, `Date.valueOf`, `rs.getString`, `rs.getBoolean`, `rs.getBytes`. Legacy delegate calls: `mutableTablesState`, `get`, `getNameInDatabase`, `format`. Current delegate calls: none.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: `DROP`.
- Any observed difference, however small: delegate calls differ: legacy [mutableTablesState, get, getNameInDatabase, format] vs current [none]; helper calls differ: legacy [mutableTablesState, get, getNameInDatabase, format, connection, conn.createStatement, insertAndVerify] vs current [uniqueTableName, env.createTrinoConnection, connection.createStatement, statement.execute, allTypesDdl, VALUES, CAST, DECIMAL, CHAR, statement.executeQuery, COUNT, rs.next, rs.getInt, values, rs.getByte, rs.getShort, rs.getLong, rs.getFloat, rs.getDouble, rs.getBigDecimal, isEqualByComparingTo, BigDecimal.valueOf, igDecimal, rs.getTimestamp, Timestamp.valueOf, rs.getDate, Date.valueOf, rs.getString, rs.getBoolean, rs.getBytes]; SQL verbs differ: legacy [none] vs current [CREATE, DROP, INSERT, SELECT]; assertion helpers differ: legacy [none] vs current [assertThat, isTrue, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [mutableTablesState, get, getNameInDatabase, format, connection, conn.createStatement, insertAndVerify], verbs [none]. Current flow summary -> helpers [uniqueTableName, env.createTrinoConnection, connection.createStatement, statement.execute, allTypesDdl, VALUES, CAST, DECIMAL, CHAR, statement.executeQuery, COUNT, rs.next, rs.getInt, values, rs.getByte, rs.getShort, rs.getLong, rs.getFloat, rs.getDouble, rs.getBigDecimal, isEqualByComparingTo, BigDecimal.valueOf, igDecimal, rs.getTimestamp, Timestamp.valueOf, rs.getDate, Date.valueOf, rs.getString, rs.getBoolean, rs.getBytes], verbs [CREATE, DROP, INSERT, SELECT].
- Audit status: `verified`

##### `preparedInsertVarbinarySql`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestPreparedStatements.java` ->
  `preparedInsertVarbinarySql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestPreparedStatements.java` ->
  `TestPreparedStatements.preparedInsertVarbinarySql`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createStatement`, `insertAndVerifyVarbinary`. Current setup shape: `CREATE`, `INSERT`, `env.createTrinoConnection`, `connection.createStatement`.
- Action parity: Legacy action shape: `mutableTablesState`, `get`, `getNameInDatabase`, `format`, `connection`, `statement.execute`. Current action shape: `SELECT`, `uniqueTableName`, `statement.execute`, `allTypesDdl`, `VALUES`, `statement.executeQuery`, `rs.next`, `rs.getBytes`. Legacy delegate calls: `mutableTablesState`, `get`, `getNameInDatabase`, `format`. Current delegate calls: none.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: `DROP`.
- Any observed difference, however small: delegate calls differ: legacy [mutableTablesState, get, getNameInDatabase, format] vs current [none]; helper calls differ: legacy [mutableTablesState, get, getNameInDatabase, format, connection, createStatement, statement.execute, insertAndVerifyVarbinary] vs current [uniqueTableName, env.createTrinoConnection, connection.createStatement, statement.execute, allTypesDdl, VALUES, statement.executeQuery, rs.next, rs.getBytes]; SQL verbs differ: legacy [none] vs current [CREATE, DROP, INSERT, SELECT]; assertion helpers differ: legacy [none] vs current [assertThat, isTrue, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [mutableTablesState, get, getNameInDatabase, format, connection, createStatement, statement.execute, insertAndVerifyVarbinary], verbs [none]. Current flow summary -> helpers [uniqueTableName, env.createTrinoConnection, connection.createStatement, statement.execute, allTypesDdl, VALUES, statement.executeQuery, rs.next, rs.getBytes], verbs [CREATE, DROP, INSERT, SELECT].
- Audit status: `verified`

##### `executeImmediateVarbinarySql`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestPreparedStatements.java` ->
  `executeImmediateVarbinarySql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestPreparedStatements.java` ->
  `TestPreparedStatements.executeImmediateVarbinarySql`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcBasicEnvironment`. Routed by source review into `SuiteClients` run 1.
- Tag parity: Current tags: `Jdbc`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `conn.createStatement`, `insertAndVerifyVarbinary`. Current setup shape: `CREATE`, `INSERT`, `env.createTrinoConnection`, `connection.createStatement`.
- Action parity: Legacy action shape: `mutableTablesState`, `get`, `getNameInDatabase`, `format`, `connection`, `statement.execute`. Current action shape: `SELECT`, `uniqueTableName`, `statement.execute`, `allTypesDdl`, `VALUES`, `statement.executeQuery`, `rs.next`, `rs.getBytes`. Legacy delegate calls: `mutableTablesState`, `get`, `getNameInDatabase`, `format`. Current delegate calls: none.
- Assertion parity: Legacy assertion helpers: none. Current assertion helpers: `assertThat`, `isTrue`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: `DROP`.
- Any observed difference, however small: delegate calls differ: legacy [mutableTablesState, get, getNameInDatabase, format] vs current [none]; helper calls differ: legacy [mutableTablesState, get, getNameInDatabase, format, connection, conn.createStatement, statement.execute, insertAndVerifyVarbinary] vs current [uniqueTableName, env.createTrinoConnection, connection.createStatement, statement.execute, allTypesDdl, VALUES, statement.executeQuery, rs.next, rs.getBytes]; SQL verbs differ: legacy [none] vs current [CREATE, DROP, INSERT, SELECT]; assertion helpers differ: legacy [none] vs current [assertThat, isTrue, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [mutableTablesState, get, getNameInDatabase, format, connection, conn.createStatement, statement.execute, insertAndVerifyVarbinary], verbs [none]. Current flow summary -> helpers [uniqueTableName, env.createTrinoConnection, connection.createStatement, statement.execute, allTypesDdl, VALUES, statement.executeQuery, rs.next, rs.getBytes], verbs [CREATE, DROP, INSERT, SELECT].
- Audit status: `verified`
