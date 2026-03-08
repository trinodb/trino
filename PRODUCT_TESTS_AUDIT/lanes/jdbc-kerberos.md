# Lane Audit: Jdbc Kerberos

## Lane Summary

- Audit status: `manual-pass-complete`
- Section start commit: `Add JDBC Kerberos environment`
- Section end commit: `Migrate TestKerberosConstrainedDelegationJdbc to JUnit`
- Introduced JUnit suites: `SuiteJdbcKerberos`.
- Extended existing suites: none.
- Retired legacy suites: none.
- Environment classes introduced: `JdbcKerberosEnvironment`.
- Method status counts: verified `4`, intentional difference `0`, needs follow-up `0`.

## Semantic Audit Status

- Baseline note: detailed method, environment, and suite records below are retained from earlier audit work and must not be treated as final semantic findings until this lane is marked `complete` in `PROGRESS.md`.

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: `Kerberos deployment cleanup without intended coverage change`.
- Needs-follow-up methods: none identified in the literal method-body comparison.

## Environment Semantic Audit
### `JdbcKerberosEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against the `EnvMultinodeTlsKerberosDelegation` run in legacy `Suite3`, the current `JdbcKerberosEnvironment`, and the current `SuiteJdbcKerberos` source.
- Recorded differences:
  - Legacy constrained-delegation JDBC coverage ran inside a broader multinode TLS/Kerberos-delegation launcher environment that also carried plain `JDBC` coverage.
  - Current JUnit environment isolates constrained delegation in a dedicated standalone KDC + Trino Testcontainers environment.
  - This falls within the approved Kerberos deployment cleanup set and does not reduce the specific constrained-delegation method coverage.
- Reviewer note: environment topology changed materially, but the change is deliberate isolation rather than an identified loss of constrained-delegation test intent.

## Suite Semantic Audit
### `SuiteJdbcKerberos`
- Suite semantic audit status: `complete`
- CI bucket: `auth-and-clients`
- Relationship to lane: `owned by this lane`.
- Legacy/current basis: compared against the effective `JDBC_KERBEROS_CONSTRAINED_DELEGATION` run in launcher `Suite3` and current `SuiteJdbcKerberos` source.
- Recorded differences:
  - Legacy constrained-delegation JDBC coverage was one run within aggregate `Suite3` on `EnvMultinodeTlsKerberosDelegation` together with plain `JDBC` coverage.
  - Current JUnit suite promotes the constrained-delegation tests into their own dedicated suite and environment.
- Reviewer note: this is a suite extraction and isolation, not a recorded coverage loss for the constrained-delegation methods.

## Ported Test Classes

### `TestKerberosConstrainedDelegationJdbc`


- Owning migration commit: `Migrate TestKerberosConstrainedDelegationJdbc to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestKerberosConstrainedDelegationJdbc.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestKerberosConstrainedDelegationJdbc.java`
- Class-level environment requirement: `JdbcKerberosEnvironment`.
- Class-level tags: `JdbcKerberosConstrainedDelegation`.
- Method inventory complete: Yes. Legacy methods: `4`. Current methods: `4`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `Kerberos deployment cleanup without intended coverage change`
- Method statuses present: `verified`.

#### Methods

##### `testSelectConstrainedDelegationKerberos`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestKerberosConstrainedDelegationJdbc.java` ->
  `testSelectConstrainedDelegationKerberos`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestKerberosConstrainedDelegationJdbc.java` ->
  `TestKerberosConstrainedDelegationJdbc.testSelectConstrainedDelegationKerberos`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcKerberosEnvironment`. Routed by source review into `SuiteJdbcKerberos` run 1.
- Tag parity: Current tags: `JdbcKerberosConstrainedDelegation`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createGssCredential`, `forResultSet`. Current setup shape: `env.createGssCredential`, `env.createConnectionWithKerberosDelegation`, `QueryResult.forResultSet`.
- Action parity: Legacy action shape: `SELECT`, `roperties`, `driverProperties.put`, `DriverManager.getConnection`, `connection.prepareStatement`, `statement.executeQuery`, `matches`, `credential.dispose`. Current action shape: `SELECT`, `connection.prepareStatement`, `statement.executeQuery`, `credential.dispose`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [roperties, createGssCredential, driverProperties.put, DriverManager.getConnection, connection.prepareStatement, statement.executeQuery, forResultSet, matches, credential.dispose] vs current [env.createGssCredential, env.createConnectionWithKerberosDelegation, connection.prepareStatement, statement.executeQuery, QueryResult.forResultSet, credential.dispose]; assertion helpers differ: legacy [assertThat] vs current [assertThat, containsOnly]
- Known intentional difference: `Kerberos deployment cleanup without intended coverage change`
- Reviewer note: Legacy flow summary -> helpers [roperties, createGssCredential, driverProperties.put, DriverManager.getConnection, connection.prepareStatement, statement.executeQuery, forResultSet, matches, credential.dispose], verbs [SELECT]. Current flow summary -> helpers [env.createGssCredential, env.createConnectionWithKerberosDelegation, connection.prepareStatement, statement.executeQuery, QueryResult.forResultSet, credential.dispose], verbs [SELECT].
- Audit status: `verified`

##### `testCtasConstrainedDelegationKerberos`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestKerberosConstrainedDelegationJdbc.java` ->
  `testCtasConstrainedDelegationKerberos`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestKerberosConstrainedDelegationJdbc.java` ->
  `TestKerberosConstrainedDelegationJdbc.testCtasConstrainedDelegationKerberos`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcKerberosEnvironment`. Routed by source review into `SuiteJdbcKerberos` run 1.
- Tag parity: Current tags: `JdbcKerberosConstrainedDelegation`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `createGssCredential`. Current setup shape: `CREATE`, `env.createGssCredential`, `env.createConnectionWithKerberosDelegation`, `connection.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `roperties`, `driverProperties.put`, `DriverManager.getConnection`, `connection.prepareStatement`, `format`, `statement.executeUpdate`, `credential.dispose`. Current action shape: `SELECT`, `connection.prepareStatement`, `statement.executeUpdate`, `statement.execute`, `credential.dispose`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: `DROP`, `cleanupCredential.dispose`.
- Any observed difference, however small: helper calls differ: legacy [roperties, createGssCredential, driverProperties.put, DriverManager.getConnection, connection.prepareStatement, format, statement.executeUpdate, credential.dispose] vs current [env.createGssCredential, env.createConnectionWithKerberosDelegation, connection.prepareStatement, statement.executeUpdate, connection.createStatement, statement.execute, cleanupCredential.dispose, credential.dispose]; SQL verbs differ: legacy [CREATE, SELECT] vs current [CREATE, SELECT, DROP]
- Known intentional difference: `Kerberos deployment cleanup without intended coverage change`
- Reviewer note: Legacy flow summary -> helpers [roperties, createGssCredential, driverProperties.put, DriverManager.getConnection, connection.prepareStatement, format, statement.executeUpdate, credential.dispose], verbs [CREATE, SELECT]. Current flow summary -> helpers [env.createGssCredential, env.createConnectionWithKerberosDelegation, connection.prepareStatement, statement.executeUpdate, connection.createStatement, statement.execute, cleanupCredential.dispose, credential.dispose], verbs [CREATE, SELECT, DROP].
- Audit status: `verified`

##### `testQueryOnDisposedCredential`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestKerberosConstrainedDelegationJdbc.java` ->
  `testQueryOnDisposedCredential`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestKerberosConstrainedDelegationJdbc.java` ->
  `TestKerberosConstrainedDelegationJdbc.testQueryOnDisposedCredential`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcKerberosEnvironment`. Routed by source review into `SuiteJdbcKerberos` run 1.
- Tag parity: Current tags: `JdbcKerberosConstrainedDelegation`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createGssCredential`. Current setup shape: `env.createGssCredential`, `env.createConnectionWithKerberosDelegation`.
- Action parity: Legacy action shape: `SELECT`, `roperties`, `credential.dispose`, `driverProperties.put`, `DriverManager.getConnection`, `connection.prepareStatement`, `cause`. Current action shape: `SELECT`, `credential.dispose`, `connection.prepareStatement`, `cause`.
- Assertion parity: Legacy assertion helpers: `assertThatThrownBy`, `isInstanceOf`, `hasMessageContaining`. Current assertion helpers: `assertThatThrownBy`, `isInstanceOf`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [roperties, createGssCredential, credential.dispose, driverProperties.put, DriverManager.getConnection, connection.prepareStatement, cause] vs current [env.createGssCredential, credential.dispose, env.createConnectionWithKerberosDelegation, connection.prepareStatement, cause]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [roperties, createGssCredential, credential.dispose, driverProperties.put, DriverManager.getConnection, connection.prepareStatement, cause], verbs [SELECT]. Current flow summary -> helpers [env.createGssCredential, credential.dispose, env.createConnectionWithKerberosDelegation, connection.prepareStatement, cause], verbs [SELECT].
- Audit status: `verified`

##### `testQueryOnExpiredCredential`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestKerberosConstrainedDelegationJdbc.java` ->
  `testQueryOnExpiredCredential`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestKerberosConstrainedDelegationJdbc.java` ->
  `TestKerberosConstrainedDelegationJdbc.testQueryOnExpiredCredential`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcKerberosEnvironment`. Routed by source review into `SuiteJdbcKerberos` run 1.
- Tag parity: Current tags: `JdbcKerberosConstrainedDelegation`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `createGssCredential`. Current setup shape: `env.createGssCredential`, `env.createConnectionWithKerberosDelegation`.
- Action parity: Legacy action shape: `SELECT`, `roperties`, `Thread.sleep`, `s`, `credential.getRemainingLifetime`, `isLessThanOrEqualTo`, `driverProperties.put`, `DriverManager.getConnection`, `connection.prepareStatement`, `credential.dispose`. Current action shape: `SELECT`, `Thread.sleep`, `s`, `credential.getRemainingLifetime`, `isLessThanOrEqualTo`, `connection.prepareStatement`, `credential.dispose`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `assertThatThrownBy`, `isInstanceOf`, `hasMessageContaining`. Current assertion helpers: `assertThat`, `assertThatThrownBy`, `isInstanceOf`, `hasMessageContaining`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [roperties, createGssCredential, Thread.sleep, s, credential.getRemainingLifetime, isLessThanOrEqualTo, driverProperties.put, DriverManager.getConnection, connection.prepareStatement, credential.dispose] vs current [env.createGssCredential, Thread.sleep, s, credential.getRemainingLifetime, isLessThanOrEqualTo, env.createConnectionWithKerberosDelegation, connection.prepareStatement, credential.dispose]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [roperties, createGssCredential, Thread.sleep, s, credential.getRemainingLifetime, isLessThanOrEqualTo, driverProperties.put, DriverManager.getConnection, connection.prepareStatement, credential.dispose], verbs [SELECT]. Current flow summary -> helpers [env.createGssCredential, Thread.sleep, s, credential.getRemainingLifetime, isLessThanOrEqualTo, env.createConnectionWithKerberosDelegation, connection.prepareStatement, credential.dispose], verbs [SELECT].
- Audit status: `verified`
