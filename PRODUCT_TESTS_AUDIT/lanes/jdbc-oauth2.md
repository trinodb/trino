# Lane Audit: Jdbc Oauth2

## Lane Summary

- Audit status: `manual-pass-complete`
- Section start commit: `Add JDBC OAuth2 environments`
- Section end commit: `Remove legacy SuiteOauth2`
- Introduced JUnit suites: `SuiteOauth2`.
- Extended existing suites: none.
- Retired legacy suites: `SuiteOauth2`.
- Environment classes introduced: `JdbcOAuth2Environment`.
- Method status counts: verified `5`, intentional difference `0`, needs follow-up `0`.

## Semantic Audit Status

- Baseline note: detailed method, environment, and suite records below are retained from earlier audit work and must not be treated as final semantic findings until this lane is marked `complete` in `PROGRESS.md`.

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: none beyond the framework baseline documented in the reviewer guide.
- Needs-follow-up methods: none at the per-method body level.

## Environment Semantic Audit
### `JdbcOAuth2BasicEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against legacy `EnvSinglenodeOauth2`, `EnvSinglenodeOauth2HttpProxy`, `EnvSinglenodeOauth2HttpsProxy`, `EnvSinglenodeOauth2AuthenticatedHttpProxy`, and `EnvSinglenodeOauth2AuthenticatedHttpsProxy`, plus the current `JdbcOAuth2BasicEnvironment`.
- Recorded differences:
  - Current JUnit environment keeps the direct OAuth2/Hydra flow, and the branch now restores the legacy HTTP proxy,
    HTTPS proxy, and authenticated proxy variants as distinct environments.
- Reviewer note: the `TestExternalAuthorizerOAuth2` method bodies are preserved, and proxy-path breadth is now
  restored at the environment/suite level.

### `JdbcOAuth2RefreshEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against legacy `EnvSinglenodeOauth2Refresh` and `EnvSinglenodeOidcRefresh`, plus the current `JdbcOAuth2RefreshEnvironment`.
- Recorded differences:
  - Current JUnit environment preserves the refresh-token Hydra flow, and the branch now restores a separate OIDC
    refresh environment.
- Reviewer note: refresh-token method semantics are represented under both legacy refresh environment families.

### `JdbcOAuth2Environment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against the legacy OAuth2 launcher environment family and the current shared base class.
- Recorded differences:
  - Current base environment centralizes Hydra, consent server, Trino HTTPS, and refresh/no-refresh configuration in one Testcontainers base class.
  - This is an approved framework/runtime consolidation, but it does not preserve every legacy environment variant as a distinct top-level class.
- Reviewer note: current base setup is semantically aligned for the direct OAuth2 flows, with the remaining legacy
  breadth restored through dedicated proxy and OIDC subclasses.

## Suite Semantic Audit
### `SuiteOauth2`
- Suite semantic audit status: `complete`
- CI bucket: `auth-and-clients`
- Relationship to lane: `owned by this lane`.
- Legacy/current basis: compared against launcher `SuiteOauth2` plus current `SuiteOauth2` source.
- Recorded differences:
  - Legacy launcher suite had eight environment runs: OAuth2 direct, HTTP proxy, HTTPS proxy, authenticated HTTP proxy, authenticated HTTPS proxy, OIDC direct, OAuth2 refresh, and OIDC refresh.
  - Current JUnit suite now has eight runs: direct OAuth2, direct OIDC, HTTP proxy, HTTPS proxy, authenticated HTTP
    proxy, authenticated HTTPS proxy, OAuth2 refresh, and OIDC refresh.
- Reviewer note: method bodies remain faithful and the legacy suite/environment breadth is now restored.

## Ported Test Classes

### `TestExternalAuthorizerOAuth2`


- Owning migration commit: `Migrate TestExternalAuthorizerOAuth2 to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestExternalAuthorizerOAuth2.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestExternalAuthorizerOAuth2.java`
- Class-level environment requirement: `JdbcOAuth2BasicEnvironment`.
- Class-level tags: `Oauth2`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `2`. Current methods: `2`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `shouldAuthenticateAndExecuteQuery`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestExternalAuthorizerOAuth2.java` ->
  `shouldAuthenticateAndExecuteQuery`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestExternalAuthorizerOAuth2.java` ->
  `TestExternalAuthorizerOAuth2.shouldAuthenticateAndExecuteQuery`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcOAuth2BasicEnvironment`. Routed by source review into `SuiteOauth2` run 1.
- Tag parity: Current tags: `Oauth2`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `forResultSet`. Current setup shape: `TestingRedirectHandlerInjector.setRedirectHandler`, `env.createRedirectHandler`, `env.createTrinoConnection`, `QueryResult.forResultSet`.
- Action parity: Legacy action shape: `SELECT`, `prepareHandler`, `DriverManager.getConnection`, `connection.prepareStatement`, `statement.executeQuery`, `matches`. Current action shape: `SELECT`, `connection.prepareStatement`, `statement.executeQuery`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [prepareHandler, DriverManager.getConnection, connection.prepareStatement, statement.executeQuery, forResultSet, matches] vs current [TestingRedirectHandlerInjector.setRedirectHandler, env.createRedirectHandler, env.createTrinoConnection, connection.prepareStatement, statement.executeQuery, QueryResult.forResultSet]; assertion helpers differ: legacy [assertThat] vs current [assertThat, containsOnly]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [prepareHandler, DriverManager.getConnection, connection.prepareStatement, statement.executeQuery, forResultSet, matches], verbs [SELECT]. Current flow summary -> helpers [TestingRedirectHandlerInjector.setRedirectHandler, env.createRedirectHandler, env.createTrinoConnection, connection.prepareStatement, statement.executeQuery, QueryResult.forResultSet], verbs [SELECT].
- Audit status: `verified`

##### `shouldAuthenticateAfterTokenExpires`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestExternalAuthorizerOAuth2.java` ->
  `shouldAuthenticateAfterTokenExpires`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestExternalAuthorizerOAuth2.java` ->
  `TestExternalAuthorizerOAuth2.shouldAuthenticateAfterTokenExpires`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcOAuth2BasicEnvironment`. Routed by source review into `SuiteOauth2` run 1.
- Tag parity: Current tags: `Oauth2`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `forResultSet`. Current setup shape: `TestingRedirectHandlerInjector.setRedirectHandler`, `env.createRedirectHandler`, `env.createTrinoConnection`, `QueryResult.forResultSet`.
- Action parity: Legacy action shape: `SELECT`, `prepareHandler`, `DriverManager.getConnection`, `connection.prepareStatement`, `statement.executeQuery`, `matches`, `SECONDS.sleep`, `repeatedStatement.executeQuery`. Current action shape: `SELECT`, `connection.prepareStatement`, `statement.executeQuery`, `SECONDS.sleep`, `repeatedStatement.executeQuery`.
- Assertion parity: Legacy assertion helpers: `assertThat`. Current assertion helpers: `assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [prepareHandler, DriverManager.getConnection, connection.prepareStatement, statement.executeQuery, forResultSet, matches, SECONDS.sleep, repeatedStatement.executeQuery] vs current [TestingRedirectHandlerInjector.setRedirectHandler, env.createRedirectHandler, env.createTrinoConnection, connection.prepareStatement, statement.executeQuery, QueryResult.forResultSet, SECONDS.sleep, repeatedStatement.executeQuery]; assertion helpers differ: legacy [assertThat] vs current [assertThat, containsOnly]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [prepareHandler, DriverManager.getConnection, connection.prepareStatement, statement.executeQuery, forResultSet, matches, SECONDS.sleep, repeatedStatement.executeQuery], verbs [SELECT]. Current flow summary -> helpers [TestingRedirectHandlerInjector.setRedirectHandler, env.createRedirectHandler, env.createTrinoConnection, connection.prepareStatement, statement.executeQuery, QueryResult.forResultSet, SECONDS.sleep, repeatedStatement.executeQuery], verbs [SELECT].
- Audit status: `verified`

### `TestExternalAuthorizerOAuth2RefreshToken`


- Owning migration commit: `Migrate TestExternalAuthorizerOAuth2RefreshToken to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestExternalAuthorizerOAuth2RefreshToken.java`
- Legacy class removed in same migration commit:
  -
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestExternalAuthorizerOAuth2RefreshToken.java`
- Class-level environment requirement: `JdbcOAuth2RefreshEnvironment`.
- Class-level tags: `Oauth2Refresh`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `3`. Current methods: `3`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: None identified at class scope.
- Method statuses present: `verified`.

#### Methods

##### `shouldRefreshTokenAfterTokenExpire`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestExternalAuthorizerOAuth2RefreshToken.java` ->
  `shouldRefreshTokenAfterTokenExpire`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestExternalAuthorizerOAuth2RefreshToken.java` ->
  `TestExternalAuthorizerOAuth2RefreshToken.shouldRefreshTokenAfterTokenExpire`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcOAuth2RefreshEnvironment`. Routed by source review into `SuiteOauth2` run 2.
- Tag parity: Current tags: `Oauth2Refresh`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `TestingRedirectHandlerInjector.setRedirectHandler`, `forResultSet`. Current setup shape: `TestingRedirectHandlerInjector.setRedirectHandler`, `env.createTrinoConnection`, `QueryResult.forResultSet`.
- Action parity: Legacy action shape: `SELECT`, `ountingRedirectHandler`, `DriverManager.getConnection`, `connection.prepareStatement`, `statement.executeQuery`, `matches`, `redirectHandler.getRedirectCount`, `SECONDS.sleep`, `repeatedStatement.executeQuery`. Current action shape: `SELECT`, `ountingRedirectHandler`, `env.getHttpClient`, `connection.prepareStatement`, `statement.executeQuery`, `redirectHandler.getRedirectCount`, `SECONDS.sleep`, `repeatedStatement.executeQuery`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `containsOnly`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [ountingRedirectHandler, TestingRedirectHandlerInjector.setRedirectHandler, DriverManager.getConnection, connection.prepareStatement, statement.executeQuery, forResultSet, matches, redirectHandler.getRedirectCount, SECONDS.sleep, repeatedStatement.executeQuery] vs current [ountingRedirectHandler, env.getHttpClient, TestingRedirectHandlerInjector.setRedirectHandler, env.createTrinoConnection, connection.prepareStatement, statement.executeQuery, QueryResult.forResultSet, redirectHandler.getRedirectCount, SECONDS.sleep, repeatedStatement.executeQuery]; assertion helpers differ: legacy [assertThat, isEqualTo] vs current [assertThat, containsOnly, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [ountingRedirectHandler, TestingRedirectHandlerInjector.setRedirectHandler, DriverManager.getConnection, connection.prepareStatement, statement.executeQuery, forResultSet, matches, redirectHandler.getRedirectCount, SECONDS.sleep, repeatedStatement.executeQuery], verbs [SELECT]. Current flow summary -> helpers [ountingRedirectHandler, env.getHttpClient, TestingRedirectHandlerInjector.setRedirectHandler, env.createTrinoConnection, connection.prepareStatement, statement.executeQuery, QueryResult.forResultSet, redirectHandler.getRedirectCount, SECONDS.sleep, repeatedStatement.executeQuery], verbs [SELECT].
- Audit status: `verified`

##### `shouldAuthenticateAfterRefreshTokenExpires`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestExternalAuthorizerOAuth2RefreshToken.java` ->
  `shouldAuthenticateAfterRefreshTokenExpires`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestExternalAuthorizerOAuth2RefreshToken.java` ->
  `TestExternalAuthorizerOAuth2RefreshToken.shouldAuthenticateAfterRefreshTokenExpires`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcOAuth2RefreshEnvironment`. Routed by source review into `SuiteOauth2` run 2.
- Tag parity: Current tags: `Oauth2Refresh`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `TestingRedirectHandlerInjector.setRedirectHandler`, `forResultSet`. Current setup shape: `TestingRedirectHandlerInjector.setRedirectHandler`, `env.createTrinoConnection`, `QueryResult.forResultSet`.
- Action parity: Legacy action shape: `SELECT`, `ountingRedirectHandler`, `DriverManager.getConnection`, `connection.prepareStatement`, `statement.executeQuery`, `matches`, `redirectHandler.getRedirectCount`, `expires`, `SECONDS.sleep`, `repeatedStatement.executeQuery`. Current action shape: `SELECT`, `ountingRedirectHandler`, `env.getHttpClient`, `connection.prepareStatement`, `statement.executeQuery`, `redirectHandler.getRedirectCount`, `expires`, `SECONDS.sleep`, `repeatedStatement.executeQuery`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `containsOnly`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [ountingRedirectHandler, TestingRedirectHandlerInjector.setRedirectHandler, DriverManager.getConnection, connection.prepareStatement, statement.executeQuery, forResultSet, matches, redirectHandler.getRedirectCount, expires, SECONDS.sleep, repeatedStatement.executeQuery] vs current [ountingRedirectHandler, env.getHttpClient, TestingRedirectHandlerInjector.setRedirectHandler, env.createTrinoConnection, connection.prepareStatement, statement.executeQuery, QueryResult.forResultSet, redirectHandler.getRedirectCount, expires, SECONDS.sleep, repeatedStatement.executeQuery]; assertion helpers differ: legacy [assertThat, isEqualTo] vs current [assertThat, containsOnly, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [ountingRedirectHandler, TestingRedirectHandlerInjector.setRedirectHandler, DriverManager.getConnection, connection.prepareStatement, statement.executeQuery, forResultSet, matches, redirectHandler.getRedirectCount, expires, SECONDS.sleep, repeatedStatement.executeQuery], verbs [SELECT]. Current flow summary -> helpers [ountingRedirectHandler, env.getHttpClient, TestingRedirectHandlerInjector.setRedirectHandler, env.createTrinoConnection, connection.prepareStatement, statement.executeQuery, QueryResult.forResultSet, redirectHandler.getRedirectCount, expires, SECONDS.sleep, repeatedStatement.executeQuery], verbs [SELECT].
- Audit status: `verified`

##### `shouldAuthenticateAfterIssuedTokenExpires`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/jdbc/TestExternalAuthorizerOAuth2RefreshToken.java` ->
  `shouldAuthenticateAfterIssuedTokenExpires`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/jdbc/TestExternalAuthorizerOAuth2RefreshToken.java` ->
  `TestExternalAuthorizerOAuth2RefreshToken.shouldAuthenticateAfterIssuedTokenExpires`
- Mapping type: `direct`
- Environment parity: Current class requires `JdbcOAuth2RefreshEnvironment`. Routed by source review into `SuiteOauth2` run 2.
- Tag parity: Current tags: `Oauth2Refresh`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `TestingRedirectHandlerInjector.setRedirectHandler`, `forResultSet`. Current setup shape: `TestingRedirectHandlerInjector.setRedirectHandler`, `env.createTrinoConnection`, `QueryResult.forResultSet`.
- Action parity: Legacy action shape: `SELECT`, `ountingRedirectHandler`, `DriverManager.getConnection`, `connection.prepareStatement`, `statement.executeQuery`, `matches`, `redirectHandler.getRedirectCount`, `SECONDS.sleep`, `repeatedStatement.executeQuery`. Current action shape: `SELECT`, `ountingRedirectHandler`, `env.getHttpClient`, `connection.prepareStatement`, `statement.executeQuery`, `redirectHandler.getRedirectCount`, `token.timeout`, `SECONDS.sleep`, `repeatedStatement.executeQuery`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `isEqualTo`. Current assertion helpers: `assertThat`, `containsOnly`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: helper calls differ: legacy [ountingRedirectHandler, TestingRedirectHandlerInjector.setRedirectHandler, DriverManager.getConnection, connection.prepareStatement, statement.executeQuery, forResultSet, matches, redirectHandler.getRedirectCount, SECONDS.sleep, repeatedStatement.executeQuery] vs current [ountingRedirectHandler, env.getHttpClient, TestingRedirectHandlerInjector.setRedirectHandler, env.createTrinoConnection, connection.prepareStatement, statement.executeQuery, QueryResult.forResultSet, redirectHandler.getRedirectCount, token.timeout, SECONDS.sleep, repeatedStatement.executeQuery]; assertion helpers differ: legacy [assertThat, isEqualTo] vs current [assertThat, containsOnly, isEqualTo]
- Known intentional difference: None.
- Reviewer note: Legacy flow summary -> helpers [ountingRedirectHandler, TestingRedirectHandlerInjector.setRedirectHandler, DriverManager.getConnection, connection.prepareStatement, statement.executeQuery, forResultSet, matches, redirectHandler.getRedirectCount, SECONDS.sleep, repeatedStatement.executeQuery], verbs [SELECT]. Current flow summary -> helpers [ountingRedirectHandler, env.getHttpClient, TestingRedirectHandlerInjector.setRedirectHandler, env.createTrinoConnection, connection.prepareStatement, statement.executeQuery, QueryResult.forResultSet, redirectHandler.getRedirectCount, token.timeout, SECONDS.sleep, repeatedStatement.executeQuery], verbs [SELECT].
- Audit status: `verified`
