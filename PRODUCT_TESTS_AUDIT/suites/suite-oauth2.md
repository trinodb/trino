# Suite Audit: SuiteOauth2

## Suite Summary

- Baseline note: the detailed coverage notes below are retained from earlier audit work and must not be treated as final semantic findings until this suite is marked `complete` in `PROGRESS.md`.

- Purpose: JUnit suite for Oauth2 coverage.
- Owning lane: `jdbc-oauth2`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteOauth2.java`
- CI bucket: `auth-and-clients`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `JdbcOAuth2BasicEnvironment`
- Include tags: `Oauth2`.
- Exclude tags: none.
- Expected mapped classes covered: `TestExternalAuthorizerOAuth2`.
- Expected mapped methods covered: `2` method(s).

### Run 2

- Run name: `default`
- Environment: `JdbcOidcEnvironment`
- Include tags: `Oauth2`.
- Exclude tags: none.
- Expected mapped classes covered: `TestExternalAuthorizerOAuth2`.
- Expected mapped methods covered: `2` method(s).

### Run 3

- Run name: `default`
- Environment: `JdbcOAuth2HttpProxyEnvironment`
- Include tags: `Oauth2`.
- Exclude tags: none.
- Expected mapped classes covered: `TestExternalAuthorizerOAuth2`.
- Expected mapped methods covered: `2` method(s).

### Run 4

- Run name: `default`
- Environment: `JdbcOAuth2HttpsProxyEnvironment`
- Include tags: `Oauth2`.
- Exclude tags: none.
- Expected mapped classes covered: `TestExternalAuthorizerOAuth2`.
- Expected mapped methods covered: `2` method(s).

### Run 5

- Run name: `default`
- Environment: `JdbcOAuth2AuthenticatedHttpProxyEnvironment`
- Include tags: `Oauth2`.
- Exclude tags: none.
- Expected mapped classes covered: `TestExternalAuthorizerOAuth2`.
- Expected mapped methods covered: `2` method(s).

### Run 6

- Run name: `default`
- Environment: `JdbcOAuth2AuthenticatedHttpsProxyEnvironment`
- Include tags: `Oauth2`.
- Exclude tags: none.
- Expected mapped classes covered: `TestExternalAuthorizerOAuth2`.
- Expected mapped methods covered: `2` method(s).

### Run 7

- Run name: `default`
- Environment: `JdbcOAuth2RefreshEnvironment`
- Include tags: `Oauth2Refresh`.
- Exclude tags: none.
- Expected mapped classes covered: `TestExternalAuthorizerOAuth2RefreshToken`.
- Expected mapped methods covered: `3` method(s).

### Run 8

- Run name: `default`
- Environment: `JdbcOidcRefreshEnvironment`
- Include tags: `Oauth2Refresh`.
- Exclude tags: none.
- Expected mapped classes covered: `TestExternalAuthorizerOAuth2RefreshToken`.
- Expected mapped methods covered: `3` method(s).

## Legacy Comparison

- Legacy launcher suite: `testing/trino-product-tests-launcher/src/main/java/io/trino/tests/product/launcher/suite/suites/SuiteOauth2.java`
- Legacy launcher runs:
  - `EnvSinglenodeOauth2`
  - `EnvSinglenodeOauth2HttpProxy`
  - `EnvSinglenodeOauth2HttpsProxy`
  - `EnvSinglenodeOauth2AuthenticatedHttpProxy`
  - `EnvSinglenodeOauth2AuthenticatedHttpsProxy`
  - `EnvSinglenodeOidc`
  - `EnvSinglenodeOauth2Refresh`
  - `EnvSinglenodeOidcRefresh`
- Current suite restores the direct OAuth2, direct OIDC, HTTP proxy, HTTPS proxy, authenticated HTTP proxy,
  authenticated HTTPS proxy, OAuth2 refresh, and OIDC refresh runs.

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `auth-and-clients`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteOauth2`

## Parity Checklist

- Legacy suite or lane source: `jdbc-oauth2` lane and the corresponding legacy launcher coverage.
- Current suite class: `SuiteOauth2`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `2`
- Expected migrated method count: `5`
- Expected migrated classes covered: `TestExternalAuthorizerOAuth2`, `TestExternalAuthorizerOAuth2RefreshToken`.
- Expected migrated methods covered: `TestExternalAuthorizerOAuth2.shouldAuthenticateAfterTokenExpires`,
  `TestExternalAuthorizerOAuth2.shouldAuthenticateAndExecuteQuery`,
  `TestExternalAuthorizerOAuth2RefreshToken.shouldAuthenticateAfterIssuedTokenExpires`,
  `TestExternalAuthorizerOAuth2RefreshToken.shouldAuthenticateAfterRefreshTokenExpires`,
  `TestExternalAuthorizerOAuth2RefreshToken.shouldRefreshTokenAfterTokenExpire`.
- Observed differences:
  - Current suite keeps the current JUnit environment split, but it now restores all eight legacy launcher execution
    paths.
- Parity status: `verified`
