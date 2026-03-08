# Suite Audit: SuiteLdap

## Suite Summary

- Baseline note: the detailed coverage notes below are retained from earlier audit work and must not be treated as final semantic findings until this suite is marked `complete` in `PROGRESS.md`.

- Purpose: JUnit suite for Ldap coverage.
- Owning lane: `ldap`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteLdap.java`
- CI bucket: `auth-and-clients`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `LdapBasicEnvironment`
- Include tags: `Ldap`.
- Exclude tags: none.
- Expected mapped classes covered: `TestTrinoLdapCli`, `TestLdapTrinoJdbc`.
- Expected mapped methods covered: `30` method(s).

### Run 2

- Run name: `default`
- Environment: `LdapAndFileEnvironment`
- Include tags: `LdapAndFile`.
- Exclude tags: none.
- Expected mapped classes covered: `TestLdapAndFileCli`, `TestLdapAndFileTrinoJdbc`.
- Expected mapped methods covered: `8` method(s).

### Run 3

- Run name: `default`
- Environment: `LdapAndFileRerunEnvironment`
- Include tags: `Ldap`.
- Exclude tags: `LdapMultipleBinds`.
- Expected mapped classes covered: `TestTrinoLdapCli`, `TestLdapTrinoJdbc`.
- Expected mapped methods covered: `30` method(s).

### Run 4

- Run name: `default`
- Environment: `LdapInsecureEnvironment`
- Include tags: `Ldap`.
- Exclude tags: none.
- Expected mapped classes covered: `TestTrinoLdapCli`, `TestLdapTrinoJdbc`.
- Expected mapped methods covered: `30` method(s).

### Run 5

- Run name: `default`
- Environment: `LdapReferralsEnvironment`
- Include tags: `Ldap`.
- Exclude tags: none.
- Expected mapped classes covered: `TestTrinoLdapCli`, `TestLdapTrinoJdbc`.
- Expected mapped methods covered: `30` method(s).

### Run 6

- Run name: `default`
- Environment: `LdapBindDnEnvironment`
- Include tags: `Ldap`.
- Exclude tags: `LdapMultipleBinds`.
- Expected mapped classes covered: `TestTrinoLdapCli`, `TestLdapTrinoJdbc`.
- Expected mapped methods covered: `28` method(s).

## Legacy Comparison

- Legacy launcher suite: `testing/trino-product-tests-launcher/src/main/java/io/trino/tests/product/launcher/suite/suites/SuiteLdap.java`
- Legacy launcher runs:
  - `EnvSinglenodeLdap` with `LDAP`
  - `EnvSinglenodeLdapAndFile` with `LDAP`, `LDAP_AND_FILE`, `LDAP_CLI`, `LDAP_AND_FILE_CLI`
  - `EnvSinglenodeLdapInsecure` with `LDAP`
  - `EnvSinglenodeLdapReferrals` with `LDAP`
  - `EnvSinglenodeLdapBindDn` with `LDAP` excluding `LDAP_MULTIPLE_BINDS`

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `auth-and-clients`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteLdap`

## Parity Checklist

- Legacy suite or lane source: `ldap` lane and the corresponding legacy launcher coverage.
- Current suite class: `SuiteLdap`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `4`
- Expected migrated method count: `38`
- Expected migrated classes covered: `TestLdapAndFileCli`, `TestLdapAndFileTrinoJdbc`, `TestLdapTrinoJdbc`,
  `TestTrinoLdapCli`.
- Expected migrated methods covered: `TestLdapAndFileCli.shouldRunQueryForAnotherUserWithOnlyFileAuthenticator`,
  `TestLdapAndFileCli.shouldRunQueryForFileOnlyUser`, `TestLdapAndFileCli.shouldRunQueryWithFileAuthenticator`,
  `TestLdapAndFileCli.shouldRunQueryWithLdapAuthenticator`,
  `TestLdapAndFileTrinoJdbc.shouldRunQueryForAnotherUserWithOnlyFileAuthenticator`,
  `TestLdapAndFileTrinoJdbc.shouldRunQueryForFileOnlyUser`,
  `TestLdapAndFileTrinoJdbc.shouldRunQueryWithFileAuthenticator`,
  `TestLdapAndFileTrinoJdbc.shouldRunQueryWithLdapAuthenticator`, `TestLdapTrinoJdbc.shouldFailForIncorrectTrustStore`,
  `TestLdapTrinoJdbc.shouldFailForUserWithColon`, `TestLdapTrinoJdbc.shouldFailQueryForEmptyUser`,
  `TestLdapTrinoJdbc.shouldFailQueryForFailedBind`, `TestLdapTrinoJdbc.shouldFailQueryForLdapUserInChildGroup`,
  `TestLdapTrinoJdbc.shouldFailQueryForLdapUserInParentGroup`,
  `TestLdapTrinoJdbc.shouldFailQueryForLdapWithoutPassword`, `TestLdapTrinoJdbc.shouldFailQueryForLdapWithoutSsl`,
  `TestLdapTrinoJdbc.shouldFailQueryForOrphanLdapUser`, `TestLdapTrinoJdbc.shouldFailQueryForWrongLdapPassword`,
  `TestLdapTrinoJdbc.shouldFailQueryForWrongLdapUser`, `TestLdapTrinoJdbc.shouldRunQueryWithAlternativeBind`,
  `TestLdapTrinoJdbc.shouldRunQueryWithLdap`, `TestTrinoLdapCli.shouldFailForIncorrectTrustStore`,
  `TestTrinoLdapCli.shouldFailForUserWithColon`, `TestTrinoLdapCli.shouldFailQueryForEmptyUser`,
  `TestTrinoLdapCli.shouldFailQueryForFailedBind`, `TestTrinoLdapCli.shouldFailQueryForLdapUserInChildGroup`,
  `TestTrinoLdapCli.shouldFailQueryForLdapUserInParentGroup`, `TestTrinoLdapCli.shouldFailQueryForLdapWithoutHttps`,
  `TestTrinoLdapCli.shouldFailQueryForLdapWithoutPassword`, `TestTrinoLdapCli.shouldFailQueryForOrphanLdapUser` ....
- Observed differences:
  - Current suite restores the legacy five launcher variants plus the legacy combined-authenticator rerun breadth
    through `LdapAndFileRerunEnvironment`.
- Parity status: `verified`
