# Suite Audit: SuiteJdbcKerberos

## Suite Summary

- Baseline note: the detailed coverage notes below are retained from earlier audit work and must not be treated as final semantic findings until this suite is marked `complete` in `PROGRESS.md`.

- Purpose: JUnit suite for JdbcKerberos coverage.
- Owning lane: `jdbc-kerberos`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteJdbcKerberos.java`
- CI bucket: `auth-and-clients`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `JdbcKerberosEnvironment`
- Include tags: `JdbcKerberosConstrainedDelegation`.
- Exclude tags: none.
- Expected mapped classes covered: `TestKerberosConstrainedDelegationJdbc`.
- Expected mapped methods covered: `4` method(s).

## Legacy Comparison

- Legacy effective source: aggregate launcher `testing/trino-product-tests-launcher/src/main/java/io/trino/tests/product/launcher/suite/suites/Suite3.java`
- Legacy constrained-delegation coverage ran on `EnvMultinodeTlsKerberosDelegation` together with plain `JDBC` group coverage.
- Current suite isolates only the `JdbcKerberosConstrainedDelegation` tests on `JdbcKerberosEnvironment`.

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `auth-and-clients`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `No dedicated legacy launcher suite identified`

## Parity Checklist

- Legacy suite or lane source: `jdbc-kerberos` lane and the corresponding legacy launcher coverage.
- Current suite class: `SuiteJdbcKerberos`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `1`
- Expected migrated method count: `4`
- Expected migrated classes covered: `TestKerberosConstrainedDelegationJdbc`.
- Expected migrated methods covered: `TestKerberosConstrainedDelegationJdbc.testCtasConstrainedDelegationKerberos`,
  `TestKerberosConstrainedDelegationJdbc.testQueryOnDisposedCredential`,
  `TestKerberosConstrainedDelegationJdbc.testQueryOnExpiredCredential`,
  `TestKerberosConstrainedDelegationJdbc.testSelectConstrainedDelegationKerberos`.
- Observed differences:
  - Current suite is extracted from a legacy aggregate launcher suite into its own dedicated suite.
  - Current suite no longer co-runs the plain `JDBC` group in the same environment run.
- Parity status: `verified`
