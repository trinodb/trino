# Suite Audit: SuiteHiveKerberos

## Suite Summary

- Baseline note: the detailed coverage notes below are retained from earlier audit work and must not be treated as final semantic findings until this suite is marked `complete` in `PROGRESS.md`.

- Purpose: JUnit suite for HiveKerberos coverage.
- Owning lane: `hive`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteHiveKerberos.java`
- CI bucket: `hive-kerberos`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `HiveKerberosEnvironment`
- Include tags: `HiveKerberos`.
- Exclude tags: none.
- Expected mapped classes covered: `TestHiveConnectorKerberosSmokeTest`.
- Expected mapped methods covered: `1` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `hive-kerberos`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `No dedicated legacy launcher suite identified`

## Parity Checklist

- Legacy suite or lane source: the `HIVE_KERBEROS` coverage inside legacy `Suite2`.
- Current suite class: `SuiteHiveKerberos`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `1`
- Expected migrated method count: `1`
- Expected migrated classes covered: `TestHiveConnectorKerberosSmokeTest`.
- Expected migrated methods covered: `TestHiveConnectorKerberosSmokeTest.kerberosTicketExpiryTest`.
- Parity status: `verified`
- Recorded differences:
  - Current suite isolates the legacy kerberized Hive smoke coverage into a dedicated JUnit suite instead of keeping it embedded in a larger launcher aggregate.
