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
- Expected mapped classes covered: none. The legacy Kerberos ticket refresh product test was replaced by direct HDFS/Kerberos tests before this migration stack.
- Expected mapped methods covered: `0` method(s).

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
- Expected migrated class count: `0`
- Expected migrated method count: `0`
- Expected migrated classes covered: none. The legacy product-test class was already removed by the direct HDFS/Kerberos replacement.
- Expected migrated methods covered: none.
- Parity status: `verified`
- Recorded differences:
  - No JUnit product-test class is expected for the old Hive Kerberos ticket refresh smoke test. That coverage was intentionally replaced by direct HDFS/Kerberos tests before this migration stack.
