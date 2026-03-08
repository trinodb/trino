# Suite Audit: SuiteTwoHives

## Suite Summary

- Baseline note: the detailed coverage notes below are retained from earlier audit work and must not be treated as final semantic findings until this suite is marked `complete` in `PROGRESS.md`.

- Purpose: JUnit suite for TwoHives coverage.
- Owning lane: `hive`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteTwoHives.java`
- CI bucket: `hive-kerberos`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `TwoKerberosHivesEnvironment`
- Include tags: `TwoHives`.
- Exclude tags: none.
- Expected mapped classes covered: none from the audited product-test classes.
- Expected mapped methods covered: `0` method(s).

### Run 2

- Run name: `default`
- Environment: `TwoMixedHivesEnvironment`
- Include tags: `TwoHives`.
- Exclude tags: none.
- Expected mapped classes covered: none from the audited product-test classes.
- Expected mapped methods covered: `0` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `hive-kerberos`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `No dedicated legacy launcher suite identified`

## Parity Checklist

- Legacy suite or lane source: the legacy two-Hives coverage previously routed through `EnvTwoKerberosHives` and `EnvTwoMixedHives`.
- Current suite class: `SuiteTwoHives`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `0`
- Expected migrated method count: `0`
- Expected migrated classes covered: none.
- Expected migrated methods covered: none.
- Parity status: `verified`
- Recorded differences:
  - Current suite preserves the two explicit environment runs while expressing them through dedicated JUnit suite wiring rather than launcher environment selection.
