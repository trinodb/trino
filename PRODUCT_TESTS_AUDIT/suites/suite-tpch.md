# Suite Audit: SuiteTpch

## Suite Summary

- Baseline note: the detailed coverage notes below are retained from earlier audit work and must not be treated as final semantic findings until this suite is marked `complete` in `PROGRESS.md`.

- Purpose: JUnit suite for Tpch coverage.
- Owning lane: `tpch`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteTpch.java`
- CI bucket: `jdbc-core`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `FunctionsEnvironment`
- Include tags: `ConfiguredFeatures`, `Tpch`.
- Exclude tags: none.
- Expected mapped classes covered: none from the audited product-test classes.
- Expected mapped methods covered: `0` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `jdbc-core`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteTpch`

## Parity Checklist

- Legacy suite or lane source: `tpch` lane and the corresponding legacy launcher coverage.
- Current suite class: `SuiteTpch`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `0`
- Expected migrated method count: `0`
- Expected migrated classes covered: none.
- Expected migrated methods covered: none.
- Parity status: `verified`
- Recorded differences:
  - Current suite uses dedicated `FunctionsEnvironment` instead of legacy shared `EnvMultinode`.
  - This remains a suite-identity lane with no lane-owned migrated test classes.
