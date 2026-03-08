# Suite Audit: SuiteExasol

## Suite Summary

- Baseline note: the detailed coverage notes below are retained from earlier audit work and must not be treated as final semantic findings until this suite is marked `complete` in `PROGRESS.md`.

- Purpose: JUnit suite for Exasol coverage.
- Owning lane: `exasol`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteExasol.java`
- CI bucket: `jdbc-external`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `ExasolEnvironment`
- Include tags: `ConfiguredFeatures`, `Exasol`.
- Exclude tags: none.
- Expected mapped classes covered: `TestExasol`.
- Expected mapped methods covered: `1` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `jdbc-external`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteExasol`

## Parity Checklist

- Legacy suite or lane source: `exasol` lane and the corresponding legacy launcher coverage.
- Current suite class: `SuiteExasol`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `1`
- Expected migrated method count: `1`
- Expected migrated classes covered: `TestExasol`.
- Expected migrated methods covered: `TestExasol.testSelect`.
- Parity status: `verified`
- Recorded differences:
  - Current suite uses dedicated `ExasolEnvironment` instead of legacy launcher `EnvMultinodeExasol`.
  - Suite remains one-run `ConfiguredFeatures` + `Exasol` coverage.
