# Suite Audit: SuiteIgnite

## Suite Summary

- Baseline note: the detailed coverage notes below are retained from earlier audit work and must not be treated as final semantic findings until this suite is marked `complete` in `PROGRESS.md`.

- Purpose: JUnit suite for Ignite coverage.
- Owning lane: `ignite`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteIgnite.java`
- CI bucket: `connector-smoke`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `IgniteEnvironment`
- Include tags: `ConfiguredFeatures`, `Ignite`.
- Exclude tags: none.
- Expected mapped classes covered: `TestIgnite`.
- Expected mapped methods covered: `1` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `connector-smoke`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteIgnite`

## Parity Checklist

- Legacy suite or lane source: `ignite` lane and the corresponding legacy launcher coverage.
- Current suite class: `SuiteIgnite`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `1`
- Expected migrated method count: `1`
- Expected migrated classes covered: `TestIgnite`.
- Expected migrated methods covered: `TestIgnite.testCreateTableAsSelect`.
- Parity status: `verified`
- Recorded differences:
  - Current suite uses dedicated `IgniteEnvironment` instead of legacy launcher `EnvMultinodeIgnite`.
  - Runtime differences stay within the approved Ignite-upgrade envelope.
