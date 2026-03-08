# Suite Audit: SuiteLoki

## Suite Summary

- Baseline note: the detailed coverage notes below are retained from earlier audit work and must not be treated as final semantic findings until this suite is marked `complete` in `PROGRESS.md`.

- Purpose: JUnit suite for Loki coverage.
- Owning lane: `loki`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteLoki.java`
- CI bucket: `auth-and-clients`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `LokiEnvironment`
- Include tags: `ConfiguredFeatures`, `Loki`.
- Exclude tags: none.
- Expected mapped classes covered: `TestLoki`.
- Expected mapped methods covered: `1` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `auth-and-clients`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteLoki`

## Parity Checklist

- Legacy suite or lane source: `loki` lane and the corresponding legacy launcher coverage.
- Current suite class: `SuiteLoki`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `1`
- Expected migrated method count: `1`
- Expected migrated classes covered: `TestLoki`.
- Expected migrated methods covered: `TestLoki.testQueryRange`.
- Parity status: `verified`
- Recorded differences:
  - Current suite uses dedicated `LokiEnvironment` instead of legacy launcher `EnvMultinodeLoki`.
  - Effective coverage remains the single `ConfiguredFeatures` + `Loki` run.
