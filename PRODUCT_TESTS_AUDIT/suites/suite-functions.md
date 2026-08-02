# Suite Audit: SuiteFunctions

## Suite Summary

- Baseline note: the detailed coverage notes below are retained from earlier audit work and must not be treated as final semantic findings until this suite is marked `complete` in `PROGRESS.md`.

- Purpose: JUnit suite for Functions coverage.
- Owning lane: `functions`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteFunctions.java`
- CI bucket: `jdbc-core`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `FunctionsEnvironment`
- Include tags: `ConfiguredFeatures`, `Functions`.
- Exclude tags: none.
- Expected mapped classes covered: `TestTeradataFunctions`.
- Expected mapped methods covered: `5` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `jdbc-core`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteFunctions`

## Parity Checklist

- Legacy suite or lane source: `functions` lane and the corresponding legacy launcher coverage.
- Current suite class: `SuiteFunctions`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `1`
- Expected migrated method count: `5`
- Expected migrated classes covered: `TestTeradataFunctions`.
- Expected migrated methods covered: `TestTeradataFunctions.testChar2HexInt`, `TestTeradataFunctions.testIndex`,
  `TestTeradataFunctions.testToChar`, `TestTeradataFunctions.testToDate`, `TestTeradataFunctions.testToTimestamp`.
- Parity status: `verified`
- Recorded differences:
  - Current suite uses dedicated `FunctionsEnvironment` instead of legacy shared `EnvMultinode`.
  - Effective coverage remains the single `ConfiguredFeatures` + `Functions` run.
