# Suite Audit: SuiteAllConnectorsSmoke

## Suite Summary

- Baseline note: the detailed coverage notes below are retained from earlier audit work and must not be treated as final semantic findings until this suite is marked `complete` in `PROGRESS.md`.

- Purpose: JUnit suite for AllConnectorsSmoke coverage.
- Owning lane: `all-connectors-smoke`
- Current suite class:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteAllConnectorsSmoke.java`
- CI bucket: `connector-smoke`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `AllConnectorsSmokeEnvironment`
- Include tags: `AllConnectorsSmoke`.
- Exclude tags: none.
- Expected mapped classes covered: none from the audited product-test classes.
- Expected mapped methods covered: `0` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `connector-smoke`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteAllConnectorsSmoke`

## Parity Checklist

- Legacy suite or lane source: legacy launcher `SuiteAllConnectorsSmoke`, which explicitly routed `TestConfiguredFeatures.selectConfiguredConnectors`.
- Current suite class: `SuiteAllConnectorsSmoke`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `0`
- Expected migrated method count: `0`
- Expected migrated classes covered: none.
- Expected migrated methods covered: none.
- Parity status: `verified`
- Recorded differences:
  - Current suite uses tag-based routing on `SuiteTag.AllConnectorsSmoke` instead of explicit launcher test-name selection.
  - Current suite uses dedicated `AllConnectorsSmokeEnvironment` instead of legacy `EnvMultinodeAllConnectors`.
