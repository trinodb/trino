# Suite Audit: SuiteClickhouse

## Suite Summary

- Purpose: JUnit 5 test suite for ClickHouse connector tests.
- Owning lane: `clickhouse`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteClickhouse.java`
- CI bucket: `connector-smoke`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `ClickHouseEnvironment`
- Include tags: `Clickhouse`.
- Exclude tags: none.
- Expected mapped classes covered: `TestClickHouse`.
- Expected mapped methods covered: `1` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `connector-smoke`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteClickhouse`

## Parity Checklist

- Legacy suite or lane source: dedicated legacy launcher `SuiteClickhouse`.
- Current suite class: `SuiteClickhouse`
- Explicit runs and environments: verified from current suite source.
- Include tags: current suite uses `Clickhouse`; legacy launcher also required `CONFIGURED_FEATURES`.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `1`
- Expected migrated method count: `1`
- Expected migrated classes covered: `TestClickHouse`.
- Expected migrated methods covered: `TestClickHouse.testCreateTableAsSelect`.
- Parity status: `verified`
- Observed differences: legacy launcher environment used a multi-node ClickHouse topology; current `ClickHouseEnvironment` uses a single ClickHouse container because the migrated coverage is limited to single-connector CTAS behavior. Current selector also drops the explicit legacy `CONFIGURED_FEATURES` conjunction.
