# Suite Audit: SuiteCassandra

## Suite Summary

- Purpose: JUnit 5 test suite for Cassandra connector tests.
- Owning lane: `cassandra`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteCassandra.java`
- CI bucket: `connector-smoke`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `CassandraEnvironment`
- Include tags: `Cassandra`.
- Exclude tags: none.
- Expected mapped classes covered: `TestCassandra`.
- Expected mapped methods covered: `1` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `connector-smoke`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteCassandra`

## Parity Checklist

- Legacy suite or lane source: dedicated legacy launcher `SuiteCassandra`.
- Current suite class: `SuiteCassandra`
- Explicit runs and environments: verified from current suite source.
- Include tags: current suite uses `Cassandra`; legacy launcher also required `CONFIGURED_FEATURES`.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `1`
- Expected migrated method count: `1`
- Expected migrated classes covered: `TestCassandra`.
- Expected migrated methods covered: `TestCassandra.testCreateTableAsSelect`.
- Parity status: `verified`
- Observed differences: current selector drops the explicit legacy `CONFIGURED_FEATURES` conjunction; the dedicated environment run and migrated class coverage remain aligned.
