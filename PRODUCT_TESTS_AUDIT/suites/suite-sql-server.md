# Suite Audit: SuiteSqlServer

## Suite Summary

- Purpose: JUnit 5 test suite for SQL Server connector tests.
- Owning lane: `sql-server`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteSqlServer.java`
- CI bucket: `jdbc-core`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `SqlServerEnvironment`
- Include tags: `Sqlserver`.
- Exclude tags: none.
- Expected mapped classes covered: `TestSqlServer`.
- Expected mapped methods covered: `1` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `jdbc-core`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteSqlServer`

## Parity Checklist

- Legacy suite or lane source: effective legacy coverage came from aggregate launcher `Suite7NonGeneric` run `EnvMultinodeSqlserver`.
- Current suite class: `SuiteSqlServer`
- Explicit runs and environments: verified from current suite source.
- Include tags: current suite uses `Sqlserver`; legacy aggregate run also required `CONFIGURED_FEATURES`.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `1`
- Expected migrated method count: `1`
- Expected migrated classes covered: `TestSqlServer`.
- Expected migrated methods covered: `TestSqlServer.testCreateTableAsSelect`.
- Parity status: `verified`
- Observed differences: current JUnit suite isolates SQL Server into a dedicated suite; legacy coverage lived inside aggregate `Suite7NonGeneric`. Current selector drops the explicit legacy `CONFIGURED_FEATURES` conjunction but keeps the same migrated class coverage.
