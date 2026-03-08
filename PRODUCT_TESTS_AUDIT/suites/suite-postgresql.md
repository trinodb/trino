# Suite Audit: SuitePostgresql

## Suite Summary

- Purpose: JUnit 5 test suite for PostgreSQL connector tests.
- Owning lane: `postgresql`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuitePostgresql.java`
- CI bucket: `jdbc-core`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `PostgresqlBasicEnvironment`
- Include tags: `Postgresql`.
- Exclude tags: `PostgresqlSpooling`.
- Expected mapped classes covered: `TestPostgresqlSqlTests`.
- Expected mapped methods covered: `8` method(s).

### Run 2

- Run name: `default`
- Environment: `PostgresqlSpoolingEnvironment`
- Include tags: `PostgresqlSpooling`.
- Exclude tags: none.
- Expected mapped classes covered: `TestPostgresqlSpoolingSqlTests`.
- Expected mapped methods covered: `8` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `jdbc-core`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuitePostgresql`

## Parity Checklist

- Legacy suite or lane source: effective legacy coverage came from aggregate launcher `Suite7NonGeneric` runs for `EnvMultinodePostgresql` and `EnvMultinodePostgresqlSpooling`.
- Current suite class: `SuitePostgresql`
- Explicit runs and environments: verified from current suite source.
- Include tags: verified from current suite source.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `2`
- Expected migrated method count: `16`
- Expected migrated classes covered: `TestPostgresqlSqlTests`, `TestPostgresqlSpoolingSqlTests`.
- Expected migrated methods covered: the 8 SQL-backed PostgreSQL methods executed once in the basic environment and once in the spooling environment.
- Parity status: `verified`
- Observed differences: current JUnit suite isolates PostgreSQL into a dedicated suite instead of inheriting the runs from legacy aggregate `Suite7NonGeneric`; effective two-environment coverage is preserved.
