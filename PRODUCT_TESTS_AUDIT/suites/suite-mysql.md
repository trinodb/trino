# Suite Audit: SuiteMysql

## Suite Summary

- Purpose: JUnit 5 test suite for MySQL and MariaDB connector tests.
- Owning lane: `mysql-mariadb`
- Current suite class: `testing/trino-product-tests/src/test/java/io/trino/tests/product/suite/SuiteMysql.java`
- CI bucket: `jdbc-core`
- Suite semantic audit status: `complete`

## Environment Runs

### Run 1

- Run name: `default`
- Environment: `MySqlEnvironment`
- Include tags: `Mysql`.
- Exclude tags: none.
- Expected mapped classes covered: `TestMySqlSqlTests`, `TestCreateTableAsSelect`, `TestJdbcDynamicFilteringJmx`.
- Expected mapped methods covered: `11` method(s).

### Run 2

- Run name: `default`
- Environment: `MariaDbEnvironment`
- Include tags: `Mariadb`.
- Exclude tags: none.
- Expected mapped classes covered: `TestMariaDb`.
- Expected mapped methods covered: `1` method(s).

## CI Wiring

- `pt` bucket in `.github/workflows/ci.yml`: `jdbc-core`
- Special secret/credential gate: None.
- Legacy launcher suite removed: `Remove legacy SuiteMysql`

## Parity Checklist

- Legacy suite or lane source: dedicated legacy launcher `SuiteMysql` with two runs: `EnvMultinodeMysql` + `EnvMultinodeMariadb`.
- Current suite class: `SuiteMysql`
- Explicit runs and environments: verified from current suite source.
- Include tags: current suite uses connector tags only; legacy launcher also required `CONFIGURED_FEATURES` in both runs.
- Exclude tags: verified from current suite source.
- Expected migrated class count: `4`
- Expected migrated method count: `12`
- Expected migrated classes covered: `TestCreateTableAsSelect`, `TestJdbcDynamicFilteringJmx`, `TestMariaDb`,
  `TestMySqlSqlTests`.
- Expected migrated methods covered: `TestCreateTableAsSelect.testCreateTableAsSelect`,
  `TestJdbcDynamicFilteringJmx.testDynamicFilteringStats`, `TestMariaDb.testCreateTableAsSelect`,
  `TestMySqlSqlTests.testDescribeRealTable`, `TestMySqlSqlTests.testDescribeTable`,
  `TestMySqlSqlTests.testJoinMysqlToMysql`, `TestMySqlSqlTests.testJoinMysqlToTpch`, `TestMySqlSqlTests.testSelect`,
  `TestMySqlSqlTests.testSelectReal`, `TestMySqlSqlTests.testShowSchemas`, `TestMySqlSqlTests.testShowTables`,
  `TestMySqlSqlTests.testTinyintFilter`.
- Parity status: `verified`
- Observed differences: legacy launcher selected `CONFIGURED_FEATURES` together with `MYSQL` / `MARIADB`; current suite relies on explicit environment runs and connector tags. Effective class coverage remains the same for the migrated classes documented above.
