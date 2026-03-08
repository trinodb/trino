# Lane Audit: Mysql Mariadb

## Lane Summary

- Audit status: `manual-pass-complete`
- Section start commit: `Add MySQL environment`
- Section end commit: `Remove legacy SuiteMysql`
- Introduced JUnit suites: `SuiteMysql`.
- Extended existing suites: none.
- Retired legacy suites: `SuiteMysql`.
- Environment classes introduced: `MariaDbEnvironment`, `MySqlEnvironment`.
- Method status counts: verified `3`, intentional difference `9`, needs follow-up `0`.

## Semantic Audit Status

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: `MySQL upgrade`.
- Needs-follow-up methods: none currently identified in this source/history pass.

## Environment Semantic Audit
### `MySqlEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against legacy launcher `EnvMultinodeMysql` and current `MySqlEnvironment`.
- Recorded differences: current environment runs outside Docker and uses a single Trino container plus a `mysql:8.0` Testcontainers service; legacy launcher used the multinode launcher environment. This falls within the approved framework replacement plus MySQL-upgrade difference set.
- Reviewer note: Current environment still preloads the same MySQL-side tables used by the SQL-backed and CTAS/JMX tests and still routes them through a dedicated MySQL catalog run in `SuiteMysql`.

### `MariaDbEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against legacy launcher `EnvMultinodeMariadb` and current `MariaDbEnvironment`.
- Recorded differences: current environment runs outside Docker and uses a single Trino container plus a `mariadb:10.10` Testcontainers service; legacy launcher used the multinode launcher environment. This falls within the approved framework replacement and MySQL/MariaDB upgrade difference set.
- Reviewer note: Current environment still provides a dedicated MariaDB catalog run in `SuiteMysql`; per-test schema cleanup moved into the environment via `afterEachTest()`.

## Suite Semantic Audit
### `SuiteMysql`
- Suite semantic audit status: `complete`
- CI bucket: `jdbc-core`
- Relationship to lane: `owned by this lane`.
- Reviewer note: Compared directly against legacy `SuiteMysql`; current suite preserves the two-run MySQL/MariaDB structure while replacing the legacy launcher `CONFIGURED_FEATURES` conjunction with explicit JUnit environment runs and connector tags.

## Ported Test Classes

### `TestMySqlSqlTests`


- Owning migration commit: `Migrate TestMySqlSqlTests to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/mysql/TestMySqlSqlTests.java`
- Legacy class removed in same migration commit:
    - None. This commit added new JUnit-side verification coverage rather than removing a legacy class.
- Class-level environment requirement: `MySqlEnvironment`.
- Class-level tags: `Mysql`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Current methods: `9`. Legacy source came from `9` deleted SQL/resource files in the
  same migration commit.
- Legacy helper/resource dependencies accounted for: Legacy helper/resource dependencies captured from deleted resource
  files.
- Intentional differences summary: `JUnit/AssertJ/Testcontainers framework replacement`, `MySQL upgrade`
- Method statuses present: `intentional difference`.

#### Methods

##### `testSelect`

- Legacy source method:
  `testing/trino-product-tests/src/main/resources/sql-tests/testcases/connectors/mysql/select.sql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/mysql/TestMySqlSqlTests.java` ->
  `TestMySqlSqlTests.testSelect`
- Mapping type: `sql-file consolidation`
- Environment parity: Current class requires `MySqlEnvironment`. Routed by source review into `SuiteMysql` run 1.
- Tag parity: Current tags: `Mysql`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy SQL file contains a single query with no explicit setup or cleanup. Current JUnit method likewise issues one query against preloaded connector fixtures.
- Action parity: Legacy action shape: `select * from mysql.test.workers_mysql` from the deleted SQL file. Current action shape: `SELECT * FROM mysql.test.workers_mysql` executed through the JUnit environment helper.
- Assertion parity: Legacy assertion shape: static result rows encoded in the SQL file. Current assertion shape: inline Row literals or string lists checked with AssertJ against the same fixture rows.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: Legacy SQL/result-file expectations are inlined into JUnit constants or row literals; the executed query remains the same in substance.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`, `MySQL upgrade`
- Reviewer note: Legacy SQL file query `select * from mysql.test.workers_mysql` is represented by current helper/query flow `SELECT * FROM mysql.test.workers_mysql` with helpers [env.executeTrino, assertThat, containsOnly, contains].
- Audit status: `intentional difference`

##### `testDescribeTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/resources/sql-tests/testcases/connectors/mysql/describe_table.sql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/mysql/TestMySqlSqlTests.java` ->
  `TestMySqlSqlTests.testDescribeTable`
- Mapping type: `sql-file consolidation`
- Environment parity: Current class requires `MySqlEnvironment`. Routed by source review into `SuiteMysql` run 1.
- Tag parity: Current tags: `Mysql`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy SQL file contains a single DESCRIBE query with no setup. Current JUnit method likewise runs one DESCRIBE query against the preloaded connector fixture table.
- Action parity: Legacy action shape: `describe mysql.test.workers_mysql` from the deleted SQL file. Current action shape: `DESCRIBE mysql.test.workers_mysql` executed through the JUnit environment helper.
- Assertion parity: Legacy assertion shape: static schema rows encoded in the SQL result block. Current assertion shape: inline Row literals checked with AssertJ against the same schema rows.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: Legacy schema rows moved from the SQL result block into inline JUnit Row literals; query semantics remain the same.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`, `MySQL upgrade`
- Reviewer note: Legacy SQL file query `describe mysql.test.workers_mysql` is represented by current helper/query flow `DESCRIBE mysql.test.workers_mysql` with helpers [env.executeTrino, assertThat, containsOnly, contains].
- Audit status: `intentional difference`

##### `testDescribeRealTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/resources/sql-tests/testcases/connectors/mysql/describe_real_table.sql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/mysql/TestMySqlSqlTests.java` ->
  `TestMySqlSqlTests.testDescribeRealTable`
- Mapping type: `sql-file consolidation`
- Environment parity: Current class requires `MySqlEnvironment`. Routed by source review into `SuiteMysql` run 1.
- Tag parity: Current tags: `Mysql`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy SQL file contains a single DESCRIBE query with no setup. Current JUnit method likewise runs one DESCRIBE query against the preloaded connector fixture table.
- Action parity: Legacy action shape: `describe mysql.test.real_table_mysql` from the deleted SQL file. Current action shape: `DESCRIBE mysql.test.real_table_mysql` executed through the JUnit environment helper.
- Assertion parity: Legacy assertion shape: static schema rows encoded in the SQL result block. Current assertion shape: inline Row literals checked with AssertJ against the same schema rows.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: Legacy schema rows moved from the SQL result block into inline JUnit Row literals; query semantics remain the same.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`, `MySQL upgrade`
- Reviewer note: Legacy SQL file query `describe mysql.test.real_table_mysql` is represented by current helper/query flow `DESCRIBE mysql.test.real_table_mysql` with helpers [env.executeTrino, assertThat, containsOnly, contains].
- Audit status: `intentional difference`

##### `testJoinMysqlToMysql`

- Legacy source method:
  `testing/trino-product-tests/src/main/resources/sql-tests/testcases/connectors/mysql/join_mysql_to_mysql.sql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/mysql/TestMySqlSqlTests.java` ->
  `TestMySqlSqlTests.testJoinMysqlToMysql`
- Mapping type: `sql-file consolidation`
- Environment parity: Current class requires `MySqlEnvironment`. Routed by source review into `SuiteMysql` run 1.
- Tag parity: Current tags: `Mysql`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy SQL file contains a single query with no explicit setup or cleanup. Current JUnit method likewise issues one query against preloaded connector fixtures.
- Action parity: Legacy action shape: `select t1.last_name, t2.first_name from mysql.test.workers_mysql t1, mysql.test.workers_mysql t2 where t1.id_department = t2.id_employee` from the deleted SQL file. Current action shape: `SELECT t1.last_name, t2.first_name
                FROM mysql.test.workers_mysql t1, mysql.test.workers_mysql t2
                WHERE t1.id_department = t2.id_employee` executed through the JUnit environment helper.
- Assertion parity: Legacy assertion shape: static result rows encoded in the SQL file. Current assertion shape: inline Row literals or string lists checked with AssertJ against the same fixture rows.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: Legacy SQL/result-file expectations are inlined into JUnit constants or row literals; the executed query remains the same in substance.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`, `MySQL upgrade`
- Reviewer note: Legacy SQL file query `select t1.last_name, t2.first_name from mysql.test.workers_mysql t1, mysql.test.workers_mysql t2 where t1.id_department = t2.id_employee` is represented by current helper/query flow `SELECT t1.last_name, t2.first_name
                FROM mysql.test.workers_mysql t1, mysql.test.workers_mysql t2
                WHERE t1.id_department = t2.id_employee` with helpers [env.executeTrino, assertThat, containsOnly, contains].
- Audit status: `intentional difference`

##### `testJoinMysqlToTpch`

- Legacy source method:
  `testing/trino-product-tests/src/main/resources/sql-tests/testcases/connectors/mysql/join_mysql_to_tpch.sql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/mysql/TestMySqlSqlTests.java` ->
  `TestMySqlSqlTests.testJoinMysqlToTpch`
- Mapping type: `sql-file consolidation`
- Environment parity: Current class requires `MySqlEnvironment`. Routed by source review into `SuiteMysql` run 1.
- Tag parity: Current tags: `Mysql`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy SQL file contains a single query with no explicit setup or cleanup. Current JUnit method likewise issues one query against preloaded connector fixtures.
- Action parity: Legacy action shape: `select t1.first_name, t2.name from mysql.test.workers_mysql t1, tpch.sf1.nation t2 where t1.id_department = t2.nationkey` from the deleted SQL file. Current action shape: `SELECT t1.first_name, t2.name
                FROM mysql.test.workers_mysql t1, tpch.sf1.nation t2
                WHERE t1.id_department = t2.nationkey` executed through the JUnit environment helper.
- Assertion parity: Legacy assertion shape: static result rows encoded in the SQL file. Current assertion shape: inline Row literals or string lists checked with AssertJ against the same fixture rows.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: Legacy SQL/result-file expectations are inlined into JUnit constants or row literals; the executed query remains the same in substance.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`, `MySQL upgrade`
- Reviewer note: Legacy SQL file query `select t1.first_name, t2.name from mysql.test.workers_mysql t1, tpch.sf1.nation t2 where t1.id_department = t2.nationkey` is represented by current helper/query flow `SELECT t1.first_name, t2.name
                FROM mysql.test.workers_mysql t1, tpch.sf1.nation t2
                WHERE t1.id_department = t2.nationkey` with helpers [env.executeTrino, assertThat, containsOnly, contains].
- Audit status: `intentional difference`

##### `testSelectReal`

- Legacy source method:
  `testing/trino-product-tests/src/main/resources/sql-tests/testcases/connectors/mysql/select_real.sql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/mysql/TestMySqlSqlTests.java` ->
  `TestMySqlSqlTests.testSelectReal`
- Mapping type: `sql-file consolidation`
- Environment parity: Current class requires `MySqlEnvironment`. Routed by source review into `SuiteMysql` run 1.
- Tag parity: Current tags: `Mysql`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy SQL file contains a single query with no explicit setup or cleanup. Current JUnit method likewise issues one query against preloaded connector fixtures.
- Action parity: Legacy action shape: `select * from mysql.test.real_table_mysql` from the deleted SQL file. Current action shape: `SELECT * FROM mysql.test.real_table_mysql` executed through the JUnit environment helper.
- Assertion parity: Legacy assertion shape: static result rows encoded in the SQL file. Current assertion shape: inline Row literals or string lists checked with AssertJ against the same fixture rows.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: Legacy SQL/result-file expectations are inlined into JUnit constants or row literals; the executed query remains the same in substance.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`, `MySQL upgrade`
- Reviewer note: Legacy SQL file query `select * from mysql.test.real_table_mysql` is represented by current helper/query flow `SELECT * FROM mysql.test.real_table_mysql` with helpers [env.executeTrino, assertThat, containsOnly, contains].
- Audit status: `intentional difference`

##### `testShowSchemas`

- Legacy source method:
  `testing/trino-product-tests/src/main/resources/sql-tests/testcases/connectors/mysql/show_schemas.sql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/mysql/TestMySqlSqlTests.java` ->
  `TestMySqlSqlTests.testShowSchemas`
- Mapping type: `sql-file consolidation`
- Environment parity: Current class requires `MySqlEnvironment`. Routed by source review into `SuiteMysql` run 1.
- Tag parity: Current tags: `Mysql`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy SQL file contains a single metadata query with no setup. Current JUnit method likewise runs one metadata query against preloaded connector fixtures.
- Action parity: Legacy action shape: `show schemas from mysql` from the deleted SQL file. Current action shape: `SHOW SCHEMAS FROM mysql` executed through the JUnit environment helper.
- Assertion parity: Legacy assertion shape: static expected single-column result block in the SQL file. Current assertion shape: collects column 1 to strings and checks membership/order against the same fixture names.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: Legacy expected table/schema names are encoded in the SQL result block; current JUnit form projects column 1 to strings before asserting the same names.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`, `MySQL upgrade`
- Reviewer note: Legacy SQL file query `show schemas from mysql` is represented by current helper/query flow `SHOW SCHEMAS FROM mysql` with helpers [env.executeTrino, assertThat, contains, result.column, map(Object::toString), toList].
- Audit status: `intentional difference`

##### `testShowTables`

- Legacy source method:
  `testing/trino-product-tests/src/main/resources/sql-tests/testcases/connectors/mysql/show_tables.sql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/mysql/TestMySqlSqlTests.java` ->
  `TestMySqlSqlTests.testShowTables`
- Mapping type: `sql-file consolidation`
- Environment parity: Current class requires `MySqlEnvironment`. Routed by source review into `SuiteMysql` run 1.
- Tag parity: Current tags: `Mysql`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy SQL file contains a single metadata query with no setup. Current JUnit method likewise runs one metadata query against preloaded connector fixtures.
- Action parity: Legacy action shape: `show tables from mysql.test` from the deleted SQL file. Current action shape: `SHOW TABLES FROM mysql.test` executed through the JUnit environment helper.
- Assertion parity: Legacy assertion shape: static expected single-column result block in the SQL file. Current assertion shape: collects column 1 to strings and checks membership/order against the same fixture names.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: Legacy expected table/schema names are encoded in the SQL result block; current JUnit form projects column 1 to strings before asserting the same names.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`, `MySQL upgrade`
- Reviewer note: Legacy SQL file query `show tables from mysql.test` is represented by current helper/query flow `SHOW TABLES FROM mysql.test` with helpers [env.executeTrino, assertThat, containsExactlyInAnyOrder, contains, result.column, map(Object::toString), toList].
- Audit status: `intentional difference`

##### `testTinyintFilter`

- Legacy source method:
  `testing/trino-product-tests/src/main/resources/sql-tests/testcases/connectors/mysql/tinyint1_as_tinyint.sql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/mysql/TestMySqlSqlTests.java` ->
  `TestMySqlSqlTests.testTinyintFilter`
- Mapping type: `sql-file consolidation`
- Environment parity: Current class requires `MySqlEnvironment`. Routed by source review into `SuiteMysql` run 1.
- Tag parity: Current tags: `Mysql`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy SQL file contains a single query with no explicit setup or cleanup. Current JUnit method likewise issues one query against preloaded connector fixtures.
- Action parity: Legacy action shape: `select * from mysql.test.workers_mysql where department = 2` from the deleted SQL file. Current action shape: `SELECT * FROM mysql.test.workers_mysql WHERE department = 2` executed through the JUnit environment helper.
- Assertion parity: Legacy assertion shape: static result rows encoded in the SQL file. Current assertion shape: inline Row literals or string lists checked with AssertJ against the same fixture rows.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: Legacy SQL/result-file expectations are inlined into JUnit constants or row literals; the executed query remains the same in substance.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`, `MySQL upgrade`
- Reviewer note: Legacy SQL file query `select * from mysql.test.workers_mysql where department = 2` is represented by current helper/query flow `SELECT * FROM mysql.test.workers_mysql WHERE department = 2` with helpers [env.executeTrino, assertThat, containsOnly, contains].
- Audit status: `intentional difference`

### `TestCreateTableAsSelect`


- Owning migration commit: `Migrate TestCreateTableAsSelect to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/mysql/TestCreateTableAsSelect.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/mysql/TestCreateTableAsSelect.java`
- Class-level environment requirement: `MySqlEnvironment`.
- Class-level tags: `Mysql`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `MySQL upgrade`
- Method statuses present: `verified`.

#### Methods

##### `testCreateTableAsSelect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/mysql/TestCreateTableAsSelect.java` ->
  `testCreateTableAsSelect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/mysql/TestCreateTableAsSelect.java` ->
  `TestCreateTableAsSelect.testCreateTableAsSelect`
- Mapping type: `direct`
- Environment parity: Current class requires `MySqlEnvironment`. Routed by source review into `SuiteMysql` run 1.
- Tag parity: Current tags: `Mysql`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`, `environment.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `format`. Current action shape: `SELECT`, `stmt.execute`, `stmt.executeUpdate`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isEqualTo`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, format] vs current [environment.createTrinoConnection, conn.createStatement, stmt.execute, stmt.executeUpdate]; SQL verbs differ: legacy [CREATE, SELECT] vs current [DROP, CREATE, SELECT]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat, isEqualTo]
- Known intentional difference: `MySQL upgrade`
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, format], verbs [CREATE, SELECT]. Current flow summary -> helpers [environment.createTrinoConnection, conn.createStatement, stmt.execute, stmt.executeUpdate], verbs [DROP, CREATE, SELECT].
- Audit status: `verified`

### `TestJdbcDynamicFilteringJmx`


- Owning migration commit: `Migrate TestJdbcDynamicFilteringJmx to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/mysql/TestJdbcDynamicFilteringJmx.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/mysql/TestJdbcDynamicFilteringJmx.java`
- Class-level environment requirement: `MySqlEnvironment`.
- Class-level tags: `Mysql`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `MySQL upgrade`
- Method statuses present: `verified`.

#### Methods

##### `testDynamicFilteringStats`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/mysql/TestJdbcDynamicFilteringJmx.java` ->
  `testDynamicFilteringStats`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/mysql/TestJdbcDynamicFilteringJmx.java` ->
  `TestJdbcDynamicFilteringJmx.testDynamicFilteringStats`
- Mapping type: `direct`
- Environment parity: Current class requires `MySqlEnvironment`. Routed by source review into `SuiteMysql` run 1.
- Tag parity: Current tags: `Mysql`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`, `SET`. Current setup shape: `CREATE`, `SET`, `environment.createTrinoConnection`, `conn.createStatement`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `format`, `COUNT`. Current action shape: `SELECT`, `stmt.execute`, `stmt.executeUpdate`, `stmt.executeQuery`, `COUNT`, `rs.next`, `rs.getLong`, `getJmxCount`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isEqualTo`, `isTrue`.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, format, COUNT] vs current [environment.createTrinoConnection, conn.createStatement, stmt.execute, stmt.executeUpdate, stmt.executeQuery, COUNT, rs.next, rs.getLong, getJmxCount]; SQL verbs differ: legacy [CREATE, SELECT, SET] vs current [DROP, CREATE, SELECT, SET]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat, isEqualTo, isTrue]
- Known intentional difference: `MySQL upgrade`
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, format, COUNT], verbs [CREATE, SELECT, SET]. Current flow summary -> helpers [environment.createTrinoConnection, conn.createStatement, stmt.execute, stmt.executeUpdate, stmt.executeQuery, COUNT, rs.next, rs.getLong, getJmxCount], verbs [DROP, CREATE, SELECT, SET].
- Audit status: `verified`

### `TestMariaDb`


- Owning migration commit: `Migrate TestMariaDb to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/mariadb/TestMariaDb.java`
- Legacy class removed in same migration commit:
    - `testing/trino-product-tests/src/main/java/io/trino/tests/product/mariadb/TestMariaDb.java`
- Class-level environment requirement: `MariaDbEnvironment`.
- Class-level tags: `Mariadb`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Legacy methods: `1`. Current methods: `1`.
- Legacy helper/resource dependencies accounted for: Legacy class source reviewed directly.
- Intentional differences summary: `MySQL upgrade`
- Method statuses present: `verified`.

#### Methods

##### `testCreateTableAsSelect`

- Legacy source method:
  `testing/trino-product-tests/src/main/java/io/trino/tests/product/mariadb/TestMariaDb.java` ->
  `testCreateTableAsSelect`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/mariadb/TestMariaDb.java` ->
  `TestMariaDb.testCreateTableAsSelect`
- Mapping type: `direct`
- Environment parity: Current class requires `MariaDbEnvironment`. Routed by source review into `SuiteMysql` run 2.
- Tag parity: Current tags: `Mariadb`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy setup shape: `CREATE`. Current setup shape: `CREATE`, `environment.createTrinoConnection`, `conn.createStatement`, `QueryResult.forResultSet`.
- Action parity: Legacy action shape: `SELECT`, `onTrino`, `executeQuery`, `updatedRowsCountIsEqualTo`, `COUNT`. Current action shape: `SELECT`, `stmt.execute`, `stmt.executeUpdate`, `stmt.executeQuery`.
- Assertion parity: Legacy assertion helpers: `assertThat`, `containsOnly`, `row`. Current assertion helpers: `assertThat`, `isEqualTo`, `QueryResultAssert.assertThat`, `containsOnly`.
- Cleanup parity: Legacy cleanup shape: `DROP`. Current cleanup shape: `DROP`.
- Any observed difference, however small: helper calls differ: legacy [onTrino, executeQuery, updatedRowsCountIsEqualTo, COUNT] vs current [environment.createTrinoConnection, conn.createStatement, stmt.execute, stmt.executeUpdate, stmt.executeQuery, QueryResult.forResultSet]; assertion helpers differ: legacy [assertThat, containsOnly, row] vs current [assertThat, isEqualTo, QueryResultAssert.assertThat, containsOnly]
- Known intentional difference: `MySQL upgrade`
- Reviewer note: Legacy flow summary -> helpers [onTrino, executeQuery, updatedRowsCountIsEqualTo, COUNT], verbs [DROP, CREATE, SELECT]. Current flow summary -> helpers [environment.createTrinoConnection, conn.createStatement, stmt.execute, stmt.executeUpdate, stmt.executeQuery, QueryResult.forResultSet], verbs [DROP, CREATE, SELECT].
- Audit status: `verified`
