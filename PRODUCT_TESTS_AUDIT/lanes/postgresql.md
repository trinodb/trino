# Lane Audit: Postgresql

## Lane Summary

- Audit status: `manual-pass-complete`
- Section start commit: `Add PostgreSQL environments`
- Section end commit: `Remove legacy SuitePostgresql`
- Introduced JUnit suites: `SuitePostgresql`.
- Extended existing suites: none.
- Retired legacy suites: `SuitePostgresql`.
- Environment classes introduced: `PostgresqlEnvironment`.
- Method status counts: verified `0`, intentional difference `8`, needs follow-up `0`.

## Semantic Audit Status

- Method semantic audit: `complete`
- Environment semantic audit: `complete`
- Suite semantic audit: `complete`

## Lane-Level Open Questions / Intentional Differences

- Lane-wide approved difference set: none beyond the framework baseline documented in the reviewer guide.
- Needs-follow-up methods: none currently identified in this source/history pass.

## Environment Semantic Audit
### `PostgresqlBasicEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against legacy launcher `EnvMultinodePostgresql` and current `PostgresqlBasicEnvironment`.
- Recorded differences: current environment runs outside Docker and uses one Trino container plus one `postgres:11` service container; legacy launcher used the standard multinode launcher environment. This falls within the approved framework replacement difference set.
- Reviewer note: Current environment preloads the same PostgreSQL-side tables consumed by the 8 legacy SQL-backed PostgreSQL cases.

### `PostgresqlSpoolingEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: compared against legacy launcher `EnvMultinodePostgresqlSpooling` and current `PostgresqlSpoolingEnvironment`.
- Recorded differences: current environment runs outside Docker and uses a single Trino container with explicit MinIO-backed spooling configuration; legacy launcher used the dedicated spooling launcher environment. This falls within the approved framework replacement difference set.
- Reviewer note: Current environment preserves the second PostgreSQL coverage run that exercises the spooling-specific tag path.

### `PostgresqlEnvironment`
- Environment semantic audit status: `complete`
- Legacy/current basis: abstract current environment base reviewed together with `PostgresqlBasicEnvironment` and `PostgresqlSpoolingEnvironment`.
- Recorded differences: this base class is a JUnit-side abstraction that did not exist in legacy Tempto form; it consolidates direct PostgreSQL query helpers without changing the two concrete runtime shapes.
- Reviewer note: No additional unresolved fidelity gap is recorded at the abstract-base level.

## Suite Semantic Audit
### `SuitePostgresql`
- Suite semantic audit status: `complete`
- CI bucket: `jdbc-core`
- Relationship to lane: `owned by this lane`.
- Reviewer note: Compared directly against the effective PostgreSQL runs in legacy aggregate `Suite7NonGeneric`; current suite preserves the same two environment runs while isolating them into an explicit PostgreSQL-owned suite.

## Ported Test Classes

### `TestPostgresqlSqlTests`


- Owning migration commit: `Migrate TestPostgresqlSqlTests to JUnit`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/postgresql/TestPostgresqlSqlTests.java`
- Legacy class removed in same migration commit:
    - None. This commit added new JUnit-side verification coverage rather than removing a legacy class.
- Class-level environment requirement: `PostgresqlBasicEnvironment`.
- Class-level tags: `Postgresql`, `ProfileSpecificTests`.
- Method inventory complete: Yes. Current inherited methods: `8`. Legacy source came from `8` deleted SQL files and `6`
  supporting dataset files in the same migration commit.
- Legacy helper/resource dependencies accounted for: Legacy helper/resource dependencies captured from deleted resource
  files.
- Intentional differences summary: `JUnit/AssertJ/Testcontainers framework replacement`
- Method statuses present: `intentional difference`.

#### Methods

##### `testSelect`

- Legacy source method:
  `testing/trino-product-tests/src/main/resources/sql-tests/testcases/connectors/postgresql/select.sql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/postgresql/BasePostgresqlSqlTests.java` ->
  `BasePostgresqlSqlTests.testSelect`
- Mapping type: `sql-file consolidation`
- Environment parity: Current class requires `PostgresqlBasicEnvironment`. Routed by source review into `SuitePostgresql` run 1.
- Tag parity: Current tags: `Postgresql`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy SQL file contains a single query with no explicit setup or cleanup. Current JUnit method likewise issues one query against preloaded connector fixtures.
- Action parity: Legacy action shape: `select * from postgresql.public.workers_psql` from the deleted SQL file. Current action shape: `SELECT * FROM postgresql.public.workers_psql` executed through the JUnit environment helper.
- Assertion parity: Legacy assertion shape: static result rows encoded in the SQL file. Current assertion shape: inline Row literals or string lists checked with AssertJ against the same fixture rows.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: Legacy SQL/result-file expectations are inlined into JUnit constants or row literals; the executed query remains the same in substance.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Legacy SQL file query `select * from postgresql.public.workers_psql` is represented by current helper/query flow `SELECT * FROM postgresql.public.workers_psql` with helpers [env.executeTrino, assertThat, containsOnly, contains].
- Audit status: `intentional difference`

##### `testDescribeTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/resources/sql-tests/testcases/connectors/postgresql/describe_table.sql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/postgresql/BasePostgresqlSqlTests.java` ->
  `BasePostgresqlSqlTests.testDescribeTable`
- Mapping type: `sql-file consolidation`
- Environment parity: Current class requires `PostgresqlBasicEnvironment`. Routed by source review into `SuitePostgresql` run 1.
- Tag parity: Current tags: `Postgresql`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy SQL file contains a single DESCRIBE query with no setup. Current JUnit method likewise runs one DESCRIBE query against the preloaded connector fixture table.
- Action parity: Legacy action shape: `describe postgresql.public.workers_psql` from the deleted SQL file. Current action shape: `DESCRIBE postgresql.public.workers_psql` executed through the JUnit environment helper.
- Assertion parity: Legacy assertion shape: static schema rows encoded in the SQL result block. Current assertion shape: inline Row literals checked with AssertJ against the same schema rows.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: Legacy schema rows moved from the SQL result block into inline JUnit Row literals; query semantics remain the same.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Legacy SQL file query `describe postgresql.public.workers_psql` is represented by current helper/query flow `DESCRIBE postgresql.public.workers_psql` with helpers [env.executeTrino, assertThat, containsOnly, contains].
- Audit status: `intentional difference`

##### `testDescribeRealTable`

- Legacy source method:
  `testing/trino-product-tests/src/main/resources/sql-tests/testcases/connectors/postgresql/describe_real_table.sql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/postgresql/BasePostgresqlSqlTests.java` ->
  `BasePostgresqlSqlTests.testDescribeRealTable`
- Mapping type: `sql-file consolidation`
- Environment parity: Current class requires `PostgresqlBasicEnvironment`. Routed by source review into `SuitePostgresql` run 1.
- Tag parity: Current tags: `Postgresql`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy SQL file contains a single DESCRIBE query with no setup. Current JUnit method likewise runs one DESCRIBE query against the preloaded connector fixture table.
- Action parity: Legacy action shape: `describe postgresql.public.real_table_psql` from the deleted SQL file. Current action shape: `DESCRIBE postgresql.public.real_table_psql` executed through the JUnit environment helper.
- Assertion parity: Legacy assertion shape: static schema rows encoded in the SQL result block. Current assertion shape: inline Row literals checked with AssertJ against the same schema rows.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: Legacy schema rows moved from the SQL result block into inline JUnit Row literals; query semantics remain the same.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Legacy SQL file query `describe postgresql.public.real_table_psql` is represented by current helper/query flow `DESCRIBE postgresql.public.real_table_psql` with helpers [env.executeTrino, assertThat, containsOnly, contains].
- Audit status: `intentional difference`

##### `testJoinPostgresqlToPostgresql`

- Legacy source method:
  `testing/trino-product-tests/src/main/resources/sql-tests/testcases/connectors/postgresql/join_postgres_to_postgres.sql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/postgresql/BasePostgresqlSqlTests.java` ->
  `BasePostgresqlSqlTests.testJoinPostgresqlToPostgresql`
- Mapping type: `sql-file consolidation`
- Environment parity: Current class requires `PostgresqlBasicEnvironment`. Routed by source review into `SuitePostgresql` run 1.
- Tag parity: Current tags: `Postgresql`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy SQL file contains a single query with no explicit setup or cleanup. Current JUnit method likewise issues one query against preloaded connector fixtures.
- Action parity: Legacy action shape: `select t1.last_name, t2.first_name from postgresql.public.workers_psql t1, postgresql.public.workers_psql t2 where t1.id_department = t2.id_employee` from the deleted SQL file. Current action shape: `SELECT t1.last_name, t2.first_name
                FROM postgresql.public.workers_psql t1, postgresql.public.workers_psql t2
                WHERE t1.id_department = t2.id_employee` executed through the JUnit environment helper.
- Assertion parity: Legacy assertion shape: static result rows encoded in the SQL file. Current assertion shape: inline Row literals or string lists checked with AssertJ against the same fixture rows.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: Legacy SQL/result-file expectations are inlined into JUnit constants or row literals; the executed query remains the same in substance.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Legacy SQL file query `select t1.last_name, t2.first_name from postgresql.public.workers_psql t1, postgresql.public.workers_psql t2 where t1.id_department = t2.id_employee` is represented by current helper/query flow `SELECT t1.last_name, t2.first_name
                FROM postgresql.public.workers_psql t1, postgresql.public.workers_psql t2
                WHERE t1.id_department = t2.id_employee` with helpers [env.executeTrino, assertThat, containsOnly, contains].
- Audit status: `intentional difference`

##### `testJoinPostgresqlToTpch`

- Legacy source method:
  `testing/trino-product-tests/src/main/resources/sql-tests/testcases/connectors/postgresql/join_postgres_to_tpch.sql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/postgresql/BasePostgresqlSqlTests.java` ->
  `BasePostgresqlSqlTests.testJoinPostgresqlToTpch`
- Mapping type: `sql-file consolidation`
- Environment parity: Current class requires `PostgresqlBasicEnvironment`. Routed by source review into `SuitePostgresql` run 1.
- Tag parity: Current tags: `Postgresql`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy SQL file contains a single query with no explicit setup or cleanup. Current JUnit method likewise issues one query against preloaded connector fixtures.
- Action parity: Legacy action shape: `select t1.first_name, t2.name from postgresql.public.workers_psql t1, tpch.sf1.nation t2 where t1.id_department = t2.nationkey` from the deleted SQL file. Current action shape: `SELECT t1.first_name, t2.name
                FROM postgresql.public.workers_psql t1, tpch.sf1.nation t2
                WHERE t1.id_department = t2.nationkey` executed through the JUnit environment helper.
- Assertion parity: Legacy assertion shape: static result rows encoded in the SQL file. Current assertion shape: inline Row literals or string lists checked with AssertJ against the same fixture rows.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: Legacy SQL/result-file expectations are inlined into JUnit constants or row literals; the executed query remains the same in substance.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Legacy SQL file query `select t1.first_name, t2.name from postgresql.public.workers_psql t1, tpch.sf1.nation t2 where t1.id_department = t2.nationkey` is represented by current helper/query flow `SELECT t1.first_name, t2.name
                FROM postgresql.public.workers_psql t1, tpch.sf1.nation t2
                WHERE t1.id_department = t2.nationkey` with helpers [env.executeTrino, assertThat, containsOnly, contains].
- Audit status: `intentional difference`

##### `testSelectReal`

- Legacy source method:
  `testing/trino-product-tests/src/main/resources/sql-tests/testcases/connectors/postgresql/select_real.sql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/postgresql/BasePostgresqlSqlTests.java` ->
  `BasePostgresqlSqlTests.testSelectReal`
- Mapping type: `sql-file consolidation`
- Environment parity: Current class requires `PostgresqlBasicEnvironment`. Routed by source review into `SuitePostgresql` run 1.
- Tag parity: Current tags: `Postgresql`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy SQL file contains a single query with no explicit setup or cleanup. Current JUnit method likewise issues one query against preloaded connector fixtures.
- Action parity: Legacy action shape: `select * from postgresql.public.real_table_psql` from the deleted SQL file. Current action shape: `SELECT * FROM postgresql.public.real_table_psql` executed through the JUnit environment helper.
- Assertion parity: Legacy assertion shape: static result rows encoded in the SQL file. Current assertion shape: inline Row literals or string lists checked with AssertJ against the same fixture rows.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: Legacy SQL/result-file expectations are inlined into JUnit constants or row literals; the executed query remains the same in substance.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Legacy SQL file query `select * from postgresql.public.real_table_psql` is represented by current helper/query flow `SELECT * FROM postgresql.public.real_table_psql` with helpers [env.executeTrino, assertThat, containsOnly, contains].
- Audit status: `intentional difference`

##### `testShowSchemas`

- Legacy source method:
  `testing/trino-product-tests/src/main/resources/sql-tests/testcases/connectors/postgresql/show_schemas.sql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/postgresql/BasePostgresqlSqlTests.java` ->
  `BasePostgresqlSqlTests.testShowSchemas`
- Mapping type: `sql-file consolidation`
- Environment parity: Current class requires `PostgresqlBasicEnvironment`. Routed by source review into `SuitePostgresql` run 1.
- Tag parity: Current tags: `Postgresql`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy SQL file contains a single metadata query with no setup. Current JUnit method likewise runs one metadata query against preloaded connector fixtures.
- Action parity: Legacy action shape: `show schemas from postgresql` from the deleted SQL file. Current action shape: `SHOW SCHEMAS FROM postgresql` executed through the JUnit environment helper.
- Assertion parity: Legacy assertion shape: static expected single-column result block in the SQL file. Current assertion shape: collects column 1 to strings and checks membership/order against the same fixture names.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: Legacy expected table/schema names are encoded in the SQL result block; current JUnit form projects column 1 to strings before asserting the same names.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Legacy SQL file query `show schemas from postgresql` is represented by current helper/query flow `SHOW SCHEMAS FROM postgresql` with helpers [env.executeTrino, assertThat, contains, result.column, map(Object::toString), toList].
- Audit status: `intentional difference`

##### `testShowTables`

- Legacy source method:
  `testing/trino-product-tests/src/main/resources/sql-tests/testcases/connectors/postgresql/show_tables.sql`
- Current target method:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/postgresql/BasePostgresqlSqlTests.java` ->
  `BasePostgresqlSqlTests.testShowTables`
- Mapping type: `sql-file consolidation`
- Environment parity: Current class requires `PostgresqlBasicEnvironment`. Routed by source review into `SuitePostgresql` run 1.
- Tag parity: Current tags: `Postgresql`, `ProfileSpecificTests`. Tag routing matches the current suite selection.
- Setup parity: Legacy SQL file contains a single metadata query with no setup. Current JUnit method likewise runs one metadata query against preloaded connector fixtures.
- Action parity: Legacy action shape: `show tables from postgresql.public` from the deleted SQL file. Current action shape: `SHOW TABLES FROM postgresql.public` executed through the JUnit environment helper.
- Assertion parity: Legacy assertion shape: static expected single-column result block in the SQL file. Current assertion shape: collects column 1 to strings and checks membership/order against the same fixture names.
- Cleanup parity: Legacy cleanup shape: none. Current cleanup shape: none.
- Any observed difference, however small: Legacy expected table/schema names are encoded in the SQL result block; current JUnit form projects column 1 to strings before asserting the same names.
- Known intentional difference: `JUnit/AssertJ/Testcontainers framework replacement`
- Reviewer note: Legacy SQL file query `show tables from postgresql.public` is represented by current helper/query flow `SHOW TABLES FROM postgresql.public` with helpers [env.executeTrino, assertThat, containsExactlyInAnyOrder, contains, result.column, map(Object::toString), toList].
- Audit status: `intentional difference`

### `TestPostgresqlSpoolingSqlTests`


- Owning migration commit: `Add TestPostgresqlSpoolingSqlTests JUnit coverage`
- Current class added in same migration commit:
  `testing/trino-product-tests/src/test/java/io/trino/tests/product/postgresql/TestPostgresqlSpoolingSqlTests.java`
- Legacy class removed in same migration commit:
    - None. This commit added new JUnit-side verification coverage rather than removing a legacy class.
- Class-level environment requirement: `PostgresqlSpoolingEnvironment`.
- Class-level tags: `Postgresql`, `PostgresqlSpooling`, `ProfileSpecificTests`.
- Method inventory complete: Not applicable. This class contributes additional environment coverage by inheriting the
  `8` audited methods from `BasePostgresqlSqlTests`.
- Legacy helper/resource dependencies accounted for: Reuses the same SQL/resource-backed mappings audited above, but
  exercises them in the spooling environment.
- Intentional differences summary: `JUnit/AssertJ/Testcontainers framework replacement`
- Method statuses present: `intentional difference`.

#### Methods
