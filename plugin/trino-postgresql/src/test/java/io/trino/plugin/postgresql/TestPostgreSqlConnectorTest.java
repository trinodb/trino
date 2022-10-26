/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.postgresql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.RemoteDatabaseEvent;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.postgresql.PostgreSqlQueryRunner.createPostgreSqlQueryRunner;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_LIMIT_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPostgreSqlConnectorTest
        extends BaseJdbcConnectorTest
{
    protected TestingPostgreSqlServer postgreSqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer());
        return createPostgreSqlQueryRunner(postgreSqlServer, Map.of(), Map.of(), REQUIRED_TPCH_TABLES);
    }

    @BeforeClass
    public void setExtensions()
    {
        onRemoteDatabase().execute("CREATE EXTENSION file_fdw");
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN -> {
                // TODO remove once super has this set to true
                verify(!super.hasBehavior(connectorBehavior));
                yield true;
            }
            // Arrays are supported conditionally. Check the defaults.
            case SUPPORTS_ARRAY -> new PostgreSqlConfig().getArrayMapping() != PostgreSqlConfig.ArrayMapping.DISABLED;
            case SUPPORTS_CANCELLATION,
                    SUPPORTS_JOIN_PUSHDOWN,
                    SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY,
                    SUPPORTS_TOPN_PUSHDOWN,
                    SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR -> true;
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                    SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                    SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN,
                    SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                    SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                    SUPPORTS_ROW_TYPE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                new JdbcSqlExecutor(postgreSqlServer.getJdbcUrl(), postgreSqlServer.getProperties()),
                "table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected TestTable createTableWithUnsupportedColumn()
    {
        return new TestTable(
                onRemoteDatabase(),
                "tpch.test_unsupported_column_present",
                "(one bigint, two decimal(50,0), three varchar(10))");
    }

    @Test(dataProvider = "testTimestampPrecisionOnCreateTable")
    public void testTimestampPrecisionOnCreateTable(String inputType, String expectedType)
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_coercion_show_create_table",
                format("(a %s)", inputType))) {
            assertEquals(getColumnType(testTable.getName(), "a"), expectedType);
        }
    }

    @DataProvider(name = "testTimestampPrecisionOnCreateTable")
    public static Object[][] timestampPrecisionOnCreateTableProvider()
    {
        return new Object[][]{
                {"timestamp(0)", "timestamp(0)"},
                {"timestamp(1)", "timestamp(1)"},
                {"timestamp(2)", "timestamp(2)"},
                {"timestamp(3)", "timestamp(3)"},
                {"timestamp(4)", "timestamp(4)"},
                {"timestamp(5)", "timestamp(5)"},
                {"timestamp(6)", "timestamp(6)"},
                {"timestamp(7)", "timestamp(6)"},
                {"timestamp(8)", "timestamp(6)"},
                {"timestamp(9)", "timestamp(6)"},
                {"timestamp(10)", "timestamp(6)"},
                {"timestamp(11)", "timestamp(6)"},
                {"timestamp(12)", "timestamp(6)"}
        };
    }

    @Test(dataProvider = "testTimestampPrecisionOnCreateTableAsSelect")
    public void testTimestampPrecisionOnCreateTableAsSelect(String inputType, String tableType, String tableValue)
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_coercion_show_create_table",
                format("AS SELECT %s a", inputType))) {
            assertEquals(getColumnType(testTable.getName(), "a"), tableType);
            assertQuery(
                    format("SELECT * FROM %s", testTable.getName()),
                    format("VALUES (%s)", tableValue));
        }
    }

    @Test(dataProvider = "testTimestampPrecisionOnCreateTableAsSelect")
    public void testTimestampPrecisionOnCreateTableAsSelectWithNoData(String inputType, String tableType, String ignored)
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_coercion_show_create_table",
                format("AS SELECT %s a WITH NO DATA", inputType))) {
            assertEquals(getColumnType(testTable.getName(), "a"), tableType);
        }
    }

    @DataProvider(name = "testTimestampPrecisionOnCreateTableAsSelect")
    public static Object[][] timestampPrecisionOnCreateTableAsSelectProvider()
    {
        return new Object[][] {
                {"TIMESTAMP '1970-01-01 00:00:00'", "timestamp(0)", "TIMESTAMP '1970-01-01 00:00:00'"},
                {"TIMESTAMP '1970-01-01 00:00:00.9'", "timestamp(1)", "TIMESTAMP '1970-01-01 00:00:00.9'"},
                {"TIMESTAMP '1970-01-01 00:00:00.56'", "timestamp(2)", "TIMESTAMP '1970-01-01 00:00:00.56'"},
                {"TIMESTAMP '1970-01-01 00:00:00.123'", "timestamp(3)", "TIMESTAMP '1970-01-01 00:00:00.123'"},
                {"TIMESTAMP '1970-01-01 00:00:00.4896'", "timestamp(4)", "TIMESTAMP '1970-01-01 00:00:00.4896'"},
                {"TIMESTAMP '1970-01-01 00:00:00.89356'", "timestamp(5)", "TIMESTAMP '1970-01-01 00:00:00.89356'"},
                {"TIMESTAMP '1970-01-01 00:00:00.123000'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.123000'"},
                {"TIMESTAMP '1970-01-01 00:00:00.999'", "timestamp(3)", "TIMESTAMP '1970-01-01 00:00:00.999'"},
                {"TIMESTAMP '1970-01-01 00:00:00.123456'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.123456'"},
                {"TIMESTAMP '2020-09-27 12:34:56.1'", "timestamp(1)", "TIMESTAMP '2020-09-27 12:34:56.1'"},
                {"TIMESTAMP '2020-09-27 12:34:56.9'", "timestamp(1)", "TIMESTAMP '2020-09-27 12:34:56.9'"},
                {"TIMESTAMP '2020-09-27 12:34:56.123'", "timestamp(3)", "TIMESTAMP '2020-09-27 12:34:56.123'"},
                {"TIMESTAMP '2020-09-27 12:34:56.123000'", "timestamp(6)", "TIMESTAMP '2020-09-27 12:34:56.123000'"},
                {"TIMESTAMP '2020-09-27 12:34:56.999'", "timestamp(3)", "TIMESTAMP '2020-09-27 12:34:56.999'"},
                {"TIMESTAMP '2020-09-27 12:34:56.123456'", "timestamp(6)", "TIMESTAMP '2020-09-27 12:34:56.123456'"},
                {"TIMESTAMP '1970-01-01 00:00:00.1234561'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.123456'"},
                {"TIMESTAMP '1970-01-01 00:00:00.123456499'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.123456'"},
                {"TIMESTAMP '1970-01-01 00:00:00.123456499999'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.123456'"},
                {"TIMESTAMP '1970-01-01 00:00:00.1234565'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.123457'"},
                {"TIMESTAMP '1970-01-01 00:00:00.111222333444'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.111222'"},
                {"TIMESTAMP '1970-01-01 00:00:00.9999995'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:01.000000'"},
                {"TIMESTAMP '1970-01-01 23:59:59.9999995'", "timestamp(6)", "TIMESTAMP '1970-01-02 00:00:00.000000'"},
                {"TIMESTAMP '1969-12-31 23:59:59.9999995'", "timestamp(6)", "TIMESTAMP '1970-01-01 00:00:00.000000'"},
                {"TIMESTAMP '1969-12-31 23:59:59.999999499999'", "timestamp(6)", "TIMESTAMP '1969-12-31 23:59:59.999999'"},
                {"TIMESTAMP '1969-12-31 23:59:59.9999994'", "timestamp(6)", "TIMESTAMP '1969-12-31 23:59:59.999999'"}};
    }

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("ERROR: column \".*\" contains null values");
    }

    @Test
    public void testViews()
    {
        onRemoteDatabase().execute("CREATE OR REPLACE VIEW test_view AS SELECT * FROM orders");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_view"));
        assertQuery("SELECT orderkey FROM test_view", "SELECT orderkey FROM orders");
        onRemoteDatabase().execute("DROP VIEW IF EXISTS test_view");
    }

    @Test
    public void testPostgreSqlMaterializedView()
    {
        onRemoteDatabase().execute("CREATE MATERIALIZED VIEW test_mv as SELECT * FROM orders");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_mv"));
        assertQuery("SELECT orderkey FROM test_mv", "SELECT orderkey FROM orders");
        onRemoteDatabase().execute("DROP MATERIALIZED VIEW test_mv");
    }

    @Test
    public void testForeignTable()
    {
        onRemoteDatabase().execute("CREATE SERVER devnull FOREIGN DATA WRAPPER file_fdw");
        onRemoteDatabase().execute("CREATE FOREIGN TABLE test_ft (x bigint) SERVER devnull OPTIONS (filename '/dev/null')");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_ft"));
        computeActual("SELECT * FROM test_ft");
        onRemoteDatabase().execute("DROP FOREIGN TABLE test_ft");
        onRemoteDatabase().execute("DROP SERVER devnull");
    }

    @Test
    public void testErrorDuringInsert()
    {
        onRemoteDatabase().execute("CREATE TABLE test_with_constraint (x bigint primary key)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_with_constraint"));
        Session nonTransactional = Session.builder(getSession())
                .setCatalogSessionProperty("postgresql", "non_transactional_insert", "true")
                .build();
        assertUpdate(nonTransactional, "INSERT INTO test_with_constraint VALUES (1)", 1);
        assertQueryFails(nonTransactional, "INSERT INTO test_with_constraint VALUES (1)", "[\\s\\S]*ERROR: duplicate key value[\\s\\S]*");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_with_constraint"));
        onRemoteDatabase().execute("DROP TABLE test_with_constraint");
    }

    @Test
    public void testSystemTable()
    {
        assertThat(computeActual("SHOW TABLES FROM pg_catalog").getOnlyColumnAsSet())
                .contains("pg_tables", "pg_views", "pg_type", "pg_index");
        // SYSTEM TABLE
        assertThat(computeActual("SELECT typname FROM pg_catalog.pg_type").getOnlyColumnAsSet())
                .contains("char", "text");
        // SYSTEM VIEW
        assertThat(computeActual("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'tpch'").getOnlyColumn())
                .contains("orders");
    }

    @Test
    public void testPartitionedTables()
    {
        try (TestTable testTable = new TestTable(
                postgreSqlServer::execute,
                "test_part_tbl",
                "(id int NOT NULL, payload varchar, logdate date NOT NULL) PARTITION BY RANGE (logdate)")) {
            String values202111 = "(1, 'A', '2021-11-01'), (2, 'B', '2021-11-25')";
            String values202112 = "(3, 'C', '2021-12-01')";
            onRemoteDatabase().execute(format("CREATE TABLE %s_2021_11 PARTITION OF %s FOR VALUES FROM ('2021-11-01') TO ('2021-12-01')", testTable.getName(), testTable.getName()));
            onRemoteDatabase().execute(format("CREATE TABLE %s_2021_12 PARTITION OF %s FOR VALUES FROM ('2021-12-01') TO ('2022-01-01')", testTable.getName(), testTable.getName()));
            onRemoteDatabase().execute(format("INSERT INTO %s VALUES %s ,%s", testTable.getName(), values202111, values202112));
            assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet())
                    .contains(testTable.getName(), testTable.getName() + "_2021_11", testTable.getName() + "_2021_12");
            assertQuery(format("SELECT * FROM %s", testTable.getName()), format("VALUES %s, %s", values202111, values202112));
            assertQuery(format("SELECT * FROM %s_2021_12", testTable.getName()), "VALUES " + values202112);
        }

        try (TestTable testTable = new TestTable(
                postgreSqlServer::execute,
                "test_part_tbl",
                "(id int NOT NULL, type varchar, logdate varchar) PARTITION BY LIST (type)")) {
            String valuesA = "(1, 'A', '2021-11-11'), (4, 'A', '2021-12-25')";
            String valuesB = "(3, 'B', '2021-12-12'), (2, 'B', '2021-12-28')";
            onRemoteDatabase().execute(format("CREATE TABLE %s_a PARTITION OF %s FOR VALUES IN ('A')", testTable.getName(), testTable.getName()));
            onRemoteDatabase().execute(format("CREATE TABLE %s_b PARTITION OF %s FOR VALUES IN ('B')", testTable.getName(), testTable.getName()));
            assertUpdate(format("INSERT INTO %s VALUES %s ,%s", testTable.getName(), valuesA, valuesB), 4);
            assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet())
                    .contains(testTable.getName(), testTable.getName() + "_a", testTable.getName() + "_b");
            assertQuery(format("SELECT * FROM %s", testTable.getName()), format("VALUES %s, %s", valuesA, valuesB));
            assertQuery(format("SELECT * FROM %s_a", testTable.getName()), "VALUES " + valuesA);
        }
    }

    @Test
    public void testTableWithNoSupportedColumns()
    {
        String unsupportedDataType = "interval";
        String supportedDataType = "varchar(5)";

        try (TestTable noSupportedColumns = new TestTable(onRemoteDatabase(), "no_supported_columns", format("(c %s)", unsupportedDataType));
                TestTable supportedColumns = new TestTable(onRemoteDatabase(), "supported_columns", format("(good %s)", supportedDataType));
                TestTable noColumns = new TestTable(onRemoteDatabase(), "no_columns", "()")) {
            assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).contains("orders", noSupportedColumns.getName(), supportedColumns.getName(), noColumns.getName());

            assertQueryFails("SELECT c FROM " + noSupportedColumns.getName(), "\\QTable 'tpch." + noSupportedColumns.getName() + "' has no supported columns (all 1 columns are not supported)");
            assertQueryFails("SELECT * FROM " + noSupportedColumns.getName(), "\\QTable 'tpch." + noSupportedColumns.getName() + "' has no supported columns (all 1 columns are not supported)");
            assertQueryFails("SELECT 'a' FROM " + noSupportedColumns.getName(), "\\QTable 'tpch." + noSupportedColumns.getName() + "' has no supported columns (all 1 columns are not supported)");

            assertQueryFails("SELECT c FROM " + noColumns.getName(), "\\QTable 'tpch." + noColumns.getName() + "' has no supported columns (all 0 columns are not supported)");
            assertQueryFails("SELECT * FROM " + noColumns.getName(), "\\QTable 'tpch." + noColumns.getName() + "' has no supported columns (all 0 columns are not supported)");
            assertQueryFails("SELECT 'a' FROM " + noColumns.getName(), "\\QTable 'tpch." + noColumns.getName() + "' has no supported columns (all 0 columns are not supported)");

            assertQueryFails("SELECT c FROM non_existent", ".* Table .*tpch.non_existent.* does not exist");
            assertQueryFails("SELECT * FROM non_existent", ".* Table .*tpch.non_existent.* does not exist");
            assertQueryFails("SELECT 'a' FROM non_existent", ".* Table .*tpch.non_existent.* does not exist");

            assertQueryFails("SHOW COLUMNS FROM " + noSupportedColumns.getName(), "\\QTable 'tpch." + noSupportedColumns.getName() + "' has no supported columns (all 1 columns are not supported)");
            assertQueryFails("SHOW COLUMNS FROM " + noColumns.getName(), "\\QTable 'tpch." + noColumns.getName() + "' has no supported columns (all 0 columns are not supported)");

            // Other tables should be visible in SHOW TABLES (the no_supported_columns might be included or might be not) and information_schema.tables
            assertThat(computeActual("SHOW TABLES").getOnlyColumn())
                    .contains("orders", noSupportedColumns.getName(), supportedColumns.getName(), noColumns.getName());
            assertThat(computeActual("SELECT table_name FROM information_schema.tables WHERE table_schema = 'tpch'").getOnlyColumn())
                    .contains("orders", noSupportedColumns.getName(), supportedColumns.getName(), noColumns.getName());

            // Other tables should be introspectable with SHOW COLUMNS and information_schema.columns
            assertQuery("SHOW COLUMNS FROM " + supportedColumns.getName(), "VALUES ('good', 'varchar(5)', '', '')");

            // Listing columns in all tables should not fail due to tables with no columns
            computeActual("SELECT column_name FROM information_schema.columns WHERE table_schema = 'tpch'");
        }
    }

    @Test
    public void testInsertWithFailureDoesNotLeaveBehindOrphanedTable()
            throws Exception
    {
        String schemaName = format("tmp_schema_%s", UUID.randomUUID().toString().replaceAll("-", ""));
        try (AutoCloseable schema = withSchema(schemaName);
                TestTable table = new TestTable(onRemoteDatabase(), format("%s.test_cleanup", schemaName), "(x INTEGER)")) {
            assertQuery(format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", schemaName), "VALUES '" + table.getName().replace(schemaName + ".", "") + "'");

            onRemoteDatabase().execute("ALTER TABLE " + table.getName() + " ADD CHECK (x > 0)");

            assertQueryFails("INSERT INTO " + table.getName() + " (x) VALUES (0)", "ERROR: new row .* violates check constraint [\\s\\S]*");
            assertQuery(format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", schemaName), "VALUES '" + table.getName().replace(schemaName + ".", "") + "'");
        }
    }

    @Test
    public void testPredicatePushdown()
    {
        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isNotFullyPushedDown(FilterNode.class);

        // varchar IN without domain compaction
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar IN with small compaction threshold
        assertThat(query(
                Session.builder(getSession())
                        .setCatalogSessionProperty("postgresql", "domain_compaction_threshold", "1")
                        .build(),
                "SELECT regionkey, nationkey, name FROM nation WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(25)))")
                // Filter node is retained as no constraint is pushed into connector.
                // The compacted domain is a range predicate which can give wrong results
                // if pushed down as PostgreSQL has different sort ordering for letters from Trino
                .isNotFullyPushedDown(
                        node(
                                FilterNode.class,
                                // verify that no constraint is applied by the connector
                                tableScan(
                                        tableHandle -> ((JdbcTableHandle) tableHandle).getConstraint().isAll(),
                                        TupleDomain.all(),
                                        ImmutableMap.of())));

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        // bigint equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey = 19"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // bigint equality with small compaction threshold
        assertThat(query(
                Session.builder(getSession())
                        .setCatalogSessionProperty("postgresql", "domain_compaction_threshold", "1")
                        .build(),
                "SELECT regionkey, nationkey, name FROM nation WHERE nationkey IN (19, 21)"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(25)))")
                .isNotFullyPushedDown(FilterNode.class);

        // bigint range, with decimal to bigint simplification
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey BETWEEN 18.5 AND 19.5"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // date equality
        assertThat(query("SELECT orderkey FROM orders WHERE orderdate = DATE '1992-09-29'"))
                .matches("VALUES BIGINT '1250', 34406, 38436, 57570")
                .isFullyPushedDown();

        // predicate over aggregation key (likely to be optimized before being pushed down into the connector)
        assertThat(query("SELECT * FROM (SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey) WHERE regionkey = 3"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isFullyPushedDown();

        // predicate over aggregation result
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey HAVING sum(nationkey) = 77"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isFullyPushedDown();

        // predicate over TopN result
        assertThat(query("" +
                "SELECT orderkey " +
                "FROM (SELECT * FROM orders ORDER BY orderdate DESC, orderkey ASC LIMIT 10)" +
                "WHERE orderdate = DATE '1998-08-01'"))
                .matches("VALUES BIGINT '27588', 22403, 37735")
                .ordered()
                .isFullyPushedDown();

        assertThat(query("" +
                "SELECT custkey " +
                "FROM (SELECT SUM(totalprice) as sum, custkey, COUNT(*) as cnt FROM orders GROUP BY custkey order by sum desc limit 10) " +
                "WHERE cnt > 30"))
                .matches("VALUES BIGINT '643', 898")
                .ordered()
                .isFullyPushedDown();

        // predicate over join
        Session joinPushdownEnabled = joinPushdownEnabled(getSession());
        assertThat(query(joinPushdownEnabled, "SELECT c.name, n.name FROM customer c JOIN nation n ON c.custkey = n.nationkey WHERE acctbal > 8000"))
                .isFullyPushedDown();

        // varchar predicate over join
        assertThat(query(joinPushdownEnabled, "SELECT c.name, n.name FROM customer c JOIN nation n ON c.custkey = n.nationkey WHERE address = 'TcGe5gaZNgVePxU5kRrvXBfkasDTea'"))
                .isFullyPushedDown();
        assertThat(query(joinPushdownEnabled, "SELECT c.name, n.name FROM customer c JOIN nation n ON c.custkey = n.nationkey WHERE address < 'TcGe5gaZNgVePxU5kRrvXBfkasDTea'"))
                .isNotFullyPushedDown(
                        node(JoinNode.class,
                                anyTree(node(TableScanNode.class)),
                                anyTree(node(TableScanNode.class))));
    }

    @Test
    public void testStringPushdownWithCollate()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("postgresql", "enable_string_pushdown_with_collate", "true")
                .build();

        // varchar range
        assertThat(query(session, "SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar IN with small compaction threshold
        assertThat(query(
                Session.builder(session)
                        .setCatalogSessionProperty("postgresql", "domain_compaction_threshold", "1")
                        .build(),
                "SELECT regionkey, nationkey, name FROM nation WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(25)))")
                // Verify that a FilterNode is retained and only a compacted domain is pushed down to connector as a range predicate
                .isNotFullyPushedDown(node(FilterNode.class, tableScan(
                        tableHandle -> {
                            TupleDomain<ColumnHandle> constraint = ((JdbcTableHandle) tableHandle).getConstraint();
                            ColumnHandle nameColumn = constraint.getDomains().orElseThrow()
                                    .keySet().stream()
                                    .map(JdbcColumnHandle.class::cast)
                                    .filter(column -> column.getColumnName().equals("name"))
                                    .collect(onlyElement());
                            return constraint.getDomains().get().get(nameColumn).getValues().getRanges().getOrderedRanges()
                                    .equals(ImmutableList.of(
                                            Range.range(
                                                    createVarcharType(25),
                                                    utf8Slice("POLAND"), true,
                                                    utf8Slice("VIETNAM"), true)));
                        },
                        TupleDomain.all(),
                        ImmutableMap.of())));

        // varchar predicate over join
        Session joinPushdownEnabled = joinPushdownEnabled(session);
        assertThat(query(joinPushdownEnabled, "SELECT c.name, n.name FROM customer c JOIN nation n ON c.custkey = n.nationkey WHERE address < 'TcGe5gaZNgVePxU5kRrvXBfkasDTea'"))
                .isFullyPushedDown();

        // join on varchar columns is not pushed down
        assertThat(query(joinPushdownEnabled, "SELECT c.name, n.name FROM customer c JOIN nation n ON c.address = n.name"))
                .isNotFullyPushedDown(
                        node(JoinNode.class,
                                anyTree(node(TableScanNode.class)),
                                anyTree(node(TableScanNode.class))));
    }

    @Test
    public void testStringJoinPushdownWithCollate()
    {
        PlanMatchPattern joinOverTableScans =
                node(JoinNode.class,
                        anyTree(node(TableScanNode.class)),
                        anyTree(node(TableScanNode.class)));

        PlanMatchPattern broadcastJoinOverTableScans =
                node(JoinNode.class,
                        node(TableScanNode.class),
                        exchange(ExchangeNode.Scope.LOCAL,
                                exchange(ExchangeNode.Scope.REMOTE, ExchangeNode.Type.REPLICATE,
                                        node(TableScanNode.class))));

        Session sessionWithCollatePushdown = Session.builder(getSession())
                .setCatalogSessionProperty("postgresql", "enable_string_pushdown_with_collate", "true")
                .build();

        Session session = joinPushdownEnabled(sessionWithCollatePushdown);

        // Disable DF here for the sake of negative test cases' expected plan. With DF enabled, some operators return in DF's FilterNode and some do not.
        Session withoutDynamicFiltering = Session.builder(getSession())
                .setSystemProperty("enable_dynamic_filtering", "false")
                .setCatalogSessionProperty("postgresql", "enable_string_pushdown_with_collate", "true")
                .build();

        String notDistinctOperator = "IS NOT DISTINCT FROM";
        List<String> nonEqualities = Stream.concat(
                        Stream.of(JoinCondition.Operator.values())
                                .filter(operator -> operator != JoinCondition.Operator.EQUAL)
                                .map(JoinCondition.Operator::getValue),
                        Stream.of(notDistinctOperator))
                .collect(toImmutableList());

        try (TestTable nationLowercaseTable = new TestTable(
                // If a connector supports Join pushdown, but does not allow CTAS, we need to make the table creation here overridable.
                getQueryRunner()::execute,
                "nation_lowercase",
                "AS SELECT nationkey, lower(name) name, regionkey FROM nation")) {
            // basic case
            assertThat(query(session, "SELECT r.name, n.name FROM nation n JOIN region r ON n.regionkey = r.regionkey")).isFullyPushedDown();

            // join over different columns
            assertThat(query(session, "SELECT r.name, n.name FROM nation n JOIN region r ON n.nationkey = r.regionkey")).isFullyPushedDown();

            // pushdown when using USING
            assertThat(query(session, "SELECT r.name, n.name FROM nation n JOIN region r USING(regionkey)")).isFullyPushedDown();

            // varchar equality predicate
            assertConditionallyPushedDown(
                    session,
                    "SELECT n.name, n2.regionkey FROM nation n JOIN nation n2 ON n.name = n2.name",
                    true,
                    joinOverTableScans);
            assertConditionallyPushedDown(
                    session,
                    format("SELECT n.name, nl.regionkey FROM nation n JOIN %s nl ON n.name = nl.name", nationLowercaseTable.getName()),
                    true,
                    joinOverTableScans);

            // multiple bigint predicates
            assertThat(query(session, "SELECT n.name, c.name FROM nation n JOIN customer c ON n.nationkey = c.nationkey and n.regionkey = c.custkey"))
                    .isFullyPushedDown();

            // inequality
            for (String operator : nonEqualities) {
                // bigint inequality predicate
                assertThat(query(withoutDynamicFiltering, format("SELECT r.name, n.name FROM nation n JOIN region r ON n.regionkey %s r.regionkey", operator)))
                        // Currently no pushdown as inequality predicate is removed from Join to maintain Cross Join and Filter as separate nodes
                        .isNotFullyPushedDown(broadcastJoinOverTableScans);

                // varchar inequality predicate
                assertThat(query(withoutDynamicFiltering, format("SELECT n.name, nl.name FROM nation n JOIN %s nl ON n.name %s nl.name", nationLowercaseTable.getName(), operator)))
                        // Currently no pushdown as inequality predicate is removed from Join to maintain Cross Join and Filter as separate nodes
                        .isNotFullyPushedDown(broadcastJoinOverTableScans);
            }

            // inequality along with an equality, which constitutes an equi-condition and allows filter to remain as part of the Join
            for (String operator : nonEqualities) {
                assertConditionallyPushedDown(
                        session,
                        format("SELECT n.name, c.name FROM nation n JOIN customer c ON n.nationkey = c.nationkey AND n.regionkey %s c.custkey", operator),
                        expectJoinPushdown(operator),
                        joinOverTableScans);
            }

            // varchar inequality along with an equality, which constitutes an equi-condition and allows filter to remain as part of the Join
            for (String operator : nonEqualities) {
                assertConditionallyPushedDown(
                        session,
                        format("SELECT n.name, nl.name FROM nation n JOIN %s nl ON n.regionkey = nl.regionkey AND n.name %s nl.name", nationLowercaseTable.getName(), operator),
                        expectJoinPushdown(operator),
                        joinOverTableScans);
            }

            // LEFT JOIN
            assertThat(query(session, "SELECT r.name, n.name FROM nation n LEFT JOIN region r ON n.nationkey = r.regionkey")).isFullyPushedDown();
            assertThat(query(session, "SELECT r.name, n.name FROM region r LEFT JOIN nation n ON n.nationkey = r.regionkey")).isFullyPushedDown();

            // RIGHT JOIN
            assertThat(query(session, "SELECT r.name, n.name FROM nation n RIGHT JOIN region r ON n.nationkey = r.regionkey")).isFullyPushedDown();
            assertThat(query(session, "SELECT r.name, n.name FROM region r RIGHT JOIN nation n ON n.nationkey = r.regionkey")).isFullyPushedDown();

            // FULL JOIN
            assertConditionallyPushedDown(
                    session,
                    "SELECT r.name, n.name FROM nation n FULL JOIN region r ON n.nationkey = r.regionkey",
                    hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN),
                    joinOverTableScans);

            // Join over a (double) predicate
            assertThat(query(session, "" +
                    "SELECT c.name, n.name " +
                    "FROM (SELECT * FROM customer WHERE acctbal > 8000) c " +
                    "JOIN nation n ON c.custkey = n.nationkey"))
                    .isFullyPushedDown();

            // Join over a varchar equality predicate
            assertConditionallyPushedDown(
                    session,
                    "SELECT c.name, n.name FROM (SELECT * FROM customer WHERE address = 'TcGe5gaZNgVePxU5kRrvXBfkasDTea') c " +
                            "JOIN nation n ON c.custkey = n.nationkey",
                    hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY),
                    joinOverTableScans);

            // join over aggregation
            assertConditionallyPushedDown(
                    session,
                    "SELECT * FROM (SELECT regionkey rk, count(nationkey) c FROM nation GROUP BY regionkey) n " +
                            "JOIN region r ON n.rk = r.regionkey",
                    hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN),
                    joinOverTableScans);

            // join over LIMIT
            assertConditionallyPushedDown(
                    session,
                    "SELECT * FROM (SELECT nationkey FROM nation LIMIT 30) n " +
                            "JOIN region r ON n.nationkey = r.regionkey",
                    hasBehavior(SUPPORTS_LIMIT_PUSHDOWN),
                    joinOverTableScans);

            // join over TopN
            assertConditionallyPushedDown(
                    session,
                    "SELECT * FROM (SELECT nationkey FROM nation ORDER BY regionkey LIMIT 5) n " +
                            "JOIN region r ON n.nationkey = r.regionkey",
                    hasBehavior(SUPPORTS_TOPN_PUSHDOWN),
                    joinOverTableScans);

            // join over join
            assertThat(query(session, "SELECT * FROM nation n, region r, customer c WHERE n.regionkey = r.regionkey AND r.regionkey = c.custkey"))
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testDecimalPredicatePushdown()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "test_decimal_pushdown",
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))",
                List.of("123.321, 123456789.987654321"))) {
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE short_decimal <= 124"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE short_decimal <= 124"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE long_decimal <= 123456790"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE short_decimal <= 123.321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE long_decimal <= 123456789.987654321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE short_decimal = 123.321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE long_decimal = 123456789.987654321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testCharPredicatePushdown()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "test_char_pushdown",
                "(char_1 char(1), char_5 char(5), char_10 char(10))",
                List.of(
                        "'0', '0', '0'",
                        "'1', '12345', '1234567890'"))) {
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE char_1 = '0' AND char_5 = '0'"))
                    .matches("VALUES (CHAR'0', CHAR'0    ', CHAR'0         ')")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE char_5 = CHAR'12345' AND char_10 = '1234567890'"))
                    .matches("VALUES (CHAR'1', CHAR'12345', CHAR'1234567890')")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE char_10 = CHAR'0'"))
                    .matches("VALUES (CHAR'0', CHAR'0    ', CHAR'0         ')")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testOrPredicatePushdown()
    {
        assertThat(query("SELECT * FROM nation WHERE nationkey != 3 OR regionkey = 4")).isFullyPushedDown();
        assertThat(query("SELECT * FROM nation WHERE nationkey != 3 OR regionkey != 4")).isFullyPushedDown();
        assertThat(query("SELECT * FROM nation WHERE name = 'ALGERIA' OR regionkey = 4")).isFullyPushedDown();
        assertThat(query("SELECT * FROM nation WHERE name IS NULL OR regionkey = 4")).isFullyPushedDown();
        assertThat(query("SELECT * FROM nation WHERE name = NULL OR regionkey = 4")).isFullyPushedDown();
    }

    @Test
    public void testLikePredicatePushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE name LIKE '%A%'"))
                .isFullyPushedDown();

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_like_predicate_pushdown",
                "(id integer, a_varchar varchar(1))",
                List.of(
                        "1, 'A'",
                        "2, 'a'",
                        "3, 'B'",
                        "4, 'ą'",
                        "5, 'Ą'"))) {
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE a_varchar LIKE '%A%'"))
                    .isFullyPushedDown();
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE a_varchar LIKE '%ą%'"))
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testLikeWithEscapePredicatePushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE name LIKE '%A%' ESCAPE '\\'"))
                .isFullyPushedDown();

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_like_with_escape_predicate_pushdown",
                "(id integer, a_varchar varchar(4))",
                List.of(
                        "1, 'A%b'",
                        "2, 'Asth'",
                        "3, 'ą%b'",
                        "4, 'ąsth'"))) {
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE a_varchar LIKE '%A\\%%' ESCAPE '\\'"))
                    .isFullyPushedDown();
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE a_varchar LIKE '%ą\\%%' ESCAPE '\\'"))
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testIsNullPredicatePushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE name IS NULL")).isFullyPushedDown();
        assertThat(query("SELECT nationkey FROM nation WHERE name IS NULL OR regionkey = 4")).isFullyPushedDown();

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_is_null_predicate_pushdown",
                "(a_int integer, a_varchar varchar(1))",
                List.of(
                        "1, 'A'",
                        "2, 'B'",
                        "1, NULL",
                        "2, NULL"))) {
            assertThat(query("SELECT a_int FROM " + table.getName() + " WHERE a_varchar IS NULL OR a_int = 1")).isFullyPushedDown();
        }
    }

    @Test
    public void testIsNotNullPredicatePushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE name IS NOT NULL OR regionkey = 4")).isFullyPushedDown();

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_is_not_null_predicate_pushdown",
                "(a_int integer, a_varchar varchar(1))",
                List.of(
                        "1, 'A'",
                        "2, 'B'",
                        "1, NULL",
                        "2, NULL"))) {
            assertThat(query("SELECT a_int FROM " + table.getName() + " WHERE a_varchar IS NOT NULL OR a_int = 1")).isFullyPushedDown();
        }
    }

    @Test
    public void testNullIfPredicatePushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE NULLIF(name, 'ALGERIA') IS NULL"))
                .matches("VALUES BIGINT '0'")
                .isFullyPushedDown();

        assertThat(query("SELECT name FROM nation WHERE NULLIF(nationkey, 0) IS NULL"))
                .matches("VALUES CAST('ALGERIA' AS varchar(25))")
                .isFullyPushedDown();

        assertThat(query("SELECT nationkey FROM nation WHERE NULLIF(name, 'Algeria') IS NULL"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        // NULLIF returns the first argument because arguments aren't the same
        assertThat(query("SELECT nationkey FROM nation WHERE NULLIF(name, 'Name not found') = name"))
                .matches("SELECT nationkey FROM nation")
                .isFullyPushedDown();
    }

    @Test
    public void testNotExpressionPushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE NOT(name LIKE '%A%' ESCAPE '\\')")).isFullyPushedDown();

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_is_not_predicate_pushdown",
                "(a_int integer, a_varchar varchar(2))",
                List.of(
                        "1, 'Aa'",
                        "2, 'Bb'",
                        "1, NULL",
                        "2, NULL"))) {
            assertThat(query("SELECT a_int FROM " + table.getName() + " WHERE NOT(a_varchar LIKE 'A%') OR a_int = 2")).isFullyPushedDown();
            assertThat(query("SELECT a_int FROM " + table.getName() + " WHERE NOT(a_varchar LIKE 'A%' OR a_int = 2)")).isFullyPushedDown();
        }
    }

    @Test
    public void testInPredicatePushdown()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_in_predicate_pushdown",
                "(id varchar(1), id2 varchar(1))",
                List.of(
                        "'a', 'b'",
                        "'b', 'c'",
                        "'c', 'c'",
                        "'d', 'd'",
                        "'a', 'f'"))) {
            // IN values cannot be represented as a domain
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE id IN ('a', id2)"))
                    .isFullyPushedDown();

            assertThat(query("SELECT id FROM " + table.getName() + " WHERE id IN ('a', 'b') OR id2 IN ('c', 'd')"))
                    .isFullyPushedDown();

            assertThat(query("SELECT id FROM " + table.getName() + " WHERE id IN ('a', 'B') OR id2 IN ('c', 'D')"))
                    .isFullyPushedDown();

            assertThat(query("SELECT id FROM " + table.getName() + " WHERE id IN ('a', 'B', NULL) OR id2 IN ('C', 'd')"))
                    // NULL constant value is currently not pushed down
                    .isNotFullyPushedDown(FilterNode.class);
        }
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format("(?s).*null value in column \"%s\" violates not-null constraint.*", columnName);
    }

    @Test
    public void testTopNWithEnum()
    {
        // Create an enum with non-lexicographically sorted entries
        String enumType = "test_enum_" + randomNameSuffix();
        onRemoteDatabase().execute("CREATE TYPE " + enumType + " AS ENUM ('A', 'b', 'B', 'a')");
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "test_case_sensitive_topn_pushdown_with_enums",
                "(an_enum " + enumType + ", a_bigint bigint)",
                List.of(
                        "'A', 1",
                        "'B', 2",
                        "'a', 3",
                        "'b', 4"))) {
            // enum values sort order is defined as the order in which the values were listed when creating the type
            // If we pushdown topn on enums our results will not be ordered according to Trino unless the enum was
            // declared with values in the same order as Trino expects
            assertThat(query("SELECT a_bigint FROM " + testTable.getName() + " ORDER BY an_enum ASC LIMIT 2"))
                    .ordered()
                    .isNotFullyPushedDown(TopNNode.class);
            assertThat(query("SELECT a_bigint FROM " + testTable.getName() + " ORDER BY an_enum DESC LIMIT 2"))
                    .ordered()
                    .isNotFullyPushedDown(TopNNode.class);
        }
        finally {
            onRemoteDatabase().execute("DROP TYPE " + enumType);
        }
    }

    /**
     * This test helps to tune TupleDomain simplification threshold.
     */
    @Test
    public void testNativeLargeIn()
    {
        onRemoteDatabase().execute("SELECT count(*) FROM orders WHERE " + getLongInClause(0, 500_000));
    }

    /**
     * This test helps to tune TupleDomain simplification threshold.
     */
    @Test
    public void testNativeMultipleInClauses()
    {
        String longInClauses = range(0, 20)
                .mapToObj(value -> getLongInClause(value * 10_000, 10_000))
                .collect(joining(" OR "));
        onRemoteDatabase().execute("SELECT count(*) FROM orders WHERE " + longInClauses);
    }

    /**
     * Regression test for https://github.com/trinodb/trino/issues/5543
     */
    @Test
    public void testTimestampColumnAndTimestampWithTimeZoneConstant()
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_timestamptz_unwrap_cast", "(id integer, ts_col timestamp(6))")) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " (id, ts_col) VALUES " +
                    "(1, timestamp '2020-01-01 01:01:01.000')," +
                    "(2, timestamp '2019-01-01 01:01:01.000')");

            assertThat(query(format("SELECT id FROM %s WHERE ts_col >= TIMESTAMP '2019-01-01 00:00:00 %s'", table.getName(), getSession().getTimeZoneKey().getId())))
                    .matches("VALUES 1, 2")
                    .isFullyPushedDown();

            assertThat(query(format("SELECT id FROM %s WHERE ts_col >= TIMESTAMP '2019-01-01 00:00:00 %s'", table.getName(), "UTC")))
                    .matches("VALUES 1")
                    .isFullyPushedDown();
        }
    }

    private String getLongInClause(int start, int length)
    {
        String longValues = range(start, start + length)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        return "orderkey IN (" + longValues + ")";
    }

    private AutoCloseable withSchema(String schema)
    {
        onRemoteDatabase().execute(format("CREATE SCHEMA %s", schema));
        return () -> onRemoteDatabase().execute(format("DROP SCHEMA %s", schema));
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return sql -> {
            try {
                try (Connection connection = DriverManager.getConnection(postgreSqlServer.getJdbcUrl(), postgreSqlServer.getProperties());
                        Statement statement = connection.createStatement()) {
                    statement.execute(sql);
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    protected List<RemoteDatabaseEvent> getRemoteDatabaseEvents()
    {
        return postgreSqlServer.getRemoteDatabaseEvents();
    }

    @Override
    protected TestView createSleepingView(Duration minimalQueryDuration)
    {
        long secondsToSleep = round(minimalQueryDuration.convertTo(SECONDS).getValue() + 1);
        return new TestView(onRemoteDatabase(), "test_sleeping_view", format("SELECT 1 FROM pg_sleep(%d)", secondsToSleep));
    }

    @Override
    protected Session joinPushdownEnabled(Session session)
    {
        return Session.builder(super.joinPushdownEnabled(session))
                // strategy is AUTOMATIC by default and would not work for certain test cases (even if statistics are collected)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .build();
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(63);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessage("Schema name must be shorter than or equal to '63' characters but got '64'");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(63);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessage("Table name must be shorter than or equal to '63' characters but got '64'");
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(63);
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Column name must be shorter than or equal to '63' characters but got '64': '.*'");
    }

    @Override
    protected void verifySetColumnTypeFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("(?s)ERROR: .*(cannot be cast automatically to type|out of range).*");
    }

    @Override
    protected Optional<SetColumnTypeSetup> filterSetColumnTypesDataProvider(SetColumnTypeSetup setup)
    {
        if (setup.sourceColumnType().equals("timestamp(3) with time zone")) {
            // The connector returns UTC instead of the given time zone
            return Optional.of(setup.withNewValueLiteral("TIMESTAMP '2020-02-12 14:03:00.123000 +00:00'"));
        }

        if (setup.sourceColumnType().equals("char(20)") && setup.newColumnType().equals("varchar")) {
            // PostgreSQL trims trailing spaces when converting
            return Optional.of(setup.withNewValueLiteral("rtrim(%s)".formatted(setup.newValueLiteral())));
        }

        return Optional.of(setup);
    }
}
