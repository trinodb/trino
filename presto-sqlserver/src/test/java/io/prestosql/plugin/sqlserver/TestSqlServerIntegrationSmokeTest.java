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
package io.prestosql.plugin.sqlserver;

import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.TestTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.prestosql.plugin.sqlserver.SqlServerQueryRunner.createSqlServerQueryRunner;
import static io.prestosql.tpch.TpchTable.CUSTOMER;
import static io.prestosql.tpch.TpchTable.NATION;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static io.prestosql.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

@Test
public class TestSqlServerIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    protected TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = new TestingSqlServer();
        sqlServer.start();
        return createSqlServerQueryRunner(sqlServer, CUSTOMER, NATION, ORDERS, REGION);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        sqlServer.close();
    }

    @Test
    public void testInsert()
    {
        sqlServer.execute("CREATE TABLE test_insert (x bigint, y varchar(100))");
        assertUpdate("INSERT INTO test_insert VALUES (123, 'test')", 1);
        assertQuery("SELECT * FROM test_insert", "SELECT 123 x, 'test' y");
        assertUpdate("DROP TABLE test_insert");
    }

    @Test
    public void testInsertInPresenceOfNotSupportedColumn()
    {
        sqlServer.execute("CREATE TABLE test_insert_not_supported_column_present(x bigint, y sql_variant, z varchar(10))");
        // Check that column y is not supported.
        assertQuery("SELECT column_name FROM information_schema.columns WHERE table_name = 'test_insert_not_supported_column_present'", "VALUES 'x', 'z'");
        assertUpdate("INSERT INTO test_insert_not_supported_column_present (x, z) VALUES (123, 'test')", 1);
        assertQuery("SELECT x, z FROM test_insert_not_supported_column_present", "SELECT 123, 'test'");
        assertUpdate("DROP TABLE test_insert_not_supported_column_present");
    }

    @Test
    public void testView()
    {
        sqlServer.execute("CREATE VIEW test_view AS SELECT * FROM orders");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_view"));
        assertQuery("SELECT orderkey FROM test_view", "SELECT orderkey FROM orders");
        sqlServer.execute("DROP VIEW IF EXISTS test_view");
    }

    @Test
    public void testAggregationPushdown()
            throws Exception
    {
        // TODO support aggregation pushdown with GROUPING SETS
        // TODO support aggregation over expressions

        assertThat(query("SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isCorrectlyPushedDown();

        assertThat(query("SELECT count(*) FROM orders")).isCorrectlyPushedDown();
        assertThat(query("SELECT count(nationkey) FROM nation")).isCorrectlyPushedDown();
        assertThat(query("SELECT min(totalprice) FROM orders")).isCorrectlyPushedDown();
        assertThat(query("SELECT regionkey, max(nationkey) FROM nation GROUP BY regionkey")).isCorrectlyPushedDown();
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey")).isCorrectlyPushedDown();

        assertThat(query("SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey")).isCorrectlyPushedDown();

        try (AutoCloseable ignoreTable = withTable("test_aggregation_pushdown", "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))")) {
            sqlServer.execute("INSERT INTO test_aggregation_pushdown VALUES (100.000, 100000000.000000000)");
            sqlServer.execute("INSERT INTO test_aggregation_pushdown VALUES (123.321, 123456789.987654321)");

            assertThat(query("SELECT min(short_decimal), min(long_decimal) FROM test_aggregation_pushdown")).isCorrectlyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal) FROM test_aggregation_pushdown")).isCorrectlyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal) FROM test_aggregation_pushdown")).isCorrectlyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(long_decimal) FROM test_aggregation_pushdown")).isCorrectlyPushedDown();
        }
    }

    @Test
    public void testStddevPushdown()
    {
        try (TestTable testTable = new TestTable(sqlServer::execute, getSession().getSchema().orElseThrow() + ".test_stddev_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (1)");

            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (3)");
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (5)");
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
        }

        try (TestTable testTable = new TestTable(sqlServer::execute, getSession().getSchema().orElseThrow() + ".test_stddev_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            // Test non-whole number results
            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (1)");
            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (2)");
            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (4)");
            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (5)");

            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
        }
    }

    @Test
    public void testVariancePushdown()
    {
        try (TestTable testTable = new TestTable(sqlServer::execute, getSession().getSchema().orElseThrow() + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (1)");

            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (3)");
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (5)");
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
        }

        try (TestTable testTable = new TestTable(sqlServer::execute, getSession().getSchema().orElseThrow() + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            // Test non-whole number results
            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (1)");
            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (2)");
            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (3)");
            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (4)");
            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (5)");

            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
        }
    }

    @Test
    public void testColumnComment()
            throws Exception
    {
        try (AutoCloseable ignoreTable = withTable("test_column_comment",
                "(col1 bigint, col2 bigint, col3 bigint)")) {
            sqlServer.execute("" +
                    "EXEC sp_addextendedproperty " +
                    " 'MS_Description', 'test comment', " +
                    " 'Schema', 'dbo', " +
                    " 'Table', 'test_column_comment', " +
                    " 'Column', 'col1'");

            // SQL Server JDBC driver doesn't support REMARKS for column comment https://github.com/Microsoft/mssql-jdbc/issues/646
            assertQuery(
                    "SELECT column_name, comment FROM information_schema.columns WHERE table_schema = 'dbo' AND table_name = 'test_column_comment'",
                    "VALUES ('col1', null), ('col2', null), ('col3', null)");
        }
    }

    @Test
    public void testDecimalPredicatePushdown()
            throws Exception
    {
        try (AutoCloseable ignoreTable = withTable("test_decimal_pushdown",
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))")) {
            sqlServer.execute("INSERT INTO test_decimal_pushdown VALUES (123.321, 123456789.987654321)");

            assertThat(query("SELECT * FROM test_decimal_pushdown WHERE short_decimal <= 124"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isNotFullyPushedDown(FilterNode.class); // TODO (https://github.com/prestosql/presto/issues/4596) eliminate filter above table scan
            assertThat(query("SELECT * FROM test_decimal_pushdown WHERE short_decimal <= 124"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isNotFullyPushedDown(FilterNode.class); // TODO (https://github.com/prestosql/presto/issues/4596) eliminate filter above table scan
            assertThat(query("SELECT * FROM test_decimal_pushdown WHERE long_decimal <= 123456790"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isNotFullyPushedDown(FilterNode.class); // TODO (https://github.com/prestosql/presto/issues/4596) eliminate filter above table scan
            assertThat(query("SELECT * FROM test_decimal_pushdown WHERE short_decimal <= 123.321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isNotFullyPushedDown(FilterNode.class); // TODO (https://github.com/prestosql/presto/issues/4596) eliminate filter above table scan
            assertThat(query("SELECT * FROM test_decimal_pushdown WHERE long_decimal <= 123456789.987654321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isNotFullyPushedDown(FilterNode.class); // TODO (https://github.com/prestosql/presto/issues/4596) eliminate filter above table scan
            assertThat(query("SELECT * FROM test_decimal_pushdown WHERE short_decimal = 123.321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isNotFullyPushedDown(FilterNode.class); // TODO (https://github.com/prestosql/presto/issues/4596) eliminate filter above table scan
            assertThat(query("SELECT * FROM test_decimal_pushdown WHERE long_decimal = 123456789.987654321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isNotFullyPushedDown(FilterNode.class); // TODO (https://github.com/prestosql/presto/issues/4596) eliminate filter above table scan
        }
    }

    @Test
    public void testLimitPushdown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isCorrectlyPushedDown(); // Use high limit for result determinism

        // with filter over numeric column
        assertThat(query("SELECT name FROM nation WHERE regionkey = 3 LIMIT 5")).isNotFullyPushedDown(FilterNode.class); // TODO (https://github.com/prestosql/presto/issues/4596) eliminate filter above table scan

        // with filter over varchar column
        assertThat(query("SELECT name FROM nation WHERE name < 'EEE' LIMIT 5")).isNotFullyPushedDown(FilterNode.class); // TODO (https://github.com/prestosql/presto/issues/4596) eliminate filter above table scan
    }

    /**
     * This test helps to tune TupleDomain simplification threshold.
     */
    @Test
    public void testNativeLargeIn()
    {
        // Using IN list of size 10_000 as bigger list (around 40_000) causes error:
        // "com.microsoft.sqlserver.jdbc.SQLServerException: Internal error: An expression services
        //  limit has been reached.Please look for potentially complex expressions in your query,
        //  and try to simplify them."
        //
        // List around 30_000 causes query to be really slow
        sqlServer.execute("SELECT count(*) FROM dbo.orders WHERE " + getLongInClause(0, 10_000));
    }

    /**
     * This test helps to tune TupleDomain simplification threshold.
     */
    @Test
    public void testNativeMultipleInClauses()
    {
        // using 1_000 for single IN list as 10_000 causes error:
        // "com.microsoft.sqlserver.jdbc.SQLServerException: Internal error: An expression services
        //  limit has been reached.Please look for potentially complex expressions in your query,
        //  and try to simplify them."
        String longInClauses = range(0, 10)
                .mapToObj(value -> getLongInClause(value * 1_000, 1_000))
                .collect(joining(" OR "));
        sqlServer.execute("SELECT count(*) FROM dbo.orders WHERE " + longInClauses);
    }

    private String getLongInClause(int start, int length)
    {
        String longValues = range(start, start + length)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        return "orderkey IN (" + longValues + ")";
    }

    private AutoCloseable withTable(String tableName, String tableDefinition)
    {
        sqlServer.execute(format("CREATE TABLE %s %s", tableName, tableDefinition));
        return () -> sqlServer.execute("DROP TABLE " + tableName);
    }
}
