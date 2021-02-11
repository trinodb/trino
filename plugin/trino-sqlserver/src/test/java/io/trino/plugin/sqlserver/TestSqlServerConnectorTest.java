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
package io.trino.plugin.sqlserver;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.USE_MARK_DISTINCT;
import static io.trino.plugin.sqlserver.DataCompression.NONE;
import static io.trino.plugin.sqlserver.DataCompression.PAGE;
import static io.trino.plugin.sqlserver.DataCompression.ROW;
import static io.trino.plugin.sqlserver.SqlServerQueryRunner.createSqlServerQueryRunner;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestSqlServerConnectorTest
        extends BaseJdbcConnectorTest
{
    protected TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = closeAfterClass(new TestingSqlServer());
        sqlServer.start();
        return createSqlServerQueryRunner(sqlServer, ImmutableMap.of(), ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }

    @Override
    protected boolean supportsDelete()
    {
        return false;
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    protected boolean supportsArrays()
    {
        return false;
    }

    @Override
    protected boolean supportsCommentOnTable()
    {
        return false;
    }

    @Override
    protected boolean supportsCommentOnColumn()
    {
        return false;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY:
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
                return false;

            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_JOIN_PUSHDOWN:
                return true;

            case SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                sqlServer::execute,
                "table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time")
                || typeName.equals("timestamp(3) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        if (typeName.equals("varbinary")) {
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
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
    public void testReadFromView()
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

        assertThat(query("SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();

        assertThat(query("SELECT count(*) FROM orders")).isFullyPushedDown();
        assertThat(query("SELECT count(nationkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT min(totalprice) FROM orders")).isFullyPushedDown();

        // GROUP BY
        assertThat(query("SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, max(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();

        Session withMarkDistinct = Session.builder(getSession())
                .setSystemProperty(USE_MARK_DISTINCT, "true")
                .build();

        // distinct aggregation
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        // distinct aggregation with GROUP BY
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        // distinct aggregation with varchar
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT comment) FROM nation")).isFullyPushedDown();
        // two distinct aggregations
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT regionkey), count(DISTINCT nationkey) FROM nation"))
                .isNotFullyPushedDown(MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
        // distinct aggregation and a non-distinct aggregation
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT regionkey), sum(nationkey) FROM nation"))
                .isNotFullyPushedDown(MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);

        Session withoutMarkDistinct = Session.builder(getSession())
                .setSystemProperty(USE_MARK_DISTINCT, "false")
                .build();

        // distinct aggregation
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        // distinct aggregation with GROUP BY
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        // distinct aggregation with varchar
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT comment) FROM nation")).isFullyPushedDown();
        // two distinct aggregations
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT regionkey), count(DISTINCT nationkey) FROM nation"))
                .isNotFullyPushedDown(AggregationNode.class, ExchangeNode.class, ExchangeNode.class);
        // distinct aggregation and a non-distinct aggregation
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT regionkey), sum(nationkey) FROM nation"))
                .isNotFullyPushedDown(AggregationNode.class, ExchangeNode.class, ExchangeNode.class);

        // GROUP BY and WHERE on bigint column
        // GROUP BY and WHERE on aggregation key
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 GROUP BY regionkey")).isFullyPushedDown();

        // GROUP BY and WHERE on varchar column
        // GROUP BY and WHERE on "other" (not aggregation key, not aggregation input)
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 AND name > 'AAA' GROUP BY regionkey"))
                // SQL Server is case insensitive by default
                .isNotFullyPushedDown(FilterNode.class);

        // GROUP BY above WHERE and LIMIT
        assertThat(query("" +
                "SELECT regionkey, sum(nationkey) " +
                "FROM (SELECT * FROM nation WHERE regionkey < 2 LIMIT 11) " +
                "GROUP BY regionkey"))
                .isFullyPushedDown();

        // decimals
        try (AutoCloseable ignoreTable = withTable("test_aggregation_pushdown", "(short_decimal decimal(9, 3), long_decimal decimal(30, 10), varchar_column varchar(10), bigint_column bigint)")) {
            sqlServer.execute("INSERT INTO test_aggregation_pushdown VALUES (100.000, 100000000.000000000, 'ala', 1)");
            sqlServer.execute("INSERT INTO test_aggregation_pushdown VALUES (123.321, 123456789.987654321, 'kot', 2)");

            assertThat(query("SELECT min(short_decimal), min(long_decimal) FROM test_aggregation_pushdown")).isFullyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal) FROM test_aggregation_pushdown")).isFullyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal) FROM test_aggregation_pushdown")).isFullyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(long_decimal) FROM test_aggregation_pushdown")).isFullyPushedDown();

            // WHERE on aggregation column
            assertThat(query("SELECT min(short_decimal), min(long_decimal) FROM test_aggregation_pushdown WHERE short_decimal < 110 AND long_decimal < 124")).isFullyPushedDown();
            // WHERE on non-aggregation column
            assertThat(query("SELECT min(long_decimal) FROM test_aggregation_pushdown WHERE short_decimal < 110")).isFullyPushedDown();
            // GROUP BY
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM test_aggregation_pushdown GROUP BY short_decimal")).isFullyPushedDown();
            // GROUP BY with WHERE on both grouping and aggregation column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM test_aggregation_pushdown WHERE short_decimal < 110 AND long_decimal < 124" + " GROUP BY short_decimal")).isFullyPushedDown();
            // GROUP BY with WHERE on grouping column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM test_aggregation_pushdown WHERE short_decimal < 110 GROUP BY short_decimal")).isFullyPushedDown();
            // GROUP BY with WHERE on aggregation column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM test_aggregation_pushdown WHERE long_decimal < 124 GROUP BY short_decimal")).isFullyPushedDown();
            // GROUP BY with WHERE on neither grouping nor aggregation column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM test_aggregation_pushdown WHERE bigint_column = 1 GROUP BY short_decimal"))
                    .isFullyPushedDown();
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM test_aggregation_pushdown WHERE varchar_column = 'ala' GROUP BY short_decimal"))
                    // SQL Server is case insensitive by default
                    .isNotFullyPushedDown(FilterNode.class);
            // aggregation on varchar column
            assertThat(query("SELECT min(varchar_column) FROM test_aggregation_pushdown")).isFullyPushedDown();
            // aggregation on varchar column with GROUPING
            assertThat(query("SELECT short_decimal, min(varchar_column) FROM test_aggregation_pushdown GROUP BY short_decimal")).isFullyPushedDown();
            // aggregation on varchar column with WHERE
            assertThat(query("SELECT min(varchar_column) FROM test_aggregation_pushdown WHERE varchar_column ='ala'"))
                    // SQL Server is case insensitive by default
                    .isNotFullyPushedDown(FilterNode.class);

            // not supported yet
            assertThat(query("SELECT min(DISTINCT short_decimal) FROM test_aggregation_pushdown")).isFullyPushedDown();
            assertThat(query("SELECT DISTINCT short_decimal, min(long_decimal) FROM test_aggregation_pushdown GROUP BY short_decimal"))
                    .isFullyPushedDown();
        }

        // array_agg returns array, which is not supported
        assertThat(query("SELECT array_agg(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);

        // histogram returns map, which is not supported
        assertThat(query("SELECT histogram(regionkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);

        // multimap_agg returns multimap, which is not supported
        assertThat(query("SELECT multimap_agg(regionkey, nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);

        // approx_set returns HyperLogLog, which is not supported
        assertThat(query("SELECT approx_set(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
    }

    @Test
    public void testStddevAggregationPushdown()
    {
        try (TestTable testTable = new TestTable(sqlServer::execute, getSession().getSchema().orElseThrow() + ".test_stddev_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (1)");

            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (3)");
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (5)");
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }

        try (TestTable testTable = new TestTable(sqlServer::execute, getSession().getSchema().orElseThrow() + ".test_stddev_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            // Test non-whole number results
            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (1)");
            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (2)");
            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (4)");
            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (5)");

            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Test
    public void testVarianceAggregationPushdown()
    {
        try (TestTable testTable = new TestTable(sqlServer::execute, getSession().getSchema().orElseThrow() + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (1)");

            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (3)");
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (5)");
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }

        try (TestTable testTable = new TestTable(sqlServer::execute, getSession().getSchema().orElseThrow() + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            // Test non-whole number results
            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (1)");
            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (2)");
            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (3)");
            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (4)");
            sqlServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (5)");

            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
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
    public void testPredicatePushdown()
            throws Exception
    {
        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                // SQL Server is case insensitive by default
                .isNotFullyPushedDown(FilterNode.class);

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                // SQL Server is case insensitive by default
                .isNotFullyPushedDown(FilterNode.class);

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                // SQL Server is case insensitive by default
                .isNotFullyPushedDown(FilterNode.class);

        // bigint equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey = 19"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // bigint equality with small compaction threshold
        assertThat(query(
                Session.builder(getSession())
                        .setCatalogSessionProperty("sqlserver", "domain_compaction_threshold", "1")
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

        // decimals
        try (AutoCloseable ignoreTable = withTable("test_decimal_pushdown",
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))")) {
            sqlServer.execute("INSERT INTO test_decimal_pushdown VALUES (123.321, 123456789.987654321)");

            assertThat(query("SELECT * FROM test_decimal_pushdown WHERE short_decimal <= 124"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM test_decimal_pushdown WHERE short_decimal <= 124"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM test_decimal_pushdown WHERE long_decimal <= 123456790"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM test_decimal_pushdown WHERE short_decimal <= 123.321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM test_decimal_pushdown WHERE long_decimal <= 123456789.987654321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM test_decimal_pushdown WHERE short_decimal = 123.321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM test_decimal_pushdown WHERE long_decimal = 123456789.987654321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testLimitPushdown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isFullyPushedDown(); // Use high limit for result determinism

        // with filter over numeric column
        assertThat(query("SELECT name FROM nation WHERE regionkey = 3 LIMIT 5")).isFullyPushedDown();

        // with filter over varchar column
        assertThat(query("SELECT name FROM nation WHERE name < 'EEE' LIMIT 5"))
                // SQL Server is case insensitive by default
                .isNotFullyPushedDown(FilterNode.class);

        // with aggregation
        assertThat(query("SELECT max(regionkey) FROM nation LIMIT 5")).isFullyPushedDown(); // global aggregation, LIMIT removed
        assertThat(query("SELECT regionkey, max(name) FROM nation GROUP BY regionkey LIMIT 5")).isFullyPushedDown();
        assertThat(query("SELECT DISTINCT regionkey FROM nation LIMIT 5")).isFullyPushedDown();

        // with filter and aggregation
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE nationkey < 5 GROUP BY regionkey LIMIT 3")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE name < 'EGYPT' GROUP BY regionkey LIMIT 3"))
                // SQL Server is case insensitive by default
                .isNotFullyPushedDown(FilterNode.class);
    }

    @Test
    public void testTooLargeDomainCompactionThreshold()
    {
        assertQueryFails(
                Session.builder(getSession())
                        .setCatalogSessionProperty("sqlserver", "domain_compaction_threshold", "10000")
                        .build(),
                "SELECT * from nation", "Domain compaction threshold \\(10000\\) cannot exceed 500");
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

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches("CREATE TABLE \\w+\\.\\w+\\.orders \\Q(\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar(79)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   data_compression = 'NONE'\n" +
                        ")");
    }

    @Test(dataProvider = "dataCompression")
    public void testCreateWithDataCompression(DataCompression dataCompression)
    {
        String tableName = "test_create_with_compression_" + randomTableSuffix();
        String createQuery = format("CREATE TABLE sqlserver.dbo.%s (\n" +
                        "   a bigint,\n" +
                        "   b bigint\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   data_compression = '%s'\n" +
                        ")",
                tableName,
                dataCompression);
        assertUpdate(createQuery);

        assertEquals(getQueryRunner().execute("SHOW CREATE TABLE " + tableName).getOnlyValue(), createQuery);

        assertUpdate("DROP TABLE " + tableName);
    }

    @DataProvider
    public Object[][] dataCompression()
    {
        return new Object[][] {
                {NONE},
                {ROW},
                {PAGE}
        };
    }

    @Test
    public void testShowCreateForPartitionedTablesWithDataCompression()
    {
        sqlServer.execute("CREATE PARTITION FUNCTION pfSales (DATE)\n" +
                "AS RANGE LEFT FOR VALUES \n" +
                "('2013-01-01', '2014-01-01', '2015-01-01')");
        sqlServer.execute("CREATE PARTITION SCHEME psSales\n" +
                "AS PARTITION pfSales \n" +
                "ALL TO ([Primary])");
        sqlServer.execute("CREATE TABLE partitionedSales (\n" +
                "   SalesDate DATE,\n" +
                "   Quantity INT\n" +
                ") ON psSales(SalesDate) WITH (DATA_COMPRESSION = PAGE)");
        assertThat((String) computeActual("SHOW CREATE TABLE partitionedSales").getOnlyValue())
                .matches("CREATE TABLE \\w+\\.\\w+\\.partitionedsales \\Q(\n" +
                        "   salesdate date,\n" +
                        "   quantity integer\n" +
                        ")");
        assertUpdate("DROP TABLE partitionedSales");
        sqlServer.execute("DROP PARTITION SCHEME psSales");
        sqlServer.execute("DROP PARTITION FUNCTION pfSales");
    }

    @Test
    public void testShowCreateForIndexedAndCompressedTable()
    {
        // SHOW CREATE doesn't expose data compression for Indexed tables
        sqlServer.execute("CREATE TABLE test_show_indexed_table (\n" +
                "   key1 BIGINT NOT NULL,\n" +
                "   key2 BIGINT NOT NULL,\n" +
                "   key3 BIGINT NOT NULL,\n" +
                "   key4 BIGINT NOT NULL,\n" +
                "   key5 BIGINT NOT NULL,\n" +
                "   CONSTRAINT PK_IndexedTable PRIMARY KEY CLUSTERED (key1),\n" +
                "   CONSTRAINT IX_IndexedTable UNIQUE (key2, key3),\n" +
                "   INDEX IX_MyTable4 NONCLUSTERED (key4, key5))\n" +
                "   WITH (DATA_COMPRESSION = PAGE)");

        assertThat((String) computeActual("SHOW CREATE TABLE test_show_indexed_table").getOnlyValue())
                .isEqualTo("CREATE TABLE sqlserver.dbo.test_show_indexed_table (\n" +
                        "   key1 bigint NOT NULL,\n" +
                        "   key2 bigint NOT NULL,\n" +
                        "   key3 bigint NOT NULL,\n" +
                        "   key4 bigint NOT NULL,\n" +
                        "   key5 bigint NOT NULL\n" +
                        ")");

        assertUpdate("DROP TABLE test_show_indexed_table");
    }

    @Test
    public void testShowCreateForUniqueConstraintCompressedTable()
    {
        sqlServer.execute("CREATE TABLE test_show_unique_constraint_table (\n" +
                "   key1 BIGINT NOT NULL,\n" +
                "   key2 BIGINT NOT NULL,\n" +
                "   key3 BIGINT NOT NULL,\n" +
                "   key4 BIGINT NOT NULL,\n" +
                "   key5 BIGINT NOT NULL,\n" +
                "   UNIQUE (key1, key4),\n" +
                "   UNIQUE (key2, key3))\n" +
                "   WITH (DATA_COMPRESSION = PAGE)");

        assertThat((String) computeActual("SHOW CREATE TABLE test_show_unique_constraint_table").getOnlyValue())
                .isEqualTo("CREATE TABLE sqlserver.dbo.test_show_unique_constraint_table (\n" +
                        "   key1 bigint NOT NULL,\n" +
                        "   key2 bigint NOT NULL,\n" +
                        "   key3 bigint NOT NULL,\n" +
                        "   key4 bigint NOT NULL,\n" +
                        "   key5 bigint NOT NULL\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   data_compression = 'PAGE'\n" +
                        ")");

        assertUpdate("DROP TABLE test_show_unique_constraint_table");
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
