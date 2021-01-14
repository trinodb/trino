/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import io.trino.Session;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.AbstractTestIntegrationSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Map;

import static com.starburstdata.presto.plugin.synapse.SynapseQueryRunner.createSynapseQueryRunner;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

// TODO(https://starburstdata.atlassian.net/browse/PRESTO-5088) Extend Synapse tests form SQL server tests
public class TestSynapseIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private SynapseServer synapseServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        synapseServer = new SynapseServer();
        return createSynapseQueryRunner(synapseServer, true, Map.of(), TpchTable.getTables());
    }

    @Override
    public void testSelectInformationSchemaTables()
    {
        // TODO(https://starburstdata.atlassian.net/browse/PRESTO-5089)
        throw new SkipException("long execution");
    }

    @Override
    public void testSelectInformationSchemaColumns()
    {
        // TODO(https://starburstdata.atlassian.net/browse/PRESTO-5089)
        throw new SkipException("long execution");
    }

    @Test
    public void testInsert()
    {
        synapseServer.execute("CREATE TABLE test_insert (x bigint, y varchar(100))");
        assertUpdate("INSERT INTO test_insert VALUES (123, 'test')", 1);
        assertQuery("SELECT * FROM test_insert", "SELECT 123 x, 'test' y");
        assertUpdate("DROP TABLE test_insert");
    }

    @Test
    public void testInsertInPresenceOfNotSupportedColumn()
    {
        throw new SkipException("sql_variant not supported in Synapse");
    }

    @Test
    public void testSelectFromView()
    {
        synapseServer.execute("CREATE VIEW test_view AS SELECT * FROM nation");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_view"));
        assertQuery("SELECT nationkey FROM test_view", "SELECT nationkey FROM nation");
        synapseServer.execute("DROP VIEW IF EXISTS test_view");
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
        assertThat(query("SELECT regionkey, max(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();

        assertThat(query("SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();

        try (AutoCloseable ignoreTable = withTable("test_aggregation_pushdown", "(short_decimal decimal(9, 3), long_decimal decimal(30, 10), varchar_column varchar(10))")) {
            synapseServer.execute("INSERT INTO test_aggregation_pushdown VALUES (100.000, 100000000.000000000, 'ala')");
            synapseServer.execute("INSERT INTO test_aggregation_pushdown VALUES (123.321, 123456789.987654321, 'kot')");

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
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM test_aggregation_pushdown WHERE varchar_column = 'ala' GROUP BY short_decimal")).isFullyPushedDown();
            // aggregation on varchar column
            assertThat(query("SELECT min(varchar_column) FROM test_aggregation_pushdown")).isFullyPushedDown();
            // aggregation on varchar column with GROUPING
            assertThat(query("SELECT short_decimal, min(varchar_column) FROM test_aggregation_pushdown GROUP BY short_decimal")).isFullyPushedDown();
            // aggregation on varchar column with WHERE
            assertThat(query("SELECT min(varchar_column) FROM test_aggregation_pushdown WHERE varchar_column ='ala'")).isFullyPushedDown();

            // not supported yet
            assertThat(query("SELECT min(DISTINCT short_decimal) FROM test_aggregation_pushdown")).isNotFullyPushedDown(AggregationNode.class);
            // TODO: Improve assertion framework. Here min(long_decimal) is pushed down. There remains ProjectNode above it which relates to DISTINCT in the query.
            assertThat(query("SELECT DISTINCT short_decimal, min(long_decimal) FROM test_aggregation_pushdown GROUP BY short_decimal")).isNotFullyPushedDown(ProjectNode.class);
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
    public void testStddevPushdown()
    {
        try (TestTable testTable = new TestTable(synapseServer::execute, getSession().getSchema().orElseThrow() + ".test_stddev_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            synapseServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (1)");

            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            synapseServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (3)");
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            synapseServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (5)");
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }

        try (TestTable testTable = new TestTable(synapseServer::execute, getSession().getSchema().orElseThrow() + ".test_stddev_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            // Test non-whole number results
            synapseServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (1)");
            synapseServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (2)");
            synapseServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (4)");
            synapseServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (5)");

            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Test
    public void testVariancePushdown()
    {
        try (TestTable testTable = new TestTable(synapseServer::execute, getSession().getSchema().orElseThrow() + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            synapseServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (1)");

            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            synapseServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (3)");
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();

            synapseServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (5)");
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }

        try (TestTable testTable = new TestTable(synapseServer::execute, getSession().getSchema().orElseThrow() + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            // Test non-whole number results
            synapseServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (1)");
            synapseServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (2)");
            synapseServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (3)");
            synapseServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (4)");
            synapseServer.execute("INSERT INTO " + testTable.getName() + " (t_double) VALUES (5)");

            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isFullyPushedDown();
        }
    }

    @Test
    public void testDecimalPredicatePushdown()
            throws Exception
    {
        try (AutoCloseable ignoreTable = withTable("test_decimal_pushdown",
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))")) {
            synapseServer.execute("INSERT INTO test_decimal_pushdown VALUES (123.321, 123456789.987654321)");

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
        assertThat(query("SELECT name FROM nation WHERE name < 'EEE' LIMIT 5")).isFullyPushedDown();

        // with aggregation
        assertThat(query("SELECT max(regionkey) FROM nation LIMIT 5")).isFullyPushedDown(); // global aggregation, LIMIT removed
        assertThat(query("SELECT regionkey, max(name) FROM nation GROUP BY regionkey LIMIT 5")).isFullyPushedDown();
        assertThat(query("SELECT DISTINCT regionkey FROM nation LIMIT 5")).isFullyPushedDown();

        // with filter and aggregation
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE nationkey < 5 GROUP BY regionkey LIMIT 3")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE name < 'EGYPT' GROUP BY regionkey LIMIT 3")).isFullyPushedDown();
    }

    @Test
    public void testTooLargeDomainCompactionThreshold()
    {
        assertQueryFails(
                Session.builder(getSession())
                        .setCatalogSessionProperty("synapse", "domain_compaction_threshold", "10000")
                        .build(),
                "SELECT * from nation", "Domain compaction threshold \\(10000\\) cannot exceed 500");
    }

    /**
     * This test helps to tune TupleDomain simplification threshold.
     */
    @Test
    public void testNativeLargeIn()
    {
        synapseServer.execute("SELECT count(*) FROM dbo.orders WHERE " + getLongInClause(0, 10_000));
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
        synapseServer.execute("SELECT count(*) FROM dbo.orders WHERE " + longInClauses);
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
                        ")");
    }

    @Test
    public void testCreateWithDataCompression()
    {
        // TODO(https://starburstdata.atlassian.net/browse/PRESTO-5073) overwrite with test of table option (WITH)
        throw new SkipException("data_compression not supported in Synapse");
    }

    @Test
    public void testShowCreateForPartitionedTablesWithDataCompression()
    {
        throw new SkipException("CREATE PARTITION FUNCTION not supported in Synapse");
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
        synapseServer.execute(format("CREATE TABLE %s %s", tableName, tableDefinition));
        return () -> synapseServer.execute("DROP TABLE " + tableName);
    }
}
