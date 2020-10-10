/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.SqlExecutor;
import io.prestosql.testing.sql.TestTable;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.executeInOracle;
import static io.prestosql.tpch.TpchTable.CUSTOMER;
import static io.prestosql.tpch.TpchTable.NATION;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static io.prestosql.tpch.TpchTable.REGION;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseStarburstOracleAggregationPushdownTest
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withUnlockEnterpriseFeatures(true)
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(TestingStarburstOracleServer.connectionProperties())
                        .put("allow-drop-table", "true")
                        .put("aggregation-pushdown.enabled", "true")
                        .build())
                .withTables(ImmutableList.of(CUSTOMER, NATION, ORDERS, REGION))
                .build();
    }

    protected SqlExecutor onOracle()
    {
        return TestingStarburstOracleServer::executeInOracle;
    }

    @Test
    public void testAggregationPushdown()
    {
        // TODO support aggregation pushdown with GROUPING SETS

        assertThat(query("SELECT count(*) FROM nation")).isCorrectlyPushedDown();
        assertThat(query("SELECT count(nationkey) FROM nation")).isCorrectlyPushedDown();
        assertThat(query("SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isCorrectlyPushedDown();
        assertThat(query("SELECT regionkey, max(nationkey) FROM nation GROUP BY regionkey")).isCorrectlyPushedDown();
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey")).isCorrectlyPushedDown();
        assertThat(query("SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey")).isCorrectlyPushedDown();

        try (TestTable testTable = new TestTable(onOracle(), getSession().getSchema().orElseThrow() + ".test_aggregation_pushdown",
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10), varchar_column varchar(10))")) {
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (100.000, 100000000.000000000, 'ala')");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (123.321, 123456789.987654321, 'kot')");

            assertThat(query("SELECT min(short_decimal), min(long_decimal) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(long_decimal) FROM " + testTable.getName())).isCorrectlyPushedDown();

            // smoke testing of more complex cases
            // WHERE on aggregation column
            assertThat(query("SELECT min(short_decimal), min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 AND long_decimal < 124")).isCorrectlyPushedDown();
            // WHERE on non-aggregation column
            assertThat(query("SELECT min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110")).isCorrectlyPushedDown();
            // GROUP BY
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " GROUP BY short_decimal")).isCorrectlyPushedDown();
            // GROUP BY with WHERE on both grouping and aggregation column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 AND long_decimal < 124" + " GROUP BY short_decimal")).isCorrectlyPushedDown();
            // GROUP BY with WHERE on grouping column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE short_decimal < 110 GROUP BY short_decimal")).isCorrectlyPushedDown();
            // GROUP BY with WHERE on aggregation column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE long_decimal < 124 GROUP BY short_decimal")).isCorrectlyPushedDown();
            // GROUP BY with WHERE on neither grouping nor aggregation column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTable.getName() + " WHERE varchar_column = 'ala' GROUP BY short_decimal")).isCorrectlyPushedDown();
            // aggregation on varchar column
            assertThat(query("SELECT min(varchar_column) FROM " + testTable.getName())).isCorrectlyPushedDown();
            // aggregation on varchar column with GROUPING
            assertThat(query("SELECT short_decimal, min(varchar_column) FROM " + testTable.getName() + " GROUP BY short_decimal")).isCorrectlyPushedDown();
            // aggregation on varchar column with WHERE
            assertThat(query("SELECT min(varchar_column) FROM " + testTable.getName() + " WHERE varchar_column ='ala'")).isCorrectlyPushedDown();

            // not supported yet
            assertThat(query("SELECT min(DISTINCT short_decimal) FROM " + testTable.getName())).isNotFullyPushedDown(AggregationNode.class);
            assertThat(query("SELECT DISTINCT short_decimal, min(long_decimal) FROM " + testTable.getName() + " GROUP BY short_decimal")).isNotFullyPushedDown(ProjectNode.class);
        }
    }

    @Test
    public void testStddevPushdown()
    {
        try (TestTable testTable = new TestTable(onOracle(), getSession().getSchema().orElseThrow() + ".test_stddev_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (1)");

            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (3)");
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (5)");
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
        }

        try (TestTable testTable = new TestTable(onOracle(), getSession().getSchema().orElseThrow() + ".test_stddev_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            // Test non-whole number results
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (1)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (2)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (4)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (5)");

            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
        }
    }

    @Test
    public void testVariancePushdown()
    {
        try (TestTable testTable = new TestTable(onOracle(), getSession().getSchema().orElseThrow() + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (1)");

            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (3)");
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (5)");
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
        }

        try (TestTable testTable = new TestTable(onOracle(), getSession().getSchema().orElseThrow() + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            // Test non-whole number results
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (1)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (2)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (3)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (4)");
            executeInOracle("INSERT INTO " + testTable.getName() + " VALUES (5)");

            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
        }
    }
}
