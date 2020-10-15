/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.TestTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.saphana.SapHanaQueryRunner.createSapHanaQueryRunner;
import static io.prestosql.tpch.TpchTable.CUSTOMER;
import static io.prestosql.tpch.TpchTable.NATION;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static io.prestosql.tpch.TpchTable.REGION;
import static org.assertj.core.api.Assertions.assertThat;

@Test
public class TestSapHanaIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private TestingSapHanaServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = new TestingSapHanaServer();
        return createSapHanaQueryRunner(
                server,
                ImmutableMap.<String, String>builder()
                        .put("metadata.cache-ttl", "0m")
                        .put("metadata.cache-missing", "false")
                        .build(),
                ImmutableMap.of(),
                ImmutableList.of(CUSTOMER, NATION, ORDERS, REGION));
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        server.close();
    }

    @Test
    public void testLimitPushdown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isCorrectlyPushedDown(); // Use high limit for result determinism

        // with filter over numeric column
        assertThat(query("SELECT name FROM nation WHERE regionkey = 3 LIMIT 5")).isCorrectlyPushedDown();

        // with filter over varchar column
        assertThat(query("SELECT name FROM nation WHERE name < 'EEE' LIMIT 5")).isCorrectlyPushedDown();
    }

    @Test
    public void testAggregationPushdown()
    {
        // TODO support aggregation pushdown with GROUPING SETS
        // TODO support aggregation over expressions

        // SELECT DISTINCT
        assertThat(query("SELECT DISTINCT regionkey FROM nation")).isCorrectlyPushedDown();

        // count()
        assertThat(query("SELECT count(*) FROM nation")).isCorrectlyPushedDown();
        assertThat(query("SELECT count(nationkey) FROM nation")).isCorrectlyPushedDown();
        assertThat(query("SELECT count(1) FROM nation")).isCorrectlyPushedDown();
        assertThat(query("SELECT count() FROM nation")).isCorrectlyPushedDown();
        assertThat(query("SELECT count(DISTINCT regionkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);

        // GROUP BY
        assertThat(query("SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isCorrectlyPushedDown();
        assertThat(query("SELECT regionkey, max(nationkey) FROM nation GROUP BY regionkey")).isCorrectlyPushedDown();
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey")).isCorrectlyPushedDown();
        assertThat(query("SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey")).isCorrectlyPushedDown();

        // GROUP BY and WHERE on bigint column
        // GROUP BY and WHERE on aggregation key
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 GROUP BY regionkey")).isCorrectlyPushedDown();

        // GROUP BY and WHERE on varchar column
        // GROUP BY and WHERE on "other" (not aggregation key, not aggregation input)
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 AND name > 'AAA' GROUP BY regionkey")).isCorrectlyPushedDown();
    }

    @Test
    public void testStddevPushdown()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(server::execute, schemaName + ".test_stddev_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            server.execute("INSERT INTO " + testTable.getName() + " VALUES (1)");

            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            server.execute("INSERT INTO " + testTable.getName() + " VALUES (3)");
            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            server.execute("INSERT INTO " + testTable.getName() + " VALUES (5)");
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
        }

        try (TestTable testTable = new TestTable(server::execute, schemaName + ".test_stddev_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            // Test non-whole number results
            server.execute("INSERT INTO " + testTable.getName() + " VALUES (1)");
            server.execute("INSERT INTO " + testTable.getName() + " VALUES (2)");
            server.execute("INSERT INTO " + testTable.getName() + " VALUES (4)");
            server.execute("INSERT INTO " + testTable.getName() + " VALUES (5)");

            assertThat(query("SELECT stddev_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT stddev_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
        }
    }

    @Test
    public void testVariancePushdown()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(server::execute, schemaName + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            server.execute("INSERT INTO " + testTable.getName() + " VALUES (1)");

            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            server.execute("INSERT INTO " + testTable.getName() + " VALUES (3)");
            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();

            server.execute("INSERT INTO " + testTable.getName() + " VALUES (5)");
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
        }

        try (TestTable testTable = new TestTable(server::execute, schemaName + ".test_variance_pushdown",
                "(t_double DOUBLE PRECISION)")) {
            // Test non-whole number results
            server.execute("INSERT INTO " + testTable.getName() + " VALUES (1)");
            server.execute("INSERT INTO " + testTable.getName() + " VALUES (2)");
            server.execute("INSERT INTO " + testTable.getName() + " VALUES (3)");
            server.execute("INSERT INTO " + testTable.getName() + " VALUES (4)");
            server.execute("INSERT INTO " + testTable.getName() + " VALUES (5)");

            assertThat(query("SELECT var_pop(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT variance(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
            assertThat(query("SELECT var_samp(t_double) FROM " + testTable.getName())).isCorrectlyPushedDown();
        }
    }
}
