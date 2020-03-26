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

import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import io.prestosql.Session;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.tests.AbstractTestQueries;
import org.testng.annotations.Test;

import java.util.function.Function;

import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.tests.QueryAssertions.assertEqualsIgnoreOrder;

public class TestOracleDistributedQueries
        extends AbstractTestQueries
{
    private static final Iterable<TpchTable<?>> TPCH_TABLES = TpchTable.getTables();

    public TestOracleDistributedQueries()
    {
        super(() -> createOracleQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("connection-url", TestingOracleServer.getJdbcUrl())
                        .put("connection-user", TestingOracleServer.USER)
                        .put("connection-password", TestingOracleServer.PASSWORD)
                        .put("oracle.connection-pool.enabled", "true")
                        .put("oracle.connection-pool.max-size", "10")
                        .put("allow-drop-table", "true")
                        .build(),
                Function.identity(),
                TPCH_TABLES));
    }

    @Override
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(),
                VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "decimal(19,0)", "", "")
                .row("custkey", "decimal(19,0)", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "timestamp", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "decimal(10,0)", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();

        assertEquals(actual, expectedParametrizedVarchar);
    }

    @Override
    public void testApproxSetBigint()
    {
        // test fail due to result mismatch because approx_set yields different results for bigint vs decimal.
    }

    @Override
    public void testApproxSetBigintGroupBy()
    {
        // test fail due to result mismatch because approx_set yields different results for bigint vs decimal.
    }

    @Override
    public void testApproxSetGroupByWithOnlyNullsInOneGroup()
    {
        // test fail due to result mismatch because approx_set yields different results for bigint vs decimal.
    }

    @Override
    public void testApproxSetWithNulls()
    {
        // test fail due to result mismatch because approx_set yields different results for bigint vs decimal.
    }

    @Override
    public void testCustomAdd()
    {
        // custom_add does not support decimal arguments
    }

    @Override
    public void testCustomSum()
    {
        // custom_sum does not support decimal arguments
    }

    @Test
    public void testDescribeInput()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT ? FROM nation WHERE nationkey = ? and name < ?")
                .build();
        MaterializedResult actual = computeActual(session, "DESCRIBE INPUT my_query");
        MaterializedResult expected = resultBuilder(session, BIGINT, VARCHAR)
                .row(0, "unknown")
                .row(1, "decimal")
                .row(2, "varchar")
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Override
    public void testDescribeOutput()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT * FROM nation")
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("nationkey", session.getCatalog().get(), session.getSchema().get(), "nation", "decimal(19,0)", 16, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(25)", 0, false)
                .row("regionkey", session.getCatalog().get(), session.getSchema().get(), "nation", "decimal(19,0)", 16, false)
                .row("comment", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(152)", 0, false)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Override
    public void testDescribeOutputNamedAndUnnamed()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT 1, name, regionkey AS my_alias FROM nation")
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("_col0", "", "", "", "integer", 4, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(25)", 0, false)
                .row("my_alias", session.getCatalog().get(), session.getSchema().get(), "nation", "decimal(19,0)", 16, true)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Override
    public void testInformationSchemaFiltering()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders' LIMIT 1",
                "SELECT 'orders' table_name");
        assertQuery(
                "SELECT table_name FROM information_schema.columns WHERE data_type = 'decimal(19,0)' AND table_name = 'customer' and column_name = 'custkey' LIMIT 1",
                "SELECT 'customer' table_name");
    }

    @Override
    public void testMergeHyperLogLog()
    {
        // create_hll does not support decimal arguments
    }

    @Override
    public void testMergeHyperLogLogGroupBy()
    {
        // create_hll does not support decimal arguments
    }

    @Override
    public void testMergeHyperLogLogGroupByWithNulls()
    {
        // create_hll does not support decimal arguments
    }

    @Override
    public void testMergeHyperLogLogWithNulls()
    {
        // create_hll does not support decimal arguments
    }

    @Override
    public void testMergeEmptyNonEmptyApproxSet()
    {
        // create_hll does not support decimal arguments
    }

    @Override
    public void testP4ApproxSetBigint()
    {
        // test fail due to result mismatch because approx_set yields different results for bigint vs decimal.
    }

    @Override
    public void testP4ApproxSetBigintGroupBy()
    {
        // test fail due to result mismatch because approx_set yields different results for bigint vs decimal.
    }

    @Override
    public void testP4ApproxSetGroupByWithNulls()
    {
        // test fail due to result mismatch because approx_set yields different results for bigint vs decimal.
    }

    @Override
    public void testP4ApproxSetGroupByWithOnlyNullsInOneGroup()
    {
        // test fail due to result mismatch because approx_set yields different results for bigint vs decimal.
    }

    @Override
    public void testP4ApproxSetWithNulls()
    {
        // test fail due to result mismatch because approx_set yields different results for bigint vs decimal.
    }
}
