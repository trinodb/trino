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
import io.prestosql.testing.AbstractTestQueries;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;

import java.util.function.Function;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Throwables.getCausalChain;
import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOracleDistributedQueries
        extends AbstractTestQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createOracleQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("connection-url", TestingOracleServer.getJdbcUrl())
                        .put("connection-user", TestingOracleServer.USER)
                        .put("connection-password", TestingOracleServer.PASSWORD)
                        .put("oracle.connection-pool.enabled", "true")
                        .put("oracle.connection-pool.max-size", "10")
                        .put("allow-drop-table", "true")
                        .build(),
                Function.identity(),
                TpchTable.getTables());
    }

    @Override
    public void testLargeIn()
    {
        // TODO: remove when https://github.com/prestosql/presto/issues/3191 is fixed
        assertThatThrownBy(super::testLargeIn)
                .matches(t -> getCausalChain(t).stream()
                        .anyMatch(e -> nullToEmpty(e.getMessage()).contains("Compiler failed")));
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
    public void testInformationSchemaFiltering()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders' LIMIT 1",
                "SELECT 'orders' table_name");
        assertQuery(
                "SELECT table_name FROM information_schema.columns WHERE data_type = 'decimal(19,0)' AND table_name = 'customer' and column_name = 'custkey' LIMIT 1",
                "SELECT 'customer' table_name");
    }
}
