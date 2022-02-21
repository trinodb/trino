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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.executeInOracle;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestOracleSynonymsTest
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(TestingStarburstOracleServer.connectionProperties())
                        .put("oracle.synonyms.enabled", "true")
                        .buildOrThrow())
                .withTables(ImmutableList.of(TpchTable.ORDERS))
                .build();
    }

    @Test
    public void testSynonyms()
    {
        executeInOracle("CREATE SYNONYM test_synonym FOR orders");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_synonym"));
        assertEquals(computeActual("SHOW TABLES").getOnlyColumn().filter("test_synonym"::equals).collect(toList()), ImmutableList.of("test_synonym"));
        assertQuery("SELECT orderkey FROM test_synonym", "SELECT orderkey FROM orders");
        executeInOracle("DROP SYNONYM test_synonym");
    }

    @Test
    public void testGetColumns()
    {
        // OracleClient.getColumns is using wildcard at the end of table name.
        // Here we test that columns do not leak between tables.
        // See OracleClient#getColumns for more details.
        executeInOracle("CREATE TABLE ordersx AS SELECT 'a' some_additional_column FROM dual");
        assertQuery(
                format("SELECT column_name FROM information_schema.columns WHERE table_name = 'orders' AND table_schema = '%s'", OracleTestUsers.USER),
                "VALUES 'orderkey', 'custkey', 'orderstatus', 'totalprice', 'orderdate', 'orderpriority', 'clerk', 'shippriority', 'comment'");
        executeInOracle("DROP TABLE ordersx");
    }
}
