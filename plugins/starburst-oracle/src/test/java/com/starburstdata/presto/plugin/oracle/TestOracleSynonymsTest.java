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
import io.airlift.tpch.TpchTable;
import io.prestosql.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import java.util.function.Function;

import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static com.starburstdata.presto.plugin.oracle.TestingOracleServer.USER;
import static com.starburstdata.presto.plugin.oracle.TestingOracleServer.executeInOracle;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertTrue;

public class TestOracleSynonymsTest
        extends AbstractTestQueryFramework
{
    public TestOracleSynonymsTest()
    {
        super(() -> createOracleQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("connection-url", TestingOracleServer.getJdbcUrl())
                        .put("connection-user", TestingOracleServer.USER)
                        .put("connection-password", TestingOracleServer.PASSWORD)
                        .put("allow-drop-table", "true")
                        .put("oracle.synonyms.enabled", "true")
                        .build(),
                Function.identity(),
                ImmutableList.of(TpchTable.ORDERS)));
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
                format("SELECT column_name FROM information_schema.columns WHERE table_name = 'orders' AND table_schema = '%s'", USER),
                "VALUES 'orderkey', 'custkey', 'orderstatus', 'totalprice', 'orderdate', 'orderpriority', 'clerk', 'shippriority', 'comment'");
        executeInOracle("DROP TABLE ordersx");
    }
}
