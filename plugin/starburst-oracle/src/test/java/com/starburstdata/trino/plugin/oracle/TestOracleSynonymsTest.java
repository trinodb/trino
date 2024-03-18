/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.SharedResource.Lease;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

@Isolated // TestingStarburstOracleServer is shared across test classes
public class TestOracleSynonymsTest
        extends AbstractTestQueryFramework
{
    private Lease<TestingStarburstOracleServer> oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = closeAfterClass(TestingStarburstOracleServer.getInstance());
        return OracleQueryRunner.builder(oracleServer)
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("oracle.synonyms.enabled", "true")
                        .buildOrThrow())
                .withTables(ImmutableList.of(TpchTable.ORDERS))
                .build();
    }

    @Test
    public void testSynonyms()
    {
        oracleServer.get().executeInOracle("CREATE SYNONYM test_synonym FOR orders");
        assertThat(getQueryRunner().tableExists(getSession(), "test_synonym")).isTrue();
        assertThat(computeActual("SHOW TABLES").getOnlyColumn().filter("test_synonym"::equals).collect(toList())).isEqualTo(ImmutableList.of("test_synonym"));
        assertQuery("SELECT orderkey FROM test_synonym", "SELECT orderkey FROM orders");
        oracleServer.get().executeInOracle("DROP SYNONYM test_synonym");
    }

    @Test
    public void testGetColumns()
    {
        // StarburstOracleClient.getColumns is using wildcard at the end of table name.
        // Here we test that columns do not leak between tables.
        // See StarburstOracleClient#getColumns for more details.
        oracleServer.get().executeInOracle("CREATE TABLE ordersx AS SELECT 'a' some_additional_column FROM dual");
        assertQuery(
                format("SELECT column_name FROM information_schema.columns WHERE table_name = 'orders' AND table_schema = '%s'", OracleTestUsers.USER),
                "VALUES 'orderkey', 'custkey', 'orderstatus', 'totalprice', 'orderdate', 'orderpriority', 'clerk', 'shippriority', 'comment'");
        oracleServer.get().executeInOracle("DROP TABLE ordersx");
    }
}
