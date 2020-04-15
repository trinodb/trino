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
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;

import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.PARTITIONED_USER;
import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createSession;
import static com.starburstdata.presto.plugin.oracle.TestingOracleServer.executeInOracle;
import static java.lang.String.format;

public class TestOracleParallelIntegrationSmokeTest
        extends BaseOracleIntegrationSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner runner = OracleQueryRunner.builder()
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("connection-url", TestingOracleServer.getJdbcUrl())
                        .put("connection-user", TestingOracleServer.USER)
                        .put("connection-password", TestingOracleServer.PASSWORD)
                        .put("allow-drop-table", "true")
                        .put("oracle.parallelism-type", "PARTITIONS")
                        .put("oracle.concurrent.max-splits-per-scan", "17")
                        .build())
                .withSessionModifier(session -> createSession(PARTITIONED_USER, PARTITIONED_USER))
                .withTables(ImmutableList.of(TpchTable.ORDERS, TpchTable.NATION))
                .build();

        partitionTables();

        return runner;
    }

    private static void partitionTables()
    {
        executeInOracle(format("ALTER TABLE %s.orders MODIFY PARTITION BY RANGE (orderdate) INTERVAL (NUMTODSINTERVAL(1,'DAY')) (PARTITION before_1900 VALUES LESS THAN (TO_DATE('01-JAN-1900','dd-MON-yyyy')))", PARTITIONED_USER));
        executeInOracle(format("ALTER TABLE %s.nation MODIFY PARTITION BY HASH(name) PARTITIONS 3", PARTITIONED_USER));
    }
}
