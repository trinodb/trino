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
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.oracle.TestOracleTableStatistics.gatherStatisticsInOracle;
import static java.lang.String.format;

public class TestStarburstOracleConnectorSmokeTest
        extends BaseUnlicensedStarburstOracleConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner.builder()
                // some tests here verify exactly the behavior when license is not present
                .withUnlockEnterpriseFeatures(false)
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(TestingStarburstOracleServer.connectionProperties())
                        .buildOrThrow())
                .withTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    /**
     * Smoke test for table statistics when license is not presented. See {@link TestOracleTableStatistics}
     * for full statistics test coverage.
     */
    @Test
    public void testBasicStatisticsWithoutLicense()
    {
        String tableName = "test_stats_orders";
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT * FROM tpch.tiny.orders", tableName));
        try {
            gatherStatisticsInOracle(tableName);
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('orderkey', null, 15000, 0, null, null, null)," +
                            "('custkey', null, 1000, 0, null, null, null)," +
                            "('orderstatus', 30000, 3, 0, null, null, null)," +
                            "('totalprice', null, 14996, 0, null, null, null)," +
                            "('orderdate', null, 2401, 0, null, null, null)," +
                            "('orderpriority', 150000, 5, 0, null, null, null)," +
                            "('clerk', 240000, 1000, 0, null, null, null)," +
                            "('shippriority', null, 1, 0, null, null, null)," +
                            "('comment', 750000, 14995, 0, null, null, null)," +
                            "(null, null, null, null, 15000, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }
}
