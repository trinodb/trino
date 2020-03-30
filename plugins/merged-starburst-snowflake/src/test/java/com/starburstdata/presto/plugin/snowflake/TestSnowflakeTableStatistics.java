/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

import io.prestosql.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.snowflake.BaseSnowflakeTypeMappingTest.randomTableSuffix;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.jdbcBuilder;
import static java.lang.String.format;

public class TestSnowflakeTableStatistics
        extends AbstractTestQueryFramework
{
    public TestSnowflakeTableStatistics()
    {
        super(() -> jdbcBuilder()
                .withAdditionalProperties(impersonationDisabled())
                .build());
    }

    @Test
    public void testBasic()
    {
        String tableName = "test_stats_orders_" + randomTableSuffix();
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        computeActual(format("CREATE TABLE %s AS SELECT name, nationkey, comment FROM tpch.tiny.nation", tableName));
        try {
            assertQuery(
                    "SHOW STATS FOR " + tableName,
                    "VALUES " +
                            "('name', null, null, null, null, null, null)," +
                            "('nationkey', null, null, null, null, null, null)," +
                            "('comment', null, null, null, null, null, null)," +
                            "(null, null, null, null, 25, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }
}
