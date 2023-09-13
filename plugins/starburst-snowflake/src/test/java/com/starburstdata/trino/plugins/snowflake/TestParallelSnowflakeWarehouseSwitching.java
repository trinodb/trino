/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake;

import io.trino.Session;
import org.testng.annotations.Test;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.parallelBuilder;

public class TestParallelSnowflakeWarehouseSwitching
        extends TestJdbcSnowflakeWarehouseSwitching
{
    @Override
    protected SnowflakeQueryRunner.Builder createBuilder()
    {
        return parallelBuilder();
    }

    // overridden because failures in either case have same error message unlike the JDBC connector
    @Test
    @Override
    public void testSwitchNotExistingWarehouse()
    {
        Session invalidWarehouseSession = Session.builder(getSession())
                .setCatalogSessionProperty("snowflake", "warehouse", INVALID_WAREHOUSE)
                .build();

        String expectedMessageRegExp = "Could not query Snowflake due to invalid warehouse configuration. " +
                "Fix configuration or select an active Snowflake warehouse with 'warehouse' catalog session property.";
        assertQuery(invalidWarehouseSession, "SELECT * FROM current_warehouse", "VALUES (NULL)");

        assertQueryFails(invalidWarehouseSession, "SELECT regionkey FROM nation WHERE name = 'ALGERIA'", expectedMessageRegExp);

        // Disable statistics precalculation so that code will throw while executing statement in SnowflakeSplitManager
        Session sessionWithoutStatisticsPrecalculation = Session.builder(invalidWarehouseSession)
                .setSystemProperty("statistics_precalculation_for_pushdown_enabled", "false")
                .build();

        assertQueryFails(sessionWithoutStatisticsPrecalculation, "SELECT COUNT(1) FROM nation WHERE name = 'ALGERIA'", expectedMessageRegExp);
    }
}
