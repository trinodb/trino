/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.plugin.jdbc.BaseJdbcTableStatisticsTest;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseStargateTableStatisticsTest
        extends BaseJdbcTableStatisticsTest
{
    @Override
    public void testStatsWithPredicatePushdownWithStatsPrecalculationDisabled()
    {
        // test overridden because Stargate connector does not leverage statistics precalculation

        assertThat(query("SHOW STATS FOR nation"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .projected(0, 2, 3, 4)
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('nationkey', 25e0, 0e0, null)," +
                        "('name', 25e0, 0e0, null)," +
                        "('regionkey', 5e0, 0e0, null)," +
                        "('comment', 25e0, 0e0, null)," +
                        "(null, null, null, 25e0)");

        // Predicate on a numeric column. Should be eligible for pushdown.
        String query = "SELECT * FROM nation WHERE regionkey = 1";
        Session session = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.STATISTICS_PRECALCULATION_FOR_PUSHDOWN_ENABLED, "false")
                .build();

        // Verify query can be pushed down, that's the situation we want to test for.
        assertThat(query(session, query)).isFullyPushedDown();

        assertThat(query(session, "SHOW STATS FOR (" + query + ")"))
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .projected(0, 2, 3, 4)
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('nationkey', 5e0, 0e0, null)," +
                        "('regionkey', 1e0, 0e0, null)," +
                        "('name', 5e0, 0e0, null)," +
                        "('comment', 5e0, 0e0, null)," +
                        "(null, null, null, 5e0)");
    }

    @Test
    public abstract void testShowStatsWithWhere();

    @Test
    public abstract void testShowStatsWithCount();

    @Test
    public abstract void testShowStatsWithGroupBy();

    @Test
    public abstract void testShowStatsWithFilterGroupBy();

    @Test
    public abstract void testShowStatsWithSelectDistinct();
}
