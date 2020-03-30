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

import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.oktaImpersonationEnabled;
import static org.testng.Assert.assertEquals;

public class TestSnowflakeJmxStats
        extends AbstractTestQueryFramework
{
    protected TestSnowflakeJmxStats()
    {
        super(() -> distributedBuilder()
                // using single worker instance, because workers overwrites their JMX stats
                .withAdditionalProperties(oktaImpersonationEnabled(false))
                .withNodeCount(1)
                .build());
    }

    @Test
    public void testAuthenticationJmxStats()
    {
        getQueryRunner().execute(getSession(), "SELECT * FROM orders LIMIT 1");
        getQueryRunner().execute(getSession(), "SELECT * FROM nation LIMIT 1");
        MaterializedResult rows = getQueryRunner().execute(
                getSession(),
                "SELECT \"authenticate.time.alltime.count\", \"obtainsamlassertion.time.alltime.count\" " +
                        "FROM jmx.current.\"presto.plugin.snowflake.auth:name=snowflake,type=statscollectingoktaauthclient\"");
        // we are running with 2 nodes
        assertEquals(rows.getRowCount(), 1);
        MaterializedRow oktaStatsRow = rows.getMaterializedRows().get(0);
        // We should have made each call to Okta only once; after that, the cache should have been used
        assertEquals(oktaStatsRow.getField(0), 1.0);
        assertEquals(oktaStatsRow.getField(1), 1.0);

        rows = getQueryRunner().execute(
                getSession(),
                "SELECT \"generatesamlrequest.time.alltime.count\", \"requestoauthtoken.time.alltime.count\" " +
                        "FROM jmx.current.\"presto.plugin.snowflake.auth:name=snowflake,type=statscollectingsnowflakeauthclient\"");
        assertEquals(rows.getRowCount(), 1);
        MaterializedRow snowflakeStatsRow = rows.getMaterializedRows().get(0);
        // We should have made each call to Snowflake only once; after that, the cache should have been used
        assertEquals(snowflakeStatsRow.getField(0), 1.0);
        assertEquals(snowflakeStatsRow.getField(1), 1.0);
    }
}
