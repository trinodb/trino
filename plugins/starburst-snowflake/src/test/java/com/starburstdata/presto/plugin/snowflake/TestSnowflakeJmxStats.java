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

import com.google.common.collect.ImmutableList;
import com.starburstdata.presto.testing.Closer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Optional;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.oktaImpersonationEnabled;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static org.testng.Assert.assertEquals;

public class TestSnowflakeJmxStats
        extends AbstractTestQueryFramework
{
    protected final SnowflakeServer server = new SnowflakeServer();
    protected final Closer closer = Closer.create();
    protected final TestDatabase testDatabase = closer.register(server.createDatabase("TEST"));

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return distributedBuilder()
                .withServer(server)
                .withDatabase(Optional.of(testDatabase.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                // using single worker instance, because workers overwrites their JMX stats
                .withConnectorProperties(oktaImpersonationEnabled(false))
                .withOktaCredentials(true)
                .withNodeCount(1)
                .withTpchTables(ImmutableList.of(ORDERS, NATION))
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        closer.close();
    }

    @Test
    public void testAuthenticationJmxStats()
    {
        getQueryRunner().execute(getSession(), "SELECT * FROM orders LIMIT 1");
        getQueryRunner().execute(getSession(), "SELECT * FROM nation LIMIT 1");
        MaterializedResult rows = getQueryRunner().execute(
                getSession(),
                "SELECT \"authenticate.time.alltime.count\", \"obtainsamlassertion.time.alltime.count\" " +
                        "FROM jmx.current.\"starburst.plugin.snowflake.auth:name=snowflake,type=statscollectingoktaauthclient\"");
        // we are running with 2 nodes
        assertEquals(rows.getRowCount(), 1);
        MaterializedRow oktaStatsRow = rows.getMaterializedRows().get(0);
        // We should have made each call to Okta only once; after that, the cache should have been used
        assertEquals(oktaStatsRow.getField(0), 1.0);
        assertEquals(oktaStatsRow.getField(1), 1.0);

        rows = getQueryRunner().execute(
                getSession(),
                "SELECT \"generatesamlrequest.time.alltime.count\", \"requestoauthtoken.time.alltime.count\" " +
                        "FROM jmx.current.\"starburst.plugin.snowflake.auth:name=snowflake,type=statscollectingsnowflakeauthclient\"");
        assertEquals(rows.getRowCount(), 1);
        MaterializedRow snowflakeStatsRow = rows.getMaterializedRows().get(0);
        // We should have made each call to Snowflake only once; after that, the cache should have been used
        assertEquals(snowflakeStatsRow.getField(0), 1.0);
        assertEquals(snowflakeStatsRow.getField(1), 1.0);
    }
}
