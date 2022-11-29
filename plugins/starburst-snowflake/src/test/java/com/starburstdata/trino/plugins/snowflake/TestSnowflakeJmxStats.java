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

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testng.services.ManageTestResources;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Optional;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestSnowflakeJmxStats
        extends AbstractTestQueryFramework
{
    @ManageTestResources.Suppress(because = "Mock to remote server")
    protected final SnowflakeServer server = new SnowflakeServer();
    @ManageTestResources.Suppress(because = "Used by mocks")
    protected final Closer closer = Closer.create();
    @ManageTestResources.Suppress(because = "Mock to remote database")
    protected final TestDatabase testDatabase = closer.register(server.createTestDatabase());

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return distributedBuilder()
                .withServer(server)
                .withDatabase(Optional.of(testDatabase.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(impersonationDisabled())
                // using single worker instance, because workers overwrites their JMX stats
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
                format(
                        "SELECT \"authenticate.time.alltime.count\", \"obtainsamlassertion.time.alltime.count\""
                        + " FROM jmx.current.\"%s.auth:name=snowflake,type=statscollectingoktaauthclient\"",
                        getJmxDomainBase()));
        // we are running with 2 nodes
        assertEquals(rows.getRowCount(), 1);
        MaterializedRow oktaStatsRow = rows.getMaterializedRows().get(0);
        // We should have made each call to Okta only once; after that, the cache should have been used
        assertEquals(oktaStatsRow.getField(0), 1.0);
        assertEquals(oktaStatsRow.getField(1), 1.0);

        rows = getQueryRunner().execute(
                getSession(),
                format(
                        "SELECT \"generatesamlrequest.time.alltime.count\", \"requestoauthtoken.time.alltime.count\""
                        + " FROM jmx.current.\"%s.auth:name=snowflake,type=statscollectingsnowflakeauthclient\"",
                        getJmxDomainBase()));
        assertEquals(rows.getRowCount(), 1);
        MaterializedRow snowflakeStatsRow = rows.getMaterializedRows().get(0);
        // We should have made each call to Snowflake only once; after that, the cache should have been used
        assertEquals(snowflakeStatsRow.getField(0), 1.0);
        assertEquals(snowflakeStatsRow.getField(1), 1.0);
    }

    /**
     * Get the JMX domain base; either the plugin's base package name or whatever
     * is provided when binding {@link ConnectorObjectNameGeneratorModule}.
     */
    protected String getJmxDomainBase()
    {
        return "starburst.plugin.snowflake";
    }
}
