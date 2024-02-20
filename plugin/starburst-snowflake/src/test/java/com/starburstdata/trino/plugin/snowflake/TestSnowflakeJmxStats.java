/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;

import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.parallelBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSnowflakeJmxStats
        extends AbstractTestQueryFramework
{
    protected final SnowflakeServer server = new SnowflakeServer();
    protected final Closer closer = Closer.create();
    protected final TestDatabase testDatabase = closer.register(server.createTestDatabase());
    protected final String catalogName = "snowflake_" + randomNameSuffix();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return parallelBuilder()
                .withServer(server)
                .withDatabase(Optional.of(testDatabase.getName()))
                .withCatalog(catalogName)
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(impersonationDisabled())
                // using single worker instance, because workers overwrites their JMX stats
                .withNodeCount(1)
                .withTpchTables(ImmutableList.of(ORDERS, NATION))
                .build();
    }

    @AfterAll
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
                        "SELECT \"authenticate.time.alltime.count\", \"obtainsamlassertion.time.alltime.count\" "
                                + "FROM jmx.current.\"%s.auth:name=%s,type=statscollectingoktaauthclient\"",
                        getJmxDomainBase(),
                        catalogName));
        // we are running with 2 nodes
        assertThat(rows.getRowCount()).isEqualTo(1);
        MaterializedRow oktaStatsRow = rows.getMaterializedRows().get(0);
        // We should have made each call to Okta only once; after that, the cache should have been used
        assertThat(oktaStatsRow.getField(0)).isEqualTo(1.0);
        assertThat(oktaStatsRow.getField(1)).isEqualTo(1.0);

        rows = getQueryRunner().execute(
                getSession(),
                format(
                        "SELECT \"generatesamlrequest.time.alltime.count\", \"requestoauthtoken.time.alltime.count\" "
                                + "FROM jmx.current.\"%s.auth:name=%s,type=statscollectingsnowflakeauthclient\"",
                        getJmxDomainBase(),
                        catalogName));
        assertThat(rows.getRowCount()).isEqualTo(1);
        MaterializedRow snowflakeStatsRow = rows.getMaterializedRows().get(0);
        // We should have made each call to Snowflake only once; after that, the cache should have been used
        assertThat(snowflakeStatsRow.getField(0)).isEqualTo(1.0);
        assertThat(snowflakeStatsRow.getField(1)).isEqualTo(1.0);
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
