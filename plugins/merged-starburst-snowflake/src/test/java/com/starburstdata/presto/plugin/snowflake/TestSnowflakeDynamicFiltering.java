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
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.AbstractDynamicFilteringTest;
import com.starburstdata.presto.testing.Closer;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;

import java.io.IOException;
import java.util.Optional;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static io.trino.tpch.TpchTable.ORDERS;

public class TestSnowflakeDynamicFiltering
        extends AbstractDynamicFilteringTest
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
                .withConnectorProperties(impersonationDisabled())
                .withTpchTables(ImmutableList.of(ORDERS))
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        closer.close();
    }

    @Override
    protected String getDynamicFilteringJmxTableName()
    {
        // SF connector uses ConnectorObjectNameGeneratorModule which overrides default name generation
        return "jmx.current.\"com.starburstdata.presto.plugin.jdbc.dynamicfiltering:catalog=snowflake,name=snowflake,type=dynamicfilteringstats\"";
    }

    @Override
    protected boolean isJoinPushdownEnabledByDefault()
    {
        return false;
    }
}
