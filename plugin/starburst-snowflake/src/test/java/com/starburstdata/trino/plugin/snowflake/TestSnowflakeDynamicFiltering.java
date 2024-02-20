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
import com.starburstdata.trino.plugin.snowflake.dynamicfiltering.AbstractDynamicFilteringTest;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;

import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeQueryRunner.parallelBuilder;
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
        return createBuilder()
                .withServer(server)
                .withDatabase(Optional.of(testDatabase.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(impersonationDisabled())
                .withTpchTables(ImmutableList.of(ORDERS))
                .build();
    }

    protected SnowflakeQueryRunner.Builder createBuilder()
    {
        return parallelBuilder();
    }

    // In the distributed SF connector, the page source on worker will accept and use dynamic filter
    // from the engine even though DFs are not pushed down to Snowflake as part of generated SQL query
    @Override
    @Test
    @Disabled
    public void testDynamicFilteringWithLimit()
    {
    }

    @AfterAll
    public void cleanup()
            throws IOException
    {
        closer.close();
    }

    @Override
    protected boolean isJoinPushdownEnabledByDefault()
    {
        return false;
    }
}
