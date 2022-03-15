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

import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.redirection.AbstractTableScanRedirectionTest;
import com.starburstdata.presto.testing.Closer;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;

import java.io.IOException;
import java.util.Optional;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;

public class TestDistributedSnowflakeTableScanRedirection
        extends AbstractTableScanRedirectionTest
{
    protected final SnowflakeServer server = new SnowflakeServer();
    protected final Closer closer = Closer.create();
    protected final TestDatabase testDatabase = closer.register(server.createTestDatabase());

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return distributedBuilder()
                .withServer(server)
                .withDatabase(Optional.of(testDatabase.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(impersonationDisabled())
                        .putAll(getRedirectionProperties("snowflake", TEST_SCHEMA))
                        .buildOrThrow())
                .withTpchTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        closer.close();
    }

    @Override
    protected String getRedirectionsJmxTableName()
    {
        // SF connector uses ConnectorObjectNameGeneratorModule which overrides default name generation
        return "jmx.current.\"com.starburstdata.presto.plugin.jdbc.redirection:name=snowflake,type=redirectionstats\"";
    }
}
