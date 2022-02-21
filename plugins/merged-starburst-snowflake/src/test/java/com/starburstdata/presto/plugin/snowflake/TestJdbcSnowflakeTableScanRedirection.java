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
import io.trino.testing.QueryRunner;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.jdbcBuilder;

public class TestJdbcSnowflakeTableScanRedirection
        extends AbstractTableScanRedirectionTest
{
    protected final SnowflakeServer server = new SnowflakeServer();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return jdbcBuilder()
                .withServer(server)
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(impersonationDisabled())
                        .putAll(getRedirectionProperties("snowflake", TEST_SCHEMA))
                        .buildOrThrow())
                .build();
    }

    @Override
    protected String getRedirectionsJmxTableName()
    {
        // SF connector uses ConnectorObjectNameGeneratorModule which overrides default name generation
        return "jmx.current.\"com.starburstdata.presto.plugin.jdbc.redirection:name=snowflake,type=redirectionstats\"";
    }
}
