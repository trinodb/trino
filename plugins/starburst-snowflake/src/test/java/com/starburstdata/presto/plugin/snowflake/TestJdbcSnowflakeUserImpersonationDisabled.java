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

import com.starburstdata.presto.testing.Closer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Optional;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.jdbcBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.USER;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class TestJdbcSnowflakeUserImpersonationDisabled
        extends AbstractTestQueryFramework
{
    protected final SnowflakeServer server = new SnowflakeServer();
    protected final Closer closer = Closer.create();
    protected final TestDatabase testDatabase = closer.register(server.createTestDatabase());

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return jdbcBuilder()
                .withServer(server)
                .withDatabase(Optional.of(testDatabase.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectionPooling()
                .withConnectorProperties(impersonationDisabled())
                .withCreateUserContextView()
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        closer.close();
    }

    @Test
    public void testUsesUserDefaultRole()
    {
        assertQuery("SELECT * FROM public.user_context", format("VALUES ('%s', 'TEST_ROLE')", USER.toUpperCase(ENGLISH)));
    }
}
