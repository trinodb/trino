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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.jdbcBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.PUBLIC_DB;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.USER;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class TestJdbcSnowflakeUserImpersonationDisabled
        extends AbstractTestQueryFramework
{
    protected final SnowflakeServer server = new SnowflakeServer();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return jdbcBuilder()
                .withServer(server)
                .withDatabase(Optional.of(PUBLIC_DB))
                .withConnectionPooling()
                .build();
    }

    @Test
    public void testUsesUserDefaultRole()
    {
        assertQuery("SELECT * FROM public.user_context", format("VALUES ('%s', 'TEST_DEFAULT_ROLE')", USER.toUpperCase(ENGLISH)));
    }
}
