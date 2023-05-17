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

import com.google.common.io.Closer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testng.services.ManageTestResources;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Optional;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.ALICE_USER;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.createSessionForUser;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.jdbcBuilder;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeServer.ROLE;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeServer.USER;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class TestJdbcSnowflakeWithFixedRole
        extends AbstractTestQueryFramework
{
    @ManageTestResources.Suppress(because = "Mock to remote server")
    protected final SnowflakeServer server = new SnowflakeServer();
    @ManageTestResources.Suppress(because = "Used by mocks")
    protected final Closer closer = Closer.create();
    @ManageTestResources.Suppress(because = "Mock to remote database")
    protected final TestDatabase testDB = closer.register(server.createTestDatabase());

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createBuilder()
                .withServer(server)
                .withConnectorProperties(impersonationDisabled())
                .withDatabase(Optional.of(testDB.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withCreateUserContextView()
                .build();
    }

    protected SnowflakeQueryRunner.Builder createBuilder()
    {
        return jdbcBuilder();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        closer.close();
    }

    @Test
    public void testUsersAreNotImpersonated()
    {
        assertQuery(
                createSessionForUser(ALICE_USER),
                "SELECT * FROM public.user_context",
                format("VALUES ('%s', '%s')", USER.toUpperCase(ENGLISH), ROLE.toUpperCase(ENGLISH)));
    }
}
