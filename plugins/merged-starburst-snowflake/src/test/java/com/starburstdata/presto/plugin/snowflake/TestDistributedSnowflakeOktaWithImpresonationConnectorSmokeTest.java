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
import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.testing.QueryRunner;

import java.util.Optional;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.oktaImpersonationEnabled;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.OKTA_PASSWORD;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.OKTA_USER;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestDistributedSnowflakeOktaWithImpresonationConnectorSmokeTest
        extends BaseDistributedSnowflakeConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return distributedBuilder()
                .withServer(server)
                .withDatabase(Optional.of(testDatabase.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(oktaImpersonationEnabled(true))
                .withOktaCredentials(true)
                .withTpchTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected Session getSession()
    {
        return testSessionBuilder()
                .setCatalog("snowflake")
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.forUser(OKTA_USER)
                        .withExtraCredentials(ImmutableMap.of(
                                "okta.user", OKTA_USER,
                                "okta.password", OKTA_PASSWORD))
                        .build())
                .build();
    }
}
