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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testng.services.ManageTestResources;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.SNOWFLAKE_CATALOG;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugins.snowflake.parallel.SnowflakeParallelSessionProperties.QUOTED_IDENTIFIERS_IGNORE_CASE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Locale.ENGLISH;

public class TestParallelSnowflakeQuotedIdentifiersIgnoreCaseWithCaseInsensitiveNameMatching
        extends AbstractTestQueryFramework
{
    @ManageTestResources.Suppress(because = "Mock to remote service provider")
    private SnowflakeServer server;
    private String testDbName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = new SnowflakeServer();
        TestDatabase testDb = closeAfterClass(server.createTestDatabase());
        testDbName = testDb.getName();
        return SnowflakeQueryRunner.parallelBuilder()
                .withServer(server)
                .withDatabase(Optional.of(testDbName))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(impersonationDisabled())
                .withConnectorProperties(ImmutableMap.of("case-insensitive-name-matching", "true"))
                .build();
    }

    @Test
    public void testEnablingIgnoreCase()
    {
        String tableName = "TaBle_" + randomNameSuffix();
        String sql = "CREATE TABLE %s.\"%s\" AS SELECT 1 test".formatted(TEST_SCHEMA, tableName);
        server.safeExecuteOnDatabase(testDbName, sql);

        assertQuerySucceeds("SELECT test FROM %s".formatted(tableName));
        assertQuerySucceeds("SELECT test FROM %s".formatted(tableName.toUpperCase(ENGLISH)));
        assertQuerySucceeds("SELECT test FROM %s".formatted(tableName.toLowerCase(ENGLISH)));
        Session ignoreCase = Session.builder(getSession())
                .setCatalogSessionProperty(SNOWFLAKE_CATALOG, QUOTED_IDENTIFIERS_IGNORE_CASE, "true")
                .build();
        assertQueryFails(ignoreCase, "SELECT test FROM %s".formatted(tableName), "snowflake.quoted_identifiers_ignore_case is invalid: true");
        assertQueryFails("SET SESSION snowflake.quoted_identifiers_ignore_case = true", "line 1:1: Enabling quoted_identifiers_ignore_case not supported for Snowflake when case-insensitive-name-matching is enabled");
    }
}
