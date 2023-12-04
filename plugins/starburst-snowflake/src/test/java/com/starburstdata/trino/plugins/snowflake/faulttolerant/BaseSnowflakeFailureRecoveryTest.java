/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.faulttolerant;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner;
import com.starburstdata.trino.plugins.snowflake.SnowflakeServer;
import com.starburstdata.trino.plugins.snowflake.TestDatabase;
import io.trino.operator.RetryPolicy;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.jdbc.BaseJdbcFailureRecoveryTest;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.jdbcBuilder;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public abstract class BaseSnowflakeFailureRecoveryTest
        extends BaseJdbcFailureRecoveryTest
{
    private Closer closer;

    public BaseSnowflakeFailureRecoveryTest(RetryPolicy retryPolicy)
    {
        super(retryPolicy);
    }

    @AfterAll
    public void cleanup()
            throws IOException
    {
        closer.close();
    }

    @Override
    protected QueryRunner createQueryRunner(List<TpchTable<?>> requiredTpchTables, Map<String, String> configProperties, Map<String, String> coordinatorProperties)
            throws Exception
    {
        SnowflakeServer snowflakeServer = new SnowflakeServer();
        closer = Closer.create();
        TestDatabase testDB = closer.register(snowflakeServer.createTestDatabase());
        return getBuilder()
                .withServer(snowflakeServer)
                .withExtraProperties(configProperties)
                .withConnectorProperties(impersonationDisabled())
                .withDatabase(Optional.of(testDB.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withCreateUserContextView()
                .withCoordinatorProperties(coordinatorProperties)
                .withTpchTables(requiredTpchTables)
                .withAdditionalSetup(runner -> {
                    runner.installPlugin(new FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", ImmutableMap.of(
                            "exchange.base-directories", System.getProperty("java.io.tmpdir") + "/trino-local-file-system-exchange-manager"));
                })
                .build();
    }

    protected SnowflakeQueryRunner.Builder getBuilder()
    {
        return jdbcBuilder();
    }

    @Test
    @Override
    protected void testUpdateWithSubquery()
    {
        assertThatThrownBy(super::testUpdateWithSubquery).hasMessageContaining("Unexpected Join over for-update table scan");
        abort("skipped");
    }

    @Test
    @Override
    protected void testUpdate()
    {
        // This simple update on JDBC ends up as a very simple, single-fragment, coordinator-only plan,
        // which has no ability to recover from errors. This test simply verifies that's still the case.
        Optional<String> setupQuery = Optional.of("CREATE TABLE <table> AS SELECT * FROM orders");
        String testQuery = "UPDATE <table> SET shippriority = 101 WHERE custkey = 1";
        Optional<String> cleanupQuery = Optional.of("DROP TABLE <table>");

        assertThatQuery(testQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .isCoordinatorOnly();
    }
}
