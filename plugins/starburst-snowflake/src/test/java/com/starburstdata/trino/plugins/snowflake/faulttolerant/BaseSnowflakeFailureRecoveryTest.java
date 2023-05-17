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
import org.testng.annotations.AfterClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.jdbcBuilder;

public abstract class BaseSnowflakeFailureRecoveryTest
        extends BaseJdbcFailureRecoveryTest
{
    private SnowflakeServer snowflakeServer;
    private Closer closer;
    private TestDatabase testDB;

    public BaseSnowflakeFailureRecoveryTest(RetryPolicy retryPolicy)
    {
        super(retryPolicy);
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        closer.close();
        snowflakeServer = null;
        closer = null;
        testDB = null;
    }

    @Override
    protected QueryRunner createQueryRunner(List<TpchTable<?>> requiredTpchTables, Map<String, String> configProperties, Map<String, String> coordinatorProperties)
            throws Exception
    {
        snowflakeServer = new SnowflakeServer();
        closer = Closer.create();
        testDB = closer.register(snowflakeServer.createTestDatabase());
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
}
