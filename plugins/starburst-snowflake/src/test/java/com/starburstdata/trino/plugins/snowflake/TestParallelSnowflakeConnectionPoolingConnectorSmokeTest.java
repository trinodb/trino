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
import io.trino.plugin.jdbc.BaseJdbcConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testng.services.ManageTestResources;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.parallelBuilder;

public class TestParallelSnowflakeConnectionPoolingConnectorSmokeTest
        extends BaseJdbcConnectorSmokeTest
{
    @ManageTestResources.Suppress(because = "Mock to remote server")
    private final SnowflakeServer server = new SnowflakeServer();
    @ManageTestResources.Suppress(because = "Used by mocks")
    private final Closer closer = Closer.create();
    @ManageTestResources.Suppress(because = "Mock to remote database")
    private final TestDatabase testDatabase = closer.register(server.createTestDatabase());

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return parallelBuilder()
                .withServer(server)
                .withDatabase(Optional.of(testDatabase.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(impersonationDisabled())
                .withConnectorProperties(Map.of("connection-pool.enabled", "true"))
                .withTpchTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_ARRAY:
            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_SET_COLUMN_TYPE:
            case SUPPORTS_ROW_TYPE:
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT:
            case SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT:
            case SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT:
                return false;
            case SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV:
            case SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE:
            case SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE:
            case SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION:
            case SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION:
            case SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT:
            case SUPPORTS_JOIN_PUSHDOWN:
                return true;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }
}
