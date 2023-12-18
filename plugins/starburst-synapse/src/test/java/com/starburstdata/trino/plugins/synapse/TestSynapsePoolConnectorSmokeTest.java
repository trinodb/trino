/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.synapse;

import io.trino.plugin.jdbc.BaseJdbcConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;

import java.util.Map;

import static com.starburstdata.trino.plugins.synapse.SynapseQueryRunner.createSynapseQueryRunner;

public final class TestSynapsePoolConnectorSmokeTest
        extends BaseJdbcConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createSynapseQueryRunner(
                Map.of(),
                new SynapseServer(),
                "sqlserver",
                // No metadata cache enabled here.
                Map.of("connection-pool.enabled", "true"),
                REQUIRED_TPCH_TABLES);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY:
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
                return false;

            case SUPPORTS_JOIN_PUSHDOWN:
                return true;

            case SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_RENAME_SCHEMA:
                return false;

            case SUPPORTS_ARRAY:
                return false;

            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }
}
