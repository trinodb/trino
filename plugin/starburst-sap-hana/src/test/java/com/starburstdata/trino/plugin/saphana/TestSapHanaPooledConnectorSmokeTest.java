/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.saphana;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.tpch.TpchTable;

import static com.starburstdata.trino.plugin.saphana.SapHanaQueryRunner.createSapHanaQueryRunner;

public class TestSapHanaPooledConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingSapHanaServer server = closeAfterClass(TestingSapHanaServer.create());
        return createSapHanaQueryRunner(
                server,
                ImmutableMap.<String, String>builder()
                        .put("connection-pool.enabled", "true")
                        .buildOrThrow(),
                ImmutableMap.<String, String>builder()
                        .put("scale-writers", "false")
                        .put("task.scale-writers.enabled", "false")
                        .buildOrThrow(),
                TpchTable.getTables());
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_RENAME_SCHEMA:
            case SUPPORTS_ARRAY:
            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
            case SUPPORTS_MERGE:
            case SUPPORTS_ROW_LEVEL_UPDATE:
                return false;

            case SUPPORTS_UPDATE:
                return true;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }
}
