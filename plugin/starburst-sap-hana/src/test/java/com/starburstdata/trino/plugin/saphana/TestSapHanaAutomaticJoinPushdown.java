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

import io.trino.plugin.jdbc.BaseAutomaticJoinPushdownTest;
import io.trino.testing.QueryRunner;

import java.util.List;
import java.util.Map;

import static com.starburstdata.trino.plugin.saphana.SapHanaQueryRunner.createSapHanaQueryRunner;
import static java.lang.String.format;

public class TestSapHanaAutomaticJoinPushdown
        extends BaseAutomaticJoinPushdownTest
{
    protected TestingSapHanaServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(TestingSapHanaServer.create());
        return createSapHanaQueryRunner(
                server,
                Map.of(),
                Map.of(),
                List.of());
    }

    @Override
    protected void gatherStats(String tableName)
    {
        String schemaTableName = format("tpch.%s", tableName);
        server.execute("CREATE STATISTICS ON " + schemaTableName + " TYPE SIMPLE");
        // MERGE DELTA is required to force a stats refresh and ensure stats are up to date.
        // If not invoked explicitly, MERGE DELTA happens periodically about every 2 minutes, unless disabled for a table.
        // If disabled, stats are never updated automatically.
        server.execute("MERGE DELTA OF " + schemaTableName + " WITH PARAMETERS ('FORCED_MERGE' = 'ON')");
    }
}
