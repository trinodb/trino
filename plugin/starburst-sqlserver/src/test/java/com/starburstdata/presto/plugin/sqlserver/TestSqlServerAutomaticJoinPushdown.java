/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import com.starburstdata.presto.plugin.jdbc.joinpushdown.BaseAutomaticJoinPushdownTest;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.testing.QueryRunner;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.createStarburstSqlServerQueryRunner;
import static java.lang.String.format;

public class TestSqlServerAutomaticJoinPushdown
        extends BaseAutomaticJoinPushdownTest
{
    private TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        sqlServer = closeAfterClass(new TestingSqlServer());
        sqlServer.start();
        return createStarburstSqlServerQueryRunner(sqlServer, false, Map.of(), List.of());
    }

    @Override
    protected void gatherStats(String tableName)
    {
        List<String> columnNames = stream(computeActual("SHOW COLUMNS FROM " + tableName))
                .map(row -> (String) row.getField(0))
                .map(columnName -> "\"" + columnName + "\"")
                .collect(toImmutableList());

        for (String columnName : columnNames) {
            sqlServer.execute(format("CREATE STATISTICS %1$s ON %2$s (%1$s)", columnName, tableName));
        }

        sqlServer.execute("UPDATE STATISTICS " + tableName);
    }
}
