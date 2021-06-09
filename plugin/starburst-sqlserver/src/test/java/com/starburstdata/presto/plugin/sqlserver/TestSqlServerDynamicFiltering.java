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

import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.AbstractDynamicFilteringTest;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;

import java.util.List;
import java.util.Map;

import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.createStarburstSqlServerQueryRunner;
import static io.trino.tpch.TpchTable.ORDERS;

public class TestSqlServerDynamicFiltering
        extends AbstractDynamicFilteringTest
{
    private TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = new TestingSqlServer();
        return createStarburstSqlServerQueryRunner(sqlServer, true, Map.of(), List.of(ORDERS));
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        sqlServer.close();
    }

    @Override
    protected boolean isJoinPushdownEnabledByDefault()
    {
        return true;
    }
}
