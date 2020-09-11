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
import io.prestosql.plugin.sqlserver.TestingSqlServer;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.AfterClass;

import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.createStarburstSqlServerQueryRunner;
import static io.prestosql.tpch.TpchTable.ORDERS;

public class TestDynamicFiltering
        extends AbstractDynamicFilteringTest
{
    private TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = new TestingSqlServer();
        sqlServer.start();
        return createStarburstSqlServerQueryRunner(sqlServer, ORDERS);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        sqlServer.close();
    }

    @Override
    protected boolean isAssertNumberOfSplits()
    {
        // JDBC connectors always generate single split
        return false;
    }
}
