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

import com.starburstdata.presto.plugin.jdbc.redirection.AbstractTableScanRedirectionTest;
import io.prestosql.plugin.sqlserver.TestingSqlServer;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;
import org.testng.annotations.AfterClass;

import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.createStarburstSqlServerQueryRunner;

public class TestStarburstSqlServerTableScanRedirection
        extends AbstractTableScanRedirectionTest
{
    private TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        sqlServer = new TestingSqlServer();
        sqlServer.start();
        return createStarburstSqlServerQueryRunner(
                sqlServer,
                true,
                getRedirectionProperties("sqlserver", "dbo"),
                tables);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        sqlServer.close();
    }
}
