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

import io.prestosql.plugin.sqlserver.TestSqlServerIntegrationSmokeTest;
import io.prestosql.plugin.sqlserver.TestingSqlServer;
import io.prestosql.testing.QueryRunner;

import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.createStarburstSqlServerQueryRunner;
import static io.prestosql.tpch.TpchTable.ORDERS;

public class TestStarburstSqlServerIntegrationSmokeTest
        extends TestSqlServerIntegrationSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = new TestingSqlServer();
        sqlServer.start();
        return createStarburstSqlServerQueryRunner(sqlServer, ORDERS);
    }
}
