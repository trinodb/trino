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

import io.trino.plugin.sqlserver.BaseSqlServerConnectorSmokeTest;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.testing.QueryRunner;

import static io.trino.plugin.sqlserver.TestingSqlServer.LATEST_VERSION;

public class TestStarburstSqlServerLatestConnectorSmokeTest
        extends BaseSqlServerConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingSqlServer sqlServer = closeAfterClass(new TestingSqlServer(LATEST_VERSION));
        return StarburstSqlServerQueryRunner.builder(sqlServer)
                .withTables(REQUIRED_TPCH_TABLES)
                .build();
    }
}
