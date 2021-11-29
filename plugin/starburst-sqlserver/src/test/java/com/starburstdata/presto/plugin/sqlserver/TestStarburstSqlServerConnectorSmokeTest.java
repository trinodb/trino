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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.sqlserver.BaseSqlServerConnectorSmokeTest;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.testing.QueryRunner;

import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerQueryRunner.createStarburstSqlServerQueryRunner;

public class TestStarburstSqlServerConnectorSmokeTest
        extends BaseSqlServerConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingSqlServer sqlServer = closeAfterClass(new TestingSqlServer());
        return createStarburstSqlServerQueryRunner(sqlServer, false, ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }
}
