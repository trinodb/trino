/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.starburstremote;

import io.trino.plugin.postgresql.TestingPostgreSqlServer;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.tpch.TpchTable;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunner;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunnerWithPostgreSql;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.starburstRemoteConnectionUrl;

public class TestStarburstRemoteIntegrationSmokeTestWithPostgreSql
        extends BaseStarburstRemoteIntegrationSmokeTest
{
    private DistributedQueryRunner remoteStarburst;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingPostgreSqlServer postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer());
        remoteStarburst = closeAfterClass(createStarburstRemoteQueryRunnerWithPostgreSql(
                postgreSqlServer,
                Map.of(),
                Map.of("connection-url", postgreSqlServer.getJdbcUrl()),
                TpchTable.getTables(),
                Optional.empty()));
        return createStarburstRemoteQueryRunner(
                false,
                Map.of(),
                Map.of("connection-url", starburstRemoteConnectionUrl(remoteStarburst, getRemoteCatalogName())));
    }

    @Override
    protected String getRemoteCatalogName()
    {
        return "postgresql";
    }

    @Override
    protected SqlExecutor getSqlExecutor()
    {
        return remoteStarburst::execute;
    }
}
