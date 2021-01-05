/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */

package com.starburstdata.presto.plugin.prestoconnector;

import io.trino.plugin.postgresql.TestingPostgreSqlServer;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createPrestoConnectorQueryRunner;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createRemotePrestoQueryRunnerWithPostgreSql;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.prestoConnectorConnectionUrl;

public class TestPrestoConnectorDistributedQueriesWithPostgreSql
        extends BasePrestoConnectorDistributedQueriesWithoutWrites
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingPostgreSqlServer postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer());
        DistributedQueryRunner remotePresto = closeAfterClass(createRemotePrestoQueryRunnerWithPostgreSql(
                postgreSqlServer,
                Map.of(),
                Map.of(
                        "connection-url", postgreSqlServer.getJdbcUrl(),
                        "connection-user", postgreSqlServer.getUser(),
                        "connection-password", postgreSqlServer.getPassword()),
                TpchTable.getTables(),
                Optional.empty()));
        return createPrestoConnectorQueryRunner(
                false,
                Map.of(),
                Map.of(
                        "connection-url", prestoConnectorConnectionUrl(remotePresto, "postgresql"),
                        "allow-drop-table", "true"));
    }
}
