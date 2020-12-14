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

import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.SqlExecutor;

import java.util.List;
import java.util.Map;

import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createPrestoConnectorQueryRunner;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createRemotePrestoQueryRunnerWithMemory;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.prestoConnectorConnectionUrl;
import static io.airlift.testing.Closeables.closeAllSuppress;

public class TestPrestoConnectorIntegrationSmokeTest
        extends BasePrestoConnectorIntegrationSmokeTest
{
    private DistributedQueryRunner remotePresto;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        remotePresto = closeAfterClass(createRemotePrestoQueryRunnerWithMemory(
                Map.of(),
                List.of()));
        DistributedQueryRunner queryRunner = createPrestoConnectorQueryRunner(
                false,
                Map.of(),
                Map.of("connection-url", prestoConnectorConnectionUrl(remotePresto, "tpch")));
        try {
            queryRunner.createCatalog("p2p_" + getRemoteCatalogName(), "presto-connector", Map.of(
                    "connection-user", "p2p",
                    "connection-url", prestoConnectorConnectionUrl(remotePresto, getRemoteCatalogName())));
        }
        catch (Exception e) {
            throw closeAllSuppress(e, queryRunner);
        }

        return queryRunner;
    }

    @Override
    protected String getRemoteCatalogName()
    {
        return "memory";
    }

    @Override
    protected SqlExecutor getSqlExecutor()
    {
        return remotePresto::execute;
    }
}
