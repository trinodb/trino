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

import com.google.common.io.Files;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.tpch.TpchTable;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createPrestoConnectorQueryRunner;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createRemotePrestoQueryRunnerWithHive;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.prestoConnectorConnectionUrl;

public class TestPrestoConnectorIntegrationSmokeTestWithHive
        extends BasePrestoConnectorIntegrationSmokeTest
{
    private DistributedQueryRunner remotePresto;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File tempDir = Files.createTempDir();
        closeAfterClass(() -> deleteRecursively(Path.of(tempDir.getPath()), ALLOW_INSECURE));
        remotePresto = closeAfterClass(createRemotePrestoQueryRunnerWithHive(
                tempDir,
                Map.of(),
                TpchTable.getTables()));
        return createPrestoConnectorQueryRunner(
                false,
                Map.of(),
                Map.of("connection-url", prestoConnectorConnectionUrl(remotePresto, getRemoteCatalogName())));
    }

    @Test
    public void testReadFromRemoteHiveView()
    {
        remotePresto.execute("CREATE OR REPLACE VIEW hive.tiny.nation_count AS SELECT COUNT(*) cnt FROM tpch.tiny.nation");
        assertQuery("SELECT cnt FROM nation_count", "SELECT 25");
    }

    @Override
    protected String getRemoteCatalogName()
    {
        return "hive";
    }

    @Override
    protected SqlExecutor getSqlExecutor()
    {
        return remotePresto::execute;
    }
}
