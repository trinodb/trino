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

import com.google.common.io.Files;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.tpch.TpchTable;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunner;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunnerWithHive;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.starburstRemoteConnectionUrl;

public class TestStarburstRemoteIntegrationSmokeTestWithHive
        extends BaseStarburstRemoteIntegrationSmokeTest
{
    private DistributedQueryRunner remoteStarburst;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File tempDir = Files.createTempDir();
        closeAfterClass(() -> deleteRecursively(Path.of(tempDir.getPath()), ALLOW_INSECURE));
        remoteStarburst = closeAfterClass(createStarburstRemoteQueryRunnerWithHive(
                tempDir,
                Map.of(),
                TpchTable.getTables(),
                Optional.empty()));
        return createStarburstRemoteQueryRunner(
                false,
                Map.of(),
                Map.of("connection-url", starburstRemoteConnectionUrl(remoteStarburst, getRemoteCatalogName())));
    }

    @Test
    public void testReadFromRemoteHiveView()
    {
        remoteStarburst.execute("CREATE OR REPLACE VIEW hive.tiny.nation_count AS SELECT COUNT(*) cnt FROM tpch.tiny.nation");
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
        return remoteStarburst::execute;
    }
}
