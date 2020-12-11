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
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.SqlExecutor;
import io.prestosql.tpch.TpchTable;
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

    @Test(enabled = false)
    @Override
    public void testAggregationWithUnsupportedResultType()
    {
        /* TODO: Investigate random test failures

           This test is disabled because it fails with:

        java.lang.AssertionError: [Rows]
        Expecting:
          <([0, 1, 2, 3, 4, 5, 6, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 7, 8, 9, 10, 11, 12, 13, 14])>
        to contain exactly in any order:
          <[([0, 7, 8, 9, 10, 11, 12, 13, 14, 3, 4, 5, 6, 1, 2, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24])]>
        elements not found:
          <([0, 7, 8, 9, 10, 11, 12, 13, 14, 3, 4, 5, 6, 1, 2, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24])>
        and elements not expected:
          <([0, 1, 2, 3, 4, 5, 6, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 7, 8, 9, 10, 11, 12, 13, 14])>

            at io.prestosql.sql.query.QueryAssertions$QueryAssert.lambda$matches$1(QueryAssertions.java:287)
            at io.prestosql.sql.query.QueryAssertions$QueryAssert.matches(QueryAssertions.java:274)
            at io.prestosql.sql.query.QueryAssertions$QueryAssert.verifyResultsWithPushdownDisabled(QueryAssertions.java:357)
            at io.prestosql.sql.query.QueryAssertions$QueryAssert.isNotFullyPushedDown(QueryAssertions.java:334)
            at com.starburstdata.presto.plugin.prestoconnector.BasePrestoConnectorIntegrationSmokeTest.testAggregationWithUnsupportedResultType(BasePrestoConnectorIntegrationSmokeTest.java:92)
         */
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
