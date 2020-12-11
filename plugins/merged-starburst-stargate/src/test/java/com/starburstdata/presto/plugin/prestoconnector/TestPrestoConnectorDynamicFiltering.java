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
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.AbstractDynamicFilteringTest;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import org.testng.SkipException;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createPrestoConnectorQueryRunner;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createRemotePrestoQueryRunnerWithHive;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.prestoConnectorConnectionUrl;
import static io.prestosql.tpch.TpchTable.ORDERS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPrestoConnectorDynamicFiltering
        extends AbstractDynamicFilteringTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File tempDir = Files.createTempDir();
        closeAfterClass(() -> deleteRecursively(Path.of(tempDir.getPath()), ALLOW_INSECURE));
        DistributedQueryRunner remotePresto = closeAfterClass(createRemotePrestoQueryRunnerWithHive(
                tempDir,
                Map.of(),
                List.of(ORDERS)));
        return createPrestoConnectorQueryRunner(
                false,
                Map.of(),
                Map.of(
                        "connection-url", prestoConnectorConnectionUrl(remotePresto, "hive"),
                        "allow-drop-table", "true"));
    }

    @Override
    protected boolean supportsSplitDynamicFiltering()
    {
        // JDBC connectors always generate single split
        // TODO https://starburstdata.atlassian.net/browse/PRESTO-4769 revisit in parallel Presto Connector
        return false;
    }

    @Override
    public void testDynamicFilteringDomainCompactionThreshold()
    {
        assertThatThrownBy(super::testDynamicFilteringDomainCompactionThreshold)
                .hasMessageContaining("This connector does not support creating tables with data");
        throw new SkipException("not supported");
    }
}
