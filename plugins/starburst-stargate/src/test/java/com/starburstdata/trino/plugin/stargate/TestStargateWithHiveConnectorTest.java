/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.createRemoteStarburstQueryRunnerWithHive;
import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.createStargateQueryRunner;
import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.stargateConnectionUrl;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStargateWithHiveConnectorTest
        extends BaseStargateConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path tempDir = createTempDirectory("HiveCatalog");
        closeAfterClass(() -> deleteRecursively(tempDir, ALLOW_INSECURE));

        remoteStarburst = closeAfterClass(createRemoteStarburstQueryRunnerWithHive(
                tempDir,
                REQUIRED_TPCH_TABLES,
                Optional.empty()));
        return createStargateQueryRunner(
                false,
                Map.of("connection-url", stargateConnectionUrl(remoteStarburst, getRemoteCatalogName())));
    }

    @Override
    protected String getRemoteCatalogName()
    {
        return "hive";
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_RENAME_TABLE:
            case SUPPORTS_INSERT:
                // Writes are not enabled
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    public void testCaseSensitiveAggregationPushdown()
    {
        // This is tested in TestStarburstRemoteWithMemoryWritesEnabledConnectorTest
        assertThatThrownBy(super::testCaseSensitiveAggregationPushdown)
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("tested elsewhere");
    }

    @Override
    public void testCaseSensitiveTopNPushdown()
    {
        // This is tested in TestStarburstRemoteWithMemoryWritesEnabledConnectorTest
        assertThatThrownBy(super::testCaseSensitiveTopNPushdown)
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("tested elsewhere");
    }

    @Override
    public void testNullSensitiveTopNPushdown()
    {
        // This is tested in TestStarburstRemoteWithMemoryWritesEnabledConnectorTest
        assertThatThrownBy(super::testNullSensitiveTopNPushdown)
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("tested elsewhere");
    }

    @Override
    public void testCommentColumn()
    {
        // TODO (https://starburstdata.atlassian.net/browse/SEP-4832) make sure this is tested
        assertThatThrownBy(super::testCommentColumn)
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Test
    public void testReadFromRemoteHiveView()
    {
        onRemoteDatabase().execute("CREATE OR REPLACE VIEW hive.tiny.nation_count AS SELECT COUNT(*) cnt FROM tpch.tiny.nation");
        assertQuery("SELECT cnt FROM nation_count", "SELECT 25");
    }

    @Test
    public void testPredicatePushdown()
    {
        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        // bigint equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey = 19"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // bigint range, with decimal to bigint simplification
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey BETWEEN 18.5 AND 19.5"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // date equality
        assertThat(query("SELECT orderkey FROM orders WHERE orderdate = DATE '1992-09-29'"))
                .matches("VALUES BIGINT '1250', 34406, 38436, 57570")
                .isFullyPushedDown();
    }

    /**
     * This test normally requires table creation, so we disable it here.
     * We will trust that
     * {@link TestStargateWithMemoryWritesEnabledConnectorTest} tests
     * join pushdown sufficiently until we can run this test without creating
     * tables using the Remote Starburst instance.
     */
    @Override
    public void testJoinPushdown()
    {
        // Make sure that the test still fails how we expect it to, on table creation instead
        // of on join pushdown.
        assertThatThrownBy(super::testJoinPushdown)
                .hasMessageMatching("This connector does not support creating tables.*");
        throw new SkipException("test requires table creation");
    }

    @Override
    public void testAddColumn()
    {
        // This test requires table creation, so we disable it here.
        // It's tested by TestStarburstWithMemoryWritesEnabledConnectorTest
        assertThatThrownBy(super::testAddColumn)
                .hasMessageMatching("This connector does not support creating tables.*");
        throw new SkipException("test requires table creation");
    }

    @Override
    public void testAddColumnWithComment()
    {
        // This test requires table creation, so we disable it here.
        // It's tested by TestStarburstRemoteWithMemoryWritesEnabledConnectorTest
        assertThatThrownBy(super::testAddColumnWithComment)
                .hasMessageMatching("This connector does not support creating tables.*");
        throw new SkipException("test requires table creation");
    }
}
