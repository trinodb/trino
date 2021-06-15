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

import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.stargate.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunner;
import static com.starburstdata.trino.plugin.stargate.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunnerWithMemory;
import static com.starburstdata.trino.plugin.stargate.StarburstRemoteQueryRunner.starburstRemoteConnectionUrl;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStarburstRemoteWithMemoryWritesEnabledConnectorTest
        extends BaseJdbcConnectorTest
{
    private DistributedQueryRunner remote;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        remote = closeAfterClass(
                createStarburstRemoteQueryRunnerWithMemory(Map.of(), REQUIRED_TPCH_TABLES, Optional.empty()));
        return createStarburstRemoteQueryRunner(
                true,
                Map.of(),
                Map.of(
                        "connection-url", starburstRemoteConnectionUrl(remote, "memory"),
                        "allow-drop-table", "true"));
    }

    @Override
    @SuppressWarnings("DuplicateBranchesInSwitch")
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                return true;

            case SUPPORTS_CREATE_TABLE:
                return true;

            case SUPPORTS_COMMENT_ON_TABLE:
                // not yet supported in Remote connector
            case SUPPORTS_COMMENT_ON_COLUMN:
                // not supported in memory connector
                return false;

            case SUPPORTS_INSERT:
                return true;
            case SUPPORTS_DELETE:
                // memory connector does not support deletes
                return false;

            case SUPPORTS_JOIN_PUSHDOWN:
                return true;

            case SUPPORTS_ARRAY:
                // TODO Add support in Remote connector (https://starburstdata.atlassian.net/browse/SEP-4798)
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("not supported");
    }

    @Test
    @Override
    public void testAddColumn()
    {
        assertThatThrownBy(super::testAddColumn)
                .hasMessageContaining("This connector does not support adding columns");
        throw new SkipException("not supported");
    }

    @Test
    @Override
    public void testDropColumn()
    {
        assertThatThrownBy(super::testDropColumn)
                .hasMessageContaining("This connector does not support dropping columns");
        throw new SkipException("not supported");
    }

    @Test
    @Override
    public void testRenameColumn()
    {
        assertThatThrownBy(super::testRenameColumn)
                .hasMessageContaining("This connector does not support renaming columns");
        throw new SkipException("not supported");
    }

    @Test
    @Override
    public void testCommentColumn()
    {
        // The super test requires a matching message, but we get it with "query failed" added
        assertThatThrownBy(super::testCommentColumn)
                .hasMessageContaining("This connector does not support setting column comments");
    }

    @Test
    @Override // override with version from smoke tests to go faster
    public void testInsert()
    {
        String tableName = "test_create_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double)");
        assertUpdate("INSERT INTO " + tableName + " (a, b) VALUES (42, -38.5)", 1);
        assertThat(query("SELECT CAST(a AS bigint), b FROM " + tableName))
                .matches("VALUES (BIGINT '42', -385e-1)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override // override with version from smoke tests to go faster
    public void testCreateTableAsSelect()
    {
        String tableName = "test_create_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT BIGINT '42' a, DOUBLE '-38.5' b", 1);
        assertThat(query("SELECT CAST(a AS bigint), b FROM " + tableName))
                .matches("VALUES (BIGINT '42', -385e-1)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test(dataProvider = "largeInValuesCount")
    @Override // skip larger inputs to go faster
    public void testLargeIn(int size)
    {
        if (size > 500) {
            throw new SkipException("skipped to save time");
        }
        super.testLargeIn(size);
    }

    @Override
    public void testAggregationPushdown()
    {
        // TODO: Migrate to BaseJdbcConnectorTest
        throw new SkipException("tested in TestStarburstRemoteWithHiveConnectorTest");
    }

    @Override
    public void testDistinctAggregationPushdown()
    {
        // TODO: Migrate to BaseJdbcConnectorTest
        throw new SkipException("tested in TestStarburstRemoteWithHiveConnectorTest");
    }

    @Override
    protected TestTable createAggregationTestTable(String name, List<String> rows)
    {
        // TODO: Migrate to BaseJdbcConnectorTest
        throw new SkipException("tested in TestStarburstRemoteWithHiveConnectorTest");
    }

    @Override
    protected TestTable createTableWithDoubleAndRealColumns(String name, List<String> rows)
    {
        // TODO: Migrate to BaseJdbcConnectorTest
        throw new SkipException("tested in TestStarburstRemoteWithHiveConnectorTest");
    }

    @Override
    protected Session joinPushdownEnabled(Session session)
    {
        return Session.builder(super.joinPushdownEnabled(session))
                // strategy is AUTOMATIC by default and would not work for certain test cases (even if statistics are collected)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return remote::execute;
    }
}
