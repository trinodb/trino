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

import com.google.common.collect.ImmutableList;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.createRemoteStarburstQueryRunnerWithMemory;
import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.createStargateQueryRunner;
import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.stargateConnectionUrl;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStargateWithMemoryWritesEnabledConnectorTest
        extends BaseStargateConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        remoteStarburst = closeAfterClass(
                createRemoteStarburstQueryRunnerWithMemory(REQUIRED_TPCH_TABLES, Optional.empty()));
        return createStargateQueryRunner(
                true,
                Map.of(
                        "connection-url", stargateConnectionUrl(remoteStarburst, "memory"),
                        "allow-drop-table", "true"));
    }

    @Override
    protected String getRemoteCatalogName()
    {
        return "memory";
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_COMMENT_ON_COLUMN:
                // not supported in memory connector
                return false;

            case SUPPORTS_DELETE:
                // memory connector does not support deletes
                return false;

            case SUPPORTS_NOT_NULL_CONSTRAINT:
                // memory connector does not support not-null in create-table
                return false;

            case SUPPORTS_RENAME_SCHEMA:
                // not supported in memory connector
                return false;

            case SUPPORTS_TRUNCATE:
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

    @Override
    public void testTruncateTable()
    {
        throw new SkipException("Memory connector does not support truncate");
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
    protected void skipTestUnlessSupportsDeletes()
    {
        // Overridden because we get an error message with "Query failed (<query_id>):" prefixed instead of one expected by superclass
        skipTestUnless(supportsCreateTable());
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete", "(col varchar(1))", ImmutableList.of("'a'", "'A'"))) {
            if (!supportsDelete()) {
                assertQueryFails("DELETE FROM " + table.getName(), ".*This connector does not support deletes");
                throw new SkipException("This connector does not support deletes");
            }
        }
    }

    @Override
    public void verifySupportsDeleteDeclaration()
    {
        // Overridden because we get an error message with "Query failed (<query_id>):" prefixed instead of one expected by superclass
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete", "AS SELECT * FROM region")) {
            assertQueryFails("DELETE FROM " + table.getName(), ".*This connector does not support deletes");
        }
    }

    @Override
    public void verifySupportsRowLevelDeleteDeclaration()
    {
        // Overridden because we get an error message with "Query failed (<query_id>):" prefixed instead of one expected by superclass
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete", "AS SELECT * FROM region")) {
            assertQueryFails("DELETE FROM " + table.getName() + " WHERE regionkey = 2", ".*This connector does not support deletes");
        }
    }

    @Override
    public void testInsertIntoNotNullColumn()
    {
        // Overridden because we get an error message with "Query failed (<query_id>):" prefixed instead of one expected by superclass
        assertQueryFails(
                "CREATE TABLE not_null_constraint (not_null_col INTEGER NOT NULL)",
                ".* line 1:53: Catalog 'memory' does not support non-null column for column name '\"not_null_col\"'");
    }

    @Override
    public void testRenameSchema()
    {
        // Overridden because we get an error message with "Query failed (<query_id>):" prefixed instead of one expected by superclass
        String schemaName = getSession().getSchema().orElseThrow();
        assertQueryFails(
                format("ALTER SCHEMA %s RENAME TO %s", schemaName, schemaName + randomTableSuffix()),
                ".*This connector does not support renaming schemas");
    }
}
