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
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.createRemoteStarburstQueryRunnerWithMemory;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;

public class TestStargateWithMemoryWritesEnabledConnectorTest
        extends BaseStargateConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        remoteStarburst = closeAfterClass(
                createRemoteStarburstQueryRunnerWithMemory(REQUIRED_TPCH_TABLES, Optional.empty()));
        return StargateQueryRunner.builder(remoteStarburst, "memory")
                .enableWrites()
                .build();
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
            case SUPPORTS_ADD_COLUMN:
            case SUPPORTS_RENAME_COLUMN:
            case SUPPORTS_SET_COLUMN_TYPE:
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
            case SUPPORTS_COMMENT_ON_COLUMN:
                return true;

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
        // Required because Stargate connector adds additional `Query failed (...):` prefix to the error message
        assertThatThrownBy(super::testAddColumn)
                .hasMessageContaining("This connector does not support adding columns");
        throw new SkipException("not supported");
    }

    @Test
    @Override
    public void testDropColumn()
    {
        // Required because Stargate connector adds additional `Query failed (...):` prefix to the error message
        assertThatThrownBy(super::testDropColumn)
                .hasMessageContaining("This connector does not support dropping columns");
        throw new SkipException("not supported");
    }

    @Test
    @Override
    public void testRenameColumn()
    {
        // Required because Stargate connector adds additional `Query failed (...):` prefix to the error message
        assertThatThrownBy(super::testRenameColumn)
                .hasMessageContaining("This connector does not support renaming columns");
        throw new SkipException("not supported");
    }

    @Test
    @Override
    public void testSetColumnType()
    {
        // Required because Stargate connector adds additional `Query failed (...):` prefix to the error message
        assertThatThrownBy(super::testSetColumnType)
                .hasMessageContaining("This connector does not support setting column types");
        throw new SkipException("not supported");
    }

    @Test
    @Override // override with version from smoke tests to go faster
    public void testInsert()
    {
        String tableName = "test_create_" + randomNameSuffix();
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
        String tableName = "test_create_" + randomNameSuffix();
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
    public void verifySupportsDeleteDeclaration()
    {
        // Overridden because we get an error message with "Query failed (<query_id>):" prefixed instead of one expected by superclass
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete", "AS SELECT * FROM region")) {
            assertQueryFails("DELETE FROM " + table.getName(), ".*This connector does not support modifying table rows");
        }
    }

    @Override
    public void verifySupportsRowLevelDeleteDeclaration()
    {
        // Overridden because we get an error message with "Query failed (<query_id>):" prefixed instead of one expected by superclass
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete", "AS SELECT * FROM region")) {
            assertQueryFails("DELETE FROM " + table.getName() + " WHERE regionkey = 2", ".*This connector does not support modifying table rows");
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
                format("ALTER SCHEMA %s RENAME TO %s", schemaName, schemaName + randomNameSuffix()),
                ".*This connector does not support renaming schemas");
    }

    @Override
    public void testNativeQuerySimple()
    {
        assertQuery("SELECT * FROM TABLE(system.query(query => 'SELECT 1 a'))", "VALUES 1");
    }

    @Override
    public void testNativeQueryCreateStatement()
    {
        // SingleStore returns a ResultSet metadata with no columns for CREATE TABLE statement.
        // This is unusual, because other connectors don't produce a ResultSet metadata for CREATE TABLE at all.
        // The query fails because there are no columns, but even if columns were not required, the query would fail
        // to execute in this connector because the connector wraps it in additional syntax, which causes syntax error.
        assertFalse(getQueryRunner().tableExists(getSession(), "numbers"));
        assertThatThrownBy(() -> query("SELECT * FROM TABLE(system.query(query => 'CREATE TABLE numbers(n INTEGER)'))"))
                .hasMessageContaining("descriptor has no fields");
        assertFalse(getQueryRunner().tableExists(getSession(), "numbers"));
    }

    @Override
    public void testNativeQueryInsertStatementTableExists()
    {
        try (TestTable testTable = simpleTable()) {
            assertThatThrownBy(() -> query(format("SELECT * FROM TABLE(system.query(query => 'INSERT INTO %s VALUES (3)'))", testTable.getName())))
                    .hasMessageContaining("mismatched input 'INSERT'");
            assertQuery("SELECT * FROM " + testTable.getName(), "VALUES 1, 2");
        }
    }
}
