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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.createRemoteStarburstQueryRunnerWithMemory;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

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
            case SUPPORTS_UPDATE:
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

    @Test
    @Override
    public void testSetColumnTypeWithDefaultColumn()
    {
        abort("not supported");
    }

    @Test
    @Override
    public void testInsertForDefaultColumn()
    {
        abort("not supported");
    }

    @Test
    @Override
    public void testTruncateTable()
    {
        abort("Memory connector does not support truncate");
    }

    @Test
    @Override
    public void testAddColumn()
    {
        // Required because Stargate connector adds additional `Query failed (...):` prefix to the error message
        assertThatThrownBy(super::testAddColumn)
                .hasMessageContaining("This connector does not support adding columns");
        abort("not supported");
    }

    @Test
    @Override
    public void testDropColumn()
    {
        // Required because Stargate connector adds additional `Query failed (...):` prefix to the error message
        assertThatThrownBy(super::testDropColumn)
                .hasMessageContaining("This connector does not support dropping columns");
        abort("not supported");
    }

    @Test
    @Override
    public void testRenameColumn()
    {
        // Required because Stargate connector adds additional `Query failed (...):` prefix to the error message
        assertThatThrownBy(super::testRenameColumn)
                .hasMessageContaining("This connector does not support renaming columns");
        abort("not supported");
    }

    @Test
    @Override
    public void testSetColumnType()
    {
        // Required because Stargate connector adds additional `Query failed (...):` prefix to the error message
        assertThatThrownBy(super::testSetColumnType)
                .hasMessageContaining("This connector does not support setting column types");
        abort("not supported");
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

    @Override
    // skip larger inputs to go faster
    protected List<Integer> largeInValuesCountData()
    {
        return ImmutableList.of(200);
    }

    @Test
    @Override
    public void verifySupportsDeleteDeclaration()
    {
        // Overridden because we get an error message with "Query failed (<query_id>):" prefixed instead of one expected by superclass
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete", "AS SELECT * FROM region")) {
            assertQueryFails("DELETE FROM " + table.getName(), ".*This connector does not support modifying table rows");
        }
    }

    @Test
    @Override
    public void verifySupportsRowLevelDeleteDeclaration()
    {
        // Overridden because we get an error message with "Query failed (<query_id>):" prefixed instead of one expected by superclass
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete", "AS SELECT * FROM region")) {
            assertQueryFails("DELETE FROM " + table.getName() + " WHERE regionkey = 2", ".*This connector does not support modifying table rows");
        }
    }

    @Test
    @Override
    public void testInsertIntoNotNullColumn()
    {
        // Overridden because we get an error message with "Query failed (<query_id>):" prefixed instead of one expected by superclass
        assertQueryFails(
                "CREATE TABLE not_null_constraint (not_null_col INTEGER NOT NULL)",
                ".* line 1:53: Catalog 'memory' does not support non-null column for column name '\"not_null_col\"'");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        // Overridden because we get an error message with "Query failed (<query_id>):" prefixed instead of one expected by superclass
        String schemaName = getSession().getSchema().orElseThrow();
        assertQueryFails(
                format("ALTER SCHEMA %s RENAME TO %s", schemaName, schemaName + randomNameSuffix()),
                ".*This connector does not support renaming schemas");
    }

    @Test
    @Override
    public void testNativeQuerySimple()
    {
        assertQuery("SELECT * FROM TABLE(system.query(query => 'SELECT 1 a'))", "VALUES 1");
    }

    @Test
    @Override
    public void testNativeQueryCreateStatement()
    {
        // SingleStore returns a ResultSet metadata with no columns for CREATE TABLE statement.
        // This is unusual, because other connectors don't produce a ResultSet metadata for CREATE TABLE at all.
        // The query fails because there are no columns, but even if columns were not required, the query would fail
        // to execute in this connector because the connector wraps it in additional syntax, which causes syntax error.
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'CREATE TABLE numbers(n INTEGER)'))"))
                .nonTrinoExceptionFailure().hasMessageContaining("descriptor has no fields");
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
    }

    @Test
    @Override
    public void testUpdateNotNullColumn()
    {
        // Required because Stargate connector adds additional `Query failed (...):` prefix to the error message and uses remote catalog name
        assertThat(query("CREATE TABLE not_null_constraint (not_null_col INTEGER NOT NULL)"))
                .failure().hasMessageContaining(format("Catalog '%s' does not support non-null column for column name '\"not_null_col\"'", getRemoteCatalogName()));
    }

    @Test
    @Override
    public void testNativeQueryInsertStatementTableExists()
    {
        try (TestTable testTable = simpleTable()) {
            assertThat(query(format("SELECT * FROM TABLE(system.query(query => 'INSERT INTO %s VALUES (3)'))", testTable.getName())))
                    .failure().hasMessageContaining("mismatched input 'INSERT'");
            assertQuery("SELECT * FROM " + testTable.getName(), "VALUES 1, 2");
        }
    }

    @Test
    @Override
    public void verifySupportsUpdateDeclaration()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_supports_update", "AS SELECT * FROM nation")) {
            assertQueryFails("UPDATE " + table.getName() + " SET nationkey = 100 WHERE regionkey = 2", ".*This connector does not support modifying table rows");
        }
    }

    @Test
    @Disabled // Disable to avoid flaky test failures
    @Override
    public void testCreateOrReplaceTableConcurrently() {}

    @Test
    @Disabled // Disable to avoid flaky test failures
    @Override
    public void testAddColumnConcurrently() {}

    @Test
    @Disabled // Disable to avoid flaky test failures
    @Override
    public void testInsertRowConcurrently() {}

    @Test
    @Disabled // Disable to avoid flaky test failures
    @Override
    public void testUpdateRowConcurrently() {}
}
