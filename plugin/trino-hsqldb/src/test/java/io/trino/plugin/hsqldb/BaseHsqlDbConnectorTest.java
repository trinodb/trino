/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hsqldb;

import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.OptionalInt;

import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_VIEW_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_INSERT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_NEGATIVE_DATE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

public abstract class BaseHsqlDbConnectorTest
        extends BaseJdbcConnectorTest
{
    protected TestingHsqlDbServer server;

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_TABLE_WITH_DATA,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_COMMENT_ON_VIEW,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_VIEW_COLUMN,
                 SUPPORTS_DROP_SCHEMA_CASCADE -> true;
            case SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_PREDICATE_ARITHMETIC_EXPRESSION_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                 SUPPORTS_ARRAY,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_DROP_NOT_NULL_CONSTRAINT,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM,
                 SUPPORTS_NEGATIVE_DATE,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_NATIVE_QUERY-> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                "table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected TestTable createTableWithUnsupportedColumn()
    {
        return new TestTable(
                onRemoteDatabase(),
                "test_unsupported_column_present",
                "(one bigint, two sql_variant, three varchar(10))");
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        return nullToEmpty(exception.getMessage()).contains("type not found or user lacks privilege:");
    }

    @Test
    public void testReadFromView()
    {
        try (TestView view = new TestView(onRemoteDatabase(), "test_view", "SELECT * FROM orders")) {
            assertThat(getQueryRunner().tableExists(getSession(), view.getName())).isTrue();
            assertQuery("SELECT orderkey FROM " + view.getName(), "SELECT orderkey FROM orders");
        }
    }

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageStartingWith("default expression needed in statement ");
    }

    @Test
    @Override
    public void testCommentTable()
    {
        if (!hasBehavior(SUPPORTS_COMMENT_ON_TABLE)) {
            assertQueryFails("COMMENT ON TABLE nation IS 'new comment'", "This connector does not support setting table comments");
            return;
        }

        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_comment_", "(a integer)")) {
            // comment initially not set
            assertThat(getTableComment(catalogName, schemaName, table.getName())).isEqualTo(null);

            // comment set
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS 'new comment'");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName())).contains("COMMENT 'new comment'");
            assertThat(getTableComment(catalogName, schemaName, table.getName())).isEqualTo("new comment");
            assertThat(query(
                    "SELECT table_name, comment FROM system.metadata.table_comments " +
                            "WHERE catalog_name = '" + catalogName + "' AND schema_name = '" + schemaName + "'")) // without table_name filter
                    .skippingTypesCheck()
                    .containsAll("VALUES ('" + table.getName() + "', 'new comment')");

            // comment deleted
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS ''");
            assertThat(getTableComment(catalogName, schemaName, table.getName())).isEqualTo("");

            // comment set to non-empty value before verifying setting empty comment
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS 'updated comment'");
            assertThat(getTableComment(catalogName, schemaName, table.getName())).isEqualTo("updated comment");
        }

        String tableName = "test_comment_" + randomNameSuffix();
        try {
            // comment set when creating a table
            assertUpdate("CREATE TABLE " + tableName + "(key integer) COMMENT 'new table comment'");
            assertThat(getTableComment(catalogName, schemaName, tableName)).isEqualTo("new table comment");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Override
    public void testCommentColumn()
    {
        if (!hasBehavior(SUPPORTS_COMMENT_ON_COLUMN)) {
            assertQueryFails("COMMENT ON COLUMN nation.nationkey IS 'new comment'", "This connector does not support setting column comments");
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_comment_column_", "(a integer)")) {
            // comment set
            assertUpdate("COMMENT ON COLUMN public." + table.getName() + ".a IS 'new comment'");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName())).contains("COMMENT 'new comment'");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo("new comment");

            // comment deleted
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS ''");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo("");

            // comment set to non-empty value before verifying setting empty comment
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS 'updated comment'");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo("updated comment");
        }
    }

    @Test
    @Override
    public void testCommentViewColumn()
    {
        if (!hasBehavior(SUPPORTS_COMMENT_ON_VIEW_COLUMN)) {
            if (hasBehavior(SUPPORTS_CREATE_VIEW)) {
                try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view_column", "SELECT * FROM region")) {
                    assertQueryFails("COMMENT ON COLUMN " + view.getName() + ".regionkey IS 'new region key comment'", "This connector does not support setting view column comments");
                }
                return;
            }
            abort("Skipping as connector does not support CREATE VIEW");
        }

        String viewColumnName = "regionkey";
        try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view_column", "SELECT * FROM region")) {
            // comment set
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS 'new region key comment'");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("new region key comment");

            // comment deleted
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS ''");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("");

            // comment set to non-empty value before verifying setting empty comment
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS 'updated region key comment'");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("updated region key comment");
        }
    }

    @Test
    public void testInsertNegativeDate()
    {
        if (!hasBehavior(SUPPORTS_INSERT)) {
            assertQueryFails("INSERT INTO orders (orderdate) VALUES (DATE '-0001-01-01')", "This connector does not support inserts");
            return;
        }
        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            throw new AssertionError("Cannot test INSERT negative dates without CREATE TABLE, the test needs to be implemented in a connector-specific way");
        }
        if (!hasBehavior(SUPPORTS_NEGATIVE_DATE)) {
            try (TestTable table = new TestTable(getQueryRunner()::execute, "insert_date", "(dt DATE)")) {
                assertQueryFails(format("INSERT INTO %s VALUES (CAST('-0001-01-01' AS DATE))", table.getName()), errorMessageForInsertNegativeDate("-0001-01-01"));
            }
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "insert_date", "(dt DATE)")) {
            assertUpdate(format("INSERT INTO %s VALUES (DATE '-0001-01-01')", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES DATE '-0001-01-01'");
            assertQuery(format("SELECT * FROM %s WHERE dt = DATE '-0001-01-01'", table.getName()), "VALUES DATE '-0001-01-01'");
        }
    }

    @Override
    protected String errorMessageForInsertNegativeDate(String date)
    {
        System.out.println("*********************************************************************************************");
        return ".*";
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        System.out.println("****************************************** " + e.getMessage());
        assertThat(e).hasMessageContaining("was deadlocked on lock resources");
    }

    @Override
    @Language("RegExp")
    protected String errorMessageForCreateTableAsSelectNegativeDate(String date)
    {
        return ".*";
    }

    @Override
    @Language("RegExp")
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return ".*";
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("user lacks privilege or object not found:");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("user lacks privilege or object not found:");
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("user lacks privilege or object not found:");
    }

    @Test
    @Override
    public void testCreateViewSchemaNotFound()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_VIEW));

        String schemaName = "test_schema_" + randomNameSuffix();
        String viewName = "test_view_create_no_schema_" + randomNameSuffix();
        try {
            assertQueryFails(
                    format("CREATE VIEW %s.%s AS SELECT 1 AS c1", schemaName, viewName),
                    format("invalid schema name: %s", schemaName));
            assertQueryFails(
                    format("CREATE OR REPLACE VIEW %s.%s AS SELECT 1 AS c1", schemaName, viewName),
                    format("invalid schema name: %s", schemaName));
        }
        finally {
            assertUpdate(format("DROP VIEW IF EXISTS %s.%s", schemaName, viewName));
        }
    }


}
