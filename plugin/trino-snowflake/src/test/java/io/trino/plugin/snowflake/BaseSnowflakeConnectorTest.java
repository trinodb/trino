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
package io.trino.plugin.snowflake;

import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.plugin.snowflake.TestingSnowflakeServer.TEST_SCHEMA;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public abstract class BaseSnowflakeConnectorTest
        extends BaseJdbcConnectorTest
{
    protected TestingSnowflakeServer server;

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_AGGREGATION_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN:
            case SUPPORTS_LIMIT_PUSHDOWN:
                return false;
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT:
            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT:
            case SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT:
            case SUPPORTS_SET_COLUMN_TYPE:
                return false;
            case SUPPORTS_DROP_FIELD:
            case SUPPORTS_ROW_TYPE:
            case SUPPORTS_ARRAY:
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                TEST_SCHEMA,
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
                TEST_SCHEMA,
                "(one bigint, two decimal(38,0), three varchar(10))");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        // TODO: Test fails with these types
        // Error: No result for query: SELECT row_id FROM test_data_mapping_smoke_real_3u8xo6hp59 WHERE rand() = 42 OR value = REAL '567.123'
        // In the testDataMappingSmokeTestDataProvider(), the type sampleValueLiteral of type real should be "DOUBLE" rather than "REAL".
        if (typeName.equals("real")) {
            return Optional.empty();
        }
        // Error: Failed to insert data: SQL compilation error: error line 1 at position 130
        if (typeName.equals("time")
                || typeName.equals("time(6)")
                || typeName.equals("timestamp(6)")) {
            return Optional.empty();
        }
        // Error: not equal
        if (typeName.equals("char(3)")) {
            return Optional.empty();
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        return nullToEmpty(exception.getMessage()).matches(".*(Incorrect column name).*");
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        // Override this test because the type of row "shippriority" should be bigint rather than integer for snowflake case
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "bigint", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();
    }

    @Test
    @Override
    public void testShowColumns()
    {
        assertThat(query("SHOW COLUMNS FROM orders")).matches(getDescribeOrdersResult());
    }

    @Test
    public void testViews()
    {
        String tableName = "test_view_" + randomNameSuffix();
        onRemoteDatabase().execute("CREATE OR REPLACE VIEW tpch." + tableName + " AS SELECT * FROM tpch.orders");
        assertQuery("SELECT orderkey FROM " + tableName, "SELECT orderkey FROM orders");
        onRemoteDatabase().execute("DROP VIEW IF EXISTS tpch." + tableName);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        // Override this test because the type of row "shippriority" should be bigint rather than integer for snowflake case
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE snowflake.tpch.orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority bigint,\n" +
                        "   comment varchar(79)\n" +
                        ")\n" +
                        "COMMENT ''");
    }

    @Test
    @Override
    public void testAddNotNullColumn()
    {
        assertThatThrownBy(super::testAddNotNullColumn)
                .isInstanceOf(AssertionError.class)
                .hasMessage("Unexpected failure when adding not null column");
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        assertThatThrownBy(super::testCharVarcharComparison)
                .hasMessageContaining("For query")
                .hasMessageContaining("Actual rows")
                .hasMessageContaining("Expected rows");
    }

    @Test
    @Override
    public void testCountDistinctWithStringTypes()
    {
        abort("TODO");
    }

    @Test
    @Override
    public void testInsertInPresenceOfNotSupportedColumn()
    {
        abort("TODO");
    }

    @Test
    @Override
    public void testAggregationPushdown()
    {
        abort("TODO");
    }

    @Test
    @Override
    public void testDistinctAggregationPushdown()
    {
        abort("TODO");
    }

    @Test
    @Override
    public void testNumericAggregationPushdown()
    {
        abort("TODO");
    }

    @Test
    @Override
    public void testLimitPushdown()
    {
        abort("TODO");
    }

    @Test
    @Override
    public void testInsertIntoNotNullColumn()
    {
        // TODO: java.lang.UnsupportedOperationException: This method should be overridden
        assertThatThrownBy(super::testInsertIntoNotNullColumn);
    }

    @Test
    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("TrinoException: " + MODIFYING_ROWS_MESSAGE);
    }

    @Test
    @Override
    public void testCreateTableAsSelect()
    {
        String tableName = "test_ctas" + randomNameSuffix();
        if (!hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA)) {
            assertQueryFails("CREATE TABLE IF NOT EXISTS " + tableName + " AS SELECT name, regionkey FROM nation", "This connector does not support creating tables with data");
            return;
        }
        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " AS SELECT name, regionkey FROM nation", "SELECT count(*) FROM nation");
        assertTableColumnNames(tableName, "name", "regionkey");

        assertEquals(getTableComment(getSession().getCatalog().orElseThrow(), getSession().getSchema().orElseThrow(), tableName), "");
        assertUpdate("DROP TABLE " + tableName);

        // Some connectors support CREATE TABLE AS but not the ordinary CREATE TABLE. Let's test CTAS IF NOT EXISTS with a table that is guaranteed to exist.
        assertUpdate("CREATE TABLE IF NOT EXISTS nation AS SELECT nationkey, regionkey FROM nation", 0);
        assertTableColumnNames("nation", "nationkey", "name", "regionkey", "comment");

        assertCreateTableAsSelect(
                "SELECT nationkey, name, regionkey FROM nation",
                "SELECT count(*) FROM nation");

        assertCreateTableAsSelect(
                "SELECT mktsegment, sum(acctbal) x FROM customer GROUP BY mktsegment",
                "SELECT count(DISTINCT mktsegment) FROM customer");

        assertCreateTableAsSelect(
                "SELECT count(*) x FROM nation JOIN region ON nation.regionkey = region.regionkey",
                "SELECT 1");

        assertCreateTableAsSelect(
                "SELECT nationkey FROM nation ORDER BY nationkey LIMIT 10",
                "SELECT 10");

        assertCreateTableAsSelect(
                "SELECT * FROM nation WITH DATA",
                "SELECT * FROM nation",
                "SELECT count(*) FROM nation");

        assertCreateTableAsSelect(
                "SELECT * FROM nation WITH NO DATA",
                "SELECT * FROM nation LIMIT 0",
                "SELECT 0");

        // Tests for CREATE TABLE with UNION ALL: exercises PushTableWriteThroughUnion optimizer

        assertCreateTableAsSelect(
                "SELECT name, nationkey, regionkey FROM nation WHERE nationkey % 2 = 0 UNION ALL " +
                        "SELECT name, nationkey, regionkey FROM nation WHERE nationkey % 2 = 1",
                "SELECT name, nationkey, regionkey FROM nation",
                "SELECT count(*) FROM nation");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "true").build(),
                "SELECT CAST(nationkey AS BIGINT) nationkey, regionkey FROM nation UNION ALL " +
                        "SELECT 1234567890, 123",
                "SELECT nationkey, regionkey FROM nation UNION ALL " +
                        "SELECT 1234567890, 123",
                "SELECT count(*) + 1 FROM nation");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "false").build(),
                "SELECT CAST(nationkey AS BIGINT) nationkey, regionkey FROM nation UNION ALL " +
                        "SELECT 1234567890, 123",
                "SELECT nationkey, regionkey FROM nation UNION ALL " +
                        "SELECT 1234567890, 123",
                "SELECT count(*) + 1 FROM nation");

        // TODO: BigQuery throws table not found at BigQueryClient.insert if we reuse the same table name
        tableName = "test_ctas" + randomNameSuffix();
        assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE " + tableName + " AS SELECT name FROM nation");
        assertQuery("SELECT * from " + tableName, "SELECT name FROM nation");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testCreateTable()
    {
        String tableName = "test_create_" + randomNameSuffix();
        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            assertQueryFails("CREATE TABLE " + tableName + " (a bigint, b double, c varchar(50))", "This connector does not support creating tables");
            return;
        }

        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()) // prime the cache, if any
                .doesNotContain(tableName);
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double, c varchar(50))");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet())
                .contains(tableName);
        assertTableColumnNames(tableName, "a", "b", "c");
        assertEquals(getTableComment(getSession().getCatalog().orElseThrow(), getSession().getSchema().orElseThrow(), tableName), "");

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet())
                .doesNotContain(tableName);

        assertQueryFails("CREATE TABLE " + tableName + " (a bad_type)", ".* Unknown type 'bad_type' for column 'a'");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        // TODO (https://github.com/trinodb/trino/issues/5901) revert to longer name when Oracle version is updated
        tableName = "test_cr_not_exists_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b varchar(50), c double)");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "a", "b", "c");

        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " (d bigint, e varchar(50))");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "a", "b", "c");

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        // Test CREATE TABLE LIKE
        tableName = "test_create_orig_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double, c varchar(50))");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "a", "b", "c");

        String tableNameLike = "test_create_like_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableNameLike + " (LIKE " + tableName + ", d bigint, e varchar(50))");
        assertTrue(getQueryRunner().tableExists(getSession(), tableNameLike));
        assertTableColumnNames(tableNameLike, "a", "b", "c", "d", "e");

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        assertUpdate("DROP TABLE " + tableNameLike);
        assertFalse(getQueryRunner().tableExists(getSession(), tableNameLike));
    }

    @Test
    @Override
    public void testNativeQueryCreateStatement()
    {
        abort("TODO");
    }

    @Test
    @Override
    public void testNativeQueryInsertStatementTableExists()
    {
        abort("TODO");
    }

    @Test
    @Override
    public void testNativeQuerySelectUnsupportedType()
    {
        abort("TODO");
    }

    @Test
    @Override
    public void testCreateTableWithLongColumnName()
    {
        String tableName = "test_long_column" + randomNameSuffix();
        String baseColumnName = "col";

        int maxLength = maxColumnNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validColumnName = baseColumnName + "z".repeat(maxLength - baseColumnName.length());
        assertUpdate("CREATE TABLE " + tableName + " (" + validColumnName + " bigint)");
        assertTrue(columnExists(tableName, validColumnName));
        assertUpdate("DROP TABLE " + tableName);

        if (maxColumnNameLength().isEmpty()) {
            return;
        }
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    @Override
    public void testCreateTableWithLongTableName()
    {
        // TODO: Find the maximum table name length in Snowflake and enable this test.
        abort("TODO");
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(251);
    }

    @Test
    @Override
    public void testAlterTableAddLongColumnName()
    {
        String tableName = "test_long_column" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);

        String baseColumnName = "col";
        int maxLength = maxColumnNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validTargetColumnName = baseColumnName + "z".repeat(maxLength - baseColumnName.length());
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN " + validTargetColumnName + " int");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertQuery("SELECT x FROM " + tableName, "VALUES 123");
        assertUpdate("DROP TABLE " + tableName);

        if (maxColumnNameLength().isEmpty()) {
            return;
        }

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);
        assertQuery("SELECT x FROM " + tableName, "VALUES 123");
    }

    @Test
    @Override
    public void testAlterTableRenameColumnToLongName()
    {
        String tableName = "test_long_column" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);

        String baseColumnName = "col";
        int maxLength = maxColumnNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validTargetColumnName = baseColumnName + "z".repeat(maxLength - baseColumnName.length());
        assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN x TO " + validTargetColumnName);
        assertQuery("SELECT " + validTargetColumnName + " FROM " + tableName, "VALUES 123");
        assertUpdate("DROP TABLE " + tableName);

        if (maxColumnNameLength().isEmpty()) {
            return;
        }

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);
        assertQuery("SELECT x FROM " + tableName, "VALUES 123");
    }

    @Test
    @Override
    public void testCreateSchemaWithLongName()
    {
        // TODO: Find the maximum table schema length in Snowflake and enable this test.
        abort("TODO");
    }

    @Test
    @Override
    public void testInsertArray()
    {
        // Snowflake does not support this feature.
        abort("Not supported");
    }

    @Test
    @Override
    public void testInsertRowConcurrently()
    {
        abort("TODO: Connection is already closed");
    }

    @Test
    @Override
    public void testNativeQueryColumnAlias()
    {
        abort("TODO: Table function system.query not registered");
    }

    @Test
    @Override
    public void testNativeQueryColumnAliasNotFound()
    {
        abort("TODO: Table function system.query not registered");
    }

    @Test
    @Override
    public void testNativeQueryIncorrectSyntax()
    {
        abort("TODO");
    }

    @Test
    @Override
    public void testNativeQueryInsertStatementTableDoesNotExist()
    {
        abort("TODO");
    }

    @Test
    @Override
    public void testNativeQueryParameters()
    {
        abort("TODO");
    }

    @Test
    @Override
    public void testNativeQuerySelectFromNation()
    {
        abort("TODO");
    }

    @Test
    @Override
    public void testNativeQuerySelectFromTestTable()
    {
        abort("TODO");
    }

    @Test
    @Override
    public void testNativeQuerySimple()
    {
        abort("TODO");
    }

    @Test
    @Override
    public void testRenameSchemaToLongName()
    {
        // TODO: Find the maximum table schema length in Snowflake and enable this test.
        abort("TODO");
    }

    @Test
    @Override
    public void testRenameTableToLongTableName()
    {
        // TODO: Find the maximum table length in Snowflake and enable this test.
        abort("TODO");
    }

    @Test
    @Override
    public void testCharTrailingSpace()
    {
        assertThatThrownBy(super::testCharVarcharComparison)
                .hasMessageContaining("For query")
                .hasMessageContaining("Actual rows")
                .hasMessageContaining("Expected rows");
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        assertThat(query("DESCRIBE orders")).matches(getDescribeOrdersResult());
    }
}
