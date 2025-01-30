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

import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Disabled;
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
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestSnowflakeConnectorTest
        extends BaseJdbcConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return SnowflakeQueryRunner.builder()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return TestingSnowflakeServer::execute;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY,
                 SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
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
                "tpch.test_unsupported_col",
                "(one bigint, two geography, three varchar(10))");
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
        if (typeName.equals("timestamp(6)")) {
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
        // Override this test because the type of columns "orderkey", "custkey" and "shippriority" should be decimal rather than integer for snowflake case
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "decimal(19,0)", "", "")
                .row("custkey", "decimal(19,0)", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "decimal(10,0)", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();
    }

    @Test
    @Override
    public void testShowColumns()
    {
        assertThat(query("SHOW COLUMNS FROM orders")).result().matches(getDescribeOrdersResult());
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
        // Override this test because the type of columns "orderkey", "custkey" and "shippriority" should be decimal rather than integer for snowflake case
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE snowflake.tpch.orders (\n" +
                        "   orderkey decimal(19, 0),\n" +
                        "   custkey decimal(19, 0),\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority decimal(10, 0),\n" +
                        "   comment varchar(79)\n" +
                        ")");
    }

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("SQL compilation error: Non-nullable column .* cannot be added to non-empty table .* unless it has a non-null default value.");
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

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return "NULL result in a non-nullable column";
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
    public void testCreateTableWithLongColumnName()
    {
        String tableName = "test_long_column" + randomNameSuffix();
        String baseColumnName = "col";

        int maxLength = maxColumnNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validColumnName = baseColumnName + "z".repeat(maxLength - baseColumnName.length());
        assertUpdate("CREATE TABLE " + tableName + " (" + validColumnName + " bigint)");
        assertThat(columnExists(tableName, validColumnName)).isTrue();
        assertUpdate("DROP TABLE " + tableName);

        if (maxColumnNameLength().isEmpty()) {
            return;
        }
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(255);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("exceeds maximum length limit of 255 characters");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(255);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("exceeds maximum length limit of 255 characters");
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
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
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
    public void testInsertRowConcurrently()
    {
        abort("TODO: Connection is already closed");
    }

    @Test
    @Disabled
    @Override
    public void testAddColumnConcurrently()
    {
        // TODO: Enable this test after finding the failure cause
    }

    @Test
    @Override // Override because the failure message is different
    public void testNativeQueryCreateStatement()
    {
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'CREATE TABLE tpch.numbers(n INTEGER)'))"))
                .failure().hasMessageContaining("syntax error");
        assertThat(getQueryRunner().tableExists(getSession(), "numbers")).isFalse();
    }

    @Test
    @Override // Override because the failure message is different
    public void testNativeQueryInsertStatementTableExists()
    {
        try (TestTable testTable = simpleTable()) {
            assertThat(query(format("SELECT * FROM TABLE(system.query(query => 'INSERT INTO %s VALUES (3)'))", testTable.getName())))
                    .failure().hasMessageContaining("syntax error");
            assertQuery("SELECT * FROM " + testTable.getName(), "VALUES 1, 2");
        }
    }

    @Test
    @Override
    public void testCharTrailingSpace()
    {
        assertThatThrownBy(super::testCharTrailingSpace)
                .hasMessageContaining("For query")
                .hasMessageContaining("Actual rows")
                .hasMessageContaining("Expected rows");
    }

    @Test
    @Override // Override to specify the schema name in WHERE condition because listing tables in all schemas is too slow
    public void testInformationSchemaFiltering()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'tpch' AND table_name = 'orders' LIMIT 1",
                "SELECT 'orders' table_name");
        assertQuery(
                "SELECT table_name FROM information_schema.columns WHERE data_type = 'decimal(19,0)' AND table_schema = 'tpch' AND table_name = 'nation' and column_name = 'nationkey' LIMIT 1",
                "SELECT 'nation' table_name");
    }

    @Test
    @Disabled
    @Override
    public void testSelectInformationSchemaColumns()
    {
        // TODO https://github.com/trinodb/trino/issues/21157 Enable this test after fixing the timeout issue
    }

    @Test
    @Disabled
    @Override
    public void testBulkColumnListingOptions()
    {
        // TODO https://github.com/trinodb/trino/issues/21157 Enable this test after fixing the timeout issue
    }

    @Test
    @Override // Override because for approx_set(nationkey) a ProjectNode is present above the TableScanNode. It's used to project decimals to doubles.
    public void testAggregationWithUnsupportedResultType()
    {
        // TODO array_agg returns array, so it could be supported
        assertThat(query("SELECT array_agg(nationkey) FROM nation"))
                .skipResultsCorrectnessCheckForPushdown() // array_agg doesn't have a deterministic order of elements in result array
                .isNotFullyPushedDown(AggregationNode.class);
        // histogram returns map, which is not supported
        assertThat(query("SELECT histogram(regionkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
        // multimap_agg returns multimap, which is not supported
        assertThat(query("SELECT multimap_agg(regionkey, nationkey) FROM nation"))
                .skipResultsCorrectnessCheckForPushdown() // multimap_agg doesn't have a deterministic order of values for a key
                .isNotFullyPushedDown(AggregationNode.class);
        // approx_set returns HyperLogLog, which is not supported
        assertThat(query("SELECT approx_set(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class, ProjectNode.class);
    }

    @Override // Override because integers are represented as decimals in Snowflake Connector.
    protected String sumDistinctAggregationPushdownExpectedResult()
    {
        return "VALUES (BIGINT '4', DECIMAL '8')";
    }
}
