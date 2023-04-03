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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.plugin.snowflake.SnowflakeQueryRunner.createSnowflakeQueryRunner;
import static io.trino.plugin.snowflake.TestingSnowflakeServer.TEST_SCHEMA;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSnowflakeConnectorTest
        extends BaseJdbcConnectorTest
{
    protected TestingSnowflakeServer snowflakeServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        snowflakeServer = new TestingSnowflakeServer();
        return createSnowflakeQueryRunner(snowflakeServer, ImmutableMap.of(), ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return snowflakeServer::execute;
    }

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
                return false;
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
        // TODO: Test fails with type real
        // Error: No result for query: SELECT row_id FROM test_data_mapping_smoke_real_3u8xo6hp59 WHERE rand() = 42 OR value = REAL '567.123'
        // In the testDataMappingSmokeTestDataProvider(), the type sampleValueLiteral of type real should be "DOUBLE" rather than "REAL".
        if (typeName.equals("real")) {
            return Optional.empty();
        }
        if (typeName.equals("time")
                || typeName.equals("time(6)")
                || typeName.equals("timestamp")
                || typeName.equals("timestamp(6)")
                || typeName.equals("timestamp(3) with time zone")
                || typeName.equals("timestamp(6) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
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
                .row("orderkey", "decimal(38,0)", "", "")
                .row("custkey", "decimal(38,0)", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "decimal(38,0)", "", "")
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
                        "   orderkey decimal(38, 0),\n" +
                        "   custkey decimal(38, 0),\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority decimal(38, 0),\n" +
                        "   comment varchar(79)\n" +
                        ")\n" +
                        "COMMENT ''");
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        // Override and skip it because snowflake not support this feature
        assertThatThrownBy(super::testCharVarcharComparison).isInstanceOf(AssertionError.class);
    }

    @Test
    @Override
    public void testCountDistinctWithStringTypes()
    {
        // Override and skip it because snowflake not support this feature
        assertThatThrownBy(super::testCountDistinctWithStringTypes).isInstanceOf(AssertionError.class);
    }

    @Test
    @Override
    public void testDeleteWithVarcharEqualityPredicate()
    {
        // Override and skip it because snowflake not support this feature
        // Unsupported delete
        assertThatThrownBy(super::testDeleteWithVarcharEqualityPredicate);
    }

    @Test
    @Override
    public void testDeleteWithVarcharGreaterAndLowerPredicate()
    {
        // Override and skip it because snowflake not support this feature
        // Unsupported delete
        assertThatThrownBy(super::testDeleteWithVarcharGreaterAndLowerPredicate);
    }

    @Test
    @Override
    public void testDeleteWithVarcharInequalityPredicate()
    {
        // Override and skip it because snowflake not support this feature
        // Unsupported delete
        assertThatThrownBy(super::testDeleteWithVarcharInequalityPredicate);
    }

    @Test
    @Override
    public void testInsertInPresenceOfNotSupportedColumn()
    {
        // Override and skip it because snowflake not support this feature
        // Invalid number precision: 50. Must be between 0 and 38.
        assertThatThrownBy(super::testInsertInPresenceOfNotSupportedColumn);
    }

    @Test
    @Override
    public void testAggregationPushdown()
    {
        // Override and skip it because snowflake not support this feature
        assertThatThrownBy(super::testAggregationPushdown).isInstanceOf(AssertionError.class);
    }

    @Test
    @Override
    public void testDistinctAggregationPushdown()
    {
        // Override and skip it because snowflake not support this feature
        assertThatThrownBy(super::testDistinctAggregationPushdown).isInstanceOf(AssertionError.class);
    }

    @Test
    @Override
    public void testNumericAggregationPushdown()
    {
        // Override and skip it because snowflake not support this feature
        assertThatThrownBy(super::testNumericAggregationPushdown).isInstanceOf(AssertionError.class);
    }

    @Test
    @Override
    public void testLimitPushdown()
    {
        // Override and skip it because snowflake not support this feature
        assertThatThrownBy(super::testLimitPushdown).isInstanceOf(AssertionError.class);
    }

    @Test
    @Override
    public void testInsertIntoNotNullColumn()
    {
        // Override and skip it because snowflake not support this feature
        assertThatThrownBy(super::testInsertIntoNotNullColumn);
    }

    @Test
    @Override
    public void testPotentialDuplicateDereferencePushdown()
    {
        // Override and skip it because snowflake not support this feature
        assertThatThrownBy(super::testPotentialDuplicateDereferencePushdown);
    }

    @Test
    @Override
    public void testDeleteWithLike()
    {
        // Override and skip it because snowflake not support this feature
        assertThatThrownBy(super::testDeleteWithLike).isInstanceOf(AssertionError.class);
    }

    @Test
    @Override
    public void testCreateTableAsSelect()
    {
        // Override and skip it because snowflake not support this feature
        assertThatThrownBy(super::testCreateTableAsSelect).isInstanceOf(AssertionError.class);
    }

    @Test
    @Override
    public void testCreateTable()
    {
        // Override and skip it because snowflake not support this feature
        assertThatThrownBy(super::testCreateTable).isInstanceOf(AssertionError.class);
    }

    @Test
    @Override
    public void testInformationSchemaFiltering()
    {
        // Override and skip it because snowflake not support this feature
        assertThatThrownBy(super::testInformationSchemaFiltering).isInstanceOf(AssertionError.class);
    }

    @Test
    @Override
    public void testReadMetadataWithRelationsConcurrentModifications()
    {
        // Override and skip it because snowflake not support this feature
        assertThatThrownBy(super::testReadMetadataWithRelationsConcurrentModifications);
    }

    @Test
    @Override
    public void testAggregationWithUnsupportedResultType()
    {
        // Override and skip it because snowflake not support this feature
        assertThatThrownBy(super::testAggregationWithUnsupportedResultType).isInstanceOf(AssertionError.class);
    }

    @Test
    @Override
    public void testNativeQueryCreateStatement()
    {
        // Override and skip it because snowflake not support this feature
        assertThatThrownBy(super::testNativeQueryCreateStatement).isInstanceOf(AssertionError.class);
    }

    @Test
    @Override
    public void testNativeQueryInsertStatementTableExists()
    {
        // Override and skip it because snowflake not support this feature
        assertThatThrownBy(super::testNativeQueryInsertStatementTableExists).isInstanceOf(AssertionError.class);
    }

    @Test
    @Override
    public void testNativeQuerySelectUnsupportedType()
    {
        // Override and skip it because snowflake not support this feature
        assertThatThrownBy(super::testNativeQueryInsertStatementTableExists).isInstanceOf(AssertionError.class);
    }

    @Override
    public void testCreateTableWithLongColumnName()
    {
        // TODO: Find the maximum column name length in Snowflake and enable this test.
        throw new SkipException("TODO");
    }

    @Override
    public void testCreateTableWithLongTableName()
    {
        // TODO: Find the maximum table name length in Snowflake and enable this test.
        throw new SkipException("TODO");
    }

    @Override
    public void testAlterTableAddLongColumnName()
    {
        // TODO: Find the maximum column name length in Snowflake and enable this test.
        throw new SkipException("TODO");
    }

    @Override
    public void testAlterTableRenameColumnToLongName()
    {
        // TODO: Find the maximum column name length in Snowflake and enable this test.
        throw new SkipException("TODO");
    }

    @Override
    public void testCreateSchemaWithLongName()
    {
        // TODO: Find the maximum table schema length in Snowflake and enable this test.
        throw new SkipException("TODO");
    }

    @Test
    @Override
    public void testDropAmbiguousRowFieldCaseSensitivity()
    {
        // Override and skip it because snowflake not support this feature
        assertThatThrownBy(super::testDropAmbiguousRowFieldCaseSensitivity);
    }
}
