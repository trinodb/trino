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
package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.clickhouse.ClickHouseQueryRunner.createClickHouseQueryRunner;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.ENGINE_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.ORDER_BY_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.PARTITION_BY_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.SAMPLE_BY_PROPERTY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestClickHouseConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingClickHouseServer clickhouseServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.clickhouseServer = closeAfterClass(new TestingClickHouseServer());
        return createClickHouseQueryRunner(
                clickhouseServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("clickhouse.map-string-as-varchar", "true")
                        .buildOrThrow(),
                REQUIRED_TPCH_TABLES);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY:
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
                return false;

            case SUPPORTS_ARRAY:
            case SUPPORTS_NEGATIVE_DATE:
                return false;

            case SUPPORTS_DELETE:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    @Test(dataProvider = "testColumnNameDataProvider")
    public void testColumnName(String columnName)
    {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testRenameColumn()
    {
        // ClickHouse need resets all data in a column for specified column which to be renamed
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDropColumn()
    {
        String tableName = "test_drop_column_" + randomTableSuffix();

        // only MergeTree engine table can drop column
        assertUpdate("CREATE TABLE " + tableName + "(x int NOT NULL, y int, a int) WITH (engine = 'MergeTree', order_by = ARRAY['x'])");
        assertUpdate("INSERT INTO " + tableName + "(x,y,a) SELECT 123, 456, 111", 1);

        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS y");
        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
        assertQueryFails("SELECT y FROM " + tableName, ".* Column 'y' cannot be resolved");

        assertUpdate("DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN notExistColumn");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        // the columns are referenced by order_by/order_by property can not be dropped
        assertUpdate("CREATE TABLE " + tableName + "(x int NOT NULL, y int, a int NOT NULL) WITH " +
                "(engine = 'MergeTree', order_by = ARRAY['x'], partition_by = ARRAY['a'])");
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN x", "Code: 47,.* Missing columns: 'x' while processing query: 'x', required columns: 'x' 'x' .*\\n.*");
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN a", "Code: 47,.* Missing columns: 'a' while processing query: 'a', required columns: 'a' 'a' .*\\n.*");
    }

    @Override
    public void testAddColumn()
    {
        String tableName = "test_add_column_" + randomTableSuffix();
        // Only MergeTree engine table can add column
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['id'])");
        assertUpdate("INSERT INTO " + tableName + " (id, x) VALUES(1, 'first')", 1);

        assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN X bigint", ".* Column 'X' already exists");
        assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN q bad_type", ".* Unknown type 'bad_type' for column 'q'");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN a varchar");
        assertUpdate("INSERT INTO " + tableName + " SELECT 2, 'second', 'xxx'", 1);
        assertQuery(
                "SELECT x, a FROM " + tableName,
                "VALUES ('first', NULL), ('second', 'xxx')");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b double");
        assertUpdate("INSERT INTO " + tableName + " SELECT 3, 'third', 'yyy', 33.3E0", 1);
        assertQuery(
                "SELECT x, a, b FROM " + tableName,
                "VALUES ('first', NULL, NULL), ('second', 'xxx', NULL), ('third', 'yyy', 33.3)");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS c varchar");
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS c varchar");
        assertUpdate("INSERT INTO " + tableName + " SELECT 4, 'fourth', 'zzz', 55.3E0, 'newColumn'", 1);
        assertQuery(
                "SELECT x, a, b, c FROM " + tableName,
                "VALUES ('first', NULL, NULL, NULL), ('second', 'xxx', NULL, NULL), ('third', 'yyy', 33.3, NULL), ('fourth', 'zzz', 55.3, 'newColumn')");
        assertUpdate("DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " ADD COLUMN x bigint");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " ADD COLUMN IF NOT EXISTS x bigint");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE clickhouse.tpch.orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar,\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar,\n" +
                        "   clerk varchar,\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar\n" +
                        ")");
    }

    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                clickhouseServer::execute,
                "tpch.tbl",
                "(col_required Int64," +
                        "col_nullable Nullable(Int64)," +
                        "col_default Nullable(Int64) DEFAULT 43," +
                        "col_nonnull_default Int64 DEFAULT 42," +
                        "col_required2 Int64) ENGINE=Log");
    }

    @Override
    public void testCharVarcharComparison()
    {
        assertThatThrownBy(super::testCharVarcharComparison)
                .hasMessageContaining("For query: ")
                .hasMessageContaining("Actual rows")
                .hasMessageContaining("Expected rows");

        // TODO run the test with clickhouse.map-string-as-varchar
        throw new SkipException("");
    }

    @Test
    public void testDifferentEngine()
    {
        String tableName = "test_add_column_" + randomTableSuffix();
        // MergeTree
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['id'])");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'mergetree', order_by = ARRAY['id'])");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);
        // MergeTree without order by
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree')", "The property of order_by is required for table engine MergeTree\\(\\)");

        // MergeTree with optional
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR, logdate DATE NOT NULL) WITH " +
                "(engine = 'MergeTree', order_by = ARRAY['id'], partition_by = ARRAY['toYYYYMM(logdate)'])");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);

        //Log families
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'log')");
        assertUpdate("DROP TABLE " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'tinylog')");
        assertUpdate("DROP TABLE " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'stripelog')");
        assertUpdate("DROP TABLE " + tableName);

        //NOT support engine
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'bad_engine')",
                "Unable to set catalog 'clickhouse' table property 'engine' to.*");
    }

    /**
     * test clickhouse table properties
     * <p>
     * Because the current connector does not support the `show create table` statement to display all the table properties,
     * so we cannot use this statement to test whether the properties of the created table meet our expectations,
     * and we will modify the test case after the `show create table` is full supported
     */
    @Test
    public void testTableProperty()
    {
        String tableName = "test_add_column_" + randomTableSuffix();
        // no table property, it should create a table with default Log engine table
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR)");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);

        // one required property
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'Log')");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'StripeLog')");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'TinyLog')");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);

        // Log engine DOES NOT any property
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'Log', order_by=ARRAY['id'])", ".* doesn't support PARTITION_BY, PRIMARY_KEY, ORDER_BY or SAMPLE_BY clauses.*\\n.*");
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'Log', partition_by=ARRAY['id'])", ".* doesn't support PARTITION_BY, PRIMARY_KEY, ORDER_BY or SAMPLE_BY clauses.*\\n.*");
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'Log', sample_by='id')", ".* doesn't support PARTITION_BY, PRIMARY_KEY, ORDER_BY or SAMPLE_BY clauses.*\\n.*");

        // optional properties
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['id'])");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);

        // the column refers by order by must be not null
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['id', 'x'])", ".* Sorting key cannot contain nullable columns.*\\n.*");

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['id'], primary_key = ARRAY['id'])");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR NOT NULL, y VARCHAR NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id', 'x', 'y'], primary_key = ARRAY['id', 'x'])");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR NOT NULL, y VARCHAR NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id', 'x'], primary_key = ARRAY['id','x'], sample_by = 'x' )");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);

        // Primary key must be a prefix of the sorting key,
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR NOT NULL, y VARCHAR NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id'], sample_by = ARRAY['x', 'y'])",
                "Invalid value for catalog 'clickhouse' table property 'sample_by': .*");

        // wrong property type
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL) WITH (engine = 'MergeTree', order_by = 'id')",
                "Invalid value for catalog 'clickhouse' table property 'order_by': .*");
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id'], primary_key = 'id')",
                "Invalid value for catalog 'clickhouse' table property 'primary_key': .*");
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id'], primary_key = ARRAY['id'], partition_by = 'id')",
                "Invalid value for catalog 'clickhouse' table property 'partition_by': .*");
    }

    @Test
    public void testSetTableProperties()
            throws Exception
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_alter_table_properties",
                "(p1 int NOT NULL, p2 int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['p1', 'p2'], primary_key = ARRAY['p1', 'p2'])")) {
            assertThat(getTableProperties("tpch", table.getName()))
                    .containsExactlyEntriesOf(ImmutableMap.of(
                            "engine", "MergeTree",
                            "order_by", "p1, p2",
                            "partition_by", "",
                            "primary_key", "p1, p2",
                            "sample_by", ""));

            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES sample_by = 'p2'");
            assertThat(getTableProperties("tpch", table.getName()))
                    .containsExactlyEntriesOf(ImmutableMap.of(
                            "engine", "MergeTree",
                            "order_by", "p1, p2",
                            "partition_by", "",
                            "primary_key", "p1, p2",
                            "sample_by", "p2"));
        }
    }

    @Test
    public void testAlterInvalidTableProperties()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_alter_table_properties",
                "(p1 int NOT NULL, p2 int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['p1', 'p2'], primary_key = ARRAY['p1', 'p2'])")) {
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " SET PROPERTIES invalid_property = 'p2'",
                    "Catalog 'clickhouse' table property 'invalid_property' does not exist");
        }
    }

    @Override
    protected TestTable createTableWithUnsupportedColumn()
    {
        return new TestTable(
                clickhouseServer::execute,
                "tpch.test_unsupported_column_present",
                "(one bigint, two Array(UInt8), three String) ENGINE=Log");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        switch (dataMappingTestSetup.getTrinoTypeName()) {
            case "boolean":
                // ClickHouse does not have built-in support for boolean type and we map boolean to tinyint.
                // Querying the column with a boolean predicate subsequently fails with "Cannot apply operator: tinyint = boolean"
                return Optional.empty();

            case "varbinary":
                // here in this test class we map ClickHouse String into varchar, so varbinary ends up a varchar
                return Optional.empty();

            case "date":
                // TODO (https://github.com/trinodb/trino/issues/7101) enable the test
                return Optional.empty();

            case "time":
            case "timestamp":
            case "timestamp(3) with time zone":
                return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return Optional.of(dataMappingTestSetup);
    }

    // TODO: Remove override once decimal predicate pushdown is implemented (https://github.com/trinodb/trino/issues/7100)
    @Override
    public void testNumericAggregationPushdown()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        try (TestTable testTable = createAggregationTestTable(schemaName + ".test_aggregation_pushdown",
                ImmutableList.of("100.000, 100000000.000000000, 100.000, 100000000", "123.321, 123456789.987654321, 123.321, 123456789"))) {
            assertThat(query("SELECT min(short_decimal), min(long_decimal), min(a_bigint), min(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal), max(a_bigint), max(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal), sum(a_bigint), sum(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT avg(a_bigint), avg(t_double) FROM " + testTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(long_decimal) FROM " + testTable.getName())).isNotFullyPushedDown(AggregationNode.class);
        }
    }

    @Override
    protected TestTable createAggregationTestTable(String name, List<String> rows)
    {
        return new TestTable(onRemoteDatabase(), name, "(short_decimal Nullable(Decimal(9, 3)), long_decimal Nullable(Decimal(30, 10)), t_double Nullable(Float64), a_bigint Nullable(Int64)) Engine=Log", rows);
    }

    @Override
    protected TestTable createTableWithDoubleAndRealColumns(String name, List<String> rows)
    {
        return new TestTable(onRemoteDatabase(), name, "(t_double Nullable(Float64), u_double Nullable(Float64), v_real Nullable(Float32), w_real Nullable(Float32)) Engine=Log", rows);
    }

    @Test
    @Override
    public void testInsertIntoNotNullColumn()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_not_null_", "(nullable_col INTEGER, not_null_col INTEGER NOT NULL)")) {
            assertUpdate(format("INSERT INTO %s (not_null_col) VALUES (2)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 2)");
            // ClickHouse inserts default values (e.g. 0 for integer column) even if we don't specify default clause in CREATE TABLE statement
            assertUpdate(format("INSERT INTO %s (nullable_col) VALUES (1)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 2), (1, 0)");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_commuted_not_null_table", "(nullable_col BIGINT, not_null_col BIGINT NOT NULL)")) {
            assertUpdate(format("INSERT INTO %s (not_null_col) VALUES (2)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 2)");
            assertQueryFails(format("INSERT INTO %s (not_null_col, nullable_col) VALUES (NULL, 3)", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
        }
    }

    @Test
    @Override
    public void testInsertNegativeDate()
    {
        // Override because the connector throws a different error message in the super class
        try (TestTable table = new TestTable(getQueryRunner()::execute, "insert_date", "(dt DATE)")) {
            assertQueryFails(format("INSERT INTO %s VALUES (DATE '-2016-12-07')", table.getName()), "Date must be between 1970-01-01 and 2106-02-07 in ClickHouse: -2016-12-07");
        }
    }

    @Test
    @Override
    public void testDateYearOfEraPredicate()
    {
        // Override because the connector throws an exception instead of an empty result when the value is out of supported range
        assertQuery("SELECT orderdate FROM orders WHERE orderdate = DATE '1997-09-14'", "VALUES DATE '1997-09-14'");
        assertQueryFails(
                "SELECT * FROM orders WHERE orderdate = DATE '-1996-09-14'",
                "Date must be between 1970-01-01 and 2106-02-07 in ClickHouse: -1996-09-14");
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return clickhouseServer::execute;
    }

    private Map<String, String> getTableProperties(String schemaName, String tableName)
            throws SQLException
    {
        String sql = "SELECT * FROM system.tables WHERE database = ? AND name = ?";
        try (Connection connection = DriverManager.getConnection(clickhouseServer.getJdbcUrl());
                PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, schemaName);
            preparedStatement.setString(2, tableName);

            ResultSet resultSet = preparedStatement.executeQuery();
            ImmutableMap.Builder<String, String> properties = new ImmutableMap.Builder<>();
            while (resultSet.next()) {
                properties.put(ENGINE_PROPERTY, resultSet.getString("engine"));
                properties.put(ORDER_BY_PROPERTY, resultSet.getString("sorting_key"));
                properties.put(PARTITION_BY_PROPERTY, resultSet.getString("partition_key"));
                properties.put(PRIMARY_KEY_PROPERTY, resultSet.getString("primary_key"));
                properties.put(SAMPLE_BY_PROPERTY, resultSet.getString("sampling_key"));
            }
            return properties.buildOrThrow();
        }
    }
}
