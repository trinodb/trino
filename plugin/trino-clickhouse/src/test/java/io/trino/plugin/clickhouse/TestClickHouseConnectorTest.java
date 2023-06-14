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
import java.util.OptionalInt;

import static io.trino.plugin.clickhouse.ClickHouseQueryRunner.createClickHouseQueryRunner;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.ENGINE_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.ORDER_BY_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.PARTITION_BY_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.SAMPLE_BY_PROPERTY;
import static io.trino.plugin.clickhouse.TestingClickHouseServer.CLICKHOUSE_LATEST_IMAGE;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestClickHouseConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingClickHouseServer clickhouseServer;

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_DELETE:
                return false;
            case SUPPORTS_TRUNCATE:
                return true;

            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY:
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV:
            case SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE:
            case SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE:
            case SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION:
            case SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION:
            case SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT:
                return false;

            case SUPPORTS_SET_COLUMN_TYPE:
                return false;

            case SUPPORTS_ARRAY:
            case SUPPORTS_ROW_TYPE:
            case SUPPORTS_NEGATIVE_DATE:
                return false;

            case SUPPORTS_NATIVE_QUERY:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.clickhouseServer = closeAfterClass(new TestingClickHouseServer(CLICKHOUSE_LATEST_IMAGE));
        return createClickHouseQueryRunner(
                clickhouseServer,
                ImmutableMap.of(),
                ImmutableMap.of("clickhouse.map-string-as-varchar", "true"),
                REQUIRED_TPCH_TABLES);
    }

    @Test
    public void testSampleBySqlInjection()
    {
        assertQueryFails("CREATE TABLE test (p1 int NOT NULL, p2 boolean NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['p1', 'p2'], primary_key = ARRAY['p1', 'p2'], sample_by = 'p2; drop table tpch.nation')", "(?s).*Missing columns: 'p2; drop table tpch.nation.*");
        assertUpdate("CREATE TABLE test (p1 int NOT NULL, p2 boolean NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['p1', 'p2'], primary_key = ARRAY['p1', 'p2'], sample_by = 'p2')");
        assertQueryFails("ALTER TABLE test SET PROPERTIES sample_by = 'p2; drop table tpch.nation'", "(?s).*Missing columns: 'p2; drop table tpch.nation.*");
        assertUpdate("ALTER TABLE test SET PROPERTIES sample_by = 'p2'");
    }

    @Override
    public void testRenameColumn()
    {
        // ClickHouse need resets all data in a column for specified column which to be renamed
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testAddColumnWithCommentSpecialCharacter(String comment)
    {
        // Override because default storage engine doesn't support renaming columns
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_column_", "(a_varchar varchar NOT NULL) WITH (engine = 'mergetree', order_by = ARRAY['a_varchar'])")) {
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN b_varchar varchar COMMENT " + varcharLiteral(comment));
            assertEquals(getColumnComment(table.getName(), "b_varchar"), comment);
        }
    }

    @Override
    public void testDropAndAddColumnWithSameName()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_add_column", "(x int NOT NULL, y int, z int) WITH (engine = 'MergeTree', order_by = ARRAY['x'])", ImmutableList.of("1,2,3"))) {
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN y");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 3)");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN y int");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 3, NULL)");
        }
    }

    @Override
    protected String createTableSqlForAddingAndDroppingColumn(String tableName, String columnNameInSql)
    {
        return format("CREATE TABLE %s(%s varchar(50), value varchar(50) NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['value'])", tableName, columnNameInSql);
    }

    @Override
    public void testRenameColumnName(String columnName)
    {
        // TODO: Enable this test
        if (columnName.equals("a.dot")) {
            assertThatThrownBy(() -> super.testRenameColumnName(columnName))
                    .hasMessageContaining("Cannot rename column from nested struct to normal column");
            throw new SkipException("TODO");
        }
        assertThatThrownBy(() -> super.testRenameColumnName(columnName))
                .hasMessageContaining("is not supported by storage Log");
        throw new SkipException("TODO");
    }

    @Override
    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        // TODO: Investigate why a\backslash allows creating a table, but it throws an exception when selecting
        if (columnName.equals("a\\backslash`")) {
            return Optional.empty();
        }
        return Optional.of(columnName);
    }

    @Override
    public void testDropColumn()
    {
        String tableName = "test_drop_column_" + randomNameSuffix();

        // only MergeTree engine table can drop column
        assertUpdate("CREATE TABLE " + tableName + "(x int NOT NULL, y int, a int NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['x'], partition_by = ARRAY['a'])");
        assertUpdate("INSERT INTO " + tableName + "(x,y,a) SELECT 123, 456, 111", 1);

        // the columns are referenced by order_by/partition_by property can not be dropped
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN x", "(?s).* Missing columns: 'x' while processing query: 'x', required columns: 'x' 'x'.*");
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN a", "(?s).* Missing columns: 'a' while processing query: 'a', required columns: 'a' 'a'.*");

        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS y");
        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
        assertQueryFails("SELECT y FROM " + tableName, ".* Column 'y' cannot be resolved");

        assertUpdate("DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN notExistColumn");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Override
    protected TestTable createTableWithOneIntegerColumn(String namePrefix)
    {
        return new TestTable(getQueryRunner()::execute, namePrefix, "(col integer NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['col'])");
    }

    @Override
    protected String tableDefinitionForAddColumn()
    {
        return "(x VARCHAR NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['x'])";
    }

    @Override // Overridden because the default storage type doesn't support adding columns
    public void testAddNotNullColumnToEmptyTable()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_notnull_col_to_empty", "(a_varchar varchar NOT NULL)  WITH (engine = 'MergeTree', order_by = ARRAY['a_varchar'])")) {
            String tableName = table.getName();

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar NOT NULL");
            assertFalse(columnIsNullable(tableName, "b_varchar"));
            assertUpdate("INSERT INTO " + tableName + " VALUES ('a', 'b')", 1);
            assertThat(query("TABLE " + tableName))
                    .skippingTypesCheck()
                    .matches("VALUES ('a', 'b')");
        }
    }

    @Override // Overridden because (a) the default storage type doesn't support adding columns and (b) ClickHouse has implicit default value for new NON NULL column
    public void testAddNotNullColumn()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_notnull_col", "(a_varchar varchar NOT NULL)  WITH (engine = 'MergeTree', order_by = ARRAY['a_varchar'])")) {
            String tableName = table.getName();

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar NOT NULL");
            assertFalse(columnIsNullable(tableName, "b_varchar"));

            assertUpdate("INSERT INTO " + tableName + " VALUES ('a', 'b')", 1);

            // ClickHouse set an empty character as the default value
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN c_varchar varchar NOT NULL");
            assertFalse(columnIsNullable(tableName, "c_varchar"));
            assertQuery("SELECT c_varchar FROM " + tableName, "VALUES ''");
        }
    }

    @Test
    @Override
    public void testAddColumnWithComment()
    {
        // Override because the default storage type doesn't support adding columns
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_col_desc_", "(a_varchar varchar NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['a_varchar'])")) {
            String tableName = table.getName();

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar COMMENT 'test new column comment'");
            assertThat(getColumnComment(tableName, "b_varchar")).isEqualTo("test new column comment");

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN empty_comment varchar COMMENT ''");
            assertNull(getColumnComment(tableName, "empty_comment"));
        }
    }

    @Override
    public void testAlterTableAddLongColumnName()
    {
        // TODO: Find the maximum column name length in ClickHouse and enable this test.
        throw new SkipException("TODO");
    }

    @Override
    public void testAlterTableRenameColumnToLongName()
    {
        // TODO: Find the maximum column name length in ClickHouse and enable this test.
        throw new SkipException("TODO");
    }

    @Test
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
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'LOG'\n" +
                        ")");
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
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
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
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
                .hasMessageContaining("For query")
                .hasMessageContaining("Actual rows")
                .hasMessageContaining("Expected rows");

        // TODO run the test with clickhouse.map-string-as-varchar
        throw new SkipException("");
    }

    @Test
    public void testDifferentEngine()
    {
        String tableName = "test_add_column_" + randomNameSuffix();
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
                "(engine = 'MergeTree', order_by = ARRAY['id'], partition_by = ARRAY['logdate'])");
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

    @Test
    public void testTableProperty()
    {
        String tableName = "test_add_column_" + randomNameSuffix();
        // no table property, it should create a table with default Log engine table
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR)");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);

        // one required property
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'Log')");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo(format("" +
                        "CREATE TABLE clickhouse.tpch.%s (\n" +
                        "   id integer NOT NULL,\n" +
                        "   x varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'LOG'\n" +
                        ")", tableName));
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'StripeLog')");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo(format("" +
                        "CREATE TABLE clickhouse.tpch.%s (\n" +
                        "   id integer NOT NULL,\n" +
                        "   x varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'STRIPELOG'\n" +
                        ")", tableName));
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'TinyLog')");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo(format("" +
                        "CREATE TABLE clickhouse.tpch.%s (\n" +
                        "   id integer NOT NULL,\n" +
                        "   x varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'TINYLOG'\n" +
                        ")", tableName));
        assertUpdate("DROP TABLE " + tableName);

        // Log engine DOES NOT any property
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'Log', order_by=ARRAY['id'])", ".* doesn't support PARTITION_BY, PRIMARY_KEY, ORDER_BY or SAMPLE_BY clauses.*\\n.*");
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'Log', partition_by=ARRAY['id'])", ".* doesn't support PARTITION_BY, PRIMARY_KEY, ORDER_BY or SAMPLE_BY clauses.*\\n.*");
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'Log', sample_by='id')", ".* doesn't support PARTITION_BY, PRIMARY_KEY, ORDER_BY or SAMPLE_BY clauses.*\\n.*");

        // optional properties
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['id'])");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo(format("" +
                        "CREATE TABLE clickhouse.tpch.%s (\n" +
                        "   id integer NOT NULL,\n" +
                        "   x varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'MERGETREE',\n" +
                        "   order_by = ARRAY['id'],\n" +
                        "   primary_key = ARRAY['id']\n" + // order_by become primary_key automatically in ClickHouse
                        ")", tableName));
        assertUpdate("DROP TABLE " + tableName);

        // the column refers by order by must be not null
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['id', 'x'])", ".* Sorting key cannot contain nullable columns.*\\n.*");

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['id'], primary_key = ARRAY['id'])");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo(format("" +
                        "CREATE TABLE clickhouse.tpch.%s (\n" +
                        "   id integer NOT NULL,\n" +
                        "   x varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'MERGETREE',\n" +
                        "   order_by = ARRAY['id'],\n" +
                        "   primary_key = ARRAY['id']\n" +
                        ")", tableName));
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR NOT NULL, y VARCHAR NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id', 'x', 'y'], primary_key = ARRAY['id', 'x'])");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo(format("" +
                        "CREATE TABLE clickhouse.tpch.%s (\n" +
                        "   id integer NOT NULL,\n" +
                        "   x varchar NOT NULL,\n" +
                        "   y varchar NOT NULL\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'MERGETREE',\n" +
                        "   order_by = ARRAY['id','x','y'],\n" +
                        "   primary_key = ARRAY['id','x']\n" +
                        ")", tableName));
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x BOOLEAN NOT NULL, y VARCHAR NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id', 'x'], primary_key = ARRAY['id','x'], sample_by = 'x' )");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo(format("" +
                        "CREATE TABLE clickhouse.tpch.%s (\n" +
                        "   id integer NOT NULL,\n" +
                        "   x smallint NOT NULL,\n" +
                        "   y varchar NOT NULL\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'MERGETREE',\n" +
                        "   order_by = ARRAY['id','x'],\n" +
                        "   primary_key = ARRAY['id','x'],\n" +
                        "   sample_by = 'x'\n" +
                        ")", tableName));
        assertUpdate("DROP TABLE " + tableName);

        // Partition column
        assertUpdate("CREATE TABLE " + tableName + "(id int NOT NULL, part int NOT NULL) WITH " +
                "(engine = 'MergeTree', order_by = ARRAY['id'], partition_by = ARRAY['part'])");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo(format("" +
                        "CREATE TABLE clickhouse.tpch.%s (\n" +
                        "   id integer NOT NULL,\n" +
                        "   part integer NOT NULL\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'MERGETREE',\n" +
                        "   order_by = ARRAY['id'],\n" +
                        "   partition_by = ARRAY['part'],\n" +
                        "   primary_key = ARRAY['id']\n" +
                        ")", tableName));
        assertUpdate("DROP TABLE " + tableName);

        // Primary key must be a prefix of the sorting key,
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x boolean NOT NULL, y boolean NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id'], sample_by = ARRAY['x', 'y'])",
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
                "(p1 int NOT NULL, p2 boolean NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['p1', 'p2'], primary_key = ARRAY['p1', 'p2'])")) {
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
                onRemoteDatabase(),
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
                // The connector supports date type, but these values are unsupported in ClickHouse
                // See BaseClickHouseTypeMapping for additional test coverage
                if (dataMappingTestSetup.getSampleValueLiteral().equals("DATE '0001-01-01'") ||
                        dataMappingTestSetup.getSampleValueLiteral().equals("DATE '1582-10-05'") ||
                        dataMappingTestSetup.getHighValueLiteral().equals("DATE '9999-12-31'")) {
                    return Optional.empty();
                }
                return Optional.of(dataMappingTestSetup);

            case "time":
            case "time(6)":
            case "timestamp":
            case "timestamp(6)":
            case "timestamp(3) with time zone":
            case "timestamp(6) with time zone":
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

        try (TestTable table = new TestTable(getQueryRunner()::execute, "not_null_no_cast", "(nullable_col INTEGER, not_null_col INTEGER NOT NULL)")) {
            assertUpdate(format("INSERT INTO %s (not_null_col) VALUES (2)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 2)");
            // This is enforced by the engine and not the connector
            assertQueryFails(format("INSERT INTO %s (not_null_col, nullable_col) VALUES (NULL, 3)", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
            assertQueryFails(format("INSERT INTO %s (not_null_col, nullable_col) VALUES (TRY(5/0), 4)", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
            assertQueryFails(format("INSERT INTO %s (not_null_col) VALUES (TRY(6/0))", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
        }
    }

    @Override
    protected String errorMessageForCreateTableAsSelectNegativeDate(String date)
    {
        return "Date must be between 1970-01-01 and 2149-06-06 in ClickHouse: " + date;
    }

    @Override
    protected String errorMessageForInsertNegativeDate(String date)
    {
        return "Date must be between 1970-01-01 and 2149-06-06 in ClickHouse: " + date;
    }

    @Test
    @Override
    public void testDateYearOfEraPredicate()
    {
        // Override because the connector throws an exception instead of an empty result when the value is out of supported range
        assertQuery("SELECT orderdate FROM orders WHERE orderdate = DATE '1997-09-14'", "VALUES DATE '1997-09-14'");
        assertQueryFails(
                "SELECT * FROM orders WHERE orderdate = DATE '-1996-09-14'",
                errorMessageForDateYearOfEraPredicate("-1996-09-14"));
    }

    protected String errorMessageForDateYearOfEraPredicate(String date)
    {
        return "Date must be between 1970-01-01 and 2149-06-06 in ClickHouse: " + date;
    }

    @Override
    public void testCharTrailingSpace()
    {
        assertThatThrownBy(super::testCharTrailingSpace)
                .hasMessageStartingWith("Failed to execute statement: CREATE TABLE tpch.char_trailing_space");
        throw new SkipException("Implement test for ClickHouse");
    }

    @Override
    protected TestTable simpleTable()
    {
        // override because Clickhouse requires engine specification
        return new TestTable(onRemoteDatabase(), "tpch.simple_table", "(col BIGINT) Engine=Log", ImmutableList.of("1", "2"));
    }

    @Override
    public void testCreateTableWithLongTableName()
    {
        // Override because ClickHouse connector can create a table which can't be dropped
        String baseTableName = "test_create_" + randomNameSuffix();
        String validTableName = baseTableName + "z".repeat(maxTableNameLength().orElseThrow() - baseTableName.length());

        assertUpdate("CREATE TABLE " + validTableName + " (a bigint)");
        assertTrue(getQueryRunner().tableExists(getSession(), validTableName));
        assertThatThrownBy(() -> assertUpdate("DROP TABLE " + validTableName))
                .hasMessageMatching("(?s).*(Bad path syntax|File name too long).*");

        String invalidTableName = baseTableName + "z".repeat(maxTableNameLength().orElseThrow() - baseTableName.length() + 1);
        assertThatThrownBy(() -> query("CREATE TABLE " + invalidTableName + " (a bigint)"))
                .hasMessageMatching("(?s).*(Cannot open file|File name too long).*");
        // ClickHouse lefts a table even if the above statement failed
        assertTrue(getQueryRunner().tableExists(getSession(), validTableName));
    }

    @Override
    public void testRenameSchemaToLongName()
    {
        // Override because the max length is different from CREATE SCHEMA case
        String sourceTableName = "test_rename_source_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + sourceTableName);

        String baseSchemaName = "test_rename_target_" + randomNameSuffix();

        // The numeric value depends on file system
        int maxLength = 255 - ".sql".length();

        String validTargetSchemaName = baseSchemaName + "z".repeat(maxLength - baseSchemaName.length());
        assertUpdate("ALTER SCHEMA " + sourceTableName + " RENAME TO " + validTargetSchemaName);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(validTargetSchemaName);
        assertUpdate("DROP SCHEMA " + validTargetSchemaName);

        assertUpdate("CREATE SCHEMA " + sourceTableName);
        String invalidTargetSchemaName = validTargetSchemaName + "z";
        assertThatThrownBy(() -> assertUpdate("ALTER SCHEMA " + sourceTableName + " RENAME TO " + invalidTargetSchemaName))
                .satisfies(this::verifySchemaNameLengthFailurePermissible);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(invalidTargetSchemaName);
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        // The numeric value depends on file system
        return OptionalInt.of(255 - ".sql.tmp".length());
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("File name too long");
    }

    @Override
    public void testRenameTableToLongTableName()
    {
        // Override because ClickHouse connector can rename to a table which can't be dropped
        String sourceTableName = "test_source_long_table_name_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + sourceTableName + " AS SELECT 123 x", 1);

        String baseTableName = "test_target_long_table_name_" + randomNameSuffix();
        // The max length is different from CREATE TABLE case
        String validTargetTableName = baseTableName + "z".repeat(255 - ".sql".length() - baseTableName.length());

        assertUpdate("ALTER TABLE " + sourceTableName + " RENAME TO " + validTargetTableName);
        assertTrue(getQueryRunner().tableExists(getSession(), validTargetTableName));
        assertQuery("SELECT x FROM " + validTargetTableName, "VALUES 123");
        assertThatThrownBy(() -> assertUpdate("DROP TABLE " + validTargetTableName))
                .hasMessageMatching("(?s).*(Bad path syntax|File name too long).*");

        assertUpdate("CREATE TABLE " + sourceTableName + " AS SELECT 123 x", 1);
        String invalidTargetTableName = validTargetTableName + "z";
        assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + sourceTableName + " RENAME TO " + invalidTargetTableName))
                .hasMessageMatching("(?s).*(Cannot rename|File name too long).*");
        assertFalse(getQueryRunner().tableExists(getSession(), invalidTargetTableName));
    }

    @Test
    public void testLargeDefaultDomainCompactionThreshold()
    {
        String catalogName = getSession().getCatalog().orElseThrow();
        String propertyName = catalogName + "." + DOMAIN_COMPACTION_THRESHOLD;
        assertQuery(
                "SHOW SESSION LIKE '" + propertyName + "'",
                "VALUES('" + propertyName + "','1000', '1000', 'integer', 'Maximum ranges to allow in a tuple domain without simplifying it')");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        // The numeric value depends on file system
        return OptionalInt.of(255 - ".sql.detached".length());
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
            ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
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
