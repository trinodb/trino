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
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.plugin.clickhouse.ClickHouseSessionProperties.MAP_STRING_AS_VARCHAR;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.ENGINE_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.ORDER_BY_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.PARTITION_BY_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.SAMPLE_BY_PROPERTY;
import static io.trino.plugin.clickhouse.TestingClickHouseServer.CLICKHOUSE_LATEST_IMAGE;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.DOMAIN_COMPACTION_THRESHOLD;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestClickHouseConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingClickHouseServer clickhouseServer;

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT,
                 SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN_WITH_LIKE,
                 SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_TRUNCATE -> true;
            case SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV,
                 SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE,
                 SUPPORTS_ARRAY,
                 SUPPORTS_DELETE,
                 SUPPORTS_DROP_NOT_NULL_CONSTRAINT,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_NEGATIVE_DATE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.clickhouseServer = closeAfterClass(new TestingClickHouseServer(CLICKHOUSE_LATEST_IMAGE));
        return ClickHouseQueryRunner.builder(clickhouseServer)
                .addConnectorProperty("clickhouse.map-string-as-varchar", "true")
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Test
    public void testSampleBySqlInjection()
    {
        String tableName = "sql_injection_" + randomNameSuffix();
        try {
            assertQueryFails("CREATE TABLE " + tableName + " (p1 int NOT NULL, p2 boolean NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['p1', 'p2'], primary_key = ARRAY['p1', 'p2'], sample_by = 'p2; drop table tpch.nation')", "(?s).*Missing columns: 'p2; drop table tpch.nation.*");
            assertUpdate("CREATE TABLE " + tableName + " (p1 int NOT NULL, p2 boolean NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['p1', 'p2'], primary_key = ARRAY['p1', 'p2'], sample_by = 'p2')");
            assertQueryFails("ALTER TABLE " + tableName + " SET PROPERTIES sample_by = 'p2; drop table tpch.nation'", "(?s).*Missing columns: 'p2; drop table tpch.nation.*");
            assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES sample_by = 'p2'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    @Override
    public void testRenameColumn()
    {
        // ClickHouse need resets all data in a column for specified column which to be renamed
        abort("TODO: test not implemented yet");
    }

    @Test
    @Override
    public void testRenameColumnWithComment()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_rename_column_",
                "(id INT NOT NULL, col INT COMMENT 'test column comment') WITH (engine = 'MergeTree', order_by = ARRAY['id'])")) {
            assertThat(getColumnComment(table.getName(), "col")).isEqualTo("test column comment");

            assertUpdate("ALTER TABLE " + table.getName() + " RENAME COLUMN col TO renamed_col");
            assertThat(getColumnComment(table.getName(), "renamed_col")).isEqualTo("test column comment");
        }
    }

    @Override
    public void testAddColumnWithCommentSpecialCharacter(String comment)
    {
        // Override because default storage engine doesn't support renaming columns
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_column_", "(a_varchar varchar NOT NULL) WITH (engine = 'mergetree', order_by = ARRAY['a_varchar'])")) {
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN b_varchar varchar COMMENT " + varcharLiteral(comment));
            assertThat(getColumnComment(table.getName(), "b_varchar")).isEqualTo(comment);
        }
    }

    @Test
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

    @Test
    @Disabled
    @Override
    public void testRenameColumnName() {}

    @Override
    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        // TODO: Investigate why a\backslash allows creating a table, but it throws an exception when selecting
        if (columnName.equals("a\\backslash`")) {
            return Optional.empty();
        }
        return Optional.of(columnName);
    }

    @Test
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

        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN notExistColumn");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
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

    @Test
    @Override // Overridden because the default storage type doesn't support adding columns
    public void testAddNotNullColumnToEmptyTable()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_notnull_col_to_empty", "(a_varchar varchar NOT NULL)  WITH (engine = 'MergeTree', order_by = ARRAY['a_varchar'])")) {
            String tableName = table.getName();

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar NOT NULL");
            assertThat(columnIsNullable(tableName, "b_varchar")).isFalse();
            assertUpdate("INSERT INTO " + tableName + " VALUES ('a', 'b')", 1);
            assertThat(query("TABLE " + tableName))
                    .skippingTypesCheck()
                    .matches("VALUES ('a', 'b')");
        }
    }

    @Test
    @Override // Overridden because (a) the default storage type doesn't support adding columns and (b) ClickHouse has implicit default value for new NON NULL column
    public void testAddNotNullColumn()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_notnull_col", "(a_varchar varchar NOT NULL)  WITH (engine = 'MergeTree', order_by = ARRAY['a_varchar'])")) {
            String tableName = table.getName();

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar NOT NULL");
            assertThat(columnIsNullable(tableName, "b_varchar")).isFalse();

            assertUpdate("INSERT INTO " + tableName + " VALUES ('a', 'b')", 1);

            // ClickHouse set an empty character as the default value
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN c_varchar varchar NOT NULL");
            assertThat(columnIsNullable(tableName, "c_varchar")).isFalse();
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
            assertThat(getColumnComment(tableName, "empty_comment")).isNull();
        }
    }

    @Test
    @Override
    public void testAlterTableAddLongColumnName()
    {
        // TODO: Find the maximum column name length in ClickHouse and enable this test.
        abort("TODO");
    }

    @Test
    @Override
    public void testAlterTableRenameColumnToLongName()
    {
        // TODO: Find the maximum column name length in ClickHouse and enable this test.
        abort("TODO");
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

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        assertThatThrownBy(super::testCharVarcharComparison)
                .hasMessageContaining("For query")
                .hasMessageContaining("Actual rows")
                .hasMessageContaining("Expected rows");

        // TODO run the test with clickhouse.map-string-as-varchar
        abort("");
    }

    @Test
    public void testDifferentEngine()
    {
        String tableName = "test_different_engine_" + randomNameSuffix();
        // MergeTree
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['id'])");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
        assertUpdate("DROP TABLE " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'mergetree', order_by = ARRAY['id'])");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
        assertUpdate("DROP TABLE " + tableName);
        // MergeTree without order by
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree')");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
        assertUpdate("DROP TABLE " + tableName);

        // MergeTree with optional
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR, logdate DATE NOT NULL) WITH " +
                "(engine = 'MergeTree', order_by = ARRAY['id'], partition_by = ARRAY['logdate'])");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
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
                ".* Unable to set catalog 'clickhouse' table property 'engine' to.*");
    }

    @Test
    public void testTableProperty()
    {
        String tableName = "test_table_property_" + randomNameSuffix();
        // no table property, it should create a table with default Log engine table
        assertUpdate("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR)");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
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
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL, x VARCHAR) WITH (engine = 'MergeTree', order_by = ARRAY['id', 'x'])", ".*Sorting key contains nullable columns, but merge tree setting `allow_nullable_key` is disabled.*\\n.*");

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
                ".* Invalid value for catalog 'clickhouse' table property 'sample_by': .*");

        // wrong property type
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL) WITH (engine = 'MergeTree', order_by = 'id')",
                ".* Invalid value for catalog 'clickhouse' table property 'order_by': .*");
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id'], primary_key = 'id')",
                ".* Invalid value for catalog 'clickhouse' table property 'primary_key': .*");
        assertQueryFails("CREATE TABLE " + tableName + " (id int NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id'], primary_key = ARRAY['id'], partition_by = 'id')",
                ".* Invalid value for catalog 'clickhouse' table property 'partition_by': .*");
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
                    "line 1:66: Catalog 'clickhouse' table property 'invalid_property' does not exist");
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
    @Test
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

    @Test
    @Override
    public void testCharTrailingSpace()
    {
        assertThatThrownBy(super::testCharTrailingSpace)
                .hasMessageStartingWith("Failed to execute statement: CREATE TABLE tpch.char_trailing_space");
        abort("Implement test for ClickHouse");
    }

    @Override
    protected TestTable simpleTable()
    {
        // override because Clickhouse requires engine specification
        return new TestTable(onRemoteDatabase(), "tpch.simple_table", "(col BIGINT) Engine=Log", ImmutableList.of("1", "2"));
    }

    @Test
    @Override
    public void testCreateTableWithLongTableName()
    {
        // Override because ClickHouse connector can create a table which can't be dropped
        String baseTableName = "test_create_" + randomNameSuffix();
        String validTableName = baseTableName + "z".repeat(maxTableNameLength().orElseThrow() - baseTableName.length());

        assertUpdate("CREATE TABLE " + validTableName + " (a bigint)");
        assertThat(getQueryRunner().tableExists(getSession(), validTableName)).isTrue();
        assertThatThrownBy(() -> assertUpdate("DROP TABLE " + validTableName))
                .hasMessageMatching("(?s).*(Bad path syntax|File name too long).*");

        String invalidTableName = baseTableName + "z".repeat(maxTableNameLength().orElseThrow() - baseTableName.length() + 1);
        assertThat(query("CREATE TABLE " + invalidTableName + " (a bigint)"))
                .failure().hasMessageMatching("(?s).*(Cannot open file|File name too long).*");
        // ClickHouse lefts a table even if the above statement failed
        assertThat(getQueryRunner().tableExists(getSession(), validTableName)).isTrue();
    }

    @Test
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

    @Test
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
        assertThat(getQueryRunner().tableExists(getSession(), validTargetTableName)).isTrue();
        assertQuery("SELECT x FROM " + validTargetTableName, "VALUES 123");
        assertThatThrownBy(() -> assertUpdate("DROP TABLE " + validTargetTableName))
                .hasMessageMatching("(?s).*(Bad path syntax|File name too long).*");

        assertUpdate("CREATE TABLE " + sourceTableName + " AS SELECT 123 x", 1);
        String invalidTargetTableName = validTargetTableName + "z";
        assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + sourceTableName + " RENAME TO " + invalidTargetTableName))
                .hasMessageMatching("(?s).*(Cannot rename|File name too long).*");
        assertThat(getQueryRunner().tableExists(getSession(), invalidTargetTableName)).isFalse();
    }

    @Test
    @Override // Override because the failure message differs
    public void testNativeQueryIncorrectSyntax()
    {
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'some wrong syntax'))"))
                .failure().hasMessage("Query not supported: ResultSetMetaData not available for query: some wrong syntax");
    }

    @Test
    @Override // Override because the failure message differs
    public void testNativeQueryInsertStatementTableDoesNotExist()
    {
        assertThat(getQueryRunner().tableExists(getSession(), "non_existent_table")).isFalse();
        assertThat(query("SELECT * FROM TABLE(system.query(query => 'INSERT INTO non_existent_table VALUES (1)'))"))
                .failure().hasMessage("Query not supported: ResultSetMetaData not available for query: INSERT INTO non_existent_table VALUES (1)");
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

    @Test
    public void testFloatPredicatePushdown()
    {
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_float_predicate_pushdown",
                """
                (
                c_real real,
                c_real_neg_infinity real,
                c_real_pos_infinity real,
                c_real_nan real,
                c_double double,
                c_double_neg_infinity double,
                c_double_pos_infinity double,
                c_double_nan double)
                """,
                List.of("3.14, -infinity(), +infinity(), nan(), 3.14, -infinity(), +infinity(), nan()"))) {
            assertThat(query("SELECT c_real FROM %s WHERE c_real = real '3.14'".formatted(table.getName())))
                    // because of https://github.com/trinodb/trino/issues/9998
                    .isNotFullyPushedDown(FilterNode.class);

            assertThat(query("SELECT c_real FROM %s WHERE c_real_neg_infinity = -infinity()".formatted(table.getName())))
                    // because of https://github.com/trinodb/trino/issues/9998
                    .isNotFullyPushedDown(FilterNode.class);

            assertThat(query("SELECT c_real FROM %s WHERE c_real_pos_infinity = +infinity()".formatted(table.getName())))
                    // because of https://github.com/trinodb/trino/issues/9998
                    .isNotFullyPushedDown(FilterNode.class);

            assertThat(query("SELECT c_real FROM %s WHERE c_real_nan = nan()".formatted(table.getName())))
                    .isReplacedWithEmptyValues();

            assertThat(query("SELECT c_real FROM %s WHERE c_double = double '3.14'".formatted(table.getName())))
                    .isFullyPushedDown();

            assertThat(query("SELECT c_real FROM %s WHERE c_double_neg_infinity = -infinity()".formatted(table.getName())))
                    .isFullyPushedDown();

            assertThat(query("SELECT c_real FROM %s WHERE c_double_pos_infinity = +infinity()".formatted(table.getName())))
                    .isFullyPushedDown();

            assertThat(query("SELECT c_real FROM %s WHERE c_double_nan = nan()".formatted(table.getName())))
                    .isReplacedWithEmptyValues();
        }
    }

    @Test
    public void testOrPredicatePushdown()
    {
        assertThat(query("SELECT * FROM nation WHERE name = 'ALGERIA' OR comment = 'comment'")).isFullyPushedDown();
    }

    @Test
    public void testTextualPredicatePushdown()
    {
        Session smallDomainCompactionThreshold = Session.builder(getSession())
                .setCatalogSessionProperty("clickhouse", "domain_compaction_threshold", "1")
                .build();

        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar))")
                .isFullyPushedDown();
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA' OR comment = 'P'"))
                .isFullyPushedDown();

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name < 'POLAND'"))
                .isFullyPushedDown();
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name < 'POLAND' OR comment < 'P'"))
                .isFullyPushedDown();

        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name <= 'POLAND'"))
                .isFullyPushedDown();
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name <= 'POLAND' OR comment <= 'P'"))
                .isFullyPushedDown();

        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name > 'POLAND'"))
                .isFullyPushedDown();
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name > 'POLAND' OR comment > 'P'"))
                .isFullyPushedDown();

        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name >= 'POLAND'"))
                .isFullyPushedDown();
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name >= 'POLAND' OR comment >= 'P'"))
                .isFullyPushedDown();

        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar))")
                .isFullyPushedDown();

        // varchar IN without domain compaction
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar)), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar))")
                .isFullyPushedDown();

        // varchar IN with small compaction threshold
        assertThat(query(
                Session.builder(getSession())
                        .setCatalogSessionProperty("clickhouse", "domain_compaction_threshold", "1")
                        .build(),
                "SELECT regionkey, nationkey, name FROM nation WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar)), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar))")
                // Filter node is retained as constraint is pushed into connector is simplified, and
                // The compacted domain is a range predicate which can give wrong results
                // so has to be filtered by trino too to ensure correct predicate.
                .isNotFullyPushedDown(FilterNode.class);

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isFullyPushedDown();
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania' OR comment = 'P'"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        Session convertToVarchar = Session.builder(getSession())
                .setCatalogSessionProperty("clickhouse", UNSUPPORTED_TYPE_HANDLING, CONVERT_TO_VARCHAR.name())
                .build();
        String withConnectorExpression = " OR some_column = 'x'";
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "tpch.test_textual_predicate_pushdown",
                """
                (
                unsupported_1 Point,
                unsupported_2 Point,
                some_column String,
                a_string String,
                a_string_alias Text,
                a_fixed_string FixedString(1),
                a_nullable_string Nullable(String),
                a_nullable_string_alias Nullable(Text),
                a_nullable_fixed_string Nullable(FixedString(1)),
                a_lowcardinality_nullable_string LowCardinality(Nullable(String)),
                a_lowcardinality_nullable_fixed_string LowCardinality(Nullable(FixedString(1))),
                a_enum_1 Enum('hello', 'world', 'a', 'b', 'c', '%', '_'),
                a_enum_2 Enum('hello', 'world', 'a', 'b', 'c', '%', '_'))
                ENGINE=Log
                """,
                List.of(
                        "(10, 10), (10, 10), 'z', '\\\\', '\\\\', '\\\\', '\\\\', '\\\\', '\\\\', '\\\\', '\\\\', 'hello', 'world'",
                        "(10, 10), (10, 10), 'z', '_', '_', '_', '_', '_', '_', '_', '_', '_', '_'",
                        "(10, 10), (10, 10), 'z', '%', '%', '%', '%', '%', '%', '%', '%', '%', '%'",
                        "(10, 10), (10, 10), 'z', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a'",
                        "(10, 10), (10, 10), 'z', 'b', 'b', 'b', 'b', 'b', 'b', 'b', 'b', 'b', 'b'",
                        "(10, 10), (10, 10), 'z', 'c', 'c', 'c', 'c', 'c', 'c', 'c', 'c', 'c', 'c'"))) {
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string = 'b'")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string = 'b'" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string_alias = 'b'")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string_alias = 'b'" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_fixed_string = 'b'")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_fixed_string = 'b'" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nullable_string = 'b'")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nullable_string = 'b'" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nullable_string_alias = 'b'")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nullable_string_alias = 'b'" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nullable_fixed_string = 'b'")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_nullable_fixed_string = 'b'" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_lowcardinality_nullable_string = 'b'")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_lowcardinality_nullable_string = 'b'" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_lowcardinality_nullable_fixed_string = 'b'")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_lowcardinality_nullable_fixed_string = 'b'" + withConnectorExpression)).isFullyPushedDown();

            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string = a_string_alias")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string = a_string_alias" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string = a_enum_1")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string = a_enum_1" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE a_string = unsupported_1")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE a_string = unsupported_1" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);

            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 = 'hello'")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 = 'hello'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 = 'not_a_value'")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 = 'not_a_value'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
            // pushdown of a condition, both sides of the same native type, which is mapped to varchar,
            // not allowed because some operations (e.g. inequalities) may not be allowed in the native system on an unknown native types
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 = a_enum_2")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 = a_enum_2" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);

            // pushdown of a condition, both sides of the same native type, which is mapped to varchar,
            // not allowed because some operations (e.g. inequalities) may not be allowed in the native system on an unknown native types
            assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE unsupported_1 = unsupported_2")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE unsupported_1 = unsupported_2" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);

            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string IN ('a', 'b')")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string IN ('a', 'b')" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 IN ('a', 'b')")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 IN ('a', 'b')" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE unsupported_1 IN ('a', 'b')")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE unsupported_1 IN ('a', 'b')" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string IN (a_string_alias, 'b')")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string IN (a_string_alias, 'b')" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(smallDomainCompactionThreshold, "SELECT some_column FROM " + table.getName() + " WHERE a_string IN ('a', 'b')")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(smallDomainCompactionThreshold, "SELECT some_column FROM " + table.getName() + " WHERE a_string IN ('a', 'b')" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string NOT IN ('a', 'b')")).isFullyPushedDown();
            assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string NOT IN ('a', 'b')" + withConnectorExpression)).isFullyPushedDown();
            assertThat(query(smallDomainCompactionThreshold, "SELECT some_column FROM " + table.getName() + " WHERE a_string NOT IN ('a', 'b')")).isNotFullyPushedDown(FilterNode.class);
            assertThat(query(smallDomainCompactionThreshold, "SELECT some_column FROM " + table.getName() + " WHERE a_string NOT IN ('a', 'b')" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);

            assertLike(true, table, withConnectorExpression, convertToVarchar);
            assertLike(false, table, withConnectorExpression, convertToVarchar);
        }
    }

    private void assertLike(boolean isPositive, TestTable table, String withConnectorExpression, Session convertToVarchar)
    {
        String like = isPositive ? "LIKE" : "NOT LIKE";
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " NULL")).returnsEmptyResult();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " 'b'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " 'b'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " 'b%'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " 'b%'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%b'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%b'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%b%'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%b%'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 " + like + " '%b%'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_enum_1 " + like + " '%b%'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE unsupported_1 " + like + " '%b%'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE unsupported_1 " + like + " '%b%'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " a_string_alias")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " a_string_alias" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " a_enum_1")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " a_enum_1" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " unsupported_1")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query(convertToVarchar, "SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " unsupported_1" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        // metacharacters
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '_'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '_'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '__'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '__'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%'" + withConnectorExpression)).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%%'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%%'" + withConnectorExpression)).isFullyPushedDown();
        // escape
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\b'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\b'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\_'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\_'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\__'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\__'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\%'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\%'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\%%'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\%%'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\\\'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\\\'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\\\\\'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\\\\\'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\' ESCAPE '\\'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\\\' ESCAPE '\\'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\%' ESCAPE '\\'")).isFullyPushedDown();
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '\\%' ESCAPE '\\'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%$_%' ESCAPE '$'")).isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT some_column FROM " + table.getName() + " WHERE a_string " + like + " '%$_%' ESCAPE '$'" + withConnectorExpression)).isNotFullyPushedDown(FilterNode.class);
    }

    @Test
    public void testIsNull()
    {
        Session mapStringAsVarbinary = Session.builder(getSession())
                .setCatalogSessionProperty("clickhouse", MAP_STRING_AS_VARCHAR, "false")
                .build();

        NullPushdownDataTypeTest.connectorExpressionOnly()
                .addSpecialColumn("String", "'z'", "CAST('z' AS varchar)")
                .addTestCase("Nullable(real)")
                .addTestCase("Nullable(decimal(3, 1))")
                .addTestCase("Nullable(decimal(30, 5))")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_is_null"));

        NullPushdownDataTypeTest.connectorExpressionOnly()
                .addSpecialColumn("String", "'z'", "CAST('z' AS varbinary)")
                .addTestCase("Nullable(char(10))")
                .addTestCase("LowCardinality(Nullable(char(10)))")
                .addTestCase("Nullable(FixedString(10))")
                .addTestCase("LowCardinality(Nullable(FixedString(10)))")
                .addTestCase("Nullable(varchar(30))")
                .addTestCase("LowCardinality(Nullable(varchar(30)))")
                .addTestCase("Nullable(String)")
                .addTestCase("LowCardinality(Nullable(String))")
                .execute(getQueryRunner(), mapStringAsVarbinary, clickhouseCreateAndInsert("tpch.test_is_null"));

        NullPushdownDataTypeTest.create()
                .addSpecialColumn("String", "'z'", "CAST('z' AS varchar)")
                .addTestCase("Nullable(tinyint)")
                .addTestCase("Nullable(smallint)")
                .addTestCase("Nullable(integer)")
                .addTestCase("Nullable(bigint)")
                .addTestCase("Nullable(UInt8)")
                .addTestCase("Nullable(UInt16)")
                .addTestCase("Nullable(UInt32)")
                .addTestCase("Nullable(UInt64)")
                .addTestCase("Nullable(double)")
                .addTestCase("Nullable(char(10))")
                .addTestCase("LowCardinality(Nullable(char(10)))")
                .addTestCase("Nullable(FixedString(10))")
                .addTestCase("LowCardinality(Nullable(FixedString(10)))")
                .addTestCase("Nullable(varchar(30))")
                .addTestCase("LowCardinality(Nullable(varchar(30)))")
                .addTestCase("Nullable(String)")
                .addTestCase("LowCardinality(Nullable(String))")
                .addTestCase("Nullable(date)")
                .addTestCase("Nullable(timestamp)")
                .addTestCase("Nullable(datetime)")
                .addTestCase("Nullable(datetime('UTC'))")
                .addTestCase("Nullable(UUID)")
                .addTestCase("Nullable(IPv4)")
                .addTestCase("Nullable(IPv6)")
                .execute(getQueryRunner(), clickhouseCreateAndInsert("tpch.test_is_null"));
    }

    @Test
    @Override // Override because ClickHouse doesn't follow SQL standard syntax
    public void testExecuteProcedure()
    {
        String tableName = "test_execute" + randomNameSuffix();
        String schemaTableName = getSession().getSchema().orElseThrow() + "." + tableName;

        assertUpdate("CREATE TABLE " + schemaTableName + "(id int NOT NULL, data int) WITH (engine = 'MergeTree', order_by = ARRAY['id'])");
        try {
            assertUpdate("CALL system.execute('INSERT INTO " + schemaTableName + " VALUES (1, 10)')");
            assertQuery("SELECT * FROM " + schemaTableName, "VALUES (1, 10)");

            assertUpdate("CALL system.execute('ALTER TABLE " + schemaTableName + " UPDATE data = 100 WHERE true')");
            assertQuery("SELECT * FROM " + schemaTableName, "VALUES (1, 100)");

            assertUpdate("CALL system.execute('ALTER TABLE " + schemaTableName + " DELETE WHERE true')");
            assertQueryReturnsEmptyResult("SELECT * FROM " + schemaTableName);

            assertUpdate("CALL system.execute('DROP TABLE " + schemaTableName + "')");
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaTableName);
        }
    }

    @Test
    @Override // Override because ClickHouse allows SELECT query in update procedure
    public void testExecuteProcedureWithInvalidQuery()
    {
        assertUpdate("CALL system.execute('SELECT 1')");
        assertQueryFails("CALL system.execute('invalid')", "(?s)Failed to execute query.*");
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

    private DataSetup clickhouseCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new ClickHouseSqlExecutor(onRemoteDatabase()), tableNamePrefix);
    }
}
