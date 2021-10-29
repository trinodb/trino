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
package io.trino.plugin.bigquery;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestIntegrationSmokeTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.plugin.bigquery.BigQueryQueryRunner.BigQuerySqlExecutor;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBigQueryIntegrationSmokeTest
        // TODO extend BaseConnectorTest
        extends AbstractTestIntegrationSmokeTest
{
    protected BigQuerySqlExecutor bigQuerySqlExecutor;

    @BeforeClass(alwaysRun = true)
    public void initBigQueryExecutor()
    {
        this.bigQuerySqlExecutor = new BigQuerySqlExecutor();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return BigQueryQueryRunner.createQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of());
    }

    @Test
    public void testCreateSchema()
    {
        String schemaName = "test_schema_create_" + randomTableSuffix();
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(schemaName);
        assertUpdate("CREATE SCHEMA " + schemaName);
        assertUpdate("CREATE SCHEMA IF NOT EXISTS " + schemaName);

        // verify listing of new schema
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);

        // verify SHOW CREATE SCHEMA works
        assertThat((String) computeScalar("SHOW CREATE SCHEMA " + schemaName))
                .startsWith(format("CREATE SCHEMA %s.%s", getSession().getCatalog().orElseThrow(), schemaName));

        // try to create duplicate schema
        assertQueryFails("CREATE SCHEMA " + schemaName, format("line 1:1: Schema '.*\\.%s' already exists", schemaName));

        // cleanup
        assertUpdate("DROP SCHEMA " + schemaName);

        // verify DROP SCHEMA for non-existing schema
        assertQueryFails("DROP SCHEMA " + schemaName, format("line 1:1: Schema '.*\\.%s' does not exist", schemaName));
        assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
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
                .row("shippriority", "bigint", "", "")
                .row("comment", "varchar", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Test(dataProvider = "createTableSupportedTypes")
    public void testCreateTableSupportedType(String createType, String expectedType)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_table_supported_type_" + createType.replaceAll("[^a-zA-Z0-9]", ""), format("(col1 %s)", createType))) {
            assertEquals(
                    computeScalar("SELECT data_type FROM information_schema.columns WHERE table_name = '" + table.getName() + "' AND column_name = 'col1'"),
                    expectedType);
        }
    }

    @DataProvider
    public Object[][] createTableSupportedTypes()
    {
        return new Object[][] {
                {"boolean", "boolean"},
                {"tinyint", "bigint"},
                {"smallint", "bigint"},
                {"integer", "bigint"},
                {"bigint", "bigint"},
                {"double", "double"},
                {"decimal", "decimal(38,9)"},
                {"date", "date"},
                {"time with time zone", "time(6)"},
                {"timestamp(6)", "timestamp(6)"},
                {"timestamp(6) with time zone", "timestamp(6) with time zone"},
                {"char", "varchar"},
                {"char(65535)", "varchar"},
                {"varchar", "varchar"},
                {"varchar(65535)", "varchar"},
                {"varbinary", "varbinary"},
                {"array(bigint)", "array(bigint)"},
                {"row(x bigint, y double)", "row(x bigint, y double)"},
                {"row(x array(bigint))", "row(x array(bigint))"},
        };
    }

    @Test(dataProvider = "createTableUnsupportedTypes")
    public void testCreateTableUnsupportedType(String createType)
    {
        String tableName = format("test_create_table_unsupported_type_%s_%s", createType.replaceAll("[^a-zA-Z0-9]", ""), randomTableSuffix());
        assertQueryFails(format("CREATE TABLE %s (col1 %s)", tableName, createType), "Unsupported column type: " + createType);
        assertUpdate("DROP TABLE IF EXISTS " + tableName);
    }

    @DataProvider
    public Object[][] createTableUnsupportedTypes()
    {
        return new Object[][] {
                {"json"},
                {"uuid"},
                {"ipaddress"},
        };
    }

    @Test
    public void testCreateTableWithRowTypeWithoutField()
    {
        String tableName = "test_row_type_table_" + randomTableSuffix();
        assertQueryFails(
                "CREATE TABLE " + tableName + "(col1 row(int))",
                "\\QROW type does not have field names declared: row(integer)\\E");
    }

    @Test
    public void testCreateTableAlreadyExists()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_table_already_exists", "(col1 int)")) {
            assertQueryFails(
                    "CREATE TABLE " + table.getName() + "(col1 int)",
                    "\\Qline 1:1: Table 'bigquery.tpch." + table.getName() + "' already exists\\E");
        }
    }

    @Test
    public void testCreateTableIfNotExists()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_table_if_not_exists", "(col1 int)")) {
            assertUpdate("CREATE TABLE IF NOT EXISTS " + table.getName() + "(col1 int)");
        }
    }

    @Test
    public void testDropTable()
    {
        String tableName = "test_drop_table_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(col bigint)");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test(enabled = false)
    public void testSelectFromHourlyPartitionedTable()
    {
        try (TestTable table = new TestTable(
                bigQuerySqlExecutor,
                "test.hourly_partitioned",
                "(value INT64, ts TIMESTAMP) PARTITION BY TIMESTAMP_TRUNC(ts, HOUR)",
                List.of("1000, '2018-01-01 10:00:00'"))) {
            assertQuery("SELECT COUNT(1) FROM " + table.getName(), "VALUES 1");
        }
    }

    @Test(enabled = false)
    public void testSelectFromYearlyPartitionedTable()
    {
        try (TestTable table = new TestTable(
                bigQuerySqlExecutor,
                "test.yearly_partitioned",
                "(value INT64, ts TIMESTAMP) PARTITION BY TIMESTAMP_TRUNC(ts, YEAR)",
                List.of("1000, '2018-01-01 10:00:00'"))) {
            assertQuery("SELECT COUNT(1) FROM " + table.getName(), "VALUES 1");
        }
    }

    @Test(description = "regression test for https://github.com/trinodb/trino/issues/7784")
    public void testSelectWithSingleQuoteInWhereClause()
    {
        try (TestTable table = new TestTable(
                bigQuerySqlExecutor,
                "test.select_with_single_quote",
                "(col INT64, val STRING)",
                List.of("1, 'escape\\'single quote'"))) {
            assertQuery("SELECT val FROM " + table.getName() + " WHERE val = 'escape''single quote'", "VALUES 'escape''single quote'");
        }
    }

    @Test(description = "regression test for https://github.com/trinodb/trino/issues/5618")
    public void testPredicatePushdownPrunnedColumns()
    {
        try (TestTable table = new TestTable(
                bigQuerySqlExecutor,
                "test.predicate_pushdown_prunned_columns",
                "(a INT64, b INT64, c INT64)",
                List.of("1, 2, 3"))) {
            assertQuery(
                    "SELECT 1 FROM " + table.getName() + " WHERE " +
                            "    ((NULL IS NULL) OR a = 100) AND " +
                            "    b = 2",
                    "VALUES (1)");
        }
    }

    @Test(description = "regression test for https://github.com/trinodb/trino/issues/5635")
    public void testCountAggregationView()
    {
        try (TestTable table = new TestTable(
                bigQuerySqlExecutor,
                "test.count_aggregation_table",
                "(a INT64, b INT64, c INT64)",
                List.of("1, 2, 3", "4, 5, 6"));
                TestView view = new TestView(bigQuerySqlExecutor, "test.count_aggregation_view", "SELECT * FROM " + table.getName())) {
            assertQuery("SELECT count(*) FROM " + view.getName(), "VALUES (2)");
            assertQuery("SELECT count(*) FROM " + view.getName() + " WHERE a = 1", "VALUES (1)");
            assertQuery("SELECT count(a) FROM " + view.getName() + " WHERE b = 2", "VALUES (1)");
        }
    }

    /**
     * regression test for https://github.com/trinodb/trino/issues/6696
     */
    @Test
    public void testRepeatCountAggregationView()
    {
        try (TestView view = new TestView(bigQuerySqlExecutor, "test.repeat_count_aggregation_view", "SELECT 1 AS col1")) {
            assertQuery("SELECT count(*) FROM " + view.getName(), "VALUES (1)");
            assertQuery("SELECT count(*) FROM " + view.getName(), "VALUES (1)");
        }
    }

    /**
     * https://github.com/trinodb/trino/issues/8183
     */
    @Test
    public void testColumnPositionMismatch()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test.test_column_position_mismatch", "(c_varchar VARCHAR, c_int INT, c_date DATE)")) {
            onBigQuery("INSERT INTO " + table.getName() + " VALUES ('a', 1, '2021-01-01')");
            // Adding a CAST makes BigQuery return columns in a different order
            assertQuery("SELECT c_varchar, CAST(c_int AS SMALLINT), c_date FROM " + table.getName(), "VALUES ('a', 1, '2021-01-01')");
        }
    }

    @Test
    public void testViewDefinitionSystemTable()
    {
        String schemaName = "test";
        String tableName = "views_system_table_base_" + randomTableSuffix();
        String viewName = "views_system_table_view_" + randomTableSuffix();

        onBigQuery(format("CREATE TABLE %s.%s (a INT64, b INT64, c INT64)", schemaName, tableName));
        onBigQuery(format("CREATE VIEW %s.%s AS SELECT * FROM %s.%s", schemaName, viewName, schemaName, tableName));

        assertEquals(
                computeScalar(format("SELECT * FROM %s.\"%s$view_definition\"", schemaName, viewName)),
                format("SELECT * FROM %s.%s", schemaName, tableName));

        assertQueryFails(
                format("SELECT * FROM %s.\"%s$view_definition\"", schemaName, tableName),
                format("Table '%s.%s\\$view_definition' not found", schemaName, tableName));

        onBigQuery(format("DROP TABLE %s.%s", schemaName, tableName));
        onBigQuery(format("DROP VIEW %s.%s", schemaName, viewName));
    }

    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE bigquery.tpch.orders (\n" +
                        "   orderkey bigint NOT NULL,\n" +
                        "   custkey bigint NOT NULL,\n" +
                        "   orderstatus varchar NOT NULL,\n" +
                        "   totalprice double NOT NULL,\n" +
                        "   orderdate date NOT NULL,\n" +
                        "   orderpriority varchar NOT NULL,\n" +
                        "   clerk varchar NOT NULL,\n" +
                        "   shippriority bigint NOT NULL,\n" +
                        "   comment varchar NOT NULL\n" +
                        ")");
    }

    @Test
    public void testSkipUnsupportedType()
    {
        try (TestTable table = new TestTable(
                bigQuerySqlExecutor,
                "test.test_skip_unsupported_type",
                "(a INT64, unsupported BIGNUMERIC, b INT64)",
                List.of("1, 999, 2"))) {
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 2)");
            assertThat((String) computeActual("SHOW CREATE TABLE " + table.getName()).getOnlyValue())
                    .isEqualTo("CREATE TABLE bigquery." + table.getName() + " (\n" +
                            "   a bigint,\n" +
                            "   b bigint\n" +
                            ")");
        }
    }

    private void onBigQuery(String sql)
    {
        bigQuerySqlExecutor.execute(sql);
    }
}
