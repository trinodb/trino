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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.plugin.bigquery.BigQueryQueryRunner.BigQuerySqlExecutor;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestBigQueryIntegrationSmokeTest
        // TODO extend BaseConnectorTest
        extends AbstractTestIntegrationSmokeTest
{
    private BigQuerySqlExecutor bigQuerySqlExecutor;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.bigQuerySqlExecutor = new BigQuerySqlExecutor();
        return BigQueryQueryRunner.createQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of());
    }

    @Test
    public void testCreateSchema()
    {
        String schemaName = "test_create_schema";

        assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);

        assertUpdate("CREATE SCHEMA " + schemaName);
        assertUpdate("CREATE SCHEMA IF NOT EXISTS " + schemaName);

        assertQueryFails(
                "CREATE SCHEMA " + schemaName,
                format("\\Qline 1:1: Schema 'bigquery.%s' already exists\\E", schemaName));

        assertUpdate("DROP SCHEMA " + schemaName);
    }

    @Test
    public void testDropSchema()
    {
        String schemaName = "test_drop_schema";

        assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        assertUpdate("CREATE SCHEMA " + schemaName);

        assertUpdate("DROP SCHEMA " + schemaName);
        assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);

        assertQueryFails(
                "DROP SCHEMA " + schemaName,
                format("\\Qline 1:1: Schema 'bigquery.%s' does not exist\\E", schemaName));
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
        String tableName = "test_create_table_supported_type_" + createType.replaceAll("[^a-zA-Z0-9]", "");

        assertUpdate("DROP TABLE IF EXISTS " + tableName);

        assertUpdate(format("CREATE TABLE %s (col1 %s)", tableName, createType));

        assertEquals(
                computeScalar("SELECT data_type FROM information_schema.columns WHERE table_name = '" + tableName + "' AND column_name = 'col1'"),
                expectedType);

        assertUpdate("DROP TABLE " + tableName);
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
                {"time with time zone", "time(3) with time zone"},
                {"timestamp", "timestamp(3)"},
                {"timestamp with time zone", "timestamp(3) with time zone"},
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
        String tableName = "test_create_table_unsupported_type_" + createType.replaceAll("[^a-zA-Z0-9]", "");

        assertUpdate("DROP TABLE IF EXISTS " + tableName);

        assertQueryFails(
                format("CREATE TABLE %s (col1 %s)", tableName, createType),
                "Unsupported column type: " + createType);

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
        String tableName = "test_row_type_table";

        assertUpdate("DROP TABLE IF EXISTS " + tableName);

        assertQueryFails(
                "CREATE TABLE " + tableName + "(col1 row(int))",
                "\\QROW type does not have field names declared: row(integer)\\E");

        assertUpdate("DROP TABLE IF EXISTS " + tableName);
    }

    @Test
    public void testCreateTableAlreadyExists()
    {
        String tableName = "test_create_table_already_exists";

        assertUpdate("DROP TABLE IF EXISTS " + tableName);

        assertUpdate("CREATE TABLE " + tableName + "(col1 int)");
        assertQueryFails(
                "CREATE TABLE " + tableName + "(col1 int)",
                "\\Qline 1:1: Table 'bigquery.tpch.test_create_table_already_exists' already exists\\E");

        assertUpdate("DROP TABLE IF EXISTS " + tableName);
    }

    @Test
    public void testCreateTableIfNotExists()
    {
        String tableName = "test_create_table_if_not_exists";

        assertUpdate("DROP TABLE IF EXISTS " + tableName);

        assertUpdate("CREATE TABLE " + tableName + "(col1 int)");
        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + "(col1 int)");

        assertUpdate("DROP TABLE IF EXISTS " + tableName);
    }

    @Test
    public void testDropTable()
    {
        String tableName = "test_drop_table";

        assertUpdate("DROP TABLE IF EXISTS " + tableName);

        assertUpdate("CREATE TABLE " + tableName + "(col bigint)");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test(enabled = false)
    public void testSelectFromHourlyPartitionedTable()
    {
        onBigQuery("DROP TABLE IF EXISTS test.hourly_partitioned");
        onBigQuery("CREATE TABLE test.hourly_partitioned (value INT64, ts TIMESTAMP) PARTITION BY TIMESTAMP_TRUNC(ts, HOUR)");
        onBigQuery("INSERT INTO test.hourly_partitioned (value, ts) VALUES (1000, '2018-01-01 10:00:00')");

        MaterializedResult actualValues = computeActual("SELECT COUNT(1) FROM test.hourly_partitioned");

        assertEquals((long) actualValues.getOnlyValue(), 1L);
    }

    @Test(enabled = false)
    public void testSelectFromYearlyPartitionedTable()
    {
        onBigQuery("DROP TABLE IF EXISTS test.yearly_partitioned");
        onBigQuery("CREATE TABLE test.yearly_partitioned (value INT64, ts TIMESTAMP) PARTITION BY TIMESTAMP_TRUNC(ts, YEAR)");
        onBigQuery("INSERT INTO test.yearly_partitioned (value, ts) VALUES (1000, '2018-01-01 10:00:00')");

        MaterializedResult actualValues = computeActual("SELECT COUNT(1) FROM test.yearly_partitioned");

        assertEquals((long) actualValues.getOnlyValue(), 1L);
    }

    @Test(description = "regression test for https://github.com/trinodb/trino/issues/7784")
    public void testSelectWithSingleQuoteInWhereClause()
    {
        String tableName = "test.select_with_single_quote";

        onBigQuery("DROP TABLE IF EXISTS " + tableName);
        onBigQuery("CREATE TABLE " + tableName + "(col INT64, val STRING)");
        onBigQuery("INSERT INTO " + tableName + " VALUES (1,'escape\\'single quote')");

        MaterializedResult actualValues = computeActual("SELECT val FROM " + tableName + " WHERE val = 'escape''single quote'");

        assertEquals(actualValues.getRowCount(), 1);
        assertEquals(actualValues.getOnlyValue(), "escape'single quote");
    }

    @Test(description = "regression test for https://github.com/trinodb/trino/issues/5618")
    public void testPredicatePushdownPrunnedColumns()
    {
        String tableName = "test.predicate_pushdown_prunned_columns";

        onBigQuery("DROP TABLE IF EXISTS " + tableName);
        onBigQuery("CREATE TABLE " + tableName + " (a INT64, b INT64, c INT64)");
        onBigQuery("INSERT INTO " + tableName + " VALUES (1,2,3)");

        assertQuery(
                "SELECT 1 FROM " + tableName + " WHERE " +
                        "    ((NULL IS NULL) OR a = 100) AND " +
                        "    b = 2",
                "VALUES (1)");
    }

    @Test(description = "regression test for https://github.com/trinodb/trino/issues/5635")
    public void testCountAggregationView()
    {
        String tableName = "test.count_aggregation_table";
        String viewName = "test.count_aggregation_view";

        onBigQuery("DROP TABLE IF EXISTS " + tableName);
        onBigQuery("DROP VIEW IF EXISTS " + viewName);
        onBigQuery("CREATE TABLE " + tableName + " (a INT64, b INT64, c INT64)");
        onBigQuery("INSERT INTO " + tableName + " VALUES (1, 2, 3), (4, 5, 6)");
        onBigQuery("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);

        assertQuery(
                "SELECT count(*) FROM " + viewName,
                "VALUES (2)");

        assertQuery(
                "SELECT count(*) FROM " + viewName + " WHERE a = 1",
                "VALUES (1)");

        assertQuery(
                "SELECT count(a) FROM " + viewName + " WHERE b = 2",
                "VALUES (1)");
    }

    /**
     * regression test for https://github.com/trinodb/trino/issues/6696
     */
    @Test
    public void testRepeatCountAggregationView()
    {
        String viewName = "test.repeat_count_aggregation_view_" + randomTableSuffix();

        onBigQuery("DROP VIEW IF EXISTS " + viewName);
        onBigQuery("CREATE VIEW " + viewName + " AS SELECT 1 AS col1");

        assertQuery("SELECT count(*) FROM " + viewName, "VALUES (1)");
        assertQuery("SELECT count(*) FROM " + viewName, "VALUES (1)");

        onBigQuery("DROP VIEW " + viewName);
    }

    /**
     * https://github.com/trinodb/trino/issues/8183
     */
    @Test
    public void testColumnPositionMismatch()
    {
        String tableName = "test.test_column_position_mismatch";

        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate(format("CREATE TABLE %s (c_varchar VARCHAR, c_int INT, c_date DATE)", tableName));
        onBigQuery(format("INSERT INTO %s VALUES ('a', 1, '2021-01-01')", tableName));

        // Adding a CAST makes BigQuery return columns in a different order
        assertQuery(
                "SELECT c_varchar, CAST(c_int AS SMALLINT), c_date FROM " + tableName,
                "VALUES ('a', 1, '2021-01-01')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testViewDefinitionSystemTable()
    {
        String schemaName = "test";
        String tableName = "views_system_table_base_" + randomTableSuffix();
        String viewName = "views_system_table_view_" + randomTableSuffix();

        onBigQuery(format("DROP TABLE IF EXISTS %s.%s", schemaName, tableName));
        onBigQuery(format("DROP VIEW IF EXISTS %s.%s", schemaName, viewName));
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

    private void onBigQuery(String sql)
    {
        bigQuerySqlExecutor.execute(sql);
    }
}
