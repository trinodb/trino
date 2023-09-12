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

import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.spi.QueryId;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedResultWithQueryId;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.plugin.bigquery.BigQueryQueryRunner.BigQuerySqlExecutor;
import static io.trino.plugin.bigquery.BigQueryQueryRunner.TEST_SCHEMA;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public abstract class BaseBigQueryConnectorTest
        extends BaseConnectorTest
{
    protected BigQuerySqlExecutor bigQuerySqlExecutor;
    private String gcpStorageBucket;

    @BeforeClass(alwaysRun = true)
    @Parameters("testing.gcp-storage-bucket")
    public void initBigQueryExecutor(String gcpStorageBucket)
    {
        this.bigQuerySqlExecutor = new BigQuerySqlExecutor();
        // Prerequisite: upload region.csv in resources directory to gs://{testing.gcp-storage-bucket}/tpch/tiny/region.csv
        this.gcpStorageBucket = gcpStorageBucket;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_TRUNCATE -> true;
            case SUPPORTS_ADD_COLUMN,
                    SUPPORTS_CREATE_MATERIALIZED_VIEW,
                    SUPPORTS_CREATE_VIEW,
                    SUPPORTS_DELETE,
                    SUPPORTS_DEREFERENCE_PUSHDOWN,
                    SUPPORTS_MERGE,
                    SUPPORTS_NEGATIVE_DATE,
                    SUPPORTS_NOT_NULL_CONSTRAINT,
                    SUPPORTS_RENAME_COLUMN,
                    SUPPORTS_RENAME_SCHEMA,
                    SUPPORTS_RENAME_TABLE,
                    SUPPORTS_SET_COLUMN_TYPE,
                    SUPPORTS_TOPN_PUSHDOWN,
                    SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testShowColumns()
    {
        assertThat(query("SHOW COLUMNS FROM orders")).matches(getDescribeOrdersResult());
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        // shippriority column is bigint (not integer) in BigQuery connector
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
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
        String tableName = format("test_create_table_unsupported_type_%s_%s", createType.replaceAll("[^a-zA-Z0-9]", ""), randomNameSuffix());
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
        String tableName = "test_row_type_table_" + randomNameSuffix();
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
    public void testEmptyProjectionTable()
    {
        testEmptyProjection(
                tableName -> onBigQuery("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.region"),
                tableName -> onBigQuery("DROP TABLE " + tableName));
    }

    @Test
    public void testEmptyProjectionView()
    {
        testEmptyProjection(
                viewName -> onBigQuery("CREATE VIEW " + viewName + " AS SELECT * FROM tpch.region"),
                viewName -> onBigQuery("DROP VIEW " + viewName));
    }

    @Test
    public void testEmptyProjectionMaterializedView()
    {
        testEmptyProjection(
                materializedViewName -> onBigQuery("CREATE MATERIALIZED VIEW " + materializedViewName + " AS SELECT * FROM tpch.region"),
                materializedViewName -> onBigQuery("DROP MATERIALIZED VIEW " + materializedViewName));
    }

    @Test
    public void testEmptyProjectionExternalTable()
    {
        testEmptyProjection(
                externalTableName -> onBigQuery("CREATE EXTERNAL TABLE " + externalTableName + " OPTIONS (format = 'CSV', uris = ['gs://" + gcpStorageBucket + "/tpch/tiny/region.csv'])"),
                externalTableName -> onBigQuery("DROP EXTERNAL TABLE " + externalTableName));
    }

    @Test
    public void testEmptyProjectionSnapshotTable()
    {
        // BigQuery has limits on how many snapshots/clones a single table can have and seems to miscount leading to failure when creating too many snapshots from single table
        // For snapshot table test we use a different source table everytime
        String regionCopy = TEST_SCHEMA + ".region_" + randomNameSuffix();
        onBigQuery("CREATE TABLE " + regionCopy + " AS SELECT * FROM tpch.region");
        try {
            testEmptyProjection(
                    snapshotTableName -> onBigQuery("CREATE SNAPSHOT TABLE " + snapshotTableName + " CLONE " + regionCopy),
                    snapshotTableName -> onBigQuery("DROP SNAPSHOT TABLE " + snapshotTableName));
        }
        finally {
            onBigQuery("DROP TABLE " + regionCopy);
        }
    }

    private void testEmptyProjection(Consumer<String> createTable, Consumer<String> dropTable)
    {
        // Regression test for https://github.com/trinodb/trino/issues/14981, https://github.com/trinodb/trino/issues/5635 and https://github.com/trinodb/trino/issues/6696
        String name = TEST_SCHEMA + ".test_empty_projection_" + randomNameSuffix();
        createTable.accept(name);
        try {
            assertQuery("SELECT count(*) FROM " + name, "VALUES 5");
            assertQuery("SELECT count(*) FROM " + name, "VALUES 5"); // repeated query to cover https://github.com/trinodb/trino/issues/6696
            assertQuery("SELECT count(*) FROM " + name + " WHERE regionkey = 1", "VALUES 1");
            assertQuery("SELECT count(name) FROM " + name + " WHERE regionkey = 1", "VALUES 1");
        }
        finally {
            dropTable.accept(name);
        }
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        switch (dataMappingTestSetup.getTrinoTypeName()) {
            case "real":
            case "char(3)":
            case "time":
            case "time(3)":
            case "time(6)":
            case "timestamp":
            case "timestamp(3)":
            case "timestamp(3) with time zone":
                return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterCaseSensitiveDataMappingTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("char(1)")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    public void testNoDataSystemTable()
    {
        // TODO (https://github.com/trinodb/trino/issues/6515): Big Query throws an error when trying to read "some_table$data".
        assertThatThrownBy(super::testNoDataSystemTable)
                .hasMessageFindingMatch("\\Q" +
                        "Expecting message:\n" +
                        "  \"Cannot read partition information from a table that is not partitioned: \\E\\S+\\Q:tpch.nation$data\"\n" +
                        "to match regex:\n" +
                        "  \"line 1:1: Table '\\w+.\\w+.nation\\$data' does not exist\"\n" +
                        "but did not.");
        throw new SkipException("TODO");
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        return nullToEmpty(exception.getMessage()).matches(".*Invalid field name \"%s\". Fields must contain the allowed characters, and be at most 300 characters long..*".formatted(columnName.replace("\\", "\\\\")));
    }

    @Override // Override because the base test exceeds rate limits per a table
    public void testCommentColumn()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_comment_column_", "(a integer)")) {
            // comment set
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS 'new comment'");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName())).contains("COMMENT 'new comment'");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo("new comment");

            // comment set to empty or deleted
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS NULL");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo(null);
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_comment_column_", "(a integer COMMENT 'test comment')")) {
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo("test comment");
            // comment set new value
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS 'updated comment'");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo("updated comment");

            // comment set empty
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS ''");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo("");
        }
    }

    @Test
    public void testPartitionDateColumn()
    {
        try (TestTable table = new TestTable(bigQuerySqlExecutor, "test.partition_date_column", "(value INT64) PARTITION BY _PARTITIONDATE")) {
            // BigQuery doesn't allow omitting column list for ingestion-time partitioned table
            // Using _PARTITIONTIME special column because _PARTITIONDATE is unsupported in INSERT statement
            onBigQuery(format("INSERT INTO %s (_PARTITIONTIME, value) VALUES ('1960-01-01', 1)", table.getName()));
            onBigQuery(format("INSERT INTO %s (_PARTITIONTIME, value) VALUES ('2159-12-31', 2)", table.getName()));

            assertThat(query("SELECT value, \"$partition_date\" FROM " + table.getName()))
                    .matches("VALUES (BIGINT '1', DATE '1960-01-01'), (BIGINT '2', DATE '2159-12-31')");

            assertQuery(format("SELECT value FROM %s WHERE \"$partition_date\" = DATE '1960-01-01'", table.getName()), "VALUES 1");
            assertQuery(format("SELECT value FROM %s WHERE \"$partition_date\" = DATE '2159-12-31'", table.getName()), "VALUES 2");

            // Verify DESCRIBE result doesn't have hidden columns
            assertThat(query("DESCRIBE " + table.getName())).projected("Column").skippingTypesCheck().matches("VALUES 'value'");
        }
    }

    @Test
    public void testPartitionTimeColumn()
    {
        try (TestTable table = new TestTable(bigQuerySqlExecutor, "test.partition_time_column", "(value INT64) PARTITION BY DATE_TRUNC(_PARTITIONTIME, HOUR)")) {
            // BigQuery doesn't allow omitting column list for ingestion-time partitioned table
            onBigQuery(format("INSERT INTO %s (_PARTITIONTIME, value) VALUES ('1960-01-01 00:00:00', 1)", table.getName()));
            onBigQuery(format("INSERT INTO %s (_PARTITIONTIME, value) VALUES ('2159-12-31 23:00:00', 2)", table.getName())); // Hour and minute must be zero

            assertThat(query("SELECT value, \"$partition_time\" FROM " + table.getName()))
                    .matches("VALUES (BIGINT '1', CAST('1960-01-01 00:00:00 UTC' AS TIMESTAMP(6) WITH TIME ZONE)), (BIGINT '2', CAST('2159-12-31 23:00:00 UTC' AS TIMESTAMP(6) WITH TIME ZONE))");

            assertQuery(format("SELECT value FROM %s WHERE \"$partition_time\" = CAST('1960-01-01 00:00:00 UTC' AS TIMESTAMP(6) WITH TIME ZONE)", table.getName()), "VALUES 1");
            assertQuery(format("SELECT value FROM %s WHERE \"$partition_time\" = CAST('2159-12-31 23:00:00 UTC' AS TIMESTAMP(6) WITH TIME ZONE)", table.getName()), "VALUES 2");

            // Verify DESCRIBE result doesn't have hidden columns
            assertThat(query("DESCRIBE " + table.getName())).projected("Column").skippingTypesCheck().matches("VALUES 'value'");
        }
    }

    @Test
    public void testIngestionTimePartitionedTableInvalidValue()
    {
        try (TestTable table = new TestTable(bigQuerySqlExecutor, "test.invalid_ingestion_time", "(value INT64) PARTITION BY _PARTITIONDATE")) {
            assertThatThrownBy(() -> onBigQuery(format("INSERT INTO %s (_PARTITIONTIME, value) VALUES ('0001-01-01', 1)", table.getName())))
                    .hasMessageMatching("Cannot set pseudo column for automatic partitioned table.* Supported values are in the range \\[1960-01-01, 2159-12-31]");

            assertThatThrownBy(() -> onBigQuery(format("INSERT INTO %s (_PARTITIONTIME, value) VALUES ('1959-12-31', 1)", table.getName())))
                    .hasMessageMatching("Cannot set pseudo column for automatic partitioned table.* Supported values are in the range \\[1960-01-01, 2159-12-31]");

            assertThatThrownBy(() -> onBigQuery(format("INSERT INTO %s (_PARTITIONTIME, value) VALUES ('2160-01-01', 1)", table.getName())))
                    .hasMessageMatching("Cannot set pseudo column for automatic partitioned table.* Supported values are in the range \\[1960-01-01, 2159-12-31]");

            assertThatThrownBy(() -> onBigQuery(format("INSERT INTO %s (_PARTITIONTIME, value) VALUES ('9999-12-31', 1)", table.getName())))
                    .hasMessageMatching("Cannot set pseudo column for automatic partitioned table.* Supported values are in the range \\[1960-01-01, 2159-12-31]");

            assertThatThrownBy(() -> onBigQuery(format("INSERT INTO %s (_PARTITIONTIME, value) VALUES (NULL, 1)", table.getName())))
                    .hasMessageContaining("Cannot set timestamp pseudo column for automatic partitioned table to NULL");
        }
    }

    @Test
    public void testPseudoColumnNotExist()
    {
        // Normal table without partitions
        try (TestTable table = new TestTable(bigQuerySqlExecutor, "test.non_partitioned_table", "(value INT64, ts TIMESTAMP)")) {
            assertQueryFails("SELECT \"$partition_date\" FROM " + table.getName(), ".* Column '\\$partition_date' cannot be resolved");
            assertQueryFails("SELECT \"$partition_time\" FROM " + table.getName(), ".* Column '\\$partition_time' cannot be resolved");
        }

        // Time-unit partitioned table
        try (TestTable table = new TestTable(bigQuerySqlExecutor, "test.time_unit_partition", "(value INT64, dt DATE) PARTITION BY dt")) {
            assertQueryFails("SELECT \"$partition_date\" FROM " + table.getName(), ".* Column '\\$partition_date' cannot be resolved");
            assertQueryFails("SELECT \"$partition_time\" FROM " + table.getName(), ".* Column '\\$partition_time' cannot be resolved");
        }

        // Integer-range partitioned table
        try (TestTable table = new TestTable(bigQuerySqlExecutor, "test.integer_range_partition", "(value INT64, dt DATE) PARTITION BY RANGE_BUCKET(value, GENERATE_ARRAY(0, 100, 10))")) {
            assertQueryFails("SELECT \"$partition_date\" FROM " + table.getName(), ".* Column '\\$partition_date' cannot be resolved");
            assertQueryFails("SELECT \"$partition_time\" FROM " + table.getName(), ".* Column '\\$partition_time' cannot be resolved");
        }
    }

    @Test
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

    @Test
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
    public void testSelectTableWithRowAccessPolicyFilterAll()
    {
        String policyName = "test_policy" + randomNameSuffix();
        try (TestTable table = new TestTable(this::onBigQuery, "test.test_row_access_policy", "AS SELECT 1 col")) {
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 1");

            // Use assertEventually because there's delay until new row access policies become effective
            onBigQuery("CREATE ROW ACCESS POLICY " + policyName + " ON " + table.getName() + " FILTER USING (true)");
            assertEventually(new Duration(1, MINUTES), () -> assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName()));

            onBigQuery("DROP ALL ROW ACCESS POLICIES ON " + table.getName());
            assertEventually(new Duration(1, MINUTES), () -> assertQuery("SELECT * FROM " + table.getName(), "VALUES 1"));
        }
    }

    @Test
    public void testSelectTableWithRowAccessPolicyFilterPartialRow()
    {
        String policyName = "test_policy" + randomNameSuffix();
        try (TestTable table = new TestTable(this::onBigQuery, "test.test_row_access_policy", "AS (SELECT 1 col UNION ALL SELECT 2 col)")) {
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1), (2)");

            // Use assertEventually because there's delay until new row access policies become effective
            onBigQuery("CREATE ROW ACCESS POLICY " + policyName + " ON " + table.getName() + " GRANT TO (\"allAuthenticatedUsers\") FILTER USING (col = 1)");
            assertEventually(new Duration(1, MINUTES), () -> assertQuery("SELECT * FROM " + table.getName(), "VALUES 1"));

            onBigQuery("DROP ALL ROW ACCESS POLICIES ON " + table.getName());
            assertEventually(new Duration(1, MINUTES), () -> assertQuery("SELECT * FROM " + table.getName(), "VALUES (1), (2)"));
        }
    }

    @Test
    public void testViewDefinitionSystemTable()
    {
        String schemaName = "test";
        String tableName = "views_system_table_base_" + randomNameSuffix();
        String viewName = "views_system_table_view_" + randomNameSuffix();

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

    @Override
    public void testReadMetadataWithRelationsConcurrentModifications()
    {
        // TODO: Enable this test after fixing "Task did not completed before timeout" (https://github.com/trinodb/trino/issues/14230)
        throw new SkipException("Test fails with a timeout sometimes and is flaky");
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

    @Test
    @Override
    public void testDateYearOfEraPredicate()
    {
        // Override because the connector throws an exception instead of an empty result when the value is out of supported range
        assertQuery("SELECT orderdate FROM orders WHERE orderdate = DATE '1997-09-14'", "VALUES DATE '1997-09-14'");
        assertThatThrownBy(() -> query("SELECT * FROM orders WHERE orderdate = DATE '-1996-09-14'"))
                .hasMessageMatching(".*Row filter for .* is invalid\\. Filter is '\\(`orderdate` = '-1996-09-14'\\)'");
    }

    @Test
    public void testBigQueryMaterializedView()
    {
        String materializedView = "test_materialized_view" + randomNameSuffix();
        try {
            onBigQuery("CREATE MATERIALIZED VIEW test." + materializedView + " AS SELECT count(1) AS cnt FROM tpch.region");
            assertQuery("SELECT table_type FROM information_schema.tables WHERE table_schema = 'test' AND table_name = '" + materializedView + "'", "VALUES 'BASE TABLE'");

            assertQuery("DESCRIBE test." + materializedView, "VALUES ('cnt', 'bigint', '', '')");
            assertQuery("SELECT * FROM test." + materializedView, "VALUES 5");

            assertUpdate("DROP TABLE test." + materializedView);
            assertQueryReturnsEmptyResult("SELECT * FROM information_schema.tables WHERE table_schema = 'test' AND table_name = '" + materializedView + "'");
        }
        finally {
            onBigQuery("DROP MATERIALIZED VIEW IF EXISTS test." + materializedView);
        }
    }

    @Test
    public void testBigQuerySnapshotTable()
    {
        // BigQuery has limits on how many snapshots/clones a single table can have and seems to miscount leading to failure when creating too many snapshots from single table
        // For snapshot table test we use a different source table everytime
        String regionCopy = "region_" + randomNameSuffix();
        String snapshotTable = "test_snapshot" + randomNameSuffix();
        try {
            onBigQuery("CREATE TABLE test." + regionCopy + " AS SELECT * FROM tpch.region");
            onBigQuery("CREATE SNAPSHOT TABLE test." + snapshotTable + " CLONE test." + regionCopy);
            assertQuery("SELECT table_type FROM information_schema.tables WHERE table_schema = 'test' AND table_name = '" + snapshotTable + "'", "VALUES 'BASE TABLE'");

            assertThat(query("DESCRIBE test." + snapshotTable)).matches("DESCRIBE tpch.region");
            assertThat(query("SELECT * FROM test." + snapshotTable)).matches("SELECT * FROM tpch.region");

            assertUpdate("DROP TABLE test." + snapshotTable);
            assertQueryReturnsEmptyResult("SELECT * FROM information_schema.tables WHERE table_schema = 'test' AND table_name = '" + snapshotTable + "'");
        }
        finally {
            onBigQuery("DROP SNAPSHOT TABLE IF EXISTS test." + snapshotTable);
            onBigQuery("DROP TABLE test." + regionCopy);
        }
    }

    @Test
    public void testBigQueryExternalTable()
    {
        String externalTable = "test_external" + randomNameSuffix();
        try {
            onBigQuery("CREATE EXTERNAL TABLE test." + externalTable + " OPTIONS (format = 'CSV', uris = ['gs://" + gcpStorageBucket + "/tpch/tiny/region.csv'])");
            assertQuery("SELECT table_type FROM information_schema.tables WHERE table_schema = 'test' AND table_name = '" + externalTable + "'", "VALUES 'BASE TABLE'");

            assertThat(query("DESCRIBE test." + externalTable)).matches("DESCRIBE tpch.region");
            assertThat(query("SELECT * FROM test." + externalTable)).matches("SELECT * FROM tpch.region");

            assertUpdate("DROP TABLE test." + externalTable);
            assertQueryReturnsEmptyResult("SELECT * FROM information_schema.tables WHERE table_schema = 'test' AND table_name = '" + externalTable + "'");
        }
        finally {
            onBigQuery("DROP EXTERNAL TABLE IF EXISTS test." + externalTable);
        }
    }

    @Test
    public void testQueryLabeling()
    {
        Function<String, Session> sessionWithToken = token -> Session.builder(getSession())
                .setTraceToken(Optional.of(token))
                .build();

        String materializedView = "test_query_label" + randomNameSuffix();
        try {
            onBigQuery("CREATE MATERIALIZED VIEW test." + materializedView + " AS SELECT count(1) AS cnt FROM tpch.region");

            @Language("SQL")
            String query = "SELECT * FROM test." + materializedView;

            MaterializedResultWithQueryId result = getDistributedQueryRunner().executeWithQueryId(sessionWithToken.apply("first_token"), query);
            assertLabelForTable(materializedView, result.getQueryId(), "first_token");

            MaterializedResultWithQueryId result2 = getDistributedQueryRunner().executeWithQueryId(sessionWithToken.apply("second_token"), query);
            assertLabelForTable(materializedView, result2.getQueryId(), "second_token");

            assertThatThrownBy(() -> getDistributedQueryRunner().executeWithQueryId(sessionWithToken.apply("InvalidToken"), query))
                    .hasMessageContaining("BigQuery label value can contain only lowercase letters, numeric characters, underscores, and dashes");
        }
        finally {
            onBigQuery("DROP MATERIALIZED VIEW IF EXISTS test." + materializedView);
        }
    }

    private void assertLabelForTable(String expectedView, QueryId queryId, String traceToken)
    {
        String expectedLabel = "q_" + queryId.toString() + "__t_" + traceToken;

        @Language("SQL")
        String checkForLabelQuery = """
                    SELECT * FROM region-us.INFORMATION_SCHEMA.JOBS_BY_USER WHERE EXISTS(
                        SELECT * FROM UNNEST(labels) AS label WHERE label.key = 'trino_query' AND label.value = '%s'
                    )""".formatted(expectedLabel);

        assertThat(bigQuerySqlExecutor.executeQuery(checkForLabelQuery).getValues())
                .extracting(values -> values.get("query").getStringValue())
                .singleElement()
                .matches(statement -> statement.contains(expectedView));
    }

    @Test
    public void testQueryCache()
    {
        Session queryResultsCacheSession = Session.builder(getSession())
                .setCatalogSessionProperty("bigquery", "query_results_cache_enabled", "true")
                .build();
        Session createNeverDisposition = Session.builder(getSession())
                .setCatalogSessionProperty("bigquery", "query_results_cache_enabled", "true")
                .setCatalogSessionProperty("bigquery", "create_disposition_type", "create_never")
                .build();

        String materializedView = "test_materialized_view" + randomNameSuffix();
        try {
            onBigQuery("CREATE MATERIALIZED VIEW test." + materializedView + " AS SELECT count(1) AS cnt FROM tpch.region");

            // Verify query cache is empty
            assertThatThrownBy(() -> query(createNeverDisposition, "SELECT * FROM test." + materializedView))
                    .hasMessageContaining("Not found");
            // Populate cache and verify it
            assertQuery(queryResultsCacheSession, "SELECT * FROM test." + materializedView, "VALUES 5");
            assertQuery(createNeverDisposition, "SELECT * FROM test." + materializedView, "VALUES 5");

            assertUpdate("DROP TABLE test." + materializedView);
        }
        finally {
            onBigQuery("DROP MATERIALIZED VIEW IF EXISTS test." + materializedView);
        }
    }

    @Test
    public void testWildcardTable()
    {
        String suffix = randomNameSuffix();
        String firstTable = format("test_wildcard_%s_1", suffix);
        String secondTable = format("test_wildcard_%s_2", suffix);
        String wildcardTable = format("test_wildcard_%s_*", suffix);
        try {
            onBigQuery("CREATE TABLE test." + firstTable + " AS SELECT 1 AS value");
            onBigQuery("CREATE TABLE test." + secondTable + " AS SELECT 2 AS value");

            assertQuery("DESCRIBE test.\"" + wildcardTable + "\"", "VALUES ('value', 'bigint', '', '')");
            assertQuery("SELECT * FROM test.\"" + wildcardTable + "\"", "VALUES (1), (2)");

            // Unsupported operations
            assertQueryFails("DROP TABLE test.\"" + wildcardTable + "\"", "This connector does not support dropping wildcard tables");
            assertQueryFails("INSERT INTO test.\"" + wildcardTable + "\" VALUES (1)", "This connector does not support inserting into wildcard tables");
            assertQueryFails("ALTER TABLE test.\"" + wildcardTable + "\" ADD COLUMN new_column INT", "This connector does not support adding columns");
            assertQueryFails("ALTER TABLE test.\"" + wildcardTable + "\" RENAME TO test.new_wildcard_table", "This connector does not support renaming tables");
        }
        finally {
            onBigQuery("DROP TABLE IF EXISTS test." + firstTable);
            onBigQuery("DROP TABLE IF EXISTS test." + secondTable);
        }
    }

    @Test
    public void testWildcardTableWithDifferentColumnDefinition()
    {
        String suffix = randomNameSuffix();
        String firstTable = format("test_invalid_wildcard_%s_1", suffix);
        String secondTable = format("test_invalid_wildcard_%s_2", suffix);
        String wildcardTable = format("test_invalid_wildcard_%s_*", suffix);
        try {
            onBigQuery("CREATE TABLE test." + firstTable + " AS SELECT 1 AS value");
            onBigQuery("CREATE TABLE test." + secondTable + " AS SELECT 'string' AS value");

            assertQuery("DESCRIBE test.\"" + wildcardTable + "\"", "VALUES ('value', 'varchar', '', '')");

            assertThatThrownBy(() -> query("SELECT * FROM test.\"" + wildcardTable + "\""))
                    .hasMessageContaining("Cannot read field of type INT64 as STRING Field: value");
        }
        finally {
            onBigQuery("DROP TABLE IF EXISTS test." + firstTable);
            onBigQuery("DROP TABLE IF EXISTS test." + secondTable);
        }
    }

    @Test
    public void testMissingWildcardTable()
    {
        assertThatThrownBy(() -> query("SELECT * FROM test.\"test_missing_wildcard_table_*\""))
                .hasMessageEndingWith("does not match any table.");
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(1024);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Invalid dataset ID");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(1024);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Invalid table ID");
    }

    // test polymorphic table function

    @Test
    public void testNativeQuerySimple()
    {
        assertQuery(
                "SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT 1'))",
                "VALUES 1");
    }

    @Test
    public void testNativeQuerySelectForCaseSensitiveColumnNames()
    {
        assertThat(computeActual("SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT 1 AS lower, 2 AS UPPER, 3 AS miXED'))").getColumnNames())
                .containsExactly("lower", "UPPER", "miXED");

        assertThat(computeActual("SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT 1 AS duplicated, 2 AS duplicated'))").getColumnNames())
                .containsExactly("duplicated", "duplicated_1");

        String tableName = "test.test_non_lowercase" + randomNameSuffix();
        onBigQuery("CREATE TABLE " + tableName + " AS SELECT 1 AS lower, 2 AS UPPER, 3 AS miXED");
        try {
            assertQuery(
                    "SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT * FROM " + tableName + "'))",
                    "VALUES (1, 2, 3)");
            assertQuery(
                    "SELECT \"lower\", \"UPPER\", \"miXED\" FROM TABLE(bigquery.system.query(query => 'SELECT * FROM " + tableName + "'))",
                    "VALUES (1, 2, 3)");
            assertQuery(
                    "SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT * FROM " + tableName + "')) WHERE \"UPPER\" = 2",
                    "VALUES (1, 2, 3)");
            assertQueryReturnsEmptyResult("SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT * FROM " + tableName + "')) WHERE \"UPPER\" = 100");
            assertQueryReturnsEmptyResult("SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT * FROM " + tableName + "')) WHERE upper = 100");
        }
        finally {
            onBigQuery("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testNativeQuerySelectFromNation()
    {
        assertQuery(
                "SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT name FROM tpch.nation WHERE nationkey = 0'))",
                "VALUES 'ALGERIA'");
        assertQuery(
                "SELECT name FROM TABLE(bigquery.system.query(query => 'SELECT * FROM tpch.nation')) WHERE nationkey = 0",
                "VALUES 'ALGERIA'");
    }

    @Test
    public void testNativeQueryColumnAlias()
    {
        assertThat(query("SELECT region_name FROM TABLE(system.query(query => 'SELECT name AS region_name FROM tpch.region WHERE regionkey = 0'))"))
                .matches("VALUES CAST('AFRICA' AS VARCHAR)");
    }

    @Test
    public void testNativeQueryColumnAliasNotFound()
    {
        assertQueryFails(
                "SELECT name FROM TABLE(system.query(query => 'SELECT name AS region_name FROM tpch.region'))",
                ".* Column 'name' cannot be resolved");
        assertQueryFails(
                "SELECT column_not_found FROM TABLE(system.query(query => 'SELECT name AS region_name FROM tpch.region'))",
                ".* Column 'column_not_found' cannot be resolved");
    }

    @Test
    public void testNativeQuerySelectFromTestTable()
    {
        String tableName = "test.test_select" + randomNameSuffix();
        try {
            onBigQuery("CREATE TABLE " + tableName + "(col BIGINT)");
            onBigQuery("INSERT INTO " + tableName + " VALUES (1), (2)");
            assertQuery(
                    "SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT * FROM " + tableName + "'))",
                    "VALUES 1, 2");
        }
        finally {
            onBigQuery("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testNativeQuerySelectUnsupportedType()
    {
        String tableName = "test_unsupported" + randomNameSuffix();
        try {
            onBigQuery("CREATE TABLE test." + tableName + "(one BIGINT, two BIGNUMERIC(40,2), three STRING)");
            // Check that column 'two' is not supported.
            assertQuery("SELECT column_name FROM information_schema.columns WHERE table_schema = 'test' AND table_name = '" + tableName + "'", "VALUES 'one', 'three'");
            assertThatThrownBy(() -> query("SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT * FROM test." + tableName + "'))"))
                    .hasMessageContaining("Unsupported type");
        }
        finally {
            onBigQuery("DROP TABLE IF EXISTS test." + tableName);
        }
    }

    @Test
    public void testNativeQueryCreateStatement()
    {
        String tableName = "test_create" + randomNameSuffix();
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertThatThrownBy(() -> query("SELECT * FROM TABLE(bigquery.system.query(query => 'CREATE TABLE test." + tableName + "(n INTEGER)'))"))
                .hasMessage("Unsupported statement type: CREATE_TABLE");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testNativeQueryInsertStatementTableDoesNotExist()
    {
        String tableName = "test_insert" + randomNameSuffix();
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertThatThrownBy(() -> query("SELECT * FROM TABLE(bigquery.system.query(query => 'INSERT INTO test." + tableName + " VALUES (1)'))"))
                .hasMessageContaining("Failed to get schema for query")
                .hasStackTraceContaining("%s was not found", tableName);
    }

    @Test
    public void testNativeQueryInsertStatementTableExists()
    {
        String tableName = "test_insert" + randomNameSuffix();
        try {
            onBigQuery("CREATE TABLE test." + tableName + "(col BIGINT)");
            assertThatThrownBy(() -> query("SELECT * FROM TABLE(bigquery.system.query(query => 'INSERT INTO test." + tableName + " VALUES (3)'))"))
                    .hasMessage("Unsupported statement type: INSERT");
        }
        finally {
            onBigQuery("DROP TABLE IF EXISTS test." + tableName);
        }
    }

    @Test
    public void testNativeQueryIncorrectSyntax()
    {
        assertThatThrownBy(() -> query("SELECT * FROM TABLE(system.query(query => 'some wrong syntax'))"))
                .hasMessageContaining("Failed to get schema for query");
    }

    @Override
    public void testInsertArray()
    {
        // Override because the connector disallows writing a NULL in ARRAY
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_array_", "(a ARRAY<DOUBLE>, b ARRAY<BIGINT>)")) {
            assertUpdate("INSERT INTO " + table.getName() + " (a, b) VALUES (ARRAY[1.23E1], ARRAY[1.23E1])", 1);
            assertQuery("SELECT a[1], b[1] FROM " + table.getName(), "VALUES (12.3, 12)");
        }
    }

    @Override
    public void testInsertRowConcurrently()
    {
        // TODO https://github.com/trinodb/trino/issues/15158 Enable this test after switching to storage write API
        throw new SkipException("Test fails with a timeout sometimes and is flaky");
    }

    @Override
    protected String errorMessageForCreateTableAsSelectNegativeDate(String date)
    {
        return format(".*Invalid date: '%s'.*", date);
    }

    @Override
    protected String errorMessageForInsertNegativeDate(String date)
    {
        return format(".*Invalid date: '%s'.*", date);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                this::onBigQuery,
                "test.test_table",
                "(col_required INT64 NOT NULL," +
                        "col_nullable INT64," +
                        "col_default INT64 DEFAULT 43," +
                        "col_nonnull_default INT64 DEFAULT 42 NOT NULL," +
                        "col_required2 INT64 NOT NULL)");
    }

    @Override
    public void testCharVarcharComparison()
    {
        assertThatThrownBy(super::testCharVarcharComparison)
                .hasMessage("Unsupported column type: char(3)");
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(300);
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e)
                .hasMessageContaining("Fields must contain only letters, numbers, and underscores, start with a letter or underscore, and be at most 300 characters long.");
    }

    private void onBigQuery(@Language("SQL") String sql)
    {
        bigQuerySqlExecutor.execute(sql);
    }
}
