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
package io.trino.plugin.deltalake;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.ColumnMappingMode;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.sql.planner.plan.TableDeleteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.DataProviders;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedResultWithQueryId;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.containers.Minio;
import io.trino.testing.minio.MinioClient;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.union;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.deltalake.DeltaLakeCdfPageSink.CHANGE_DATA_FOLDER_NAME;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.CHANGE_DATA_FEED_COLUMN_NAMES;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.TRANSACTION_LOG_DIRECTORY;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.DataProviders.trueFalse;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.EXECUTE_FUNCTION;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_SCHEMA;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDeltaLakeConnectorTest
        extends BaseConnectorTest
{
    protected static final String SCHEMA = "test_schema";

    protected final String bucketName = "test-bucket-" + randomNameSuffix();
    protected MinioClient minioClient;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Minio minio = closeAfterClass(Minio.builder().build());
        minio.start();
        minio.createBucket(bucketName);
        minioClient = closeAfterClass(minio.createMinioClient());

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(DELTA_CATALOG)
                        .setSchema(SCHEMA)
                        .build())
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(new DeltaLakePlugin());
            queryRunner.createCatalog(DELTA_CATALOG, DeltaLakeConnectorFactory.CONNECTOR_NAME, ImmutableMap.<String, String>builder()
                    .put("hive.metastore", "file")
                    .put("hive.metastore.catalog.dir", queryRunner.getCoordinator().getBaseDataDir().resolve("file-metastore").toString())
                    .put("hive.metastore.disable-location-checks", "true")
                    .put("hive.s3.aws-access-key", MINIO_ACCESS_KEY)
                    .put("hive.s3.aws-secret-key", MINIO_SECRET_KEY)
                    .put("hive.s3.endpoint", minio.getMinioAddress())
                    .put("hive.s3.path-style-access", "true")
                    .put("delta.enable-non-concurrent-writes", "true")
                    .put("delta.register-table-procedure.enabled", "true")
                    .buildOrThrow());

            queryRunner.execute("CREATE SCHEMA " + SCHEMA + " WITH (location = 's3://" + bucketName + "/" + SCHEMA + "')");
            queryRunner.execute("CREATE SCHEMA schemawithoutunderscore WITH (location = 's3://" + bucketName + "/schemawithoutunderscore')");
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, queryRunner.getDefaultSession(), REQUIRED_TPCH_TABLES);
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }

        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        minioClient = null; // closed by closeAfterClass
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_REPORTING_WRITTEN_BYTES -> true;
            case SUPPORTS_ADD_FIELD,
                    SUPPORTS_AGGREGATION_PUSHDOWN,
                    SUPPORTS_CREATE_MATERIALIZED_VIEW,
                    SUPPORTS_DROP_FIELD,
                    SUPPORTS_LIMIT_PUSHDOWN,
                    SUPPORTS_PREDICATE_PUSHDOWN,
                    SUPPORTS_RENAME_FIELD,
                    SUPPORTS_RENAME_SCHEMA,
                    SUPPORTS_SET_COLUMN_TYPE,
                    SUPPORTS_TOPN_PUSHDOWN -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return "NULL value not allowed for NOT NULL column: " + columnName;
    }

    @Override
    protected void verifyConcurrentUpdateFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessage("Failed to write Delta Lake transaction log entry")
                .cause()
                .hasMessageMatching(transactionConflictErrors());
    }

    @Override
    protected void verifyConcurrentInsertFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessage("Failed to write Delta Lake transaction log entry")
                .cause()
                .hasMessageMatching(transactionConflictErrors());
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessageMatching("Unable to add '.*' column for: .*")
                .cause()
                .hasMessageMatching(transactionConflictErrors());
    }

    @Language("RegExp")
    private static String transactionConflictErrors()
    {
        return "Transaction log locked.*" +
                "|Target file already exists: .*/_delta_log/\\d+.json" +
                "|Conflicting concurrent writes found\\..*" +
                "|Multiple live locks found for:.*" +
                "|Target file was created during locking: .*";
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
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time") ||
                typeName.equals("time(6)") ||
                typeName.equals("timestamp") ||
                typeName.equals("timestamp(6)") ||
                typeName.equals("timestamp(6) with time zone") ||
                typeName.equals("char(3)")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Delta Lake does not support columns with a default value");
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
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

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE orders"))
                .matches("\\QCREATE TABLE " + DELTA_CATALOG + "." + SCHEMA + ".orders (\n" +
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
                        "   location = \\E'.*/test_schema/orders.*'\n\\Q" +
                        ")");
    }

    // not pushdownable means not convertible to a tuple domain
    @Test
    public void testQueryNullPartitionWithNotPushdownablePredicate()
    {
        String tableName = "test_null_partitions_" + randomNameSuffix();
        assertUpdate("" +
                        "CREATE TABLE " + tableName + " (a, b, c) WITH (location = '" + format("s3://%s/%s", bucketName, tableName) + "', partitioned_by = ARRAY['c']) " +
                        "AS VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), (null, null, null), (4, 4, 4)",
                "VALUES 5");
        assertQuery("SELECT a FROM " + tableName + " WHERE c % 5 = 1", "VALUES (1)");
    }

    @Test
    public void testPartitionColumnOrderIsDifferentFromTableDefinition()
    {
        String tableName = "test_partition_order_is_different_from_table_definition_" + randomNameSuffix();
        assertUpdate("" +
                "CREATE TABLE " + tableName + "(data int, first varchar, second varchar) " +
                "WITH (" +
                "partitioned_by = ARRAY['second', 'first'], " +
                "location = '" + format("s3://%s/%s", bucketName, tableName) + "')");

        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'first#1', 'second#1')", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'first#1', 'second#1')");

        assertUpdate("INSERT INTO " + tableName + " (data, first) VALUES (2, 'first#2')", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'first#1', 'second#1'), (2, 'first#2', NULL)");

        assertUpdate("INSERT INTO " + tableName + " (data, second) VALUES (3, 'second#3')", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'first#1', 'second#1'), (2, 'first#2', NULL), (3, NULL, 'second#3')");

        assertUpdate("INSERT INTO " + tableName + " (data) VALUES (4)", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'first#1', 'second#1'), (2, 'first#2', NULL), (3, NULL, 'second#3'), (4, NULL, NULL)");
    }

    @Test
    public void testCreateTableWithAllPartitionColumns()
    {
        String tableName = "test_create_table_all_partition_columns_" + randomNameSuffix();
        assertQueryFails(
                "CREATE TABLE " + tableName + "(part INT) WITH (partitioned_by = ARRAY['part'])",
                "Using all columns for partition columns is unsupported");
    }

    @Test
    public void testCreateTableAsSelectAllPartitionColumns()
    {
        String tableName = "test_create_table_all_partition_columns_" + randomNameSuffix();
        assertQueryFails(
                "CREATE TABLE " + tableName + " WITH (partitioned_by = ARRAY['part']) AS SELECT 1 part",
                "Using all columns for partition columns is unsupported");
    }

    @Test
    public void testCreateTableWithUnsupportedPartitionType()
    {
        String tableName = "test_create_table_unsupported_partition_types_" + randomNameSuffix();
        assertQueryFails(
                "CREATE TABLE " + tableName + "(a INT, part ARRAY(INT)) WITH (partitioned_by = ARRAY['part'])",
                "Using array, map or row type on partitioned columns is unsupported");
        assertQueryFails(
                "CREATE TABLE " + tableName + "(a INT, part MAP(INT,INT)) WITH (partitioned_by = ARRAY['part'])",
                "Using array, map or row type on partitioned columns is unsupported");
        assertQueryFails(
                "CREATE TABLE " + tableName + "(a INT, part ROW(field INT)) WITH (partitioned_by = ARRAY['part'])",
                "Using array, map or row type on partitioned columns is unsupported");
    }

    @Test
    public void testCreateTableAsSelectWithUnsupportedPartitionType()
    {
        String tableName = "test_ctas_unsupported_partition_types_" + randomNameSuffix();
        assertQueryFails(
                "CREATE TABLE " + tableName + " WITH (partitioned_by = ARRAY['part']) AS SELECT 1 a, array[1] part",
                "Using array, map or row type on partitioned columns is unsupported");
        assertQueryFails(
                "CREATE TABLE " + tableName + " WITH (partitioned_by = ARRAY['part']) AS SELECT 1 a, map() part",
                "Using array, map or row type on partitioned columns is unsupported");
        assertQueryFails(
                "CREATE TABLE " + tableName + " WITH (partitioned_by = ARRAY['part']) AS SELECT 1 a, row(1) part",
                "Using array, map or row type on partitioned columns is unsupported");
    }

    @Override
    public void testShowCreateSchema()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE SCHEMA " + schemaName))
                .isEqualTo(format("CREATE SCHEMA %s.%s\n" +
                        "WITH (\n" +
                        "   location = 's3://%s/test_schema'\n" +
                        ")", getSession().getCatalog().orElseThrow(), schemaName, bucketName));
    }

    @Override
    public void testDropNonEmptySchemaWithTable()
    {
        String schemaName = "test_drop_non_empty_schema_" + randomNameSuffix();
        if (!hasBehavior(SUPPORTS_CREATE_SCHEMA)) {
            return;
        }

        assertUpdate("CREATE SCHEMA " + schemaName + " WITH (location = 's3://" + bucketName + "/" + schemaName + "')");
        assertUpdate("CREATE TABLE " + schemaName + ".t(x int)");
        assertQueryFails("DROP SCHEMA " + schemaName, ".*Cannot drop non-empty schema '\\Q" + schemaName + "\\E'");
        assertUpdate("DROP TABLE " + schemaName + ".t");
        assertUpdate("DROP SCHEMA " + schemaName);
    }

    @Override
    public void testDropColumn()
    {
        // Override because the connector doesn't support dropping columns with 'none' column mapping
        // There are some tests in in io.trino.tests.product.deltalake.TestDeltaLakeColumnMappingMode
        assertThatThrownBy(super::testDropColumn)
                .hasMessageContaining("Cannot drop column from table using column mapping mode NONE");
    }

    @Override
    public void testAddAndDropColumnName(String columnName)
    {
        // Override because the connector doesn't support dropping columns with 'none' column mapping
        // There are some tests in in io.trino.tests.product.deltalake.TestDeltaLakeColumnMappingMode
        assertThatThrownBy(() -> super.testAddAndDropColumnName(columnName))
                .hasMessageContaining("Cannot drop column from table using column mapping mode NONE");
    }

    @Override
    public void testDropAndAddColumnWithSameName()
    {
        // Override because the connector doesn't support dropping columns with 'none' column mapping
        // There are some tests in in io.trino.tests.product.deltalake.TestDeltaLakeColumnMappingMode
        assertThatThrownBy(super::testDropAndAddColumnWithSameName)
                .hasMessageContaining("Cannot drop column from table using column mapping mode NONE");
    }

    @Test
    public void testDropLastNonPartitionColumn()
    {
        String tableName = "test_drop_last_non_partition_column_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(data int, part int) WITH (partitioned_by = ARRAY['part'], column_mapping_mode = 'name')");

        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN data", "Dropping the last non-partition column is unsupported");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    public void testRenameColumn()
    {
        // Override because the connector doesn't support renaming columns with 'none' column mapping
        // There are some tests in in io.trino.tests.product.deltalake.TestDeltaLakeColumnMappingMode
        assertThatThrownBy(super::testRenameColumn)
                .hasMessageContaining("Cannot rename column in table using column mapping mode NONE");
    }

    @Override
    public void testRenameColumnWithComment()
    {
        // Override because the connector doesn't support renaming columns with 'none' column mapping
        // There are some tests in in io.trino.tests.product.deltalake.TestDeltaLakeColumnMappingMode
        assertThatThrownBy(super::testRenameColumnWithComment)
                .hasMessageContaining("Cannot rename column in table using column mapping mode NONE");
    }

    @Test(dataProvider = "columnMappingModeDataProvider")
    public void testDeltaRenameColumnWithComment(ColumnMappingMode mode)
    {
        if (mode == ColumnMappingMode.NONE) {
            throw new SkipException("The connector doesn't support renaming columns with 'none' column mapping");
        }

        String tableName = "test_rename_column_" + randomNameSuffix();
        assertUpdate("" +
                "CREATE TABLE " + tableName +
                "(col INT COMMENT 'test column comment', part INT COMMENT 'test partition comment')" +
                "WITH (" +
                "partitioned_by = ARRAY['part']," +
                "location = 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'," +
                "column_mapping_mode = '" + mode + "')");

        assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN col TO new_col");
        assertEquals(getColumnComment(tableName, "new_col"), "test column comment");

        assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN part TO new_part");
        assertEquals(getColumnComment(tableName, "new_part"), "test partition comment");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    public void testAlterTableRenameColumnToLongName()
    {
        // Override because the connector doesn't support renaming columns with 'none' column mapping
        // There are some tests in in io.trino.tests.product.deltalake.TestDeltaLakeColumnMappingMode
        assertThatThrownBy(super::testAlterTableRenameColumnToLongName)
                .hasMessageContaining("Cannot rename column in table using column mapping mode NONE");
    }

    @Override
    public void testRenameColumnName(String columnName)
    {
        // Override because the connector doesn't support renaming columns with 'none' column mapping
        // There are some tests in in io.trino.tests.product.deltalake.TestDeltaLakeColumnMappingMode
        assertThatThrownBy(() -> super.testRenameColumnName(columnName))
                .hasMessageContaining("Cannot rename column in table using column mapping mode NONE");
    }

    @Override
    public void testCharVarcharComparison()
    {
        // Delta Lake doesn't have a char type
        assertThatThrownBy(super::testCharVarcharComparison)
                .hasStackTraceContaining("Unsupported type: char(3)");
    }

    @Test(dataProvider = "timestampValues")
    public void testTimestampPredicatePushdown(String value)
    {
        String tableName = "test_parquet_timestamp_predicate_pushdown_" + randomNameSuffix();

        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " (t TIMESTAMP WITH TIME ZONE)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (TIMESTAMP '" + value + "')", 1);

        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        MaterializedResultWithQueryId queryResult = queryRunner.executeWithQueryId(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE t < TIMESTAMP '" + value + "'");
        assertEquals(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputDataSize().toBytes(), 0);

        queryResult = queryRunner.executeWithQueryId(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE t > TIMESTAMP '" + value + "'");
        assertEquals(getQueryInfo(queryRunner, queryResult).getQueryStats().getProcessedInputDataSize().toBytes(), 0);

        assertQueryStats(
                getSession(),
                "SELECT * FROM " + tableName + " WHERE t = TIMESTAMP '" + value + "'",
                queryStats -> assertThat(queryStats.getProcessedInputDataSize().toBytes()).isGreaterThan(0),
                results -> {});
    }

    @Test
    public void testTimestampWithTimeZonePartition()
    {
        String tableName = "test_timestamp_tz_partition_" + randomNameSuffix();

        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate("CREATE TABLE " + tableName + "(id INT, part TIMESTAMP WITH TIME ZONE) WITH (partitioned_by = ARRAY['part'])");
        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(1, NULL)," +
                        "(2, TIMESTAMP '0001-01-01 00:00:00.000 UTC')," +
                        "(3, TIMESTAMP '2023-07-20 01:02:03.9999 -01:00')," +
                        "(4, TIMESTAMP '9999-12-31 23:59:59.999 UTC')",
                4);

        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES " +
                        "(1, NULL)," +
                        "(2, TIMESTAMP '0001-01-01 00:00:00.000 UTC')," +
                        "(3, TIMESTAMP '2023-07-20 02:02:04.000 UTC')," +
                        "(4, TIMESTAMP '9999-12-31 23:59:59.999 UTC')");
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('id', null, 4.0, 0.0, null, 1, 4)," +
                        "('part', null, 3.0, 0.25, null, null, null)," +
                        "(null, null, null, null, 4.0, null, null)");

        assertThat((String) computeScalar("SELECT \"$path\" FROM " + tableName + " WHERE id = 1"))
                .contains("/part=__HIVE_DEFAULT_PARTITION__/");
        assertThat((String) computeScalar("SELECT \"$path\" FROM " + tableName + " WHERE id = 2"))
                .contains("/part=0001-01-01 00%3A00%3A00/");
        assertThat((String) computeScalar("SELECT \"$path\" FROM " + tableName + " WHERE id = 3"))
                .contains("/part=2023-07-20 02%3A02%3A04/");
        assertThat((String) computeScalar("SELECT \"$path\" FROM " + tableName + " WHERE id = 4"))
                .contains("/part=9999-12-31 23%3A59%3A59.999/");

        assertUpdate("DROP TABLE " + tableName);
    }

    @DataProvider
    public Object[][] timestampValues()
    {
        return new Object[][] {
                {"1965-10-31 01:00:08.123 UTC"},
                {"1965-10-31 01:00:08.999 UTC"},
                {"1970-01-01 01:13:42.000 America/Bahia_Banderas"}, // There is a gap in JVM zone
                {"1970-01-01 00:00:00.000 Asia/Kathmandu"},
                {"2018-10-28 01:33:17.456 Europe/Vilnius"},
                {"9999-12-31 23:59:59.999 UTC"}};
    }

    @Test
    public void testAddColumnToPartitionedTable()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_column_partitioned_table_", "(x VARCHAR, part VARCHAR) WITH (partitioned_by = ARRAY['part'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " SELECT 'first', 'part-0001'", 1);
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN x bigint", ".* Column 'x' already exists");
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN part bigint", ".* Column 'part' already exists");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN a varchar(50)");
            assertUpdate("INSERT INTO " + table.getName() + " SELECT 'second', 'part-0002', 'xxx'", 1);
            assertQuery(
                    "SELECT x, part, a FROM " + table.getName(),
                    "VALUES ('first', 'part-0001', NULL), ('second', 'part-0002', 'xxx')");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN b double");
            assertUpdate("INSERT INTO " + table.getName() + " SELECT 'third', 'part-0003', 'yyy', 33.3E0", 1);
            assertQuery(
                    "SELECT x, part, a, b FROM " + table.getName(),
                    "VALUES ('first', 'part-0001', NULL, NULL), ('second', 'part-0002', 'xxx', NULL), ('third', 'part-0003', 'yyy', 33.3)");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN IF NOT EXISTS c varchar(50)");
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN IF NOT EXISTS part varchar(50)");
            assertUpdate("INSERT INTO " + table.getName() + " SELECT 'fourth', 'part-0004', 'zzz', 55.3E0, 'newColumn'", 1);
            assertQuery(
                    "SELECT x, part, a, b, c FROM " + table.getName(),
                    "VALUES ('first', 'part-0001', NULL, NULL, NULL), ('second', 'part-0002', 'xxx', NULL, NULL), ('third', 'part-0003', 'yyy', 33.3, NULL), ('fourth', 'part-0004', 'zzz', 55.3, 'newColumn')");
        }
    }

    private QueryInfo getQueryInfo(DistributedQueryRunner queryRunner, MaterializedResultWithQueryId queryResult)
    {
        return queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(queryResult.getQueryId());
    }

    @Test
    public void testAddColumnAndOptimize()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_column_and_optimize", "(x VARCHAR)")) {
            assertUpdate("INSERT INTO " + table.getName() + " SELECT 'first'", 1);

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN a varchar(50)");
            assertUpdate("INSERT INTO " + table.getName() + " SELECT 'second', 'xxx'", 1);
            assertQuery(
                    "SELECT x, a FROM " + table.getName(),
                    "VALUES ('first', NULL), ('second', 'xxx')");

            Set<String> beforeActiveFiles = getActiveFiles(table.getName());
            computeActual("ALTER TABLE " + table.getName() + " EXECUTE OPTIMIZE");

            // Verify OPTIMIZE happened, but table data didn't change
            assertThat(beforeActiveFiles).isNotEqualTo(getActiveFiles(table.getName()));
            assertQuery(
                    "SELECT x, a FROM " + table.getName(),
                    "VALUES ('first', NULL), ('second', 'xxx')");
        }
    }

    @Test
    public void testAddColumnAndVacuum()
            throws Exception
    {
        Session sessionWithShortRetentionUnlocked = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "vacuum_min_retention", "0s")
                .build();

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_column_and_optimize", "(x VARCHAR)")) {
            assertUpdate("INSERT INTO " + table.getName() + " SELECT 'first'", 1);
            assertUpdate("INSERT INTO " + table.getName() + " SELECT 'second'", 1);

            Set<String> initialFiles = getActiveFiles(table.getName());
            assertThat(initialFiles).hasSize(2);

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN a varchar(50)");

            assertUpdate("UPDATE " + table.getName() + " SET a = 'new column'", 2);
            Stopwatch timeSinceUpdate = Stopwatch.createStarted();
            Set<String> updatedFiles = getActiveFiles(table.getName());
            assertThat(updatedFiles)
                    .hasSizeGreaterThanOrEqualTo(1)
                    .hasSizeLessThanOrEqualTo(2)
                    .doesNotContainAnyElementsOf(initialFiles);
            assertThat(getAllDataFilesFromTableDirectory(table.getName())).isEqualTo(union(initialFiles, updatedFiles));

            assertQuery(
                    "SELECT x, a FROM " + table.getName(),
                    "VALUES ('first', 'new column'), ('second', 'new column')");

            MILLISECONDS.sleep(1_000 - timeSinceUpdate.elapsed(MILLISECONDS) + 1);
            assertUpdate(sessionWithShortRetentionUnlocked, "CALL system.vacuum(schema_name => CURRENT_SCHEMA, table_name => '" + table.getName() + "', retention => '1s')");

            // Verify VACUUM happened, but table data didn't change
            assertThat(getAllDataFilesFromTableDirectory(table.getName())).isEqualTo(updatedFiles);
            assertQuery(
                    "SELECT x, a FROM " + table.getName(),
                    "VALUES ('first', 'new column'), ('second', 'new column')");
        }
    }

    @Test
    public void testTargetMaxFileSize()
    {
        String tableName = "test_default_max_file_size" + randomNameSuffix();
        @Language("SQL") String createTableSql = format("CREATE TABLE %s AS SELECT * FROM tpch.sf1.lineitem LIMIT 100000", tableName);

        Session session = Session.builder(getSession())
                .setSystemProperty("task_writer_count", "1")
                // task scale writers should be disabled since we want to write with a single task writer
                .setSystemProperty("task_scale_writers_enabled", "false")
                .build();
        assertUpdate(session, createTableSql, 100000);
        Set<String> initialFiles = getActiveFiles(tableName);
        assertThat(initialFiles.size()).isLessThanOrEqualTo(3);
        assertUpdate(format("DROP TABLE %s", tableName));

        DataSize maxSize = DataSize.of(40, DataSize.Unit.KILOBYTE);
        session = Session.builder(getSession())
                .setSystemProperty("task_writer_count", "1")
                // task scale writers should be disabled since we want to write with a single task writer
                .setSystemProperty("task_scale_writers_enabled", "false")
                .setCatalogSessionProperty("delta", "target_max_file_size", maxSize.toString())
                .build();

        assertUpdate(session, createTableSql, 100000);
        assertThat(query(format("SELECT count(*) FROM %s", tableName))).matches("VALUES BIGINT '100000'");
        Set<String> updatedFiles = getActiveFiles(tableName);
        assertThat(updatedFiles.size()).isGreaterThan(10);

        MaterializedResult result = computeActual("SELECT DISTINCT \"$path\", \"$file_size\" FROM " + tableName);
        for (MaterializedRow row : result) {
            // allow up to a larger delta due to the very small max size and the relatively large writer chunk size
            assertThat((Long) row.getField(1)).isLessThan(maxSize.toBytes() * 5);
        }
    }

    @Test
    public void testPathColumn()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_path_column", "(x VARCHAR)")) {
            assertUpdate("INSERT INTO " + table.getName() + " SELECT 'first'", 1);
            String firstFilePath = (String) computeScalar("SELECT \"$path\" FROM " + table.getName());
            assertUpdate("INSERT INTO " + table.getName() + " SELECT 'second'", 1);
            String secondFilePath = (String) computeScalar("SELECT \"$path\" FROM " + table.getName() + " WHERE x = 'second'");

            // Verify predicate correctness on $path column
            assertQuery("SELECT x FROM " + table.getName() + " WHERE \"$path\" = '" + firstFilePath + "'", "VALUES 'first'");
            assertQuery("SELECT x FROM " + table.getName() + " WHERE \"$path\" <> '" + firstFilePath + "'", "VALUES 'second'");
            assertQuery("SELECT x FROM " + table.getName() + " WHERE \"$path\" IN ('" + firstFilePath + "', '" + secondFilePath + "')", "VALUES ('first'), ('second')");
            assertQuery("SELECT x FROM " + table.getName() + " WHERE \"$path\" IS NOT NULL", "VALUES ('first'), ('second')");
            assertQueryReturnsEmptyResult("SELECT x FROM " + table.getName() + " WHERE \"$path\" IS NULL");
        }
    }

    @Test
    public void testTableLocationTrailingSpace()
    {
        String tableName = "table_with_space_" + randomNameSuffix();
        String tableLocationWithTrailingSpace = "s3://" + bucketName + "/" + tableName + " ";

        assertUpdate(format("CREATE TABLE %s (customer VARCHAR) WITH (location = '%s')", tableName, tableLocationWithTrailingSpace));
        assertUpdate("INSERT INTO " + tableName + " (customer) VALUES ('Aaron'), ('Bill')", 2);
        assertQuery("SELECT * FROM " + tableName, "VALUES ('Aaron'), ('Bill')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testTableLocationTrailingSlash()
    {
        String tableWithSlash = "table_with_slash";
        String tableWithoutSlash = "table_without_slash";

        assertUpdate(format("CREATE TABLE %s (customer VARCHAR) WITH (location = 's3://%s/%s/')", tableWithSlash, bucketName, tableWithSlash));
        assertUpdate(format("INSERT INTO %s (customer) VALUES ('Aaron'), ('Bill')", tableWithSlash), 2);
        assertQuery("SELECT * FROM " + tableWithSlash, "VALUES ('Aaron'), ('Bill')");

        assertUpdate(format("CREATE TABLE %s (customer VARCHAR) WITH (location = 's3://%s/%s')", tableWithoutSlash, bucketName, tableWithoutSlash));
        assertUpdate(format("INSERT INTO %s (customer) VALUES ('Carol'), ('Dave')", tableWithoutSlash), 2);
        assertQuery("SELECT * FROM " + tableWithoutSlash, "VALUES ('Carol'), ('Dave')");

        assertUpdate("DROP TABLE " + tableWithSlash);
        assertUpdate("DROP TABLE " + tableWithoutSlash);
    }

    @Test
    public void testMergeSimpleSelectPartitioned()
    {
        String targetTable = "merge_simple_target_" + randomNameSuffix();
        String sourceTable = "merge_simple_source_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['address'])", targetTable, bucketName, targetTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s')", sourceTable, bucketName, sourceTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable), 4);

        @Language("SQL") String sql = format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";

        assertUpdate(sql, 4);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Arches'), ('Ed', 7, 'Etherville'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test(dataProvider = "partitionedProvider")
    public void testMergeUpdateWithVariousLayouts(String partitionPhase)
    {
        String targetTable = "merge_formats_target_" + randomNameSuffix();
        String sourceTable = "merge_formats_source_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR) WITH (location = 's3://%s/%s'%s)", targetTable, bucketName, targetTable, partitionPhase));

        assertUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Dave', 'dates'), ('Lou', 'limes'), ('Carol', 'candles')", targetTable), 3);
        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Dave', 'dates'), ('Lou', 'limes'), ('Carol', 'candles')");

        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR) WITH (location = 's3://%s/%s')", sourceTable, bucketName, sourceTable));

        assertUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Craig', 'candles'), ('Len', 'limes'), ('Joe', 'jellybeans')", sourceTable), 3);

        @Language("SQL") String sql = format("MERGE INTO %s t USING %s s ON (t.purchase = s.purchase)", targetTable, sourceTable) +
                "    WHEN MATCHED AND s.purchase = 'limes' THEN DELETE" +
                "    WHEN MATCHED THEN UPDATE SET customer = CONCAT(t.customer, '_', s.customer)" +
                "    WHEN NOT MATCHED THEN INSERT (customer, purchase) VALUES(s.customer, s.purchase)";

        assertUpdate(sql, 3);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Dave', 'dates'), ('Carol_Craig', 'candles'), ('Joe', 'jellybeans')");
        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @DataProvider
    public Object[][] partitionedProvider()
    {
        return new Object[][] {
                {""},
                {", partitioned_by = ARRAY['customer']"},
                {", partitioned_by = ARRAY['purchase']"}
        };
    }

    @Test(dataProvider = "partitionedProvider")
    public void testMergeMultipleOperations(String partitioning)
    {
        int targetCustomerCount = 32;
        String targetTable = "merge_multiple_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (purchase INT, zipcode INT, spouse VARCHAR, address VARCHAR, customer VARCHAR) WITH (location = 's3://%s/%s'%s)", targetTable, bucketName, targetTable, partitioning));
        String originalInsertFirstHalf = IntStream.range(1, targetCustomerCount / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 1000, 91000, intValue, intValue))
                .collect(Collectors.joining(", "));
        String originalInsertSecondHalf = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 2000, 92000, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertUpdate(format("INSERT INTO %s (customer, purchase, zipcode, spouse, address) VALUES %s, %s", targetTable, originalInsertFirstHalf, originalInsertSecondHalf), targetCustomerCount - 1);

        String firstMergeSource = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct')", intValue, 3000, 83000, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertUpdate(format("MERGE INTO %s t USING (VALUES %s) AS s(customer, purchase, zipcode, spouse, address)", targetTable, firstMergeSource) +
                        "    ON t.customer = s.customer" +
                        "    WHEN MATCHED THEN UPDATE SET purchase = s.purchase, zipcode = s.zipcode, spouse = s.spouse, address = s.address",
                targetCustomerCount / 2);

        assertQuery(
                "SELECT customer, purchase, zipcode, spouse, address FROM " + targetTable,
                format("VALUES %s, %s", originalInsertFirstHalf, firstMergeSource));

        String nextInsert = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 4000, 74000, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertUpdate(format("INSERT INTO %s (customer, purchase, zipcode, spouse, address) VALUES %s", targetTable, nextInsert), targetCustomerCount / 2);

        String secondMergeSource = IntStream.range(1, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 5000, 85000, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertUpdate(format("MERGE INTO %s t USING (VALUES %s) AS s(customer, purchase, zipcode, spouse, address)", targetTable, secondMergeSource) +
                        "    ON t.customer = s.customer" +
                        "    WHEN MATCHED AND t.zipcode = 91000 THEN DELETE" +
                        "    WHEN MATCHED AND s.zipcode = 85000 THEN UPDATE SET zipcode = 60000" +
                        "    WHEN MATCHED THEN UPDATE SET zipcode = s.zipcode, spouse = s.spouse, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchase, zipcode, spouse, address) VALUES(s.customer, s.purchase, s.zipcode, s.spouse, s.address)",
                targetCustomerCount * 3 / 2 - 1);

        String updatedBeginning = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct')", intValue, 3000, 60000, intValue, intValue))
                .collect(Collectors.joining(", "));
        String updatedMiddle = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 5000, 85000, intValue, intValue))
                .collect(Collectors.joining(", "));
        String updatedEnd = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 4000, 74000, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertQuery(
                "SELECT customer, purchase, zipcode, spouse, address FROM " + targetTable,
                format("VALUES %s, %s, %s", updatedBeginning, updatedMiddle, updatedEnd));

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeSimpleQueryPartitioned()
    {
        String targetTable = "merge_simple_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['address'])", targetTable, bucketName, targetTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        @Language("SQL") String query = format("MERGE INTO %s t USING ", targetTable) +
                "(SELECT * FROM (VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville'))) AS s(customer, purchases, address)" +
                "    " +
                "ON (t.customer = s.customer)" +
                "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";
        assertUpdate(query, 4);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire'), ('Ed', 7, 'Etherville')");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test(dataProvider = "targetWithDifferentPartitioning")
    public void testMergeMultipleRowsMatchFails(String createTableSql)
    {
        String targetTable = "merge_multiple_target_" + randomNameSuffix();
        String sourceTable = "merge_multiple_source_" + randomNameSuffix();
        assertUpdate(format(createTableSql, targetTable, bucketName, targetTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Antioch')", targetTable), 2);

        assertUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s')", sourceTable, bucketName, sourceTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Adelphi'), ('Aaron', 8, 'Ashland')", sourceTable), 2);

        assertThatThrownBy(() -> computeActual(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                "    WHEN MATCHED THEN UPDATE SET address = s.address"))
                .hasMessage("One MERGE target table row matched more than one source row");

        assertUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED AND s.address = 'Adelphi' THEN UPDATE SET address = s.address",
                1);
        assertQuery("SELECT customer, purchases, address FROM " + targetTable, "VALUES ('Aaron', 5, 'Adelphi'), ('Bill', 7, 'Antioch')");
        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @DataProvider
    public Object[][] targetWithDifferentPartitioning()
    {
        return new Object[][] {
                {"CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s')"},
                {"CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['customer'])"},
                {"CREATE TABLE %s (customer VARCHAR, address VARCHAR, purchases INT) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['address'])"},
                {"CREATE TABLE %s (purchases INT, customer VARCHAR, address VARCHAR) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['address', 'customer'])"},
                {"CREATE TABLE %s (purchases INT, address VARCHAR, customer VARCHAR) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['address', 'customer'])"}
        };
    }

    @Test(dataProvider = "targetAndSourceWithDifferentPartitioning")
    public void testMergeWithDifferentPartitioning(String testDescription, String createTargetTableSql, String createSourceTableSql)
    {
        String targetTable = format("%s_target_%s", testDescription, randomNameSuffix());
        String sourceTable = format("%s_source_%s", testDescription, randomNameSuffix());

        assertUpdate(format(createTargetTableSql, targetTable, bucketName, targetTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        assertUpdate(format(createSourceTableSql, sourceTable, bucketName, sourceTable));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable), 4);

        @Language("SQL") String sql = format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";
        assertUpdate(sql, 4);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire'), ('Ed', 7, 'Etherville')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @DataProvider
    public Object[][] targetAndSourceWithDifferentPartitioning()
    {
        return new Object[][] {
                {
                        "target_partitioned_source_and_target_partitioned",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['address', 'customer'])",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['address'])",
                },
                {
                        "target_partitioned_source_and_target_partitioned",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['customer', 'address'])",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['address'])",
                },
                {
                        "target_flat_source_partitioned_by_customer",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s')",
                        "CREATE TABLE %s (purchases INT, address VARCHAR, customer VARCHAR) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['customer'])"
                },
                {
                        "target_partitioned_by_customer_source_flat",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['address'])",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s')",
                },
                {
                        "target_bucketed_by_customer_source_flat",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['customer', 'address'])",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s')",
                },
                {
                        "target_partitioned_source_partitioned",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['customer'])",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['address'])",
                },
                {
                        "target_partitioned_target_partitioned",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['address'])",
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['customer'])",
                }
        };
    }

    @Test
    public void testTableWithNonNullableColumns()
    {
        String tableName = "test_table_with_non_nullable_columns_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(col1 INTEGER NOT NULL, col2 INTEGER, col3 INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES(1, 10, 100)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES(2, 20, 200)", 1);
        assertThatThrownBy(() -> query("INSERT INTO " + tableName + " VALUES(null, 30, 300)"))
                .hasMessageContaining("NULL value not allowed for NOT NULL column: col1");
        assertThatThrownBy(() -> query("INSERT INTO " + tableName + " VALUES(TRY(5/0), 40, 400)"))
                .hasMessageContaining("NULL value not allowed for NOT NULL column: col1");

        assertThatThrownBy(() -> query("UPDATE " + tableName + " SET col1 = NULL where col3 = 100"))
                .hasMessageContaining("NULL value not allowed for NOT NULL column: col1");
        assertThatThrownBy(() -> query("UPDATE " + tableName + " SET col1 = TRY(5/0) where col3 = 200"))
                .hasMessageContaining("NULL value not allowed for NOT NULL column: col1");

        assertQuery("SELECT * FROM " + tableName, "VALUES(1, 10, 100), (2, 20, 200)");
    }

    @Test(dataProvider = "changeDataFeedColumnNamesDataProvider")
    public void testCreateTableWithChangeDataFeedColumnName(String columnName)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_table_cdf", "(" + columnName + " int)")) {
            assertTableColumnNames(table.getName(), columnName);
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_table_cdf", "AS SELECT 1 AS " + columnName)) {
            assertTableColumnNames(table.getName(), columnName);
        }
    }

    @Test(dataProvider = "changeDataFeedColumnNamesDataProvider")
    public void testUnsupportedCreateTableWithChangeDataFeed(String columnName)
    {
        String tableName = "test_unsupported_create_table_cdf" + randomNameSuffix();

        assertQueryFails(
                "CREATE TABLE " + tableName + "(" + columnName + " int) WITH (change_data_feed_enabled = true)",
                "\\QUnable to use [%s] when change data feed is enabled\\E".formatted(columnName));
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        assertQueryFails(
                "CREATE TABLE " + tableName + " WITH (change_data_feed_enabled = true) AS SELECT 1 AS " + columnName,
                "\\QUnable to use [%s] when change data feed is enabled\\E".formatted(columnName));
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test(dataProvider = "changeDataFeedColumnNamesDataProvider")
    public void testUnsupportedAddColumnWithChangeDataFeed(String columnName)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_column", "(col int) WITH (change_data_feed_enabled = true)")) {
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " ADD COLUMN " + columnName + " int",
                    "\\QColumn name %s is forbidden when change data feed is enabled\\E".formatted(columnName));
            assertTableColumnNames(table.getName(), "col");

            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES change_data_feed_enabled = false");
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN " + columnName + " int");
            assertTableColumnNames(table.getName(), "col", columnName);
        }
    }

    @Test(dataProvider = "changeDataFeedColumnNamesDataProvider")
    public void testUnsupportedRenameColumnWithChangeDataFeed(String columnName)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_rename_column", "(col int) WITH (change_data_feed_enabled = true)")) {
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " RENAME COLUMN col TO " + columnName,
                    "Cannot rename column when change data feed is enabled");
            assertTableColumnNames(table.getName(), "col");
        }
    }

    @Test(dataProvider = "changeDataFeedColumnNamesDataProvider")
    public void testUnsupportedSetTablePropertyWithChangeDataFeed(String columnName)
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_properties", "(" + columnName + " int)")) {
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " SET PROPERTIES change_data_feed_enabled = true",
                    "\\QUnable to enable change data feed because table contains [%s] columns\\E".formatted(columnName));
            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName()))
                    .doesNotContain("change_data_feed_enabled = true");
        }
    }

    @DataProvider
    public Object[][] changeDataFeedColumnNamesDataProvider()
    {
        return CHANGE_DATA_FEED_COLUMN_NAMES.stream().collect(toDataProvider());
    }

    @Test
    public void testThatEnableCdfTablePropertyIsShownForCtasTables()
    {
        String tableName = "test_show_create_show_property_for_table_created_with_ctas_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(page_url, views)" +
                "WITH (change_data_feed_enabled = true) " +
                "AS VALUES ('url1', 1), ('url2', 2)", 2);
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .contains("change_data_feed_enabled = true");
    }

    @Test(dataProvider = "columnMappingModeDataProvider")
    public void testCreateTableWithColumnMappingMode(ColumnMappingMode mode)
    {
        testCreateTableColumnMappingMode(mode, tableName -> {
            assertUpdate("CREATE TABLE " + tableName + "(a_int integer, a_row row(x integer)) WITH (column_mapping_mode='" + mode + "')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, row(11))", 1);
        });
    }

    @Test(dataProvider = "columnMappingModeDataProvider")
    public void testCreateTableAsSelectWithColumnMappingMode(ColumnMappingMode mode)
    {
        testCreateTableColumnMappingMode(mode, tableName ->
                assertUpdate("CREATE TABLE " + tableName + " WITH (column_mapping_mode='" + mode + "')" +
                        " AS SELECT 1 AS a_int, CAST(row(11) AS row(x integer)) AS a_row", 1));
    }

    @Test(dataProvider = "columnMappingModeDataProvider")
    public void testCreatePartitionTableAsSelectWithColumnMappingMode(ColumnMappingMode mode)
    {
        testCreateTableColumnMappingMode(mode, tableName ->
                assertUpdate("CREATE TABLE " + tableName + " WITH (column_mapping_mode='" + mode + "', partitioned_by=ARRAY['a_int'])" +
                        " AS SELECT 1 AS a_int, CAST(row(11) AS row(x integer)) AS a_row", 1));
    }

    private void testCreateTableColumnMappingMode(ColumnMappingMode mode, Consumer<String> createTable)
    {
        String tableName = "test_create_table_column_mapping_" + randomNameSuffix();
        createTable.accept(tableName);

        String showCreateTableResult = (String) computeScalar("SHOW CREATE TABLE " + tableName);
        if (mode != ColumnMappingMode.NONE) {
            assertThat(showCreateTableResult).contains("column_mapping_mode = '" + mode + "'");
        }
        else {
            assertThat(showCreateTableResult).doesNotContain("column_mapping_mode");
        }

        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES (1, CAST(row(11) AS row(x integer)))");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test(dataProvider = "columnMappingWithTrueAndFalseDataProvider")
    public void testDeltaColumnMappingModeAllDataTypes(ColumnMappingMode mode, boolean partitioned)
    {
        String tableName = "test_column_mapping_mode_name_all_types_" + randomNameSuffix();

        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "    a_boolean BOOLEAN," +
                "    a_tinyint TINYINT," +
                "    a_smallint SMALLINT," +
                "    a_int INT," +
                "    a_bigint BIGINT," +
                "    a_decimal_5_2 DECIMAL(5,2)," +
                "    a_decimal_21_3 DECIMAL(21,3)," +
                "    a_double DOUBLE," +
                "    a_float REAL," +
                "    a_string VARCHAR," +
                "    a_date DATE," +
                "    a_timestamp TIMESTAMP(3) WITH TIME ZONE," +
                "    a_binary VARBINARY," +
                "    a_string_array ARRAY(VARCHAR)," +
                "    a_struct_array ARRAY(ROW(a_string VARCHAR))," +
                "    a_map MAP(VARCHAR, VARCHAR)," +
                "    a_complex_map MAP(VARCHAR, ROW(a_string VARCHAR))," +
                "    a_struct ROW(a_string VARCHAR, a_int INT)," +
                "    a_complex_struct ROW(nested_struct ROW(a_string VARCHAR), a_int INT)" +
                (partitioned ? ", part VARCHAR" : "") +
                ")" +
                "WITH (" +
                (partitioned ? " partitioned_by = ARRAY['part']," : "") +
                "column_mapping_mode = '" + mode + "'" +
                ")");

        assertUpdate("" +
                "INSERT INTO " + tableName +
                " VALUES " +
                "(" +
                "   true, " +
                "   1, " +
                "   10," +
                "   100, " +
                "   1000, " +
                "   CAST('123.12' AS DECIMAL(5,2)), " +
                "   CAST('123456789012345678.123' AS DECIMAL(21,3)), " +
                "   DOUBLE '0', " +
                "   REAL '0', " +
                "   'a', " +
                "   DATE '2020-08-21', " +
                "   TIMESTAMP '2020-10-21 01:00:00.123 UTC', " +
                "   X'abcd', " +
                "   ARRAY['element 1'], " +
                "   ARRAY[ROW('nested 1')], " +
                "   MAP(ARRAY['key'], ARRAY['value1']), " +
                "   MAP(ARRAY['key'], ARRAY[ROW('nested value1')]), " +
                "   ROW('item 1', 1), " +
                "   ROW(ROW('nested item 1'), 11) " +
                (partitioned ? ", 'part1'" : "") +
                "), " +
                "(" +
                "   true, " +
                "   2, " +
                "   20," +
                "   200, " +
                "   2000, " +
                "   CAST('223.12' AS DECIMAL(5,2)), " +
                "   CAST('223456789012345678.123' AS DECIMAL(21,3)), " +
                "   DOUBLE '0', " +
                "   REAL '0', " +
                "   'b', " +
                "   DATE '2020-08-22', " +
                "   TIMESTAMP '2020-10-22 02:00:00.456 UTC', " +
                "   X'abcd', " +
                "   ARRAY['element 2'], " +
                "   ARRAY[ROW('nested 2')], " +
                "   MAP(ARRAY['key'], ARRAY[null]), " +
                "   MAP(ARRAY['key'], ARRAY[null]), " +
                "   ROW('item 2', 2), " +
                "   ROW(ROW('nested item 2'), 22) " +
                (partitioned ? ", 'part2'" : "") +
                ")", 2);

        String selectTrinoValues = "SELECT " +
                "a_boolean, a_tinyint, a_smallint, a_int, a_bigint, a_decimal_5_2, a_decimal_21_3, a_double , a_float, a_string, a_date, a_binary, a_string_array[1], a_struct_array[1].a_string, a_map['key'], a_complex_map['key'].a_string, a_struct.a_string, a_struct.a_int, a_complex_struct.nested_struct.a_string, a_complex_struct.a_int " +
                "FROM " + tableName;

        assertThat(query(selectTrinoValues))
                .skippingTypesCheck()
                .matches("VALUES" +
                        "(true, tinyint '1', smallint '10', integer '100', bigint '1000', decimal '123.12', decimal '123456789012345678.123', double '0', real '0', 'a', date '2020-08-21', X'abcd', 'element 1', 'nested 1', 'value1', 'nested value1', 'item 1', 1, 'nested item 1', 11)," +
                        "(true, tinyint '2', smallint '20', integer '200', bigint '2000', decimal '223.12', decimal '223456789012345678.123', double '0.0', real '0.0', 'b', date '2020-08-22', X'abcd', 'element 2', 'nested 2', null, null, 'item 2', 2, 'nested item 2', 22)");

        assertQuery(
                "SELECT format('%1$tF %1$tT.%1$tL', a_timestamp) FROM " + tableName,
                "VALUES '2020-10-21 01:00:00.123', '2020-10-22 02:00:00.456'");

        assertUpdate("UPDATE " + tableName + " SET a_boolean = false where a_tinyint = 1", 1);
        assertThat(query(selectTrinoValues))
                .skippingTypesCheck()
                .matches("VALUES" +
                        "(false, tinyint '1', smallint '10', integer '100', bigint '1000', decimal '123.12', decimal '123456789012345678.123', double '0', real '0', 'a', date '2020-08-21', X'abcd', 'element 1', 'nested 1', 'value1', 'nested value1', 'item 1', 1, 'nested item 1', 11)," +
                        "(true, tinyint '2', smallint '20', integer '200', bigint '2000', decimal '223.12', decimal '223456789012345678.123', double '0.0', real '0.0', 'b', date '2020-08-22', X'abcd', 'element 2', 'nested 2', null, null, 'item 2', 2, 'nested item 2', 22)");

        assertUpdate("DELETE FROM " + tableName + " WHERE a_tinyint = 2", 1);
        assertThat(query(selectTrinoValues))
                .skippingTypesCheck()
                .matches("VALUES" +
                        "(false, tinyint '1', smallint '10', integer '100', bigint '1000', decimal '123.12', decimal '123456789012345678.123', double '0', real '0', 'a', date '2020-08-21', X'abcd', 'element 1', 'nested 1', 'value1', 'nested value1', 'item 1', 1, 'nested item 1', 11)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test(dataProvider = "columnMappingWithTrueAndFalseDataProvider")
    public void testOptimizeProcedureColumnMappingMode(ColumnMappingMode mode, boolean partitioned)
    {
        String tableName = "test_optimize_column_mapping_mode_" + randomNameSuffix();

        assertUpdate("" +
                "CREATE TABLE " + tableName +
                "(a_number INT, a_struct ROW(x INT), a_string VARCHAR) " +
                "WITH (" +
                (partitioned ? "partitioned_by=ARRAY['a_string']," : "") +
                "location='s3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'," +
                "column_mapping_mode='" + mode + "')");

        assertUpdate("INSERT INTO " + tableName + " VALUES (1, row(11), 'a')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, row(22), 'b')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (3, row(33), 'c')", 1);

        Double stringColumnSize = partitioned ? null : 3.0;
        String expectedStats = "VALUES" +
                "('a_number', null, 3.0, 0.0, null, '1', '3')," +
                "('a_struct', null, null, null, null, null, null)," +
                "('a_string', " + stringColumnSize + ", 3.0, 0.0, null, null, null)," +
                "(null, null, null, null, 3.0, null, null)";
        assertQuery("SHOW STATS FOR " + tableName, expectedStats);

        // Execute OPTIMIZE procedure and verify that the statistics is preserved and the table is still writable and readable
        assertUpdate("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");

        assertQuery("SHOW STATS FOR " + tableName, expectedStats);

        assertUpdate("INSERT INTO " + tableName + " VALUES (4, row(44), 'd')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (5, row(55), 'e')", 1);

        assertQuery(
                "SELECT a_number, a_struct.x, a_string FROM " + tableName,
                "VALUES" +
                        "(1, 11, 'a')," +
                        "(2, 22, 'b')," +
                        "(3, 33, 'c')," +
                        "(4, 44, 'd')," +
                        "(5, 55, 'e')");

        assertUpdate("DROP TABLE " + tableName);
    }

    /**
     * @see deltalake.write_stats_as_json_column_mapping_id
     */
    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSupportedNonPartitionedColumnMappingIdWrites(boolean statsAsJsonEnabled)
            throws Exception
    {
        testSupportedNonPartitionedColumnMappingWrites("write_stats_as_json_column_mapping_id", statsAsJsonEnabled);
    }

    /**
     * @see deltalake.write_stats_as_json_column_mapping_name
     */
    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSupportedNonPartitionedColumnMappingNameWrites(boolean statsAsJsonEnabled)
            throws Exception
    {
        testSupportedNonPartitionedColumnMappingWrites("write_stats_as_json_column_mapping_name", statsAsJsonEnabled);
    }

    /**
     * @see deltalake.write_stats_as_json_column_mapping_none
     */
    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSupportedNonPartitionedColumnMappingNoneWrites(boolean statsAsJsonEnabled)
            throws Exception
    {
        testSupportedNonPartitionedColumnMappingWrites("write_stats_as_json_column_mapping_none", statsAsJsonEnabled);
    }

    private void testSupportedNonPartitionedColumnMappingWrites(String resourceName, boolean statsAsJsonEnabled)
            throws Exception
    {
        String tableName = "test_column_mapping_mode_" + randomNameSuffix();

        String entry = Resources.toString(Resources.getResource("deltalake/%s/_delta_log/00000000000000000000.json".formatted(resourceName)), UTF_8)
                .replace("%WRITE_STATS_AS_JSON%", Boolean.toString(statsAsJsonEnabled))
                .replace("%WRITE_STATS_AS_STRUCT%", Boolean.toString(!statsAsJsonEnabled));

        String targetPath = "%s/%s/_delta_log/00000000000000000000.json".formatted(SCHEMA, tableName);
        minioClient.putObject(bucketName, entry.getBytes(UTF_8), targetPath);
        String tableLocation = "s3://%s/%s/%s".formatted(bucketName, SCHEMA, tableName);

        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(SCHEMA, tableName, tableLocation));
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(1, 'first value', ARRAY[ROW('nested 1')], ROW('databricks 1'))," +
                        "(2, 'two', ARRAY[ROW('nested 2')], ROW('databricks 2'))," +
                        "(3, 'third value', ARRAY[ROW('nested 3')], ROW('databricks 3'))," +
                        "(4, 'four', ARRAY[ROW('nested 4')], ROW('databricks 4'))",
                4);

        assertQuery(
                "SELECT a_number, a_string, array_col[1].array_struct_element, nested.field1 FROM " + tableName,
                "VALUES" +
                        "(1, 'first value', 'nested 1', 'databricks 1')," +
                        "(2, 'two', 'nested 2', 'databricks 2')," +
                        "(3, 'third value', 'nested 3', 'databricks 3')," +
                        "(4, 'four', 'nested 4', 'databricks 4')");

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES" +
                        "('a_number', null, 4.0, 0.0, null, '1', '4')," +
                        "('a_string', 29.0, 4.0, 0.0, null, null, null)," +
                        "('array_col', null, null, null, null, null, null)," +
                        "('nested', null, null, null, null, null, null)," +
                        "(null, null, null, null, 4.0, null, null)");

        assertUpdate("UPDATE " + tableName + " SET a_number = a_number + 10 WHERE a_number in (3, 4)", 2);
        assertUpdate("UPDATE " + tableName + " SET a_number = a_number + 20 WHERE a_number in (1, 2)", 2);
        assertQuery(
                "SELECT a_number, a_string, array_col[1].array_struct_element, nested.field1 FROM " + tableName,
                "VALUES" +
                        "(21, 'first value', 'nested 1', 'databricks 1')," +
                        "(22, 'two', 'nested 2', 'databricks 2')," +
                        "(13, 'third value', 'nested 3', 'databricks 3')," +
                        "(14, 'four', 'nested 4', 'databricks 4')");

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES" +
                        "('a_number', null, 4.0, 0.0, null, '13', '22')," +
                        "('a_string', 29.0, 4.0, 0.0, null, null, null)," +
                        "('array_col', null, null, null, null, null, null)," +
                        "('nested', null, null, null, null, null, null)," +
                        "(null, null, null, null, 4.0, null, null)");

        assertUpdate("DELETE FROM " + tableName + " WHERE a_number = 22", 1);
        assertUpdate("DELETE FROM " + tableName + " WHERE a_number = 13", 1);
        assertUpdate("DELETE FROM " + tableName + " WHERE a_number = 21", 1);
        assertQuery(
                "SELECT a_number, a_string, array_col[1].array_struct_element, nested.field1 FROM " + tableName,
                "VALUES (14, 'four', 'nested 4', 'databricks 4')");

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES" +
                        "('a_number', null, 1.0, 0.0, null, '14', '14')," +
                        "('a_string', 29.0, 1.0, 0.0, null, null, null)," +
                        "('array_col', null, null, null, null, null, null)," +
                        "('nested', null, null, null, null, null, null)," +
                        "(null, null, null, null, 1.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    /**
     * @see deltalake.write_stats_as_json_partition_column_mapping_id
     */
    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSupportedPartitionedColumnMappingIdWrites(boolean statsAsJsonEnabled)
            throws Exception
    {
        testSupportedPartitionedColumnMappingWrites("write_stats_as_json_partition_column_mapping_id", statsAsJsonEnabled);
    }

    /**
     * @see deltalake.write_stats_as_json_partition_column_mapping_name
     */
    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSupportedPartitionedColumnMappingNameWrites(boolean statsAsJsonEnabled)
            throws Exception
    {
        testSupportedPartitionedColumnMappingWrites("write_stats_as_json_partition_column_mapping_name", statsAsJsonEnabled);
    }

    /**
     * @see deltalake.write_stats_as_json_partition_column_mapping_none
     */
    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSupportedPartitionedColumnMappingNoneWrites(boolean statsAsJsonEnabled)
            throws Exception
    {
        testSupportedPartitionedColumnMappingWrites("write_stats_as_json_partition_column_mapping_none", statsAsJsonEnabled);
    }

    private void testSupportedPartitionedColumnMappingWrites(String resourceName, boolean statsAsJsonEnabled)
            throws Exception
    {
        String tableName = "test_column_mapping_mode_" + randomNameSuffix();

        String entry = Resources.toString(Resources.getResource("deltalake/%s/_delta_log/00000000000000000000.json".formatted(resourceName)), UTF_8)
                .replace("%WRITE_STATS_AS_JSON%", Boolean.toString(statsAsJsonEnabled))
                .replace("%WRITE_STATS_AS_STRUCT%", Boolean.toString(!statsAsJsonEnabled));

        String targetPath = "%s/%s/_delta_log/00000000000000000000.json".formatted(SCHEMA, tableName);
        minioClient.putObject(bucketName, entry.getBytes(UTF_8), targetPath);
        String tableLocation = "s3://%s/%s/%s".formatted(bucketName, SCHEMA, tableName);

        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(SCHEMA, tableName, tableLocation));
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate(
                "INSERT INTO " + tableName + " VALUES" +
                        "(1, 'first value', ARRAY[ROW('nested 1')], ROW('databricks 1'))," +
                        "(2, 'two', ARRAY[ROW('nested 2')], ROW('databricks 2'))," +
                        "(3, 'third value', ARRAY[ROW('nested 3')], ROW('databricks 3'))," +
                        "(4, 'four', ARRAY[ROW('nested 4')], ROW('databricks 4'))",
                4);

        assertQuery(
                "SELECT a_number, a_string, array_col[1].array_struct_element, nested.field1 FROM " + tableName,
                "VALUES" +
                        "(1, 'first value', 'nested 1', 'databricks 1')," +
                        "(2, 'two', 'nested 2', 'databricks 2')," +
                        "(3, 'third value', 'nested 3', 'databricks 3')," +
                        "(4, 'four', 'nested 4', 'databricks 4')");

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES" +
                        "('a_number', null, 4.0, 0.0, null, '1', '4')," +
                        "('a_string', null, 4.0, 0.0, null, null, null)," +
                        "('array_col', null, null, null, null, null, null)," +
                        "('nested', null, null, null, null, null, null)," +
                        "(null, null, null, null, 4.0, null, null)");

        assertUpdate("UPDATE " + tableName + " SET a_number = a_number + 10 WHERE a_number in (3, 4)", 2);
        assertUpdate("UPDATE " + tableName + " SET a_number = a_number + 20 WHERE a_number in (1, 2)", 2);
        assertQuery(
                "SELECT a_number, a_string, array_col[1].array_struct_element, nested.field1 FROM " + tableName,
                "VALUES" +
                        "(21, 'first value', 'nested 1', 'databricks 1')," +
                        "(22, 'two', 'nested 2', 'databricks 2')," +
                        "(13, 'third value', 'nested 3', 'databricks 3')," +
                        "(14, 'four', 'nested 4', 'databricks 4')");

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES" +
                        "('a_number', null, 4.0, 0.0, null, '13', '22')," +
                        "('a_string', null, 4.0, 0.0, null, null, null)," +
                        "('array_col', null, null, null, null, null, null)," +
                        "('nested', null, null, null, null, null, null)," +
                        "(null, null, null, null, 4.0, null, null)");

        assertUpdate("DELETE FROM " + tableName + " WHERE a_number = 22", 1);
        assertUpdate("DELETE FROM " + tableName + " WHERE a_number = 13", 1);
        assertUpdate("DELETE FROM " + tableName + " WHERE a_number = 21", 1);
        assertQuery(
                "SELECT a_number, a_string, array_col[1].array_struct_element, nested.field1 FROM " + tableName,
                "VALUES (14, 'four', 'nested 4', 'databricks 4')");

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES" +
                        "('a_number', null, 1.0, 0.0, null, '14', '14')," +
                        "('a_string', null, 1.0, 0.0, null, null, null)," +
                        "('array_col', null, null, null, null, null, null)," +
                        "('nested', null, null, null, null, null, null)," +
                        "(null, null, null, null, 1.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @DataProvider
    public Object[][] columnMappingWithTrueAndFalseDataProvider()
    {
        return cartesianProduct(columnMappingModeDataProvider(), trueFalse());
    }

    @DataProvider
    public Object[][] columnMappingModeDataProvider()
    {
        return Arrays.stream(ColumnMappingMode.values())
                .filter(mode -> mode != ColumnMappingMode.UNKNOWN)
                .collect(toDataProvider());
    }

    @Test
    public void testCreateTableUnsupportedColumnMappingMode()
    {
        String tableName = "test_unsupported_column_mapping_mode_" + randomNameSuffix();

        assertQueryFails("CREATE TABLE " + tableName + "(a integer) WITH (column_mapping_mode = 'illegal')",
                ".* \\QInvalid value [illegal]. Valid values: [ID, NAME, NONE]");
        assertQueryFails("CREATE TABLE " + tableName + " WITH (column_mapping_mode = 'illegal') AS SELECT 1 a",
                ".* \\QInvalid value [illegal]. Valid values: [ID, NAME, NONE]");

        assertQueryFails("CREATE TABLE " + tableName + "(a integer) WITH (column_mapping_mode = 'unknown')",
                ".* \\QInvalid value [unknown]. Valid values: [ID, NAME, NONE]");
        assertQueryFails("CREATE TABLE " + tableName + " WITH (column_mapping_mode = 'unknown') AS SELECT 1 a",
                ".* \\QInvalid value [unknown]. Valid values: [ID, NAME, NONE]");

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testAlterTableWithUnsupportedProperties()
    {
        String tableName = "test_alter_table_with_unsupported_properties_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a_number INT)");

        assertQueryFails("ALTER TABLE " + tableName + " SET PROPERTIES change_data_feed_enabled = true, checkpoint_interval = 10",
                "The following properties cannot be updated: checkpoint_interval");
        assertQueryFails("ALTER TABLE " + tableName + " SET PROPERTIES partitioned_by = ARRAY['a']",
                "The following properties cannot be updated: partitioned_by");
        assertQueryFails("ALTER TABLE " + tableName + " SET PROPERTIES column_mapping_mode = 'ID'",
                "The following properties cannot be updated: column_mapping_mode");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testSettingChangeDataFeedEnabledProperty()
    {
        String tableName = "test_enable_and_disable_cdf_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (page_url VARCHAR, domain VARCHAR, views INTEGER)");

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES change_data_feed_enabled = false");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .contains("change_data_feed_enabled = false");

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES change_data_feed_enabled = true");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName)).contains("change_data_feed_enabled = true");

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES change_data_feed_enabled = false");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName)).contains("change_data_feed_enabled = false");

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES change_data_feed_enabled = true");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .contains("change_data_feed_enabled = true");
    }

    @Test
    public void testProjectionPushdownOnPartitionedTables()
    {
        String tableNamePartitionAtBeginning = "test_table_with_partition_at_beginning_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableNamePartitionAtBeginning + " (id BIGINT, root ROW(f1 BIGINT, f2 BIGINT)) WITH (partitioned_by = ARRAY['id'])");
        assertUpdate("INSERT INTO " + tableNamePartitionAtBeginning + " VALUES (1, ROW(1, 2)), (1, ROW(2, 3)), (1, ROW(3, 4))", 3);
        assertQuery("SELECT root.f1, id, root.f2 FROM " + tableNamePartitionAtBeginning, "VALUES (1, 1, 2), (2, 1, 3), (3, 1, 4)");
        assertUpdate("DROP TABLE " + tableNamePartitionAtBeginning);

        String tableNamePartitioningAtEnd = "tes_table_with_partition_at_end_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableNamePartitioningAtEnd + " (root ROW(f1 BIGINT, f2 BIGINT), id BIGINT) WITH (partitioned_by = ARRAY['id'])");
        assertUpdate("INSERT INTO " + tableNamePartitioningAtEnd + " VALUES (ROW(1, 2), 1), (ROW(2, 3), 1), (ROW(3, 4), 1)", 3);
        assertQuery("SELECT root.f2, id, root.f1 FROM " + tableNamePartitioningAtEnd, "VALUES (2, 1, 1), (3, 1, 2), (4, 1, 3)");
        assertUpdate("DROP TABLE " + tableNamePartitioningAtEnd);
    }

    @Test
    public void testProjectionPushdownColumnReorderInSchemaAndDataFile()
    {
        try (TestTable testTable = new TestTable(getQueryRunner()::execute,
                "test_projection_pushdown_column_reorder_",
                "(id BIGINT, nested1 ROW(a BIGINT, b VARCHAR, c INT), nested2 ROW(d DOUBLE, e BOOLEAN, f DATE))")) {
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES (100, ROW(10, 'a', 100), ROW(10.10, true, DATE '2023-04-19'))", 1);
            String tableDataFile = ((String) computeScalar("SELECT \"$path\" FROM " + testTable.getName()))
                    .replaceFirst("s3://" + bucketName, "");

            try (TestTable temporaryTable = new TestTable(
                    getQueryRunner()::execute,
                    "test_projection_pushdown_column_reorder_temporary_",
                    "(nested2 ROW(d DOUBLE, e BOOLEAN, f DATE), id BIGINT, nested1 ROW(a BIGINT, b VARCHAR, c INT))")) {
                assertUpdate("INSERT INTO " + temporaryTable.getName() + " VALUES (ROW(10.10, true, DATE '2023-04-19'), 100, ROW(10, 'a', 100))", 1);

                String temporaryDataFile = ((String) computeScalar("SELECT \"$path\" FROM " + temporaryTable.getName()))
                        .replaceFirst("s3://" + bucketName, "");

                // Replace table1 data file with table2 data file, so that the table's schema and data's schema has different column order
                minioClient.copyObject(bucketName, temporaryDataFile, bucketName, tableDataFile);
            }

            assertThat(query("SELECT nested2.e, nested1.a, nested2.f, nested1.b, id FROM " + testTable.getName()))
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testProjectionPushdownExplain()
    {
        String tableName = "test_projection_pushdown_explain_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, root ROW(f1 BIGINT, f2 BIGINT)) WITH (partitioned_by = ARRAY['id'])");

        assertExplain(
                "EXPLAIN SELECT root.f2 FROM " + tableName,
                "TableScan\\[table = (.*)]",
                "root#f2 := root#f2:bigint:REGULAR");

        Session sessionWithoutPushdown = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "projection_pushdown_enabled", "false")
                .build();
        assertExplain(
                sessionWithoutPushdown,
                "EXPLAIN SELECT root.f2 FROM " + tableName,
                "ScanProject\\[table = (.*)]",
                "expr := \"root\"\\[2]",
                "root := root:row\\(f1 bigint, f2 bigint\\):REGULAR");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testProjectionPushdownNonPrimitiveTypeExplain()
    {
        String tableName = "test_projection_pushdown_non_primtive_type_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName +
                " (id BIGINT, _row ROW(child BIGINT), _array ARRAY(ROW(child BIGINT)), _map MAP(BIGINT, BIGINT))");

        assertExplain(
                "EXPLAIN SELECT id, _row.child, _array[1].child, _map[1] FROM " + tableName,
                "ScanProject\\[table = (.*)]",
                "expr(.*) := \"_array(.*)\"\\[BIGINT '1']\\[1]",
                "id(.*) := id:bigint:REGULAR",
                // _array:array\\(row\\(child bigint\\)\\) is a symbol name, not a dereference expression.
                "_array(.*) := _array:array\\(row\\(child bigint\\)\\):REGULAR",
                "_map(.*) := _map:map\\(bigint, bigint\\):REGULAR",
                "_row#child := _row#child:bigint:REGULAR");
    }

    @Test(dataProvider = "columnMappingModeDataProvider")
    public void testReadCdfChanges(ColumnMappingMode mode)
    {
        String tableName = "test_basic_operations_on_table_with_cdf_enabled_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (page_url VARCHAR, domain VARCHAR, views INTEGER) " +
                "WITH (change_data_feed_enabled = true, column_mapping_mode = '" + mode + "')");
        assertUpdate("INSERT INTO " + tableName + " VALUES('url1', 'domain1', 1), ('url2', 'domain2', 2), ('url3', 'domain3', 3)", 3);
        assertUpdate("INSERT INTO " + tableName + " VALUES('url4', 'domain4', 4), ('url5', 'domain5', 2), ('url6', 'domain6', 6)", 3);

        assertUpdate("UPDATE " + tableName + " SET page_url = 'url22' WHERE views = 2", 2);
        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "'))",
                """
                        VALUES
                            ('url1', 'domain1', 1, 'insert', BIGINT '1'),
                            ('url2', 'domain2', 2, 'insert', BIGINT '1'),
                            ('url3', 'domain3', 3, 'insert', BIGINT '1'),
                            ('url4', 'domain4', 4, 'insert', BIGINT '2'),
                            ('url5', 'domain5', 2, 'insert', BIGINT '2'),
                            ('url6', 'domain6', 6, 'insert', BIGINT '2'),
                            ('url2', 'domain2', 2, 'update_preimage', BIGINT '3'),
                            ('url22', 'domain2', 2, 'update_postimage', BIGINT '3'),
                            ('url5', 'domain5', 2, 'update_preimage', BIGINT '3'),
                            ('url22', 'domain5', 2, 'update_postimage', BIGINT '3')
                        """);

        assertUpdate("DELETE FROM " + tableName + " WHERE views = 2", 2);
        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "', 3))",
                """
                        VALUES
                            ('url22', 'domain2', 2, 'delete', BIGINT '4'),
                            ('url22', 'domain5', 2, 'delete', BIGINT '4')
                        """);

        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "')) ORDER BY _commit_version, _change_type, domain",
                """
                        VALUES
                            ('url1', 'domain1', 1, 'insert', BIGINT '1'),
                            ('url2', 'domain2', 2, 'insert', BIGINT '1'),
                            ('url3', 'domain3', 3, 'insert', BIGINT '1'),
                            ('url4', 'domain4', 4, 'insert', BIGINT '2'),
                            ('url5', 'domain5', 2, 'insert', BIGINT '2'),
                            ('url6', 'domain6', 6, 'insert', BIGINT '2'),
                            ('url22', 'domain2', 2, 'update_postimage', BIGINT '3'),
                            ('url22', 'domain5', 2, 'update_postimage', BIGINT '3'),
                            ('url2', 'domain2', 2, 'update_preimage', BIGINT '3'),
                            ('url5', 'domain5', 2, 'update_preimage', BIGINT '3'),
                            ('url22', 'domain2', 2, 'delete', BIGINT '4'),
                            ('url22', 'domain5', 2, 'delete', BIGINT '4')
                        """);
    }

    @Test(dataProvider = "columnMappingModeDataProvider")
    public void testReadCdfChangesOnPartitionedTable(ColumnMappingMode mode)
    {
        String tableName = "test_basic_operations_on_table_with_cdf_enabled_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (page_url VARCHAR, domain VARCHAR, views INTEGER) " +
                "WITH (change_data_feed_enabled = true, partitioned_by = ARRAY['domain'], column_mapping_mode = '" + mode + "')");
        assertUpdate("INSERT INTO " + tableName + " VALUES('url1', 'domain1', 1), ('url2', 'domain2', 2), ('url3', 'domain1', 3)", 3);
        assertUpdate("INSERT INTO " + tableName + " VALUES('url4', 'domain1', 400), ('url5', 'domain2', 500), ('url6', 'domain3', 2)", 3);

        assertUpdate("UPDATE " + tableName + " SET domain = 'domain4' WHERE views = 2", 2);
        assertQuery("SELECT * FROM " + tableName, "" +
                """
                            VALUES
                                ('url1', 'domain1', 1),
                                ('url2', 'domain4', 2),
                                ('url3', 'domain1', 3),
                                ('url4', 'domain1', 400),
                                ('url5', 'domain2', 500),
                                ('url6', 'domain4', 2)
                        """);

        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "'))",
                """
                        VALUES
                            ('url1', 'domain1', 1, 'insert', BIGINT '1'),
                            ('url2', 'domain2', 2, 'insert', BIGINT '1'),
                            ('url3', 'domain1', 3, 'insert', BIGINT '1'),
                            ('url4', 'domain1', 400, 'insert', BIGINT '2'),
                            ('url5', 'domain2', 500, 'insert', BIGINT '2'),
                            ('url6', 'domain3', 2, 'insert', BIGINT '2'),
                            ('url2', 'domain2', 2, 'update_preimage', BIGINT '3'),
                            ('url2', 'domain4', 2, 'update_postimage', BIGINT '3'),
                            ('url6', 'domain3', 2, 'update_preimage', BIGINT '3'),
                            ('url6', 'domain4', 2, 'update_postimage', BIGINT '3')
                        """);

        assertUpdate("DELETE FROM " + tableName + " WHERE domain = 'domain4'", 2);
        assertQuery("SELECT * FROM " + tableName,
                """
                            VALUES
                                ('url1', 'domain1', 1),
                                ('url3', 'domain1', 3),
                                ('url4', 'domain1', 400),
                                ('url5', 'domain2', 500)
                        """);
        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "', 3))",
                """
                        VALUES
                            ('url2', 'domain4', 2, 'delete', BIGINT '4'),
                            ('url6', 'domain4', 2, 'delete', BIGINT '4')
                        """);
    }

    @Test
    public void testCdfWithNameMappingModeOnTableWithColumnDropped()
    {
        testCdfWithMappingModeOnTableWithColumnDropped(ColumnMappingMode.NAME);
    }

    @Test
    public void testCdfWithIdMappingModeOnTableWithColumnDropped()
    {
        testCdfWithMappingModeOnTableWithColumnDropped(ColumnMappingMode.ID);
    }

    private void testCdfWithMappingModeOnTableWithColumnDropped(ColumnMappingMode mode)
    {
        String tableName = "test_dropping_column_with_cdf_enabled_and_mapping_mode_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (page_url VARCHAR, page_views INTEGER, column_to_drop INTEGER) " +
                "WITH (change_data_feed_enabled = true, column_mapping_mode = '" + mode + "')");

        assertUpdate("INSERT INTO " + tableName + " VALUES('url1', 1, 111)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES('url2', 2, 222)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES('url3', 3, 333)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES('url4', 4, 444)", 1);

        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN column_to_drop");

        assertUpdate("INSERT INTO " + tableName + " VALUES('url5', 5)", 1);

        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "', 0))",
                """
                        VALUES
                            ('url1', 1, 'insert', BIGINT '1'),
                            ('url2', 2, 'insert', BIGINT '2'),
                            ('url3', 3, 'insert', BIGINT '3'),
                            ('url4', 4, 'insert', BIGINT '4'),
                            ('url5', 5, 'insert', BIGINT '6')
                        """);
    }

    @Test(dataProvider = "columnMappingModeDataProvider")
    public void testReadMergeChanges(ColumnMappingMode mode)
    {
        String tableName1 = "test_basic_operations_on_table_with_cdf_enabled_merge_into_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName1 + " (page_url VARCHAR, domain VARCHAR, views INTEGER) " +
                "WITH (change_data_feed_enabled = true, column_mapping_mode = '" + mode + "')");
        assertUpdate("INSERT INTO " + tableName1 + " VALUES('url1', 'domain1', 1), ('url2', 'domain2', 2), ('url3', 'domain3', 3), ('url4', 'domain4', 4)", 4);

        String tableName2 = "test_basic_operations_on_table_with_cdf_enabled_merge_from_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName2 + " (page_url VARCHAR, domain VARCHAR, views INTEGER)");
        assertUpdate("INSERT INTO " + tableName2 + " VALUES('url1', 'domain10', 10), ('url2', 'domain20', 20), ('url5', 'domain5', 50)", 3);
        assertUpdate("INSERT INTO " + tableName2 + " VALUES('url4', 'domain40', 40)", 1);

        assertUpdate("MERGE INTO " + tableName1 + " tableWithCdf USING " + tableName2 + " source " +
                "ON (tableWithCdf.page_url = source.page_url) " +
                "WHEN MATCHED AND tableWithCdf.views > 1 " +
                "THEN UPDATE SET views = (tableWithCdf.views + source.views) " +
                "WHEN MATCHED AND tableWithCdf.views <= 1 " +
                "THEN DELETE " +
                "WHEN NOT MATCHED " +
                "THEN INSERT (page_url, domain, views) VALUES (source.page_url, source.domain, source.views)", 4);

        assertQuery("SELECT * FROM " + tableName1,
                """
                            VALUES
                                ('url2', 'domain2', 22),
                                ('url3', 'domain3', 3),
                                ('url4', 'domain4', 44),
                                ('url5', 'domain5', 50)
                        """);
        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName1 + "', 0))",
                """
                        VALUES
                            ('url1', 'domain1', 1, 'insert', BIGINT '1'),
                            ('url2', 'domain2', 2, 'insert', BIGINT '1'),
                            ('url3', 'domain3', 3, 'insert', BIGINT '1'),
                            ('url4', 'domain4', 4, 'insert', BIGINT '1'),
                            ('url4', 'domain4', 4, 'update_preimage', BIGINT '2'),
                            ('url4', 'domain4', 44, 'update_postimage', BIGINT '2'),
                            ('url2', 'domain2', 2, 'update_preimage', BIGINT '2'),
                            ('url2', 'domain2', 22, 'update_postimage', BIGINT '2'),
                            ('url1', 'domain1', 1, 'delete', BIGINT '2'),
                            ('url5', 'domain5', 50, 'insert', BIGINT '2')
                        """);
    }

    @Test(dataProvider = "columnMappingModeDataProvider")
    public void testReadMergeChangesOnPartitionedTable(ColumnMappingMode mode)
    {
        String targetTable = "test_basic_operations_on_partitioned_table_with_cdf_enabled_target_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + targetTable + " (page_url VARCHAR, domain VARCHAR, views INTEGER) " +
                "WITH (change_data_feed_enabled = true, partitioned_by = ARRAY['domain'], column_mapping_mode = '" + mode + "')");
        assertUpdate("INSERT INTO " + targetTable + " VALUES('url1', 'domain1', 1), ('url2', 'domain2', 2), ('url3', 'domain3', 3), ('url4', 'domain1', 4)", 4);

        String sourceTable1 = "test_basic_operations_on_partitioned_table_with_cdf_enabled_source_1_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + sourceTable1 + " (page_url VARCHAR, domain VARCHAR, views INTEGER)");
        assertUpdate("INSERT INTO " + sourceTable1 + " VALUES('url1', 'domain1', 10), ('url2', 'domain2', 20), ('url5', 'domain3', 5)", 3);
        assertUpdate("INSERT INTO " + sourceTable1 + " VALUES('url4', 'domain2', 40)", 1);

        assertUpdate("MERGE INTO " + targetTable + " target USING " + sourceTable1 + " source " +
                "ON (target.page_url = source.page_url) " +
                "WHEN MATCHED AND target.views > 2 " +
                "THEN UPDATE SET views = (target.views + source.views) " +
                "WHEN MATCHED AND target.views <= 2 " +
                "THEN DELETE " +
                "WHEN NOT MATCHED " +
                "THEN INSERT (page_url, domain, views) VALUES (source.page_url, source.domain, source.views)", 4);

        assertQuery("SELECT * FROM " + targetTable,
                """
                        VALUES
                            ('url3', 'domain3', 3),
                            ('url4', 'domain1', 44),
                            ('url5', 'domain3', 5)
                        """);

        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + targetTable + "'))",
                """
                        VALUES
                            ('url1', 'domain1', 1, 'insert', 1),
                            ('url2', 'domain2', 2, 'insert', BIGINT '1'),
                            ('url3', 'domain3', 3, 'insert', BIGINT '1'),
                            ('url4', 'domain1', 4, 'insert', BIGINT '1'),
                            ('url1', 'domain1', 1, 'delete', BIGINT '2'),
                            ('url2', 'domain2', 2, 'delete', BIGINT '2'),
                            ('url4', 'domain1', 4, 'update_preimage', BIGINT '2'),
                            ('url4', 'domain1', 44, 'update_postimage', BIGINT '2'),
                            ('url5', 'domain3', 5, 'insert', BIGINT '2')
                        """);

        String sourceTable2 = "test_basic_operations_on_partitioned_table_with_cdf_enabled_source_1_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + sourceTable2 + " (page_url VARCHAR, domain VARCHAR, views INTEGER)");
        assertUpdate("INSERT INTO " + sourceTable2 +
                " VALUES('url3', 'domain1', 300), ('url4', 'domain2', 400), ('url5', 'domain3', 500), ('url6', 'domain1', 600)", 4);

        assertUpdate("MERGE INTO " + targetTable + " target USING " + sourceTable2 + " source " +
                "ON (target.page_url = source.page_url) " +
                "WHEN MATCHED AND target.views > 3 " +
                "THEN UPDATE SET domain = source.domain, views = (source.views + target.views) " +
                "WHEN MATCHED AND target.views <= 3 " +
                "THEN DELETE " +
                "WHEN NOT MATCHED " +
                "THEN INSERT (page_url, domain, views) VALUES (source.page_url, source.domain, source.views)", 4);

        assertQuery("SELECT * FROM " + targetTable,
                """
                        VALUES
                           ('url4', 'domain2', 444),
                           ('url5', 'domain3', 505),
                           ('url6', 'domain1', 600)
                        """);

        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + targetTable + "', 2))",
                """
                        VALUES
                            ('url3', 'domain3', 3, 'delete', BIGINT '3'),
                            ('url4', 'domain1', 44, 'update_preimage', BIGINT '3'),
                            ('url4', 'domain2', 444, 'update_postimage', BIGINT '3'),
                            ('url5', 'domain3', 5, 'update_preimage', BIGINT '3'),
                            ('url5', 'domain3', 505, 'update_postimage', BIGINT '3'),
                            ('url6', 'domain1', 600, 'insert', BIGINT '3')
                        """);
    }

    @Test(dataProvider = "columnMappingModeDataProvider")
    public void testCdfCommitTimestamp(ColumnMappingMode mode)
    {
        String tableName = "test_cdf_commit_timestamp_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (page_url VARCHAR, domain VARCHAR, views INTEGER) " +
                "WITH (change_data_feed_enabled = true, column_mapping_mode = '" + mode + "')");
        assertUpdate("INSERT INTO " + tableName + " VALUES('url1', 'domain1', 1)", 1);
        ZonedDateTime historyCommitTimestamp = (ZonedDateTime) computeScalar("SELECT timestamp FROM \"" + tableName + "$history\" WHERE version = 1");
        ZonedDateTime tableChangesCommitTimestamp = (ZonedDateTime) computeScalar("SELECT _commit_timestamp FROM TABLE(system.table_changes('test_schema', '" + tableName + "', 0)) WHERE _commit_Version = 1");
        assertThat(historyCommitTimestamp).isEqualTo(tableChangesCommitTimestamp);
    }

    @Test(dataProvider = "columnMappingModeDataProvider")
    public void testReadDifferentChangeRanges(ColumnMappingMode mode)
    {
        String tableName = "test_reading_ranges_of_changes_on_table_with_cdf_enabled_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (page_url VARCHAR, domain VARCHAR, views INTEGER) " +
                "WITH (change_data_feed_enabled = true, column_mapping_mode = '" + mode + "')");
        assertQueryReturnsEmptyResult("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "'))");
        assertUpdate("INSERT INTO " + tableName + " VALUES('url1', 'domain1', 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES('url2', 'domain2', 2)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES('url3', 'domain3', 3)", 1);
        assertQueryReturnsEmptyResult("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "', 3))");

        assertUpdate("UPDATE " + tableName + " SET page_url = 'url22' WHERE domain = 'domain2'", 1);
        assertUpdate("UPDATE " + tableName + " SET page_url = 'url33' WHERE views = 3", 1);
        assertUpdate("DELETE FROM " + tableName + " WHERE page_url = 'url1'", 1);

        assertQuery("SELECT * FROM " + tableName,
                """
                        VALUES
                           ('url22', 'domain2', 2),
                           ('url33', 'domain3', 3)
                        """);

        assertQueryFails("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "', 1000))",
                "since_version: 1000 is higher then current table version: 6");
        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "', 0))",
                """
                        VALUES
                            ('url1', 'domain1', 1, 'insert', BIGINT '1'),
                            ('url2', 'domain2', 2, 'insert', BIGINT '2'),
                            ('url3', 'domain3', 3, 'insert', BIGINT '3'),
                            ('url2', 'domain2', 2, 'update_preimage', BIGINT '4'),
                            ('url22', 'domain2', 2, 'update_postimage', BIGINT '4'),
                            ('url3', 'domain3', 3, 'update_preimage', BIGINT '5'),
                            ('url33', 'domain3', 3, 'update_postimage', BIGINT '5'),
                            ('url1', 'domain1', 1, 'delete', BIGINT '6')
                        """);

        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "'))",
                """
                        VALUES
                            ('url1', 'domain1', 1, 'insert', BIGINT '1'),
                            ('url2', 'domain2', 2, 'insert', BIGINT '2'),
                            ('url3', 'domain3', 3, 'insert', BIGINT '3'),
                            ('url2', 'domain2', 2, 'update_preimage', BIGINT '4'),
                            ('url22', 'domain2', 2, 'update_postimage', BIGINT '4'),
                            ('url3', 'domain3', 3, 'update_preimage', BIGINT '5'),
                            ('url33', 'domain3', 3, 'update_postimage', BIGINT '5'),
                            ('url1', 'domain1', 1, 'delete', BIGINT '6')
                        """);

        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "', 3))",
                """
                        VALUES
                            ('url2', 'domain2', 2, 'update_preimage', BIGINT '4'),
                            ('url22', 'domain2', 2, 'update_postimage', BIGINT '4'),
                            ('url3', 'domain3', 3, 'update_preimage', BIGINT '5'),
                            ('url33', 'domain3', 3, 'update_postimage', BIGINT '5'),
                            ('url1', 'domain1', 1, 'delete', BIGINT '6')
                        """);

        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "', 5))",
                "VALUES ('url1', 'domain1', 1, 'delete', BIGINT '6')");
        assertQueryFails("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "', 10))", "since_version: 10 is higher then current table version: 6");
    }

    @Test(dataProvider = "columnMappingModeDataProvider")
    public void testReadChangesOnTableWithColumnAdded(ColumnMappingMode mode)
    {
        String tableName = "test_reading_changes_on_table_with_columns_added_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (page_url VARCHAR, domain VARCHAR, views INTEGER) " +
                "WITH (change_data_feed_enabled = true, column_mapping_mode = '" + mode + "')");
        assertUpdate("INSERT INTO " + tableName + " VALUES('url1', 'domain1', 1)", 1);
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN company VARCHAR");
        assertUpdate("INSERT INTO " + tableName + " VALUES('url2', 'domain2', 2, 'starburst')", 1);

        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "'))",
                """
                        VALUES
                            ('url1', 'domain1', 1, null, 'insert', BIGINT '1'),
                            ('url2', 'domain2', 2, 'starburst', 'insert', BIGINT '3')
                        """);
    }

    @Test(dataProvider = "columnMappingModeDataProvider")
    public void testReadChangesOnTableWithRowColumn(ColumnMappingMode mode)
    {
        String tableName = "test_reading_changes_on_table_with_columns_added_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (page_url VARCHAR, costs ROW(month VARCHAR, amount BIGINT)) " +
                "WITH (change_data_feed_enabled = true, column_mapping_mode = '" + mode + "')");
        assertUpdate("INSERT INTO " + tableName + " VALUES('url1', ROW('01', 11))", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES('url2', ROW('02', 19))", 1);
        assertUpdate("UPDATE " + tableName + " SET costs = ROW('02', 37) WHERE costs.month = '02'", 1);

        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "'))",
                """
                        VALUES
                            ('url1', ROW('01', BIGINT '11') , 'insert', BIGINT '1'),
                            ('url2', ROW('02', BIGINT '19') , 'insert', BIGINT '2'),
                            ('url2', ROW('02', BIGINT '19') , 'update_preimage', BIGINT '3'),
                            ('url2', ROW('02', BIGINT '37') , 'update_postimage', BIGINT '3')
                        """);

        assertThat(query("SELECT costs.month, costs.amount, _commit_version FROM TABLE(system.table_changes('test_schema', '" + tableName + "'))"))
                .matches("""
                        VALUES
                            (VARCHAR '01', BIGINT '11', BIGINT '1'),
                            (VARCHAR '02', BIGINT '19', BIGINT '2'),
                            (VARCHAR '02', BIGINT '19', BIGINT '3'),
                            (VARCHAR '02', BIGINT '37', BIGINT '3')
                        """);
    }

    @Test(dataProvider = "columnMappingModeDataProvider")
    public void testCdfOnTableWhichDoesntHaveItEnabledInitially(ColumnMappingMode mode)
    {
        String tableName = "test_cdf_on_table_without_it_initially_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (page_url VARCHAR, domain VARCHAR, views INTEGER) " +
                "WITH (column_mapping_mode = '" + mode + "')");
        assertUpdate("INSERT INTO " + tableName + " VALUES('url1', 'domain1', 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES('url2', 'domain2', 2)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES('url3', 'domain3', 3)", 1);
        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "', 0))",
                """
                        VALUES
                            ('url1', 'domain1', 1, 'insert', BIGINT '1'),
                            ('url2', 'domain2', 2, 'insert', BIGINT '2'),
                            ('url3', 'domain3', 3, 'insert', BIGINT '3')
                        """);

        assertUpdate("UPDATE " + tableName + " SET page_url = 'url22' WHERE domain = 'domain2'", 1);
        assertQuerySucceeds("ALTER TABLE " + tableName + " SET PROPERTIES change_data_feed_enabled = true");
        assertUpdate("UPDATE " + tableName + " SET page_url = 'url33' WHERE views = 3", 1);
        assertUpdate("DELETE FROM " + tableName + " WHERE page_url = 'url1'", 1);

        assertQuery("SELECT * FROM " + tableName,
                """
                        VALUES
                           ('url22', 'domain2', 2),
                           ('url33', 'domain3', 3)
                        """);

        assertQueryFails("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "', 3))",
                "Change Data Feed is not enabled at version 4. Version contains 'remove' entries without 'cdc' entries");
        assertQueryFails("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "'))",
                "Change Data Feed is not enabled at version 4. Version contains 'remove' entries without 'cdc' entries");
        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "', 5))",
                """
                        VALUES
                            ('url3', 'domain3', 3, 'update_preimage', BIGINT '6'),
                            ('url33', 'domain3', 3, 'update_postimage', BIGINT '6'),
                            ('url1', 'domain1', 1, 'delete', BIGINT '7')
                        """);
    }

    @Test(dataProvider = "columnMappingModeDataProvider")
    public void testReadChangesFromCtasTable(ColumnMappingMode mode)
    {
        String tableName = "test_basic_operations_on_table_with_cdf_enabled_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (change_data_feed_enabled = true, column_mapping_mode = '" + mode + "') " +
                        "AS SELECT * FROM (VALUES" +
                        "('url1', 'domain1', 1), " +
                        "('url2', 'domain2', 2)) t(page_url, domain, views)",
                2);

        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "'))",
                """
                        VALUES
                            ('url1', 'domain1', 1, 'insert', BIGINT '0'),
                            ('url2', 'domain2', 2, 'insert', BIGINT '0')
                        """);
    }

    @Test(dataProvider = "columnMappingModeDataProvider")
    public void testVacuumDeletesCdfFiles(ColumnMappingMode mode)
            throws InterruptedException
    {
        String tableName = "test_vacuum_correctly_deletes_cdf_files_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (page_url VARCHAR, domain VARCHAR, views INTEGER) " +
                "WITH (change_data_feed_enabled = true, column_mapping_mode = '" + mode + "')");
        assertUpdate("INSERT INTO " + tableName + " VALUES('url1', 'domain1', 1), ('url3', 'domain3', 3), ('url2', 'domain2', 2)", 3);
        assertUpdate("UPDATE " + tableName + " SET views = views * 10 WHERE views = 1", 1);
        assertUpdate("UPDATE " + tableName + " SET views = views * 10 WHERE views = 2", 1);
        Stopwatch timeSinceUpdate = Stopwatch.createStarted();
        Thread.sleep(2000);
        assertUpdate("UPDATE " + tableName + " SET views = views * 30 WHERE views = 3", 1);
        Session sessionWithShortRetentionUnlocked = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "vacuum_min_retention", "0s")
                .build();
        Set<String> allFilesFromCdfDirectory = getAllFilesFromCdfDirectory(tableName);
        assertThat(allFilesFromCdfDirectory).hasSizeGreaterThanOrEqualTo(3);
        long retention = timeSinceUpdate.elapsed().getSeconds();
        getQueryRunner().execute(sessionWithShortRetentionUnlocked, "CALL delta.system.vacuum('test_schema', '" + tableName + "', '" + retention + "s')");
        allFilesFromCdfDirectory = getAllFilesFromCdfDirectory(tableName);
        assertThat(allFilesFromCdfDirectory).hasSizeBetween(1, 2);
        assertQueryFails("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "', 2))", "Error opening Hive split.*/_change_data/.*The specified key does not exist.*");
        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "', 3))",
                """
                        VALUES
                            ('url3', 'domain3', 3, 'update_preimage', BIGINT '4'),
                            ('url3', 'domain3', 90, 'update_postimage', BIGINT '4')
                        """);
    }

    @Test(dataProvider = "columnMappingModeDataProvider")
    public void testCdfWithOptimize(ColumnMappingMode mode)
    {
        String tableName = "test_cdf_with_optimize_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (page_url VARCHAR, domain VARCHAR, views INTEGER) " +
                "WITH (change_data_feed_enabled = true, column_mapping_mode = '" + mode + "')");
        assertUpdate("INSERT INTO " + tableName + " VALUES('url1', 'domain1', 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES('url2', 'domain2', 2)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES('url3', 'domain3', 3)", 1);
        assertUpdate("UPDATE " + tableName + " SET views = views * 30 WHERE views = 3", 1);
        computeActual("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
        assertUpdate("INSERT INTO " + tableName + " VALUES('url10', 'domain10', 10)", 1);
        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + tableName + "', 0))",
                """
                        VALUES
                            ('url1', 'domain1', 1, 'insert', BIGINT '1'),
                            ('url2', 'domain2', 2, 'insert', BIGINT '2'),
                            ('url3', 'domain3', 3, 'insert', BIGINT '3'),
                            ('url10', 'domain10', 10, 'insert', BIGINT '6'),
                            ('url3', 'domain3', 3, 'update_preimage', BIGINT '4'),
                            ('url3', 'domain3', 90, 'update_postimage', BIGINT '4')
                        """);
    }

    @Test
    public void testTableChangesAccessControl()
    {
        String tableName = "test_deny_table_changes_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (page_url VARCHAR, domain VARCHAR, views INTEGER) ");
        assertUpdate("INSERT INTO " + tableName + " VALUES('url1', 'domain1', 1)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES('url2', 'domain2', 2)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES('url3', 'domain3', 3)", 1);

        assertAccessDenied(
                "SELECT * FROM TABLE(system.table_changes('" + SCHEMA + "', '" + tableName + "', 0))",
                "Cannot execute function .*",
                privilege("delta.system.table_changes", EXECUTE_FUNCTION));

        assertAccessDenied(
                "SELECT * FROM TABLE(system.table_changes('" + SCHEMA + "', '" + tableName + "', 0))",
                "Cannot select from columns .*",
                privilege(tableName, SELECT_COLUMN));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testTableWithTrailingSlashLocation(boolean partitioned)
    {
        String tableName = "test_table_with_trailing_slash_location_" + randomNameSuffix();
        String location = format("s3://%s/%s/", bucketName, tableName);

        assertUpdate("CREATE TABLE " + tableName + "(col_str, col_int)" +
                "WITH (location = '" + location + "'" +
                (partitioned ? ",partitioned_by = ARRAY['col_str']" : "") +
                ") " +
                "AS VALUES ('str1', 1), ('str2', 2)", 2);
        assertQuery("SELECT * FROM " + tableName, "VALUES ('str1', 1), ('str2', 2)");

        assertUpdate("UPDATE " + tableName + " SET col_str = 'other'", 2);
        assertQuery("SELECT * FROM " + tableName, "VALUES ('other', 1), ('other', 2)");

        assertUpdate("INSERT INTO " + tableName + " VALUES ('str3', 3)", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES ('other', 1), ('other', 2), ('str3', 3)");

        assertUpdate("DELETE FROM " + tableName + " WHERE col_int = 2", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES ('other', 1), ('str3', 3)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test(dataProvider = "deleteFiltersForTable")
    public void testDeleteWithFilter(String createTableSql, String deleteFilter, boolean pushDownDelete)
    {
        String table = "delete_with_filter_" + randomNameSuffix();
        assertUpdate(format(createTableSql, table, bucketName, table));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Antioch'), ('Mary', 10, 'Adelphi'), ('Aaron', 3, 'Dallas')", table), 4);

        assertUpdate(
                getSession(),
                format("DELETE FROM %s WHERE %s", table, deleteFilter),
                2,
                plan -> {
                    if (pushDownDelete) {
                        boolean tableDelete = searchFrom(plan.getRoot()).where(node -> node instanceof TableDeleteNode).matches();
                        assertTrue(tableDelete, "A TableDeleteNode should be present");
                    }
                    else {
                        TableFinishNode finishNode = searchFrom(plan.getRoot())
                                .where(TableFinishNode.class::isInstance)
                                .findOnlyElement();
                        assertTrue(finishNode.getTarget() instanceof TableWriterNode.MergeTarget, "Delete operation should be performed through MERGE mechanism");
                    }
                });
        assertQuery("SELECT customer, purchases, address FROM " + table, "VALUES ('Mary', 10, 'Adelphi'), ('Aaron', 3, 'Dallas')");
        assertUpdate("DROP TABLE " + table);
    }

    @DataProvider
    public Object[][] deleteFiltersForTable()
    {
        return new Object[][]{
                {
                        "CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (location = 's3://%s/%s')",
                        "address = 'Antioch'",
                        false
                },
                {
                        // delete filter applied on function over non-partitioned field
                        "CREATE TABLE %s (customer VARCHAR, address VARCHAR, purchases INT) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['address'])",
                        "starts_with(address, 'Antioch')",
                        false
                },
                {
                        // delete filter applied on partitioned field
                        "CREATE TABLE %s (customer VARCHAR, address VARCHAR, purchases INT) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['address'])",
                        "address = 'Antioch'",
                        true
                },
                {
                        // delete filter applied on partitioned field and on synthesized field
                        "CREATE TABLE %s (customer VARCHAR, address VARCHAR, purchases INT) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['address'])",
                        "address = 'Antioch' AND \"$file_size\" > 0",
                        false
                },
                {
                        // delete filter applied on function over partitioned field
                        "CREATE TABLE %s (customer VARCHAR, address VARCHAR, purchases INT) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['address'])",
                        "starts_with(address, 'Antioch')",
                        false
                },
                {
                        // delete filter applied on non-partitioned field
                        "CREATE TABLE %s (customer VARCHAR, address VARCHAR, purchases INT) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['customer'])",
                        "address = 'Antioch'",
                        false
                },
                {
                        // delete filter fully applied on composed partition
                        "CREATE TABLE %s (purchases INT, customer VARCHAR, address VARCHAR) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['address', 'customer'])",
                        "address = 'Antioch' AND (customer = 'Aaron' OR customer = 'Bill')",
                        true
                },
                {
                        // delete filter applied only partly on first partitioned field
                        "CREATE TABLE %s (purchases INT, address VARCHAR, customer VARCHAR) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['address', 'customer'])",
                        "address = 'Antioch'",
                        true
                },
                {
                        // delete filter applied only partly on second partitioned field
                        "CREATE TABLE %s (purchases INT, address VARCHAR, customer VARCHAR) WITH (location = 's3://%s/%s', partitioned_by = ARRAY['customer', 'address'])",
                        "address = 'Antioch'",
                        true
                },
        };
    }

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Unable to add NOT NULL column '.*' for non-empty table: .*");
    }

    @Override
    protected String createSchemaSql(String schemaName)
    {
        return "CREATE SCHEMA " + schemaName + " WITH (location = 's3://" + bucketName + "/" + schemaName + "')";
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Schema name must be shorter than or equal to '128' characters but got.*");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Table name must be shorter than or equal to '128' characters but got.*");
    }

    private Set<String> getActiveFiles(String tableName)
    {
        return getActiveFiles(tableName, getQueryRunner().getDefaultSession());
    }

    private Set<String> getActiveFiles(String tableName, Session session)
    {
        return computeActual(session, "SELECT DISTINCT \"$path\" FROM " + tableName).getOnlyColumnAsSet().stream()
                .map(String.class::cast)
                .collect(toImmutableSet());
    }

    private Set<String> getAllDataFilesFromTableDirectory(String tableName)
    {
        return getTableFiles(tableName).stream()
                .filter(path -> !path.contains("/" + TRANSACTION_LOG_DIRECTORY))
                .collect(toImmutableSet());
    }

    private List<String> getTableFiles(String tableName)
    {
        return minioClient.listObjects(bucketName, format("%s/%s", SCHEMA, tableName)).stream()
                .map(path -> format("s3://%s/%s", bucketName, path))
                .collect(toImmutableList());
    }

    private void assertTableChangesQuery(@Language("SQL") String sql, @Language("SQL") String expectedResult)
    {
        assertThat(query(sql))
                .exceptColumns("_commit_timestamp")
                .skippingTypesCheck()
                .matches(expectedResult);
    }

    private Set<String> getAllFilesFromCdfDirectory(String tableName)
    {
        return getTableFiles(tableName).stream()
                .filter(path -> path.contains("/" + CHANGE_DATA_FOLDER_NAME))
                .collect(toImmutableSet());
    }

    @Test
    public void testPartitionFilterQueryNotDemanded()
    {
        Map<String, String> catalogProperties = getSession().getCatalogProperties(getSession().getCatalog().orElseThrow());
        assertThat(catalogProperties).doesNotContainKey("query_partition_filter_required");
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_partition_filter_not_demanded",
                "(x varchar, part varchar) WITH (partitioned_by = ARRAY['part'])",
                ImmutableList.of("'a', 'part_a'", "'b', 'part_b'"))) {
            assertQuery("SELECT * FROM %s WHERE x='a'".formatted(table.getName()), "VALUES('a', 'part_a')");
            assertQuery("SELECT * FROM %s WHERE part='part_a'".formatted(table.getName()), "VALUES('a', 'part_a')");
        }
    }

    @Test
    public void testQueryWithoutPartitionOnNonPartitionedTableNotDemanded()
    {
        Session session = sessionWithPartitionFilterRequirement();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_no_partition_table_",
                "(x varchar, part varchar)",
                ImmutableList.of("('a', 'part_a')", "('b', 'part_b')"))) {
            assertQuery(session, "SELECT * FROM %s WHERE x='a'".formatted(table.getName()), "VALUES('a', 'part_a')");
            assertQuery(session, "SELECT * FROM %s WHERE part='part_a'".formatted(table.getName()), "VALUES('a', 'part_a')");
        }
    }

    @Test
    public void testQueryWithoutPartitionFilterNotAllowed()
    {
        Session session = sessionWithPartitionFilterRequirement();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_no_partition_filter_",
                "(x varchar, part varchar) WITH (partitioned_by = ARRAY['part'])",
                ImmutableList.of("('a', 'part_a')", "('b', 'part_b')"))) {
            assertQueryFails(
                    session,
                    "SELECT * FROM %s WHERE x='a'".formatted(table.getName()),
                    "Filter required on .*" + table.getName() + " for at least one partition column:.*");
        }
    }

    @Test
    public void testPartitionFilterRemovedByPlanner()
    {
        Session session = sessionWithPartitionFilterRequirement();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_partition_filter_removed_",
                "(x varchar, part varchar) WITH (partitioned_by = ARRAY['part'])",
                ImmutableList.of("('a', 'part_a')", "('b', 'part_b')"))) {
            assertQueryFails(
                    session,
                    "SELECT x FROM " + table.getName() + " WHERE part IS NOT NULL OR TRUE",
                    "Filter required on .*" + table.getName() + " for at least one partition column:.*");
        }
    }

    @Test
    public void testPartitionFilterIncluded()
    {
        Session session = sessionWithPartitionFilterRequirement();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_partition_filter_included",
                "(x varchar, part integer) WITH (partitioned_by = ARRAY['part'])",
                ImmutableList.of("('a', 1)", "('a', 2)", "('a', 3)", "('a', 4)", "('b', 1)", "('b', 2)", "('b', 3)", "('b', 4)"))) {
            assertQuery(session, "SELECT * FROM " + table.getName() + " WHERE part = 1", "VALUES ('a', 1), ('b', 1)");
            assertQuery(session, "SELECT count(*) FROM " + table.getName() + " WHERE part < 2", "VALUES 2");
            assertQuery(session, "SELECT count(*) FROM " + table.getName() + " WHERE Part < 2", "VALUES 2");
            assertQuery(session, "SELECT count(*) FROM " + table.getName() + " WHERE PART < 2", "VALUES 2");
            assertQuery(session, "SELECT count(*) FROM " + table.getName() + " WHERE parT < 2", "VALUES 2");
            assertQuery(session, "SELECT count(*) FROM " + table.getName() + " WHERE part % 2 = 0", "VALUES 4");
            assertQuery(session, "SELECT count(*) FROM " + table.getName() + " WHERE part - 2 = 0", "VALUES 2");
            assertQuery(session, "SELECT count(*) FROM " + table.getName() + " WHERE part * 4 = 4", "VALUES 2");
            assertQuery(session, "SELECT count(*) FROM " + table.getName() + " WHERE part % 2 > 0", "VALUES 4");
            assertQuery(session, "SELECT count(*) FROM " + table.getName() + " WHERE part % 2 = 1 and part IS NOT NULL", "VALUES 4");
            assertQuery(session, "SELECT count(*) FROM " + table.getName() + " WHERE part IS NULL", "VALUES 0");
            assertQuery(session, "SELECT count(*) FROM " + table.getName() + " WHERE part = 1 OR x = 'a' ", "VALUES 5");
            assertQuery(session, "SELECT count(*) FROM " + table.getName() + " WHERE part = 1 AND  x = 'a' ", "VALUES 1");
            assertQuery(session, "SELECT count(*) FROM " + table.getName() + " WHERE part IS NOT NULL", "VALUES 8");
            assertQuery(session, "SELECT x, count(*) AS COUNT FROM " + table.getName() + " WHERE part > 2 GROUP BY x ", "VALUES ('a', 2), ('b', 2)");
            assertQueryFails(session, "SELECT count(*) FROM " + table.getName() + " WHERE x= 'a'", "Filter required on .*" + table.getName() + " for at least one partition column:.*");
        }
    }

    @Test
    public void testRequiredPartitionFilterOnJoin()
    {
        Session session = sessionWithPartitionFilterRequirement();

        try (TestTable leftTable = new TestTable(
                getQueryRunner()::execute,
                "test_partition_left_",
                "(x varchar, part varchar)",
                ImmutableList.of("('a', 'part_a')"));
                    TestTable rightTable = new TestTable(
                            new TrinoSqlExecutor(getQueryRunner(), session),
                           "test_partition_right_",
                            "(x varchar, part varchar) WITH (partitioned_by = ARRAY['part'])",
                            ImmutableList.of("('a', 'part_a')"))) {
            assertQueryFails(
                    session,
                    "SELECT a.x, b.x from %s a JOIN %s b on (a.x = b.x) where a.x = 'a'".formatted(leftTable.getName(), rightTable.getName()),
                    "Filter required on .*" + rightTable.getName() + " for at least one partition column:.*");
            assertQuery(
                    session,
                    "SELECT a.x, b.x from %s a JOIN %s b on (a.part = b.part) where a.part = 'part_a'".formatted(leftTable.getName(), rightTable.getName()),
                    "VALUES ('a', 'a')");
        }
    }

    @Test
    public void testRequiredPartitionFilterOnJoinBothTablePartitioned()
    {
        Session session = sessionWithPartitionFilterRequirement();

        try (TestTable leftTable = new TestTable(
                getQueryRunner()::execute,
                "test_partition_inferred_left_",
                "(x varchar, part varchar) WITH (partitioned_by = ARRAY['part'])",
                ImmutableList.of("('a', 'part_a')"));
                    TestTable rightTable = new TestTable(
                            new TrinoSqlExecutor(getQueryRunner(), session),
                            "test_partition_inferred_right_",
                            "(x varchar, part varchar) WITH (partitioned_by = ARRAY['part'])",
                            ImmutableList.of("('a', 'part_a')"))) {
            assertQueryFails(
                    session,
                    "SELECT a.x, b.x from %s a JOIN %s b on (a.x = b.x) where a.x = 'a'".formatted(leftTable.getName(), rightTable.getName()),
                    "Filter required on .*" + leftTable.getName() + " for at least one partition column:.*");
            assertQuery(
                    session,
                    "SELECT a.x, b.x from %s a JOIN %s b on (a.part = b.part) where a.part = 'part_a'".formatted(leftTable.getName(), rightTable.getName()),
                    "VALUES ('a', 'a')");
        }
    }

    @Test
    public void testComplexPartitionPredicateWithCasting()
    {
        Session session = sessionWithPartitionFilterRequirement();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_partition_predicate",
                "(x varchar, part varchar) WITH (partitioned_by = ARRAY['part'])",
                ImmutableList.of("('a', '1')", "('b', '2')"))) {
            assertQuery(session, "SELECT * FROM " + table.getName() + " WHERE CAST (part AS integer) = 1", "VALUES ('a', 1)");
        }
    }

    @Test
    public void testPartitionPredicateInOuterQuery()
    {
        Session session = sessionWithPartitionFilterRequirement();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_partition_predicate",
                "(x integer, part integer) WITH (partitioned_by = ARRAY['part'])",
                ImmutableList.of("(1, 11)", "(2, 22)"))) {
            assertQuery(session, "SELECT * FROM (SELECT * FROM " + table.getName() + " WHERE x = 1) WHERE part = 11", "VALUES (1, 11)");
        }
    }

    @Test
    public void testPartitionPredicateInInnerQuery()
    {
        Session session = sessionWithPartitionFilterRequirement();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_partition_predicate",
                "(x integer, part integer) WITH (partitioned_by = ARRAY['part'])",
                ImmutableList.of("(1, 11)", "(2, 22)"))) {
            assertQuery(session, "SELECT * FROM (SELECT * FROM " + table.getName() + " WHERE part = 11) WHERE x = 1", "VALUES (1, 11)");
        }
    }

    @Test
    public void testPartitionPredicateFilterAndAnalyzeOnPartitionedTable()
    {
        Session session = sessionWithPartitionFilterRequirement();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_partition_predicate_analyze_",
                "(x integer, part integer) WITH (partitioned_by = ARRAY['part'])",
                ImmutableList.of("(1, 11)", "(2, 22)"))) {
            String expectedMessageRegExp = "ANALYZE statement can not be performed on partitioned tables because filtering is required on at least one partition." +
                                           " However, the partition filtering check can be disabled with the catalog session property 'query_partition_filter_required'.";
            assertQueryFails(session, "ANALYZE " + table.getName(), expectedMessageRegExp);
            assertQueryFails(session, "EXPLAIN ANALYZE " + table.getName(), expectedMessageRegExp);
        }
    }

    @Test
    public void testPartitionPredicateFilterAndAnalyzeOnNonPartitionedTable()
    {
        Session session = sessionWithPartitionFilterRequirement();
        try (TestTable nonPartitioned = new TestTable(
                getQueryRunner()::execute,
                "test_partition_predicate_analyze_nonpartitioned",
                "(a integer, b integer) ",
                ImmutableList.of("(1, 11)", "(2, 22)"))) {
            assertUpdate(session, "ANALYZE " + nonPartitioned.getName());
            computeActual(session, "EXPLAIN ANALYZE " + nonPartitioned.getName());
        }
    }

    @Test
    public void testPartitionFilterMultiplePartition()
    {
        Session session = sessionWithPartitionFilterRequirement();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_partition_filter_multiple_partition_",
                "(x varchar, part1 integer, part2 integer) WITH (partitioned_by = ARRAY['part1', 'part2'])",
                ImmutableList.of("('a', 1, 1)", "('a', 1, 2)", "('a', 2, 1)", "('a', 2, 2)", "('b', 1, 1)", "('b', 1, 2)", "('b', 2, 1)", "('b', 2, 2)"))) {
            assertQuery(session, "SELECT count(*) FROM %s WHERE part1 = 1".formatted(table.getName()), "VALUES 4");
            assertQuery(session, "SELECT count(*) FROM %s WHERE part2 = 1".formatted(table.getName()), "VALUES 4");
            assertQuery(session, "SELECT count(*) FROM %s WHERE part1 = 1 AND part2 = 2".formatted(table.getName()), "VALUES 2");
            assertQuery(session, "SELECT count(*) FROM %s WHERE part2 IS NOT NULL".formatted(table.getName()), "VALUES 8");
            assertQuery(session, "SELECT count(*) FROM %s WHERE part2 IS NULL".formatted(table.getName()), "VALUES 0");
            assertQuery(session, "SELECT count(*) FROM %s WHERE part2 < 0".formatted(table.getName()), "VALUES 0");
            assertQuery(session, "SELECT count(*) FROM %s WHERE part1 = 1 OR part2 > 1".formatted(table.getName()), "VALUES 6");
            assertQuery(session, "SELECT count(*) FROM %s WHERE part1 = 1 AND part2 > 1".formatted(table.getName()), "VALUES 2");
            assertQuery(session, "SELECT count(*) FROM %s WHERE part1 IS NOT NULL OR part2 > 1".formatted(table.getName()), "VALUES 8");
            assertQuery(session, "SELECT count(*) FROM %s WHERE part1 IS NOT NULL AND part2 > 1".formatted(table.getName()), "VALUES 4");
            assertQuery(session, "SELECT count(*) FROM %s WHERE x = 'a' AND part2 = 2".formatted(table.getName()), "VALUES 2");
            assertQuery(session, "SELECT x, PART1 * 10 + PART2 AS Y FROM %s WHERE x = 'a' AND part2 = 2".formatted(table.getName()), "VALUES ('a', 12), ('a', 22)");
            assertQuery(session, "SELECT x, CAST (PART1 AS varchar) || CAST (PART2 AS varchar) FROM %s WHERE x = 'a' AND part2 = 2".formatted(table.getName()), "VALUES ('a', '12'), ('a', '22')");
            assertQuery(session, "SELECT x, MAX(PART1) FROM %s WHERE part2 = 2 GROUP BY X".formatted(table.getName()), "VALUES ('a', 2), ('b', 2)");
            assertQuery(session, "SELECT x, reduce_agg(part1, 0, (a, b) -> a + b, (a, b) -> a + b) FROM " + table.getName() + " WHERE part2 > 1 GROUP BY X", "VALUES ('a', 3), ('b', 3)");
            String expectedMessageRegExp = "Filter required on .*" + table.getName() + " for at least one partition column:.*";
            assertQueryFails(session, "SELECT X, CAST (PART1 AS varchar) || CAST (PART2 AS varchar) FROM %s WHERE x = 'a'".formatted(table.getName()), expectedMessageRegExp);
            assertQueryFails(session, "SELECT count(*) FROM %s WHERE x='a'".formatted(table.getName()), expectedMessageRegExp);
        }
    }

    @Test
    public void testPartitionFilterRequiredAndOptimize()
    {
        Session session = sessionWithPartitionFilterRequirement();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_partition_filter_optimize",
                "(part integer, name varchar(50)) WITH (partitioned_by = ARRAY['part'])",
                ImmutableList.of("(1, 'Bob')", "(2, 'Alice')"))) {
            assertUpdate(session, "ALTER TABLE " + table.getName() + " ADD COLUMN last_name varchar(50)");
            assertUpdate(session, "INSERT INTO " + table.getName() + " SELECT 3, 'John', 'Doe'", 1);

            assertQuery(session,
                    "SELECT part, name, last_name  FROM " + table.getName() + " WHERE part < 4",
                    "VALUES (1, 'Bob', NULL), (2, 'Alice', NULL), (3, 'John', 'Doe')");

            Set<String> beforeActiveFiles = getActiveFiles(table.getName());
            assertQueryFails(session, "ALTER TABLE " + table.getName() + " EXECUTE OPTIMIZE", "Filter required on .*" + table.getName() + " for at least one partition column:.*");
            computeActual(session, "ALTER TABLE " + table.getName() + " EXECUTE OPTIMIZE WHERE part=1");

            assertThat(beforeActiveFiles).isNotEqualTo(getActiveFiles(table.getName()));
            assertQuery(session,
                    "SELECT part, name, last_name  FROM " + table.getName() + " WHERE part < 4",
                    "VALUES (1, 'Bob', NULL), (2, 'Alice', NULL), (3, 'John', 'Doe')");
        }
    }

    @Test
    public void testPartitionFilterEnabledAndOptimizeForNonPartitionedTable()
    {
        Session session = sessionWithPartitionFilterRequirement();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_partition_filter_nonpartitioned_optimize",
                "(part integer, name varchar(50))",
                ImmutableList.of("(1, 'Bob')", "(2, 'Alice')"))) {
            assertUpdate(session, "ALTER TABLE " + table.getName() + " ADD COLUMN last_name varchar(50)");
            assertUpdate(session, "INSERT INTO " + table.getName() + " SELECT 3, 'John', 'Doe'", 1);

            assertQuery(session,
                    "SELECT part, name, last_name  FROM " + table.getName() + " WHERE part < 4",
                    "VALUES (1, 'Bob', NULL), (2, 'Alice', NULL), (3, 'John', 'Doe')");

            Set<String> beforeActiveFiles = getActiveFiles(table.getName());
            computeActual(session, "ALTER TABLE " + table.getName() + " EXECUTE OPTIMIZE (file_size_threshold => '10kB')");

            assertThat(beforeActiveFiles).isNotEqualTo(getActiveFiles(table.getName()));
            assertQuery(session,
                    "SELECT part, name, last_name  FROM " + table.getName() + " WHERE part < 4",
                    "VALUES (1, 'Bob', NULL), (2, 'Alice', NULL), (3, 'John', 'Doe')");
        }
    }

    @Test
    public void testPartitionFilterRequiredAndWriteOperation()
    {
        Session session = sessionWithPartitionFilterRequirement();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_partition_filter_table_changes",
                "(x integer, part integer) WITH (partitioned_by = ARRAY['part'], change_data_feed_enabled = true)",
                ImmutableList.of("(1, 11)", "(2, 22)", "(3, 33)"))) {
            @Language("RegExp")
            String expectedMessageRegExp = "Filter required on test_schema\\." + table.getName() + " for at least one partition column: part";

            assertQueryFails(session, "UPDATE " + table.getName() + " SET x = 10 WHERE x = 1", expectedMessageRegExp);
            assertUpdate(session, "UPDATE " + table.getName() + " SET x = 20 WHERE part = 22", 1);

            assertQueryFails(session, "MERGE INTO " + table.getName() + " t " +
                                      "USING (SELECT * FROM (VALUES (3, 99), (4,44))) AS s(x, part) " +
                                      "ON t.x = s.x " +
                                      "WHEN MATCHED THEN DELETE ", expectedMessageRegExp);
            assertUpdate(session, "MERGE INTO " + table.getName() + " t " +
                                  "USING (SELECT * FROM (VALUES (2, 22), (4 , 44))) AS s(x, part) " +
                                  "ON (t.part = s.part) " +
                                  "WHEN MATCHED THEN UPDATE " +
                                  " SET x = t.x + s.x, part = t.part ", 1);

            assertQueryFails(session, "MERGE INTO " + table.getName() + " t " +
                                      "USING (SELECT * FROM (VALUES (4,44))) AS s(x, part) " +
                                      "ON t.x = s.x " +
                                      "WHEN NOT MATCHED THEN INSERT (x, part) VALUES(s.x, s.part) ", expectedMessageRegExp);
            assertUpdate(session, "MERGE INTO " + table.getName() + " t " +
                                  "USING (SELECT * FROM (VALUES (4, 44))) AS s(x, part) " +
                                  "ON (t.part = s.part) " +
                                  "WHEN NOT MATCHED THEN INSERT (x, part) VALUES(s.x, s.part) ", 1);

            assertQueryFails(session, "DELETE FROM " + table.getName() + " WHERE x = 3", expectedMessageRegExp);
            assertUpdate(session, "DELETE FROM " + table.getName() + " WHERE part = 33 and x = 3", 1);
        }
    }

    @Test
    public void testPartitionFilterRequiredAndTableChanges()
    {
        Session session = sessionWithPartitionFilterRequirement();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_partition_filter_table_changes",
                "(x integer, part integer) WITH (partitioned_by = ARRAY['part'], change_data_feed_enabled = true)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 11)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2, 22)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, 33)", 1);

            @Language("RegExp")
            String expectedMessageRegExp = "Filter required on test_schema\\." + table.getName() + " for at least one partition column: part";

            assertQueryFails(session, "UPDATE " + table.getName() + " SET x = 10 WHERE x = 1", expectedMessageRegExp);
            assertUpdate(session, "UPDATE " + table.getName() + " SET x = 20 WHERE part = 22", 1);
            // TODO (https://github.com/trinodb/trino/issues/18498) Check for partition filter for table_changes when the following issue will be completed https://github.com/trinodb/trino/pull/17928
            assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + table.getName() + "'))",
                    """
                            VALUES
                                (1,   11,  'insert',           BIGINT '1'),
                                (2,   22,  'insert',           BIGINT '2'),
                                (3,   33,  'insert',           BIGINT '3'),
                                (2,   22,  'update_preimage',  BIGINT '4'),
                                (20,  22,  'update_postimage', BIGINT '4')
                            """);

            assertQueryFails(session, "DELETE FROM " + table.getName() + " WHERE x = 3", expectedMessageRegExp);
            assertUpdate(session, "DELETE FROM " + table.getName() + " WHERE part = 33 and x = 3", 1);
            assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + table.getName() + "', 4))",
                    """
                            VALUES
                                (3, 33, 'delete', BIGINT '5')
                            """);

            assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('test_schema', '" + table.getName() + "')) ORDER BY _commit_version, _change_type, part",
                    """
                            VALUES
                                (1,   11,  'insert',           BIGINT '1'),
                                (2,   22,  'insert',           BIGINT '2'),
                                (3,   33,  'insert',           BIGINT '3'),
                                (2,   22,  'update_preimage',  BIGINT '4'),
                                (20,  22,  'update_postimage', BIGINT '4'),
                                (3,   33,  'delete',           BIGINT '5')
                            """);
        }
    }

    @Test
    public void testPartitionFilterRequiredAndHistoryTable()
    {
        Session session = sessionWithPartitionFilterRequirement();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_partition_filter_table_changes",
                "(x integer, part integer) WITH (partitioned_by = ARRAY['part'], change_data_feed_enabled = true)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 11)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2, 22)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (3, 33)", 1);

            @Language("RegExp")
            String expectedMessageRegExp = "Filter required on test_schema\\." + table.getName() + " for at least one partition column: part";

            assertQuery("SELECT version, operation, read_version FROM \"" + table.getName() + "$history\"",
                    """
                            VALUES
                                (0, 'CREATE TABLE', 0),
                                (1, 'WRITE', 0),
                                (2, 'WRITE', 1),
                                (3, 'WRITE', 2)
                            """);

            assertQueryFails(session, "UPDATE " + table.getName() + " SET x = 10 WHERE x = 1", expectedMessageRegExp);
            assertUpdate(session, "UPDATE " + table.getName() + " SET x = 20 WHERE part = 22", 1);

            assertQuery("SELECT version, operation, read_version FROM \"" + table.getName() + "$history\"",
                    """
                            VALUES
                                (0, 'CREATE TABLE', 0),
                                (1, 'WRITE', 0),
                                (2, 'WRITE', 1),
                                (3, 'WRITE', 2),
                                (4, 'MERGE', 3)
                            """);
        }
    }

    private Session sessionWithPartitionFilterRequirement()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "query_partition_filter_required", "true")
                .build();
    }
}
