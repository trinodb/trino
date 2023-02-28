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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedResultWithQueryId;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.union;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.TRANSACTION_LOG_DIRECTORY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_SCHEMA;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public abstract class BaseDeltaLakeMinioConnectorTest
        extends BaseConnectorTest
{
    private static final String SCHEMA = "test_schema";

    protected String bucketName;
    protected String resourcePath;
    protected HiveMinioDataLake hiveMinioDataLake;

    public BaseDeltaLakeMinioConnectorTest(String bucketName, String resourcePath)
    {
        this.bucketName = requireNonNull(bucketName);
        this.resourcePath = requireNonNull(resourcePath);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName));
        hiveMinioDataLake.start();
        QueryRunner queryRunner = DeltaLakeQueryRunner.createS3DeltaLakeQueryRunner(
                DELTA_CATALOG,
                SCHEMA,
                ImmutableMap.of(
                        "delta.enable-non-concurrent-writes", "true",
                        "delta.register-table-procedure.enabled", "true"),
                hiveMinioDataLake.getMinio().getMinioAddress(),
                hiveMinioDataLake.getHiveHadoop());
        queryRunner.execute("CREATE SCHEMA " + SCHEMA + " WITH (location = 's3://" + bucketName + "/" + SCHEMA + "')");
        TpchTable.getTables().forEach(table -> {
            String tableName = table.getTableName();
            hiveMinioDataLake.copyResources(resourcePath + tableName, SCHEMA + "/" + tableName);
            queryRunner.execute(format("CALL system.register_table('%1$s', '%2$s', 's3://%3$s/%1$s/%2$s')",
                    SCHEMA,
                    tableName,
                    bucketName));
        });
        return queryRunner;
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_PREDICATE_PUSHDOWN:
            case SUPPORTS_LIMIT_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN:
            case SUPPORTS_AGGREGATION_PUSHDOWN:
                return false;

            case SUPPORTS_RENAME_SCHEMA:
                return false;

            case SUPPORTS_DROP_COLUMN:
            case SUPPORTS_RENAME_COLUMN:
            case SUPPORTS_SET_COLUMN_TYPE:
                return false;

            case SUPPORTS_DELETE:
            case SUPPORTS_UPDATE:
            case SUPPORTS_MERGE:
            case SUPPORTS_CREATE_VIEW:
                return true;

            default:
                return super.hasBehavior(connectorBehavior);
        }
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
                .getCause()
                .hasMessageMatching(
                        "Transaction log locked.*" +
                                "|.*/_delta_log/\\d+.json already exists" +
                                "|Conflicting concurrent writes found..*" +
                                "|Multiple live locks found for:.*" +
                                "|Target file .* was created during locking");
    }

    @Override
    protected void verifyConcurrentInsertFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessage("Failed to write Delta Lake transaction log entry")
                .getCause()
                .hasMessageMatching(
                        "Transaction log locked.*" +
                                "|.*/_delta_log/\\d+.json already exists" +
                                "|Conflicting concurrent writes found..*" +
                                "|Multiple live locks found for:.*" +
                                "|Target file .* was created during locking");
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessageMatching("Unable to add '.*' column for: .*")
                .getCause()
                .hasMessageMatching(
                        "Transaction log locked.*" +
                                "|.*/_delta_log/\\d+.json already exists" +
                                "|Conflicting concurrent writes found..*" +
                                "|Multiple live locks found for:.*" +
                                "|Target file .* was created during locking");
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
    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        // TODO https://github.com/trinodb/trino/issues/11297: these should be cleanly rejected and filterColumnNameTestData() replaced with isColumnNameRejected()
        Set<String> unsupportedColumnNames = ImmutableSet.of(
                "atrailingspace ",
                " aleadingspace",
                "a,comma",
                "a;semicolon",
                "a space");
        if (unsupportedColumnNames.contains(columnName)) {
            return Optional.empty();
        }

        return Optional.of(columnName);
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
                        "   location = \\E'.*/test_schema/orders',\n\\Q" +
                        "   partitioned_by = ARRAY[]\n" +
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

    /**
     * @see io.trino.plugin.deltalake.BaseDeltaLakeConnectorSmokeTest#testRenameExternalTable for more test coverage
     */
    @Override
    public void testRenameTable()
    {
        assertThatThrownBy(super::testRenameTable)
                .hasMessage("Renaming managed tables is not allowed with current metastore configuration")
                .hasStackTraceContaining("SQL: ALTER TABLE test_rename_");
    }

    /**
     * @see io.trino.plugin.deltalake.BaseDeltaLakeConnectorSmokeTest#testRenameExternalTableAcrossSchemas for more test coverage
     */
    @Override
    public void testRenameTableAcrossSchema()
    {
        assertThatThrownBy(super::testRenameTableAcrossSchema)
                .hasMessage("Renaming managed tables is not allowed with current metastore configuration")
                .hasStackTraceContaining("SQL: ALTER TABLE test_rename_");
    }

    @Override
    public void testRenameTableToUnqualifiedPreservesSchema()
    {
        assertThatThrownBy(super::testRenameTableToUnqualifiedPreservesSchema)
                .hasMessage("Renaming managed tables is not allowed with current metastore configuration")
                .hasStackTraceContaining("SQL: ALTER TABLE test_source_schema_");
    }

    @Override
    public void testRenameTableToLongTableName()
    {
        assertThatThrownBy(super::testRenameTableToLongTableName)
                .hasMessage("Renaming managed tables is not allowed with current metastore configuration")
                .hasStackTraceContaining("SQL: ALTER TABLE test_rename_");
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
                .setCatalogSessionProperty("delta_lake", "target_max_file_size", maxSize.toString())
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

        //TODO these 2 should fail  https://github.com/trinodb/trino/issues/13435
        assertUpdate("UPDATE " + tableName + " SET col2 = NULL where col3 = 100", 1);
        assertUpdate("UPDATE " + tableName + " SET col2 = TRY(5/0) where col3 = 200", 1);

        assertQuery("SELECT * FROM " + tableName, "VALUES(1, null, 100), (2, null, 200)");
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

    @Test
    public void testAlterTableWithUnsupportedProperties()
    {
        String tableName = "test_alter_table_with_unsupported_properties_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a_number INT)");

        assertQueryFails("ALTER TABLE " + tableName + " SET PROPERTIES change_data_feed_enabled = true, checkpoint_interval = 10",
                "The following properties cannot be updated: checkpoint_interval");
        assertQueryFails("ALTER TABLE " + tableName + " SET PROPERTIES partitioned_by = ARRAY['a']",
                "The following properties cannot be updated: partitioned_by");

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
        assertThat(e).hasMessageMatching("(?s)(.*Read timed out)|(.*\"`NAME`\" that has maximum length of 128.*)");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("(?s)(.*Read timed out)|(.*\"`TBL_NAME`\" that has maximum length of 128.*)");
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
        return hiveMinioDataLake.listFiles(format("%s/%s", SCHEMA, tableName)).stream()
                .map(path -> format("s3://%s/%s", bucketName, path))
                .collect(toImmutableList());
    }

    private void assertThatShowCreateTable(String tableName, String expectedRegex)
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .matches(Pattern.compile(expectedRegex, Pattern.DOTALL));
    }
}
