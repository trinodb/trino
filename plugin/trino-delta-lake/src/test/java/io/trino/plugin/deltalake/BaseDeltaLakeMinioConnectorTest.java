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
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.ResultWithQueryId;
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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.union;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.TRANSACTION_LOG_DIRECTORY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_SCHEMA;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
                ImmutableMap.<String, String>builder()
                        .put("delta.enable-non-concurrent-writes", "true")
                        .buildOrThrow(),
                hiveMinioDataLake.getMinioAddress(),
                hiveMinioDataLake.getHiveHadoop());
        queryRunner.execute("CREATE SCHEMA " + SCHEMA + " WITH (location = 's3://" + bucketName + "/" + SCHEMA + "')");
        TpchTable.getTables().forEach(table -> {
            String tableName = table.getTableName();
            hiveMinioDataLake.copyResources(resourcePath + tableName, SCHEMA + "/" + tableName);
            queryRunner.execute(format("CREATE TABLE %1$s.%2$s.%3$s (dummy int) WITH (location = 's3://%4$s/%2$s/%3$s')",
                    DELTA_CATALOG,
                    SCHEMA,
                    tableName,
                    bucketName));
        });
        return queryRunner;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_DELETE:
            case SUPPORTS_ROW_LEVEL_DELETE:
                return true;
            case SUPPORTS_UPDATE:
                return true;
            case SUPPORTS_PREDICATE_PUSHDOWN:
            case SUPPORTS_LIMIT_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN:
            case SUPPORTS_AGGREGATION_PUSHDOWN:
            case SUPPORTS_DROP_COLUMN:
            case SUPPORTS_RENAME_COLUMN:
            case SUPPORTS_RENAME_SCHEMA:
            case SUPPORTS_NOT_NULL_CONSTRAINT:
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
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

    @Test
    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
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

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches("CREATE TABLE \\w+\\.\\w+\\.orders \\Q(\n" +
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
        String tableName = "test_null_partitions_" + randomTableSuffix();
        assertUpdate("" +
                        "CREATE TABLE " + tableName + " (a, b, c) WITH (location = '" + format("s3://%s/%s", bucketName, tableName) + "', partitioned_by = ARRAY['c']) " +
                        "AS VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), (null, null, null), (4, 4, 4)",
                "VALUES 5");
        assertQuery("SELECT a FROM " + tableName + " WHERE c % 5 = 1", "VALUES (1)");
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
                .hasMessage("Renaming managed tables is not supported")
                .hasStackTraceContaining("SQL: ALTER TABLE test_rename_");
    }

    /**
     * @see io.trino.plugin.deltalake.BaseDeltaLakeConnectorSmokeTest#testRenameExternalTableAcrossSchemas for more test coverage
     */
    @Override
    public void testRenameTableAcrossSchema()
    {
        assertThatThrownBy(super::testRenameTableAcrossSchema)
                .hasMessage("Renaming managed tables is not supported")
                .hasStackTraceContaining("SQL: ALTER TABLE test_rename_");
    }

    @Override
    public void testRenameTableToUnqualifiedPreservesSchema()
    {
        assertThatThrownBy(super::testRenameTableToUnqualifiedPreservesSchema)
                .hasMessage("Renaming managed tables is not supported")
                .hasStackTraceContaining("SQL: ALTER TABLE test_source_schema_");
    }

    @Override
    public void testDropNonEmptySchemaWithTable()
    {
        String schemaName = "test_drop_non_empty_schema_" + randomTableSuffix();
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
        String tableName = "test_parquet_timestamp_predicate_pushdown_" + randomTableSuffix();

        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " (t TIMESTAMP WITH TIME ZONE)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (TIMESTAMP '" + value + "')", 1);

        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> queryResult = queryRunner.executeWithQueryId(
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

    private QueryInfo getQueryInfo(DistributedQueryRunner queryRunner, ResultWithQueryId<MaterializedResult> queryResult)
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
            assertThat(updatedFiles).hasSize(2).doesNotContainAnyElementsOf(initialFiles);
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
        String tableName = "test_default_max_file_size" + randomTableSuffix();
        @Language("SQL") String createTableSql = format("CREATE TABLE %s AS SELECT * FROM tpch.sf1.lineitem LIMIT 100000", tableName);

        Session session = Session.builder(getSession())
                .setSystemProperty("task_writer_count", "1")
                .setCatalogSessionProperty("delta_lake", "experimental_parquet_optimized_writer_enabled", "true")
                .build();
        assertUpdate(session, createTableSql, 100000);
        Set<String> initialFiles = getActiveFiles(tableName);
        assertThat(initialFiles.size()).isLessThanOrEqualTo(3);
        assertUpdate(format("DROP TABLE %s", tableName));

        DataSize maxSize = DataSize.of(40, DataSize.Unit.KILOBYTE);
        session = Session.builder(getSession())
                .setSystemProperty("task_writer_count", "1")
                .setCatalogSessionProperty("delta_lake", "experimental_parquet_optimized_writer_enabled", "true")
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

    @Override
    protected String createSchemaSql(String schemaName)
    {
        return "CREATE SCHEMA " + schemaName + " WITH (location = 's3://" + bucketName + "/" + schemaName + "')";
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
}
