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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import io.airlift.concurrent.MoreFutures;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.QueryManager;
import io.trino.metastore.HiveMetastore;
import io.trino.operator.OperatorStats;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.spi.QueryId;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.collect.Sets.union;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.CREATE_OR_REPLACE_TABLE_AS_OPERATION;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.CREATE_OR_REPLACE_TABLE_OPERATION;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.EXTENDED_STATISTICS_COLLECT_ON_WRITE;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.getConnectorService;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.getTableActiveFiles;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.TRANSACTION_LOG_DIRECTORY;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.QueryAssertions.getTrinoExceptionCause;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DELETE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DELETE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public abstract class BaseDeltaLakeConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    protected static final String SCHEMA = "smoke_test";

    private static final List<TpchTable<?>> REQUIRED_TPCH_TABLES =
            ImmutableSet.<TpchTable<?>>builder()
                    .addAll(BaseConnectorSmokeTest.REQUIRED_TPCH_TABLES)
                    .add(CUSTOMER, LINE_ITEM, ORDERS)
                    .build()
                    .asList();

    private static final List<ResourceTable> NON_TPCH_TABLES = ImmutableList.of(
            new ResourceTable("invariants", "deltalake/invariants"),
            new ResourceTable("person", "databricks73/person"),
            new ResourceTable("foo", "databricks73/foo"),
            new ResourceTable("bar", "databricks73/bar"),
            new ResourceTable("old_dates", "databricks73/old_dates"),
            new ResourceTable("old_timestamps", "databricks73/old_timestamps"),
            new ResourceTable("nested_timestamps", "databricks73/nested_timestamps"),
            new ResourceTable("nested_timestamps_parquet_stats", "databricks73/nested_timestamps_parquet_stats"),
            new ResourceTable("json_stats_on_row_type", "databricks104/json_stats_on_row_type"),
            new ResourceTable("parquet_stats_missing", "databricks73/parquet_stats_missing"),
            new ResourceTable("uppercase_columns", "databricks73/uppercase_columns"),
            new ResourceTable("default_partitions", "databricks73/default_partitions"),
            new ResourceTable("insert_nonlowercase_columns", "databricks73/insert_nonlowercase_columns"),
            new ResourceTable("insert_nested_nonlowercase_columns", "databricks73/insert_nested_nonlowercase_columns"),
            new ResourceTable("insert_nonlowercase_columns_partitioned", "databricks73/insert_nonlowercase_columns_partitioned"));

    // Cannot be too small, as implicit (time-based) cache invalidation can mask issues. Cannot be too big as some tests need to wait for cache
    // to be outdated.
    private static final int TEST_METADATA_CACHE_TTL_SECONDS = 15;

    protected final String bucketName = "test-delta-lake-integration-smoke-test-" + randomNameSuffix();

    protected HiveHadoop hiveHadoop;
    private HiveMetastore metastore;
    private TransactionLogAccess transactionLogAccess;

    protected void environmentSetup() {}

    protected abstract HiveHadoop createHiveHadoop()
            throws Exception;

    protected abstract Map<String, String> hiveStorageConfiguration();

    protected abstract Map<String, String> deltaStorageConfiguration();

    protected abstract void registerTableFromResources(String table, String resourcePath, QueryRunner queryRunner);

    protected abstract String getLocationForTable(String bucketName, String tableName);

    protected abstract List<String> getTableFiles(String tableName);

    protected abstract List<String> listFiles(String directory);

    protected abstract void deleteFile(String filePath);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        environmentSetup();

        this.hiveHadoop = closeAfterClass(createHiveHadoop());
        this.metastore = new BridgingHiveMetastore(
                testingThriftHiveMetastoreBuilder()
                        .metastoreClient(hiveHadoop.getHiveMetastoreEndpoint())
                        .build(this::closeAfterClass));

        QueryRunner queryRunner = createDeltaLakeQueryRunner();
        try {
            this.transactionLogAccess = getConnectorService(queryRunner, TransactionLogAccess.class);

            REQUIRED_TPCH_TABLES.forEach(table -> queryRunner.execute(format(
                    "CREATE TABLE %s WITH (location = '%s') AS SELECT * FROM tpch.tiny.%1$s",
                    table.getTableName(),
                    getLocationForTable(bucketName, table.getTableName()))));

            /* Data (across 2 files) generated using:
             * INSERT INTO foo VALUES
             *   (1, 100, 'data1'),
             *   (2, 200, 'data2')
             *
             * Data (across 2 files) generated using:
             * INSERT INTO bar VALUES
             *   (100, 'data100'),
             *   (200, 'data200')
             *
             * INSERT INTO old_dates
             * VALUES (DATE '0100-01-01', 1), (DATE '1582-10-15', 2), (DATE '1960-01-01', 3), (DATE '2020-01-01', 4)
             *
             * INSERT INTO test_timestamps VALUES
             * (TIMESTAMP '0100-01-01 01:02:03', 1), (TIMESTAMP '1582-10-15 01:02:03', 2), (TIMESTAMP '1960-01-01 01:02:03', 3), (TIMESTAMP '2020-01-01 01:02:03', 4);
             */
            NON_TPCH_TABLES.forEach(table -> {
                registerTableFromResources(table.tableName(), table.resourcePath(), queryRunner);
            });

            queryRunner.installPlugin(new TestingHivePlugin(queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data")));

            queryRunner.createCatalog(
                    "hive",
                    "hive",
                    ImmutableMap.<String, String>builder()
                            .put("hive.metastore", "thrift")
                            .put("hive.metastore.uri", hiveHadoop.getHiveMetastoreEndpoint().toString())
                            .putAll(hiveStorageConfiguration())
                            .buildOrThrow());

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private QueryRunner createDeltaLakeQueryRunner()
            throws Exception
    {
        return DeltaLakeQueryRunner.builder(SCHEMA)
                .setDeltaProperties(ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", hiveHadoop.getHiveMetastoreEndpoint().toString())
                        .put("delta.metadata.cache-ttl", TEST_METADATA_CACHE_TTL_SECONDS + "s")
                        .put("delta.metadata.live-files.cache-ttl", TEST_METADATA_CACHE_TTL_SECONDS + "s")
                        .put("hive.metastore-cache-ttl", TEST_METADATA_CACHE_TTL_SECONDS + "s")
                        .put("delta.register-table-procedure.enabled", "true")
                        .put("hive.metastore.thrift.client.read-timeout", "1m") // read timed out sometimes happens with the default timeout
                        .putAll(deltaStorageConfiguration())
                        .buildOrThrow())
                .setSchemaLocation(getLocationForTable(bucketName, SCHEMA))
                .build();
    }

    @AfterAll
    public void cleanUp()
    {
        hiveHadoop = null; // closed by closeAfterClass
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_SCHEMA -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    public void testDropSchemaExternalFiles()
    {
        String schemaName = "externalFileSchema";
        String schemaDir = bucketUrl() + "drop-schema-with-external-files/";
        String subDir = schemaDir + "subdir/";
        String externalFile = subDir + "external-file";

        // Create file in a subdirectory of the schema directory before creating schema
        hiveHadoop.executeInContainerFailOnError("hdfs", "dfs", "-mkdir", "-p", subDir);
        hiveHadoop.executeInContainerFailOnError("hdfs", "dfs", "-touchz", externalFile);

        assertUpdate(format("CREATE SCHEMA %s WITH (location = '%s')", schemaName, schemaDir));
        assertThat(hiveHadoop.executeInContainer("hdfs", "dfs", "-test", "-e", externalFile).getExitCode())
                .as("external file exists after creating schema")
                .isEqualTo(0);

        assertUpdate("DROP SCHEMA " + schemaName);
        assertThat(hiveHadoop.executeInContainer("hdfs", "dfs", "-test", "-e", externalFile).getExitCode())
                .as("external file exists after dropping schema")
                .isEqualTo(0);

        // Test behavior without external file
        hiveHadoop.executeInContainerFailOnError("hdfs", "dfs", "-rm", "-r", subDir);

        assertUpdate(format("CREATE SCHEMA %s WITH (location = '%s')", schemaName, schemaDir));
        assertThat(hiveHadoop.executeInContainer("hdfs", "dfs", "-test", "-d", schemaDir).getExitCode())
                .as("schema directory exists after creating schema")
                .isEqualTo(0);

        assertUpdate("DROP SCHEMA " + schemaName);
        assertThat(hiveHadoop.executeInContainer("hdfs", "dfs", "-test", "-e", externalFile).getExitCode())
                .as("schema directory deleted after dropping schema without external file")
                .isEqualTo(1);
    }

    protected abstract String bucketUrl();

    @Test
    public void testCreateTableInNonexistentSchemaFails()
    {
        String tableName = "test_create_table_in_nonexistent_schema_" + randomNameSuffix();
        String location = getLocationForTable(bucketName, tableName);
        assertQueryFails(
                "CREATE TABLE doesnotexist." + tableName + " (a int, b int) WITH (location = '" + location + "')",
                "Schema doesnotexist not found");
        assertThat(getTableFiles(tableName)).isEmpty();

        assertQueryFails(
                "CREATE TABLE doesnotexist." + tableName + " (a, b) WITH (location = '" + location + "') AS VALUES (1, 2), (3, 4)",
                "Schema doesnotexist not found");
        assertThat(getTableFiles(tableName)).isEmpty();
    }

    @Test
    public void testCreatePartitionedTable()
    {
        String tableName = "test_create_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a int, b VARCHAR, c TIMESTAMP WITH TIME ZONE) " +
                "WITH (location = '" + getLocationForTable(bucketName, tableName) + "', partitioned_by = ARRAY['b'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a', TIMESTAMP '2020-01-01 01:22:34.000 UTC')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b', TIMESTAMP '2021-01-01 01:22:34.000 UTC')", 1);
        assertQuery("SELECT a, b, CAST(c AS VARCHAR) FROM " + tableName, "VALUES (1, 'a', '2020-01-01 01:22:34.000 UTC'), (2, 'b', '2021-01-01 01:22:34.000 UTC')");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPathUriDecoding()
    {
        String tableName = "test_uri_table_" + randomNameSuffix();
        registerTableFromResources(tableName, "deltalake/uri", getQueryRunner());

        assertQuery("SELECT * FROM " + tableName, "VALUES ('a=equal', 1), ('a:colon', 2), ('a+plus', 3), ('a space', 4), ('a%percent', 5), ('a/forwardslash', 6)");
        String firstFilePath = (String) computeScalar("SELECT \"$path\" FROM " + tableName + " WHERE y = 1");
        assertQuery("SELECT * FROM " + tableName + " WHERE \"$path\" = '" + firstFilePath + "'", "VALUES ('a=equal', 1)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPathUriEncoding()
    {
        String tableName = "test_uri_table_encoding_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (part varchar, data integer) " +
                "WITH (location = '" + getLocationForTable(bucketName, tableName) + "', partitioned_by = ARRAY['part'])");

        assertUpdate("INSERT INTO " + tableName + " VALUES ('a=equal', 1), ('a:colon', 2), ('a+plus', 3), ('a space', 4), ('a%percent', 5), ('a/forwardslash', 6)", 6);
        assertQuery("SELECT * FROM " + tableName, "VALUES ('a=equal', 1), ('a:colon', 2), ('a+plus', 3), ('a space', 4), ('a%percent', 5), ('a/forwardslash', 6)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTablePartitionValidation()
    {
        String tableName = "test_create_table_partition_validation_" + randomNameSuffix();
        assertQueryFails("CREATE TABLE " + tableName + " (a int, b VARCHAR, c TIMESTAMP WITH TIME ZONE) " +
                        "WITH (location = '" + getLocationForTable(bucketName, tableName) + "', partitioned_by = ARRAY['a', 'd', 'e'])",
                "Table property 'partitioned_by' contained column names which do not exist: \\[d, e]");

        assertQueryFails("CREATE TABLE " + tableName + " (a, b, c) " +
                        "WITH (location = '" + getLocationForTable(bucketName, tableName) + "', partitioned_by = ARRAY['a', 'd', 'e']) " +
                        "AS VALUES (1, 'one', TIMESTAMP '2020-02-03 01:02:03.123 UTC')",
                "Table property 'partitioned_by' contained column names which do not exist: \\[d, e]");
    }

    @Test
    public void testCreateTableThatAlreadyExists()
    {
        assertQueryFails("CREATE TABLE person (a int, b int) WITH (location = '" + getLocationForTable(bucketName, "different_person") + "')",
                format(".*Table 'delta.%s.person' already exists.*", SCHEMA));
    }

    @Test
    public void testCreateTablePartitionOrdering()
    {
        String tableName = "test_create_table_partition_ordering_" + randomNameSuffix();
        assertUpdate(
                "CREATE TABLE " + tableName + " WITH (location = '" + getLocationForTable(bucketName, tableName) + "', " +
                        "partitioned_by = ARRAY['nationkey', 'regionkey']) AS SELECT regionkey, nationkey, name, comment FROM nation",
                25);
        assertQuery("SELECT regionkey, nationkey, name, comment FROM " + tableName, "SELECT regionkey, nationkey, name, comment FROM nation");
    }

    @Test
    public void testOptimizeRewritesTable()
    {
        String tableName = "test_optimize_rewrites_table_" + randomNameSuffix();
        String tableLocation = getLocationForTable(bucketName, tableName);
        assertUpdate("CREATE TABLE " + tableName + " (key integer, value varchar) WITH (location = '" + tableLocation + "')");
        try {
            // DistributedQueryRunner sets node-scheduler.include-coordinator by default, so include coordinator
            int workerCount = getQueryRunner().getNodeCount();

            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one')", 1);

            for (int i = 0; i < 3; i++) {
                Set<String> initialFiles = getActiveFiles(tableName);
                computeActual("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
                Set<String> filesAfterOptimize = getActiveFiles(tableName);
                assertThat(filesAfterOptimize)
                        .hasSizeBetween(1, workerCount)
                        .containsExactlyElementsOf(initialFiles);
            }

            assertQuery("SELECT * FROM " + tableName, "VALUES(1, 'one')");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testOptimizeTableWithSmallFileAndLargeFiles()
    {
        String tableName = "test_optimize_rewrites_table_with_small_and_large_file" + randomNameSuffix();
        String tableLocation = getLocationForTable(bucketName, tableName);
        assertUpdate("CREATE TABLE " + tableName + " (key integer, value varchar) WITH (location = '" + tableLocation + "')");
        try {
            // Adds a small file of size < 1 kB
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one')", 1);
            // Adds other "large" files of size greater than 1 kB
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, '" + "two".repeat(1000) + "')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, '" + "three".repeat(1000) + "')", 1);

            Set<String> initialFiles = getActiveFiles(tableName);
            assertThat(initialFiles).hasSize(3);

            for (int i = 0; i < 3; i++) {
                computeActual("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE (file_size_threshold => '1kB')");
                Set<String> filesAfterOptimize = getActiveFiles(tableName);
                assertThat(filesAfterOptimize)
                        .containsExactlyInAnyOrderElementsOf(initialFiles);
            }
            assertQuery(
                    "SELECT * FROM " + tableName,
                    "VALUES (1, 'one'), (2, '%s'), (3, '%s')".formatted("two".repeat(1000), "three".repeat(1000)));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testOptimizeRewritesPartitionedTable()
    {
        String tableName = "test_optimize_rewrites_partitioned_table_" + randomNameSuffix();
        String tableLocation = getLocationForTable(bucketName, tableName);
        assertUpdate("CREATE TABLE " + tableName + " (key integer, value varchar) WITH (location = '" + tableLocation + "', partitioned_by = ARRAY['key'])");
        try {
            // DistributedQueryRunner sets node-scheduler.include-coordinator by default, so include coordinator
            int workerCount = getQueryRunner().getNodeCount();

            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'two')", 1);

            for (int i = 0; i < 3; i++) {
                Set<String> initialFiles = getActiveFiles(tableName);
                computeActual("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
                Set<String> filesAfterOptimize = getActiveFiles(tableName);
                assertThat(filesAfterOptimize)
                        .hasSizeBetween(1, workerCount)
                        .containsExactlyInAnyOrderElementsOf(initialFiles);
            }
            assertQuery("SELECT * FROM " + tableName, "VALUES(1, 'one'), (2, 'two')");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat(computeScalar("SHOW CREATE TABLE person"))
                .isEqualTo(format(
                        "CREATE TABLE delta.%s.person (\n" +
                                "   name varchar,\n" +
                                "   age integer,\n" +
                                "   married boolean,\n" +
                                "   phones array(ROW(number varchar, label varchar)),\n" +
                                "   address ROW(street varchar, city varchar, state varchar, zip varchar),\n" +
                                "   income double,\n" +
                                "   gender varchar\n" +
                                ")\n" +
                                "WITH (\n" +
                                "   location = '%s',\n" +
                                "   partitioned_by = ARRAY['age']\n" +
                                ")",
                        SCHEMA,
                        getLocationForTable(bucketName, "person")));
    }

    @Test
    public void testInputDataSize()
    {
        QueryRunner queryRunner = getQueryRunner();

        String hiveTableName = "foo_hive";
        queryRunner.execute(
                format("CREATE TABLE hive.%s.%s (foo_id bigint, bar_id bigint, data varchar) WITH (format = 'PARQUET', external_location = '%s')",
                        SCHEMA,
                        hiveTableName,
                        getLocationForTable(bucketName, "foo")));

        MaterializedResultWithPlan deltaResult = queryRunner.executeWithPlan(broadcastJoinDistribution(true), "SELECT * FROM foo");
        assertThat(deltaResult.result().getRowCount()).isEqualTo(2);
        MaterializedResultWithPlan hiveResult = queryRunner.executeWithPlan(broadcastJoinDistribution(true), format("SELECT * FROM %s.%s.%s", "hive", SCHEMA, hiveTableName));
        assertThat(hiveResult.result().getRowCount()).isEqualTo(2);

        QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
        assertThat(queryManager.getFullQueryInfo(deltaResult.queryId()).getQueryStats().getProcessedInputDataSize()).as("delta processed input data size")
                .isGreaterThan(DataSize.ofBytes(0))
                .isEqualTo(queryManager.getFullQueryInfo(hiveResult.queryId()).getQueryStats().getProcessedInputDataSize());
        queryRunner.execute(format("DROP TABLE hive.%s.%s", SCHEMA, hiveTableName));
    }

    @Test
    public void testHiddenColumns()
    {
        assertQuery("SELECT DISTINCT \"$path\" FROM foo",
                format("VALUES '%s/part-00000-6f261ad3-ab3a-45e1-9047-01f9491f5a8c-c000.snappy.parquet'," +
                        " '%1$s/part-00000-f61316e9-b279-4efa-94c8-5ababdacf768-c000.snappy.parquet'", getLocationForTable(bucketName, "foo")));
        assertQuery("SELECT DISTINCT \"$file_size\" FROM foo", "VALUES 935");
        assertQuery("SELECT DISTINCT CAST(\"$file_modified_time\" AS varchar) FROM foo", "VALUES '2020-03-26 02:41:24.000 UTC', '2020-03-26 02:41:43.000 UTC'");
    }

    @Test
    public void testHiveViewsCannotBeAccessed()
    {
        String schemaName = "test_schema" + randomNameSuffix();
        String viewName = "dummy_view";
        assertUpdate("CREATE SCHEMA " + schemaName);
        hiveHadoop.runOnHive(format("CREATE VIEW %s.%s AS SELECT * FROM %s.customer", schemaName, viewName, SCHEMA));
        assertThat(computeScalar(format("SHOW TABLES FROM %s LIKE '%s'", schemaName, viewName))).isEqualTo(viewName);
        assertThatThrownBy(() -> computeActual("DESCRIBE " + schemaName + "." + viewName)).hasMessageContaining(format("%s.%s is not a Delta Lake table", schemaName, viewName));
        hiveHadoop.runOnHive("DROP DATABASE " + schemaName + " CASCADE");
    }

    @Test
    public void testDropDatabricksTable()
    {
        testDropTable(
                "testdrop_databricks",
                "io/trino/plugin/deltalake/testing/resources/databricks73/nation");
    }

    @Test
    public void testDropOssDataLakeTable()
    {
        testDropTable(
                "testdrop_datalake",
                "io/trino/plugin/deltalake/testing/resources/ossdeltalake/nation");
    }

    private void testDropTable(String tableName, String resourcePath)
    {
        registerTableFromResources(tableName, resourcePath, getQueryRunner());
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(getTableFiles(tableName)).hasSizeGreaterThan(1); // the data should not be deleted
    }

    @Test
    public void testDropAndRecreateTable()
    {
        String tableName = "testDropAndRecreate_" + randomNameSuffix();
        assertUpdate(format("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')", tableName, getLocationForTable(bucketName, "nation")));
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");

        assertUpdate("DROP TABLE " + tableName);
        assertUpdate(format("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')", tableName, getLocationForTable(bucketName, "customer")));
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM customer");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropColumnNotSupported()
    {
        registerTableFromResources("testdropcolumn", "io/trino/plugin/deltalake/testing/resources/databricks73/nation", getQueryRunner());
        assertQueryFails("ALTER TABLE testdropcolumn DROP COLUMN comment", "Cannot drop column from table using column mapping mode NONE");
    }

    @Test
    public void testCreatePartitionedTableAs()
    {
        String tableName = "test_create_partitioned_table_as_" + randomNameSuffix();
        assertUpdate(
                format("CREATE TABLE " + tableName + " WITH (location = '%s', partitioned_by = ARRAY['regionkey']) AS SELECT name, regionkey, comment from nation",
                        getLocationForTable(bucketName, tableName)),
                25);
        assertThat(computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo(format(
                        "CREATE TABLE %s.%s.%s (\n" +
                                "   name varchar,\n" +
                                "   regionkey bigint,\n" +
                                "   comment varchar\n" +
                                ")\n" +
                                "WITH (\n" +
                                "   location = '%s',\n" +
                                "   partitioned_by = ARRAY['regionkey']\n" +
                                ")",
                        DELTA_CATALOG, SCHEMA, tableName, getLocationForTable(bucketName, tableName)));
        assertQuery("SELECT * FROM " + tableName, "SELECT name, regionkey, comment FROM nation");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreatePartitionedDefaultPartitionKeys()
    {
        String tableName = "test_create_partitioned_table_default_as_" + randomNameSuffix();
        assertUpdate(
                format("CREATE TABLE " + tableName + "(number_partition, string_partition, a_value) " +
                                "WITH (location = '%s', " +
                                "partitioned_by = ARRAY['number_partition', 'string_partition']) " +
                                "AS VALUES (NULL, 'partition_a', 'jarmuz'), (1, NULL, 'brukselka'), (NULL, NULL, 'kalafior')",
                        getLocationForTable(bucketName, tableName),
                        tableName),
                3);
        assertQuery("SELECT * FROM " + tableName, "VALUES (NULL, 'partition_a', 'jarmuz'), (1, NULL, 'brukselka'), (NULL, NULL, 'kalafior')");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTablePartitionedByDate()
    {
        String tableName = "test_create_table_partitioned_by_date_" + randomNameSuffix();
        assertUpdate(
                format("CREATE TABLE %s (i, d) WITH (location = '%s', partitioned_by = ARRAY['d']) AS VALUES (1, DATE '2020-01-01'), (2, DATE '1700-01-01')",
                        tableName, getLocationForTable(bucketName, tableName)),
                2);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, DATE '2020-01-01'), (2, DATE '1700-01-01')");
    }

    /**
     * More tests in {@link TestDeltaLakeCreateTableStatistics}.
     */
    @Test
    public void testCreateTableAsStatistics()
    {
        String tableName = "test_ctats_stats_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName
                + " WITH ("
                + "location = '" + getLocationForTable(bucketName, tableName) + "'"
                + ")"
                + " AS SELECT * FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', 1857.0, 25.0, 0.0, null, null, null)," +
                        "('name', 177.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");
    }

    @Test
    public void testCleanupForFailedCreateTableAs()
    {
        String controlTableName = "test_cleanup_for_failed_create_table_as_control_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE " + controlTableName + " WITH (location = '%s') AS " +
                        "SELECT nationkey from tpch.sf1.nation", getLocationForTable(bucketName, controlTableName)),
                25);
        assertThat(getTableFiles(controlTableName)).isNotEmpty();

        String tableName = "test_cleanup_for_failed_create_table_as_" + randomNameSuffix();
        assertThat(query(
                format("CREATE TABLE " + tableName + " WITH (location = '%s') AS " +
                                "SELECT nationkey from tpch.sf1.nation " + // writer for this part finishes quickly
                                "UNION ALL " +
                                "SELECT 10/(max(orderkey)-max(orderkey)) from tpch.sf10.orders", // writer takes longer to complete and fails at the end
                        getLocationForTable(bucketName, tableName))))
                .failure().hasMessageContaining("Division by zero");
        assertEventually(new Duration(5, SECONDS), () -> assertThat(getTableFiles(tableName)).isEmpty());
    }

    @Test
    public void testCleanupForFailedPartitionedCreateTableAs()
    {
        String tableName = "test_cleanup_for_failed_partitioned_create_table_as_" + randomNameSuffix();
        assertThat(query(
                format("CREATE TABLE " + tableName + "(a, b) WITH (location = '%s', partitioned_by = ARRAY['b']) AS " +
                                "SELECT nationkey, regionkey from tpch.sf1.nation " + // writer for this part finishes quickly
                                "UNION ALL " +
                                "SELECT 10/(max(orderkey)-max(orderkey)), orderkey %% 5 from tpch.sf10.orders group by orderkey %% 5", // writer takes longer to complete and fails at the end
                        getLocationForTable(bucketName, tableName))))
                .failure().hasMessageContaining("Division by zero");
        assertEventually(new Duration(5, SECONDS), () -> assertThat(getTableFiles(tableName)).isEmpty());
    }

    @Test
    public void testCreateTableAsExistingLocation()
    {
        String tableName = "test_create_table_as_existing_location_" + randomNameSuffix();
        String createTableStatement = format("CREATE TABLE " + tableName + " WITH (location = '%s') AS SELECT name from nation", getLocationForTable(bucketName, tableName));

        // run create without table directory
        assertThat(getTableFiles(tableName)).as("table files").isEmpty();
        assertUpdate(createTableStatement, 25);

        // drop table
        assertUpdate("DROP TABLE " + tableName);

        // list remaining files
        assertThat(getTableFiles(tableName)).as("remaining table files").isNotEmpty();

        // crate with non-empty target directory should fail
        assertThat(query(createTableStatement)).failure().hasMessageContaining("Target location cannot contain any files");
    }

    @Test
    public void testCreateSchemaWithLocation()
    {
        String schemaName = "test_create_schema_with_location_" + randomNameSuffix();
        assertQuerySucceeds(
                format("CREATE SCHEMA %s WITH ( location = '%s' )",
                        schemaName,
                        getLocationForTable(bucketName, schemaName)));
    }

    @Test
    public void testCreateTableAsWithSchemaLocation()
    {
        String tableName = "table1_with_curr_schema_loc_" + randomNameSuffix();
        String tableName2 = "table2_with_curr_schema_loc_" + randomNameSuffix();
        String schemaName = "test_schema" + randomNameSuffix();
        String schemaLocation = getLocationForTable(bucketName, schemaName);

        assertUpdate(
                format("CREATE SCHEMA %s WITH ( location = '%s' )",
                        schemaName,
                        schemaLocation));
        assertUpdate(format("CREATE TABLE %s.%s AS SELECT name FROM nation", schemaName, tableName), "SELECT count(*) FROM nation");
        assertUpdate(format("CREATE TABLE %s.%s AS SELECT name FROM nation", schemaName, tableName2), "SELECT count(*) FROM nation");
        assertQuery(format("SELECT * FROM %s.%s", schemaName, tableName), "SELECT name FROM nation");
        assertQuery(format("SELECT * FROM %s.%s", schemaName, tableName2), "SELECT name FROM nation");
        validatePath(schemaLocation, schemaName, tableName);
        validatePath(schemaLocation, schemaName, tableName2);
    }

    @Test
    public void testCreateTableWithSchemaLocation()
    {
        String tableName = "table1_with_curr_schema_loc_" + randomNameSuffix();
        String tableName2 = "table2_with_curr_schema_loc_" + randomNameSuffix();
        String schemaName = "test_schema" + randomNameSuffix();
        String schemaLocation = getLocationForTable(bucketName, schemaName);
        assertUpdate(
                format("CREATE SCHEMA %s WITH ( location = '%s' )",
                        schemaName,
                        schemaLocation));
        assertUpdate(format("CREATE TABLE %s.%s (name VARCHAR)", schemaName, tableName));
        assertUpdate(format("CREATE TABLE %s.%s (name VARCHAR)", schemaName, tableName2));
        assertUpdate(format("INSERT INTO %s.%s SELECT name FROM nation", schemaName, tableName), "SELECT count(*) FROM nation");
        assertUpdate(format("INSERT INTO %s.%s SELECT name FROM nation", schemaName, tableName2), "SELECT count(*) FROM nation");
        assertQuery(format("SELECT * FROM %s.%s", schemaName, tableName), "SELECT name FROM nation");
        assertQuery(format("SELECT * FROM %s.%s", schemaName, tableName2), "SELECT name FROM nation");
        validatePath(schemaLocation, schemaName, tableName);
        validatePath(schemaLocation, schemaName, tableName2);
    }

    private void validatePath(String schemaLocation, String schemaName, String tableName)
    {
        List<MaterializedRow> materializedRows = getQueryRunner()
                .execute("SELECT DISTINCT regexp_replace(\"$path\", '(.*[/][^/]*)[/][^/]*$', '$1') FROM " + schemaName + "." + tableName)
                .getMaterializedRows();
        assertThat(materializedRows).hasSize(1);
        assertThat((String) materializedRows.get(0).getField(0)).matches(format("%s/%s.*", schemaLocation, tableName));
    }

    @Test
    @Override
    public void testRenameTable()
    {
        assertThatThrownBy(super::testRenameTable)
                .hasMessage("Renaming managed tables is not allowed with current metastore configuration")
                .hasStackTraceContaining("SQL: ALTER TABLE test_rename_");
    }

    @Test
    public void testRenameExternalTable()
    {
        String oldTable = "test_external_table_rename_old_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s (a bigint, b double) WITH (location = '%s')", oldTable, getLocationForTable(bucketName, oldTable)));
        assertUpdate("INSERT INTO " + oldTable + " VALUES (42, 43)", 1);
        String oldLocation = (String) computeScalar("SELECT \"$path\" FROM " + oldTable);

        String newTable = "test_rename_new_" + randomNameSuffix();
        assertUpdate("ALTER TABLE " + oldTable + " RENAME TO " + newTable);

        assertThat(query("SHOW TABLES LIKE '" + oldTable + "'"))
                .returnsEmptyResult();
        assertThat(query("SELECT a, b FROM " + newTable))
                .matches("VALUES (BIGINT '42', DOUBLE '43')");
        assertThat((String) computeScalar("SELECT \"$path\" FROM " + newTable))
                .isEqualTo(oldLocation);

        assertUpdate("INSERT INTO " + newTable + " (a, b) VALUES (42, -38.5)", 1);
        assertThat(query("SELECT a, b FROM " + newTable))
                .matches("VALUES (BIGINT '42', DOUBLE '43'), (42, -385e-1)");

        assertUpdate("DROP TABLE " + newTable);
    }

    @Test
    @Override
    public void testRenameTableAcrossSchemas()
    {
        assertThatThrownBy(super::testRenameTableAcrossSchemas)
                .hasMessage("Renaming managed tables is not allowed with current metastore configuration")
                .hasStackTraceContaining("SQL: ALTER TABLE test_rename_");
    }

    @Test
    public void testRenameExternalTableAcrossSchemas()
    {
        String oldTable = "test_rename_old_" + randomNameSuffix();
        assertUpdate(format("CREATE TABLE %s (a bigint, b double) WITH (location = '%s')", oldTable, getLocationForTable(bucketName, oldTable)));
        assertUpdate("INSERT INTO " + oldTable + " VALUES (42, 43)", 1);
        String oldLocation = (String) computeScalar("SELECT \"$path\" FROM " + oldTable);

        String schemaName = "test_schema_" + randomNameSuffix();
        assertUpdate(createSchemaSql(schemaName));

        String newTableName = "test_rename_new_" + randomNameSuffix();
        String newTable = schemaName + "." + newTableName;
        assertUpdate("ALTER TABLE " + oldTable + " RENAME TO " + newTable);

        assertThat(query("SHOW TABLES LIKE '" + oldTable + "'"))
                .returnsEmptyResult();
        assertThat(query("SELECT a, b FROM " + newTable))
                .matches("VALUES (BIGINT '42', DOUBLE '43')");
        assertThat((String) computeScalar("SELECT \"$path\" FROM " + newTable))
                .isEqualTo(oldLocation);

        assertUpdate("INSERT INTO " + newTable + " (a, b) VALUES (42, -38.5)", 1);
        assertThat(query("SELECT CAST(a AS bigint), b FROM " + newTable))
                .matches("VALUES (BIGINT '42', DOUBLE '43'), (42, -385e-1)");

        assertUpdate("DROP TABLE " + newTable);
        assertThat(query("SHOW TABLES LIKE '" + newTable + "'"))
                .returnsEmptyResult();

        assertUpdate("DROP SCHEMA " + schemaName);
    }

    @Test
    public void testOverrideSchemaLocation()
    {
        String tableName = "test_override_schema_location_" + randomNameSuffix();
        String schemaName = "test_override_schema_location_schema_" + randomNameSuffix();
        String schemaLocation = getLocationForTable(bucketName, schemaName);
        assertUpdate(
                format("CREATE SCHEMA %s WITH ( location = '%s' )",
                        schemaName,
                        schemaLocation));

        String tableLocation = getLocationForTable(bucketName, "a_different_directory") + "/" + tableName;
        assertUpdate(format("CREATE TABLE %s.%s WITH (location = '%s') AS SELECT * FROM nation", schemaName, tableName, tableLocation), "SELECT count(*) FROM nation");
        assertQuery(
                "SELECT DISTINCT regexp_replace(\"$path\", '(.*[/][^/]*)[/][^/]*$', '$1') FROM " + schemaName + "." + tableName,
                format("VALUES '%s'", tableLocation));
    }

    @Test
    public void testManagedTableFilesCleanedOnDrop()
    {
        String tableName = "test_managed_table_cleanup_" + randomNameSuffix();
        String schemaName = "test_managed_table_cleanup_" + randomNameSuffix();
        String schemaLocation = getLocationForTable(bucketName, schemaName);
        assertUpdate(format("CREATE SCHEMA %s WITH (location = '%s')", schemaName, schemaLocation));
        assertUpdate(format("CREATE TABLE %s.%s AS SELECT * FROM nation", schemaName, tableName), "SELECT count(*) FROM nation");
        assertThat(getTableFiles(schemaName + "/" + tableName).size()).isGreaterThan(0);
        assertUpdate(format("DROP TABLE %s.%s", schemaName, tableName));
        assertThat(getTableFiles(schemaName + "/" + tableName)).isEmpty();
    }

    @Test
    public void testExternalTableFilesRetainedOnDrop()
    {
        String tableName = "test_external_table_files_retained_" + randomNameSuffix();
        String schemaName = "test_external_table_files_retained_" + randomNameSuffix();
        String tableLocation = getLocationForTable(bucketName, tableName);
        String schemaLocation = getLocationForTable(bucketName, schemaName);
        assertUpdate(format("CREATE SCHEMA %s WITH (location = '%s')", schemaName, schemaLocation));
        assertUpdate(
                format("CREATE TABLE %s.%s WITH (location = '%s') AS SELECT * FROM nation", schemaName, tableName, tableLocation),
                "SELECT count(*) FROM nation");
        int fileCount = getTableFiles(tableName).size();
        assertUpdate(format("DROP TABLE %s.%s", schemaName, tableName));
        assertThat(getTableFiles(tableName)).hasSize(fileCount);
    }

    @Test
    public void testTimestampWithTimeZoneMillis()
    {
        String tableName = "test_timestamp_with_time_zone_" + randomNameSuffix();
        assertUpdate(
                format("CREATE TABLE " + tableName + " (ts_tz) WITH (location = '%s') AS " +
                                "VALUES timestamp '2012-10-31 01:00:00.123 America/New_York', timestamp '2012-10-31 01:00:00.123 America/Los_Angeles', timestamp '2012-10-31 01:00:00.123 UTC'",
                        getLocationForTable(bucketName, tableName)),
                3);
        assertQuery(
                "SELECT CAST(ts_tz AS VARCHAR) FROM " + tableName,
                "VALUES '2012-10-31 05:00:00.123 UTC', " +
                        "'2012-10-31 08:00:00.123 UTC', " +
                        "'2012-10-31 01:00:00.123 UTC'");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testTimestampWithTimeZoneMicro()
    {
        String tableName = "test_timestamp_with_time_zone_micro_" + randomNameSuffix();
        assertQueryFails(
                format("CREATE TABLE " + tableName + " (ts_tz) WITH (location = '%s') AS " +
                                "VALUES timestamp '2012-10-31 01:00:00.123456 America/New_York', timestamp '2012-10-31 01:00:00.123456 America/Los_Angeles'",
                        getLocationForTable(bucketName, tableName)),
                "Unsupported type:.*");
    }

    @Test
    public void testTimestampWithTimeZoneInArrayType()
    {
        String tableName = "test_timestamp_with_time_zone_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id int, data array(timestamp with time zone)) " +
                "WITH (location = '" + getLocationForTable(bucketName, tableName) + "') ");

        String values =
                """
                (1, ARRAY[timestamp '2012-01-01 01:02:03.123 UTC']),
                (2, ARRAY[NULL]),
                (3, NULL)
                """;
        assertUpdate("INSERT INTO " + tableName + " VALUES " + values, 3);

        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES " + values);
        assertThat(query("SELECT id FROM " + tableName + " WHERE data = ARRAY[timestamp '2012-01-01 01:02:03.123 UTC']"))
                .matches("VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testTimestampWithTimeZoneInMapType()
    {
        String tableName = "test_timestamp_with_time_zone_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id int, data map(timestamp with time zone, timestamp with time zone)) " +
                "WITH (location = '" + getLocationForTable(bucketName, tableName) + "') ");

        String values =
                """
                (1, MAP(ARRAY[timestamp '2012-01-01 01:02:03.123 UTC'], ARRAY[timestamp '2012-02-01 01:02:03.123 UTC'])),
                (2, MAP(ARRAY[timestamp '2023-12-31 03:02:01.321 UTC'], ARRAY[NULL])),
                (3, MAP(NULL, NULL)),
                (4, NULL)
                """;
        assertUpdate("INSERT INTO " + tableName + " VALUES " + values, 4);

        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES " + values);
        assertThat(query("SELECT id FROM " + tableName + " WHERE data = MAP(ARRAY[timestamp '2012-01-01 01:02:03.123 UTC'], ARRAY[timestamp '2012-02-01 01:02:03.123 UTC'])"))
                .matches("VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testTimestampWithTimeZoneInRowType()
    {
        String tableName = "test_timestamp_with_time_zone_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id int, data row(ts_tz timestamp with time zone)) " +
                "WITH (location = '" + getLocationForTable(bucketName, tableName) + "') ");

        String values =
                """
                (1, CAST(ROW(timestamp '2012-01-01 01:02:03.123 UTC') AS ROW(ts_tz timestamp with time zone))),
                (2, CAST(ROW(NULL) AS ROW(ts_tz timestamp with time zone))),
                (3, CAST(NULL AS ROW(ts_tz timestamp with time zone)))
                """;
        assertUpdate("INSERT INTO " + tableName + " VALUES " + values, 3);

        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES " + values);
        assertThat(query("SELECT id FROM " + tableName + " WHERE data = ROW(timestamp '2012-01-01 01:02:03.123 UTC')"))
                .matches("VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testTimestampWithTimeZoneInNestedField()
    {
        String tableName = "test_timestamp_with_time_zone_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id int, parent row(child row(grandchild timestamp with time zone))) " +
                "WITH (location = '" + getLocationForTable(bucketName, tableName) + "') ");

        String values =
                """
                (1, CAST(ROW(ROW(timestamp '2012-01-01 01:02:03.123 UTC')) AS ROW(child ROW(grandchild timestamp with time zone)))),
                (2, CAST(ROW(ROW(NULL)) AS ROW(child ROW(grandchild timestamp with time zone)))),
                (3, CAST(NULL AS ROW(child ROW(grandchild timestamp with time zone))))
                """;
        assertUpdate("INSERT INTO " + tableName + " VALUES " + values, 3);

        assertThat(query("SELECT * FROM " + tableName))
                .matches("VALUES " + values);
        assertThat(query("SELECT id FROM " + tableName + " WHERE parent = ROW(ROW(timestamp '2012-01-01 01:02:03.123 UTC'))"))
                .matches("VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testTimestampWithTimeZoneInComplexTypesFails()
    {
        String location = getLocationForTable("delta", "foo");
        assertQueryFails(
                "CREATE TABLE should_fail (a, b) WITH (location = '" + location + "') AS VALUES (ROW(timestamp '2012-10-31 01:00:00.1234 UTC', timestamp '2012-10-31 01:00:00.4321 UTC'), 1)",
                "Unsupported type:.*");
        assertQueryFails(
                "CREATE TABLE should_fail (a) WITH (location = '" + location + "') AS VALUES ARRAY[timestamp '2012-10-31 01:00:00.1234 UTC', timestamp '2012-10-31 01:00:00.4321 UTC']",
                "Unsupported type:.*");
        assertQueryFails(
                "CREATE TABLE should_fail (a) WITH (location = '" + location + "') AS VALUES MAP(ARRAY[ARRAY[timestamp '2012-10-31 01:00:00.1234 UTC', timestamp '2012-10-31 01:00:00.4321 UTC']], ARRAY[42])",
                "Unsupported type:.*");
        assertQueryFails(
                "CREATE TABLE should_fail (a) WITH (location = '" + location + "') AS VALUES MAP(ARRAY[42], ARRAY[ARRAY[timestamp '2012-10-31 01:00:00.1234 UTC', timestamp '2012-10-31 01:00:00.4321 UTC']])",
                "Unsupported type:.*");
    }

    @Test
    public void testSelectOldDate()
    {
        // Due to calendar shifts the read value is different than the written value.
        // This means that the Trino value falls outside of the Databricks min/max values from the file statistics
        assertQuery("SELECT * FROM old_dates", "VALUES (DATE '0099-12-30', 1), (DATE '1582-10-15', 2), (DATE '1960-01-01', 3), (DATE '2020-01-01', 4)");
        assertQuery("SELECT * FROM old_dates WHERE d = DATE '0099-12-30'", "VALUES (DATE '0099-12-30', 1)");
        assertQuery("SELECT * FROM old_dates WHERE d = DATE '1582-10-15'", "VALUES (DATE '1582-10-15', 2)");
        assertQuery("SELECT * FROM old_dates WHERE d = DATE '1960-01-01'", "VALUES (DATE '1960-01-01', 3)");
        assertQuery("SELECT * FROM old_dates WHERE d = DATE '2020-01-01'", "VALUES (DATE '2020-01-01', 4)");
    }

    @Test
    public void testSelectOldTimestamps()
    {
        assertQuery(
                "SELECT CAST(ts AS VARCHAR), i FROM old_timestamps",
                "VALUES ('0099-12-30 01:02:03.000 UTC', 1), ('1582-10-15 01:02:03.000 UTC', 2), ('1960-01-01 01:02:03.000 UTC', 3), ('2020-01-01 01:02:03.000 UTC', 4);");
        assertQuery("SELECT CAST(ts AS VARCHAR), i FROM old_timestamps WHERE ts = TIMESTAMP '0099-12-30 01:02:03 UTC'", "VALUES ('0099-12-30 01:02:03.000 UTC', 1)");
        assertQuery("SELECT CAST(ts AS VARCHAR), i FROM old_timestamps WHERE ts = TIMESTAMP '1582-10-15 01:02:03 UTC'", "VALUES ('1582-10-15 01:02:03.000 UTC', 2)");
        assertQuery("SELECT CAST(ts AS VARCHAR), i FROM old_timestamps WHERE ts = TIMESTAMP '1960-01-01 01:02:03 UTC'", "VALUES ('1960-01-01 01:02:03.000 UTC', 3)");
        assertQuery("SELECT CAST(ts AS VARCHAR), i FROM old_timestamps WHERE ts = TIMESTAMP '2020-01-01 01:02:03 UTC'", "VALUES ('2020-01-01 01:02:03.000 UTC', 4)");
    }

    @Test
    public void testSelectNestedTimestamps()
    {
        assertQuery("SELECT CAST(col1[1].ts AS VARCHAR) FROM nested_timestamps", "VALUES '2010-02-03 12:11:10.000 UTC'");
        assertQuery("SELECT CAST(col1[1].ts AS VARCHAR) FROM nested_timestamps_parquet_stats LIMIT 1", "VALUES '2010-02-03 12:11:10.000 UTC'");
    }

    @Test
    public void testConvertJsonStatisticsToParquetOnRowType()
            throws Exception
    {
        assertQuery("SELECT count(*) FROM json_stats_on_row_type", "VALUES 2");
        String transactionLogDirectory = "json_stats_on_row_type/_delta_log";
        String tableLocation = getLocationForTable(bucketName, "json_stats_on_row_type");
        String newTransactionFile = tableLocation + "/_delta_log/00000000000000000004.json";
        String newCheckpointFile = tableLocation + "/_delta_log/00000000000000000004.checkpoint.parquet";
        assertThat(getTableFiles(transactionLogDirectory))
                .doesNotContain(newTransactionFile, newCheckpointFile);

        assertUpdate("INSERT INTO json_stats_on_row_type SELECT CAST(row(3) AS row(x bigint)), CAST(row(row('test insert')) AS row(y row(nested varchar)))", 1);
        assertThat(getTableFiles(transactionLogDirectory))
                .contains(newTransactionFile, newCheckpointFile);

        // The first two entries created by Databricks have column stats.
        // The last one doesn't have column stats because the connector doesn't support collecting it on row columns.
        List<AddFileEntry> addFileEntries = getTableActiveFiles(transactionLogAccess, tableLocation).stream()
                .sorted(comparing(AddFileEntry::getModificationTime))
                .toList();
        assertThat(addFileEntries).hasSize(3);
        assertJsonStatistics(
                addFileEntries.get(0),
                "{" +
                        "\"numRecords\":1," +
                        "\"minValues\":{\"nested_struct_col\":{\"y\":{\"nested\":\"test\"}},\"struct_col\":{\"x\":1}}," +
                        "\"maxValues\":{\"nested_struct_col\":{\"y\":{\"nested\":\"test\"}},\"struct_col\":{\"x\":1}}," +
                        "\"nullCount\":{\"struct_col\":{\"x\":0},\"nested_struct_col\":{\"y\":{\"nested\":0}}}" +
                        "}");
        assertJsonStatistics(
                addFileEntries.get(1),
                "{" +
                        "\"numRecords\":1," +
                        "\"minValues\":{\"nested_struct_col\":{\"y\":{}},\"struct_col\":{}}," +
                        "\"maxValues\":{\"nested_struct_col\":{\"y\":{}},\"struct_col\":{}}," +
                        "\"nullCount\":{\"struct_col\":{\"x\":1},\"nested_struct_col\":{\"y\":{\"nested\":1}}}" +
                        "}");
        assertJsonStatistics(
                addFileEntries.get(2),
                "{\"numRecords\":1,\"minValues\":{},\"maxValues\":{},\"nullCount\":{}}");
    }

    private static void assertJsonStatistics(AddFileEntry addFileEntry, @Language("JSON") String jsonStatistics)
    {
        assertThat(addFileEntry.getStatsString().orElseThrow(() ->
                new AssertionError("statsString is empty: " + addFileEntry)))
                .isEqualTo(jsonStatistics);
    }

    @Test
    public void testMissingParquetStats()
    {
        assertQuery("SELECT count(*) FROM parquet_stats_missing WHERE i IS NULL", "VALUES 1");
        assertQuery("SELECT max(i) FROM parquet_stats_missing", "VALUES 8");
        assertQuery("SELECT min(i) FROM parquet_stats_missing", "VALUES 1");
    }

    @Test
    public void testUppercaseColumnNames()
    {
        assertQuery("SELECT * FROM uppercase_columns", "VALUES (1,1), (1,2), (2,1)");
        assertQuery("SELECT * FROM uppercase_columns WHERE ALA=1", "VALUES (1,1), (1,2)");
        assertQuery("SELECT * FROM uppercase_columns WHERE ala=1", "VALUES (1,1), (1,2)");
        assertQuery("SELECT * FROM uppercase_columns WHERE KOTA=1", "VALUES (1,1), (2,1)");
        assertQuery("SELECT * FROM uppercase_columns WHERE kota=1", "VALUES (1,1), (2,1)");
    }

    @Test
    public void testInsertIntoNonLowercaseColumnTable()
    {
        assertQuery(
                "SELECT * FROM insert_nonlowercase_columns",
                "VALUES " +
                        "('databricks', 'DATABRICKS', 'DaTaBrIcKs')," +
                        "('databricks', 'DATABRICKS', NULL)," +
                        "(NULL, NULL, 'DaTaBrIcKs')," +
                        "(NULL, NULL, NULL)");

        assertUpdate("INSERT INTO insert_nonlowercase_columns VALUES ('trino', 'TRINO', 'TrInO'), ('trino', 'TRINO', NULL)", 2);
        assertUpdate("INSERT INTO insert_nonlowercase_columns VALUES (NULL, NULL, 'TrInO'), (NULL, NULL, NULL)", 2);

        assertQuery(
                "SELECT * FROM insert_nonlowercase_columns",
                "VALUES " +
                        "('databricks', 'DATABRICKS', 'DaTaBrIcKs')," +
                        "('databricks', 'DATABRICKS', NULL)," +
                        "(NULL, NULL, 'DaTaBrIcKs')," +
                        "(NULL, NULL, NULL), " +
                        "('trino', 'TRINO', 'TrInO')," +
                        "('trino', 'TRINO', NULL)," +
                        "(NULL, NULL, 'TrInO')," +
                        "(NULL, NULL, NULL)");

        assertQuery(
                "SHOW STATS FOR insert_nonlowercase_columns",
                "VALUES " +
                        //  column_name | data_size | distinct_values_count | nulls_fraction | row_count | low_value | high_value
                        "('lower_case_string', 10.0, 1.0, 0.5, null, null, null)," +
                        "('upper_case_string', 10.0, 1.0, 0.5, null, null, null)," +
                        "('mixed_case_string', 10.0, 1.0, 0.5, null, null, null)," +
                        "(null, null, null, null, 8.0, null, null)");
    }

    @Test
    public void testInsertNestedNonLowercaseColumns()
    {
        assertQuery(
                "SELECT an_int, nested.lower_case_string, nested.upper_case_string, nested.mixed_case_string FROM insert_nested_nonlowercase_columns",
                "VALUES " +
                        "(1, 'databricks', 'DATABRICKS', 'DaTaBrIcKs')," +
                        "(2, 'databricks', 'DATABRICKS', NULL)," +
                        "(3, NULL, NULL, 'DaTaBrIcKs')," +
                        "(4, NULL, NULL, NULL)");

        assertUpdate("INSERT INTO insert_nested_nonlowercase_columns VALUES " +
                        "(10, ROW('trino', 'TRINO', 'TrInO'))," +
                        "(20, ROW('trino', 'TRINO', NULL))," +
                        "(30, ROW(NULL, NULL, 'TrInO'))," +
                        "(40, ROW(NULL, NULL, NULL))",
                4);

        assertQuery(
                "SELECT an_int, nested.lower_case_string, nested.upper_case_string, nested.mixed_case_string FROM insert_nested_nonlowercase_columns",
                "VALUES " +
                        "(1, 'databricks', 'DATABRICKS', 'DaTaBrIcKs')," +
                        "(2, 'databricks', 'DATABRICKS', NULL)," +
                        "(3, NULL, NULL, 'DaTaBrIcKs')," +
                        "(4, NULL, NULL, NULL)," +
                        "(10, 'trino', 'TRINO', 'TrInO')," +
                        "(20, 'trino', 'TRINO', NULL)," +
                        "(30, NULL, NULL, 'TrInO')," +
                        "(40, NULL, NULL, NULL)");

        assertQuery(
                "SHOW STATS FOR insert_nested_nonlowercase_columns",
                "VALUES " +
                        //  column_name | data_size | distinct_values_count | nulls_fraction | row_count | low_value | high_value
                        "('an_int', null, 4.0, 0.0, null, 1, 40)," +
                        "('nested', null, null, null, null, null, null)," +
                        "(null, null, null, null, 8.0, null, null)");
    }

    @Test
    public void testInsertIntoPartitionedTable()
    {
        String tableName = "test_insert_partitioned_" + randomNameSuffix();
        assertUpdate(
                format("CREATE TABLE %s (a_number, a_string) " +
                                " WITH (location = '%s', " +
                                "       partitioned_by = ARRAY['a_number']) " +
                                " AS VALUES (1, 'ala')",
                        tableName,
                        getLocationForTable(bucketName, tableName)),
                1);

        assertUpdate(format("INSERT INTO %s VALUES (2, 'kota'), (3, 'psa')", tableName), 2);
        assertUpdate(format("INSERT INTO %s VALUES (2, 'bobra'), (5, 'kreta')", tableName), 2);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1,'ala'), (2,'kota'), (3,'psa'), (2, 'bobra'), (5, 'kreta')");
        assertQuery(
                "SELECT DISTINCT regexp_replace(\"$path\", '.*[/]([^/]*)[/][^/]*$', '$1') FROM " + tableName,
                "VALUES 'a_number=1', 'a_number=2', 'a_number=3', 'a_number=5'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInsertIntoPartitionedNonLowercaseColumnTable()
    {
        assertQuery(
                "SELECT * FROM insert_nonlowercase_columns_partitioned",
                "VALUES " +
                        "('databricks', 'DATABRICKS', 'DaTaBrIcKs')," +
                        "('databricks', 'DATABRICKS', NULL)," +
                        "(NULL, NULL, 'DaTaBrIcKs')," +
                        "(NULL, NULL, NULL)");

        assertUpdate("INSERT INTO insert_nonlowercase_columns_partitioned VALUES ('trino', 'TRINO', 'TrInO'), ('trino', 'TRINO', NULL)", 2);
        assertUpdate("INSERT INTO insert_nonlowercase_columns_partitioned VALUES (NULL, NULL, 'TrInO'), (NULL, NULL, NULL)", 2);

        assertQuery(
                "SELECT * FROM insert_nonlowercase_columns_partitioned",
                "VALUES " +
                        "('databricks', 'DATABRICKS', 'DaTaBrIcKs')," +
                        "('databricks', 'DATABRICKS', NULL)," +
                        "(NULL, NULL, 'DaTaBrIcKs')," +
                        "(NULL, NULL, NULL), " +
                        "('trino', 'TRINO', 'TrInO')," +
                        "('trino', 'TRINO', NULL)," +
                        "(NULL, NULL, 'TrInO')," +
                        "(NULL, NULL, NULL)");

        assertQuery(
                "SELECT DISTINCT regexp_replace(\"$path\", '.*[/]([^/]*)[/][^/]*$', '$1') FROM insert_nonlowercase_columns_partitioned",
                "VALUES 'MiXeD_CaSe_StRiNg=DaTaBrIcKs', 'MiXeD_CaSe_StRiNg=__HIVE_DEFAULT_PARTITION__', 'MiXeD_CaSe_StRiNg=TrInO'");

        assertQuery(
                "SHOW STATS FOR insert_nonlowercase_columns_partitioned",
                "VALUES " +
                        //  column_name | data_size | distinct_values_count | nulls_fraction | row_count | low_value | high_value
                        "('lower_case_string', 10.0, 1.0, 0.5, null, null, null)," +
                        "('upper_case_string', 10.0, 1.0, 0.5, null, null, null)," +
                        "('mixed_case_string', null, 2.0, 0.5, null, null, null)," +
                        "(null, null, null, null, 8.0, null, null)");
    }

    @Test
    public void testPartialInsert()
    {
        String tableName = "test_partial_insert_" + randomNameSuffix();
        assertUpdate(
                format("CREATE TABLE %s (a_number, a_string) WITH (location = '%s') AS " +
                                "VALUES (1, 'ala')",
                        tableName,
                        getLocationForTable(bucketName, tableName)),
                1);
        assertUpdate(format("INSERT INTO %s(a_number) VALUES (2), (3)", tableName), 2);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'ala'), (2, NULL), (3, NULL)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPartialInsertIntoPartitionedTable()
    {
        String tableName = "test_partial_insert_partitioned_" + randomNameSuffix();
        assertUpdate(
                format("CREATE TABLE %s (a_number, a_string) " +
                                " WITH (location = '%s', " +
                                "       partitioned_by = ARRAY['a_number']) " +
                                " AS VALUES (1, 'ala')",
                        tableName,
                        getLocationForTable(bucketName, tableName)),
                1);

        assertUpdate(format("INSERT INTO %s(a_number) VALUES (2), (3)", tableName), 2);
        assertUpdate(format("INSERT INTO %s(a_string) VALUES ('lwa')", tableName), 1);

        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'ala'), (2, NULL), (3, NULL), (NULL, 'lwa')");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInsertColumnOrdering()
    {
        String tableName = "test_insert_column_ordering_" + randomNameSuffix();
        assertUpdate(
                format("CREATE TABLE %s (a INT, b INT, c INT) WITH (location = '%s', partitioned_by = ARRAY['a', 'b'])",
                        tableName,
                        getLocationForTable(bucketName, tableName)));
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 2, 3), (4, 5, 6)", 2);
        assertUpdate("INSERT INTO " + tableName + " (c, b, a) VALUES (9, 8, 7)", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9)");
    }

    @Test
    public void testDefaultPartitions()
    {
        assertQuery(
                "SELECT * FROM default_partitions",
                "VALUES (NULL, 'partition_a', 'jarmuz'), (1, NULL, 'brukselka'), (NULL, NULL, 'kalafior')");
        assertQuery(
                "SELECT * FROM default_partitions WHERE number_partition IS NULL",
                "VALUES (NULL, 'partition_a', 'jarmuz'), (NULL, NULL, 'kalafior')");
        assertQuery(
                "SELECT * FROM default_partitions WHERE number_partition IS NOT NULL",
                "VALUES (1, NULL, 'brukselka')");
        assertQuery(
                "SELECT * FROM default_partitions WHERE string_partition IS NULL",
                "VALUES (1, NULL, 'brukselka'), (NULL, NULL, 'kalafior')");
        assertQuery(
                "SELECT * FROM default_partitions WHERE string_partition IS NOT NULL",
                "VALUES (NULL, 'partition_a', 'jarmuz')");
        assertQuery(
                "SELECT * FROM default_partitions WHERE number_partition > 0",
                "VALUES (1, NULL, 'brukselka')");
    }

    @Test
    public void testCheckpointing()
    {
        String tableName = "test_insert_checkpointing_" + randomNameSuffix();
        assertUpdate(
                format("CREATE TABLE %s (a_number, a_string) " +
                                " WITH (location = '%s', " +
                                "       partitioned_by = ARRAY['a_number'], " +
                                "       checkpoint_interval = 5) " +
                                " AS VALUES (1, 'ala')",
                        tableName,
                        getLocationForTable(bucketName, tableName)),
                1);
        String transactionLogDirectory = format("%s/_delta_log", tableName);

        assertUpdate(format("INSERT INTO %s VALUES (2, 'kota'), (3, 'psa')", tableName), 2);
        assertUpdate(format("INSERT INTO %s VALUES (2, 'bobra'), (5, 'kreta')", tableName), 2);
        assertUpdate(format("DELETE FROM %s WHERE a_string = 'kota'", tableName), 1);
        assertUpdate(format("DELETE FROM %s WHERE a_string = 'kreta'", tableName), 1);

        // sanity check
        assertThat(listCheckpointFiles(transactionLogDirectory)).isEmpty();
        assertQuery("SELECT * FROM " + tableName, "VALUES (1,'ala'),  (3,'psa'), (2, 'bobra')");

        // fill to first checkpoint which reads JSON files
        fillWithInserts(tableName, "(1, 'fill')", 1);
        assertThat(listCheckpointFiles(transactionLogDirectory)).hasSize(1);
        assertQuery("SELECT * FROM " + tableName + " WHERE a_string <> 'fill'", "VALUES (1,'ala'),  (3,'psa'), (2, 'bobra')");

        // fill to another checkpoint which reads previous checkpoint
        fillWithInserts(tableName, "(1, 'fill')", 5);
        assertThat(listCheckpointFiles(transactionLogDirectory)).hasSize(2);
        assertQuery("SELECT * FROM " + tableName + " WHERE a_string <> 'fill'", "VALUES (1,'ala'),  (3,'psa'), (2, 'bobra')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCheckpointWriteStatsAsStruct()
    {
        testCheckpointWriteStatsAsStruct("boolean", "true", "false", "0.0", "null", "null");
        testCheckpointWriteStatsAsStruct("integer", "1", "2147483647", "0.0", "1", "2147483647");
        testCheckpointWriteStatsAsStruct("tinyint", "2", "127", "0.0", "2", "127");
        testCheckpointWriteStatsAsStruct("smallint", "3", "32767", "0.0", "3", "32767");
        testCheckpointWriteStatsAsStruct("bigint", "1000", "9223372036854775807", "0.0", "1000", "9223372036854775807");
        testCheckpointWriteStatsAsStruct("real", "0.1", "999999.999", "0.0", "0.1", "1000000.0");
        testCheckpointWriteStatsAsStruct("double", "1.0", "9999999999999.999", "0.0", "1.0", "'1.0E13'");
        testCheckpointWriteStatsAsStruct("decimal(3,2)", "3.14", "9.99", "0.0", "3.14", "9.99");
        testCheckpointWriteStatsAsStruct("decimal(30,1)", "12345", "99999999999999999999999999999.9", "0.0", "12345.0", "'1.0E29'");
        testCheckpointWriteStatsAsStruct("varchar", "'test'", "'ŻŻŻŻŻŻŻŻŻŻ'", "0.0", "null", "null");
        testCheckpointWriteStatsAsStruct("varbinary", "X'65683F'", "X'ffffffffffffffffffff'", "0.0", "null", "null");
        testCheckpointWriteStatsAsStruct("date", "date '2021-02-03'", "date '9999-12-31'", "0.0", "'2021-02-03'", "'9999-12-31'");
        testCheckpointWriteStatsAsStruct("timestamp(3) with time zone", "timestamp '2001-08-22 03:04:05.321 -08:00'", "timestamp '9999-12-31 23:59:59.999 +12:00'", "0.0", "'2001-08-22 11:04:05.321 UTC'", "'9999-12-31 11:59:59.999 UTC'");
        testCheckpointWriteStatsAsStruct("array(int)", "array[1]", "array[2147483647]", "null", "null", "null");
        testCheckpointWriteStatsAsStruct("map(varchar,int)", "map(array['foo', 'bar'], array[1, 2])", "map(array['foo', 'bar'], array[-2147483648, 2147483647])", "null", "null", "null");
        testCheckpointWriteStatsAsStruct("row(x bigint)", "cast(row(1) as row(x bigint))", "cast(row(9223372036854775807) as row(x bigint))", "null", "null", "null");
    }

    private void testCheckpointWriteStatsAsStruct(String type, String sampleValue, String highValue, String nullsFraction, String minValue, String maxValue)
    {
        String tableName = "test_checkpoint_write_stats_as_struct_" + randomNameSuffix();

        // Set 'checkpoint_interval' as 1 to write 'stats_parsed' field every INSERT
        assertUpdate(
                format("CREATE TABLE %s (col %s) WITH (location = '%s', checkpoint_interval = 1)",
                        tableName,
                        type,
                        getLocationForTable(bucketName, tableName)));
        assertUpdate(
                disableStatisticsCollectionOnWrite(getSession()),
                "INSERT INTO " + tableName + " SELECT " + sampleValue + " UNION ALL SELECT " + highValue, 2);

        // TODO: Open checkpoint parquet file and verify 'stats_parsed' field directly
        assertThat(getTableFiles(tableName))
                .contains(getTableLocation(tableName) + "/_delta_log/_last_checkpoint");

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('col', null, null, " + nullsFraction + ", null, " + minValue + ", " + maxValue + ")," +
                        "(null, null, null, null, 2.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCheckpointWriteStatsAsStructWithPartiallyUnsupportedColumnStats()
    {
        String tableName = "test_checkpoint_write_stats_as_struct_partially_unsupported_" + randomNameSuffix();

        // Column statistics on boolean column is unsupported
        assertUpdate(
                format("CREATE TABLE %s (col integer, unsupported boolean) WITH (location = '%s', checkpoint_interval = 1)",
                        tableName,
                        getLocationForTable(bucketName, tableName)));
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, true)", 1);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('col', null, 1.0, 0.0, null, 1, 1)," +
                        "('unsupported', null, 1.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 1.0, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateOrReplaceCheckpointing()
    {
        String tableName = "test_create_or_replace_checkpointing_" + randomNameSuffix();
        assertUpdate(
                format("CREATE OR REPLACE TABLE %s (a_number, a_string) " +
                                " WITH (location = '%s', " +
                                "       partitioned_by = ARRAY['a_number']) " +
                                " AS VALUES (1, 'ala')",
                        tableName,
                        getLocationForTable(bucketName, tableName)),
                1);
        String transactionLogDirectory = format("%s/_delta_log", tableName);

        assertUpdate(format("INSERT INTO %s VALUES (2, 'kota'), (3, 'psa')", tableName), 2);
        assertThat(listCheckpointFiles(transactionLogDirectory)).isEmpty();
        assertQuery("SELECT * FROM " + tableName, "VALUES (1,'ala'),  (2,'kota'), (3, 'psa')");

        // replace table
        assertUpdate(
                format("CREATE OR REPLACE TABLE %s (a_number integer) " +
                                " WITH (checkpoint_interval = 2)",
                        tableName));
        assertThat(listCheckpointFiles(transactionLogDirectory)).hasSize(1);
        assertThat(query("SELECT * FROM " + tableName)).returnsEmptyResult();

        assertUpdate(format("INSERT INTO " + tableName + " VALUES 1", tableName), 1);
        assertThat(listCheckpointFiles(transactionLogDirectory)).hasSize(1);
        assertQuery("SELECT * FROM " + tableName, "VALUES 1");

        // replace table with selection
        assertUpdate(
                format("CREATE OR REPLACE TABLE %s (a_string) " +
                                " WITH (checkpoint_interval = 2) " +
                                " AS VALUES 'bobra', 'kreta'",
                        tableName),
                2);
        assertThat(listCheckpointFiles(transactionLogDirectory)).hasSize(2);
        assertQuery("SELECT * FROM " + tableName, "VALUES 'bobra', 'kreta'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDeltaLakeTableLocationChanged()
            throws Exception
    {
        testDeltaLakeTableLocationChanged(true, false, false);
    }

    @Test
    public void testDeltaLakeTableLocationChangedSameVersionNumber()
            throws Exception
    {
        testDeltaLakeTableLocationChanged(false, false, false);
    }

    @Test
    public void testDeltaLakeTableLocationChangedPartitioned()
            throws Exception
    {
        testDeltaLakeTableLocationChanged(true, true, false);
        testDeltaLakeTableLocationChanged(true, false, true);
        testDeltaLakeTableLocationChanged(true, true, true);
    }

    private void testDeltaLakeTableLocationChanged(boolean fewerEntries, boolean firstPartitioned, boolean secondPartitioned)
            throws Exception
    {
        // Create a table with a bunch of transaction log entries
        String tableName = "test_table_location_changed_" + randomNameSuffix();
        String initialLocation = getLocationForTable(bucketName, tableName);
        assertUpdate(format(
                "CREATE TABLE %s (a_number int, a_string varchar) WITH (location = '%s' %s)",
                tableName,
                initialLocation,
                firstPartitioned ? ", partitioned_by = ARRAY['a_number']" : ""));

        BiConsumer<QueryRunner, String> insertABunchOfRows = (queryRunner, prefix) -> {
            queryRunner.execute(format("INSERT INTO %s (a_number, a_string) VALUES (1, '%s one')", tableName, prefix));
            queryRunner.execute(format("INSERT INTO %s (a_number, a_string) VALUES (2, '%s two')", tableName, prefix));
            queryRunner.execute(format("INSERT INTO %s (a_number, a_string) VALUES (3, '%s tree')", tableName, prefix));
            queryRunner.execute(format("INSERT INTO %s (a_number, a_string) VALUES (4, '%s four')", tableName, prefix));
        };

        insertABunchOfRows.accept(getQueryRunner(), "first");

        MaterializedResult initialData = computeActual("SELECT * FROM " + tableName);
        assertThat(initialData.getMaterializedRows()).hasSize(4);

        MaterializedResult expectedDataAfterChange;
        String newLocation;
        try (QueryRunner independentQueryRunner = createDeltaLakeQueryRunner()) {
            // Change table's location without main Delta Lake connector (main query runner) knowing about this
            newLocation = getLocationForTable(bucketName, "test_table_location_changed_new_" + randomNameSuffix());

            independentQueryRunner.execute("DROP TABLE " + tableName);
            independentQueryRunner.execute(format(
                    "CREATE TABLE %s (a_number int, a_string varchar, another_string varchar) WITH (location = '%s' %s) ",
                    tableName,
                    newLocation,
                    secondPartitioned ? ", partitioned_by = ARRAY['a_number']" : ""));

            if (fewerEntries) {
                // Have fewer transaction log entries so that version mismatch is more apparent (but easier to detect)
                independentQueryRunner.execute(format("INSERT INTO %s VALUES (1, 'second one', 'third column')", tableName));
            }
            else {
                insertABunchOfRows.accept(independentQueryRunner, "second");
            }

            expectedDataAfterChange = independentQueryRunner.execute("SELECT * FROM " + tableName);
            assertThat(expectedDataAfterChange.getMaterializedRows()).hasSize(fewerEntries ? 1 : 4);
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        while (true) {
            MaterializedResult currentVisibleData = computeActual("SELECT * FROM " + tableName);
            if (Set.copyOf(currentVisibleData.getMaterializedRows()).equals(Set.copyOf(expectedDataAfterChange.getMaterializedRows()))) {
                // satisfied
                break;
            }
            if (!Set.copyOf(currentVisibleData.getMaterializedRows()).equals(Set.copyOf(initialData.getMaterializedRows()))) {
                throw new AssertionError(format(
                        "Unexpected result when reading table: %s,\n expected either initialData: %s\n or expectedDataAfterChange: %s",
                        currentVisibleData,
                        initialData,
                        expectedDataAfterChange));
            }

            if (stopwatch.elapsed(SECONDS) > TEST_METADATA_CACHE_TTL_SECONDS + 10) {
                throw new RuntimeException("Timed out waiting on table to reflect new data from new location");
            }

            SECONDS.sleep(1);
        }

        // Verify table schema gets reflected correctly
        String qualifiedTableName = "%s.%s.%s".formatted(getSession().getCatalog().orElseThrow(), SCHEMA, tableName);
        assertThat(computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo("" +
                        "CREATE TABLE " + qualifiedTableName + " (\n" +
                        "   a_number integer,\n" +
                        "   a_string varchar,\n" +
                        "   another_string varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   location = '" + newLocation + "'" + (secondPartitioned ? "," : "") + "\n" +
                        (secondPartitioned ? "   partitioned_by = ARRAY['a_number']\n" : "") +
                        ")");
    }

    /**
     * Smoke test for compatibility with different file systems; verbose
     * testing in {@link TestDeltaLakeAnalyze}.
     */
    @Test
    public void testAnalyze()
    {
        String tableName = "test_analyze_" + randomNameSuffix();
        assertUpdate(
                disableStatisticsCollectionOnWrite(getSession()),
                "CREATE TABLE " + tableName
                        + " WITH ("
                        + "location = '" + getLocationForTable(bucketName, tableName) + "'"
                        + ")"
                        + " AS SELECT * FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, null, 0.0, null, 0, 24)," +
                        "('regionkey', null, null, 0.0, null, 0, 4)," +
                        "('comment', null, null, 0.0, null, null, null)," +
                        "('name', null, null, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");

        getQueryRunner().execute("ANALYZE " + tableName);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25.0, 0.0, null, 0, 24)," +
                        "('regionkey', null, 5.0, 0.0, null, 0, 4)," +
                        "('comment', 1857.0, 25.0, 0.0, null, null, null)," +
                        "('name', 177.0, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");
    }

    @Test
    public void testStatsSplitPruningBasedOnSepCreatedCheckpoint()
    {
        String tableName = "test_sep_checkpoint_stats_pruning_" + randomNameSuffix();
        String transactionLogDirectory = format("%s/_delta_log", tableName);
        assertUpdate(
                format("CREATE TABLE %s (a_number, a_string)" +
                                " WITH (location = '%s')" +
                                " AS VALUES (1, 'ala')",
                        tableName,
                        getLocationForTable(bucketName, tableName)),
                1);

        assertUpdate(format("INSERT INTO %s VALUES (2, 'kota')", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES (3, 'kota')", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES (4, 'kota')", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES (5, 'kota')", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES (6, 'kota')", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES (7, 'kota')", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES (8, 'kota')", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES (9, 'kota')", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES (10, 'kota')", tableName), 1);

        // there should not be a checkpoint yet
        assertThat(listCheckpointFiles(transactionLogDirectory)).isEmpty();
        testCountQuery(format("SELECT count(*) FROM %s WHERE a_number <= 3", tableName), 3, 3);

        // perform one more insert to ensure checkpoint
        assertUpdate(format("INSERT INTO %s VALUES (11, 'kota')", tableName), 1);
        assertThat(listCheckpointFiles(transactionLogDirectory)).hasSize(1);
        testCountQuery(format("SELECT count(*) FROM %s WHERE a_number <= 3", tableName), 3, 3);

        // split pruning should still be working when table metadata (snapshot, active files) is read anew
        invalidateMetadataCache(tableName);
        testCountQuery(format("SELECT count(*) FROM %s WHERE a_number <= 3", tableName), 3, 3);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testStatsSplitPruningBasedOnSepCreatedCheckpointOnTopOfCheckpointWithJustStructStats()
    {
        String tableName = "test_sep_checkpoint_stats_pruning_struct_stats_" + randomNameSuffix();
        registerTableFromResources(tableName, "databricks73/pruning/parquet_struct_statistics", getQueryRunner());
        String transactionLogDirectory = format("%s/_delta_log", tableName);

        // there should be one checkpoint already (created by DB)
        assertThat(listCheckpointFiles(transactionLogDirectory)).hasSize(1);
        testCountQuery(format("SELECT count(*) FROM %s WHERE l = 0", tableName), 3, 3);

        // fill in with extra transaction to force one more checkpoint made by Trino
        // the checkpoint will be based on DB checkpoint which includes only struct stats
        assertUpdate(format("INSERT INTO %s VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", tableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", tableName), 1);
        assertThat(listCheckpointFiles(transactionLogDirectory)).hasSize(2);

        // split pruning should still be working
        testCountQuery(format("SELECT count(*) FROM %s WHERE l = 0", tableName), 3, 3);

        // split pruning should still be working when table metadata (snapshot, active files) is read anew
        invalidateMetadataCache(tableName);
        testCountQuery(format("SELECT count(*) FROM %s WHERE l = 0", tableName), 3, 3);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testVacuum()
            throws Exception
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String tableName = "test_vacuum" + randomNameSuffix();
        String tableLocation = getLocationForTable(bucketName, tableName);
        Session sessionWithShortRetentionUnlocked = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "vacuum_min_retention", "0s")
                .build();
        assertUpdate(
                format("CREATE TABLE %s WITH (location = '%s', partitioned_by = ARRAY['regionkey']) AS SELECT * FROM tpch.tiny.nation", tableName, tableLocation),
                25);
        try {
            Set<String> initialFiles = getActiveFiles(tableName);
            assertThat(initialFiles).hasSize(5);

            computeActual("UPDATE " + tableName + " SET nationkey = nationkey + 100");
            Stopwatch timeSinceUpdate = Stopwatch.createStarted();
            Set<String> updatedFiles = getActiveFiles(tableName);
            assertThat(updatedFiles).hasSize(5).doesNotContainAnyElementsOf(initialFiles);
            assertThat(getAllDataFilesFromTableDirectory(tableName)).isEqualTo(union(initialFiles, updatedFiles));

            // vacuum with high retention period, nothing should change
            assertUpdate(sessionWithShortRetentionUnlocked, "CALL system.vacuum(schema_name => CURRENT_SCHEMA, table_name => '" + tableName + "', retention => '10m')");
            assertThat(query("SELECT * FROM " + tableName))
                    .matches("SELECT nationkey + 100, CAST(name AS varchar), regionkey, CAST(comment AS varchar) FROM tpch.tiny.nation");
            assertThat(getActiveFiles(tableName)).isEqualTo(updatedFiles);
            assertThat(getAllDataFilesFromTableDirectory(tableName)).isEqualTo(union(initialFiles, updatedFiles));

            // vacuum with low retention period
            MILLISECONDS.sleep(1_000 - timeSinceUpdate.elapsed(MILLISECONDS) + 1);
            assertUpdate(sessionWithShortRetentionUnlocked, "CALL system.vacuum(schema_name => CURRENT_SCHEMA, table_name => '" + tableName + "', retention => '1s')");
            // table data shouldn't change
            assertThat(query("SELECT * FROM " + tableName))
                    .matches("SELECT nationkey + 100, CAST(name AS varchar), regionkey, CAST(comment AS varchar) FROM tpch.tiny.nation");
            // active files shouldn't change
            assertThat(getActiveFiles(tableName)).isEqualTo(updatedFiles);
            // old files should be cleaned up
            assertThat(getAllDataFilesFromTableDirectory(tableName)).isEqualTo(updatedFiles);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testVacuumWithTrailingSlash()
            throws Exception
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String tableName = "test_vacuum" + randomNameSuffix();
        String tableLocation = getLocationForTable(bucketName, tableName) + "/";
        Session sessionWithShortRetentionUnlocked = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "vacuum_min_retention", "0s")
                .build();
        assertUpdate(
                format("CREATE TABLE %s WITH (location = '%s', partitioned_by = ARRAY['regionkey']) AS SELECT * FROM tpch.tiny.nation", tableName, tableLocation),
                25);
        try {
            Set<String> initialFiles = getActiveFiles(tableName);
            assertThat(initialFiles).hasSize(5);

            computeActual("UPDATE " + tableName + " SET nationkey = nationkey + 100");
            Stopwatch timeSinceUpdate = Stopwatch.createStarted();
            Set<String> updatedFiles = getActiveFiles(tableName);
            assertThat(updatedFiles).hasSize(5).doesNotContainAnyElementsOf(initialFiles);
            assertThat(getAllDataFilesFromTableDirectory(tableName)).isEqualTo(union(initialFiles, updatedFiles));

            // vacuum with high retention period, nothing should change
            assertUpdate(sessionWithShortRetentionUnlocked, "CALL system.vacuum(schema_name => CURRENT_SCHEMA, table_name => '" + tableName + "', retention => '10m')");
            assertThat(query("SELECT * FROM " + tableName))
                    .matches("SELECT nationkey + 100, CAST(name AS varchar), regionkey, CAST(comment AS varchar) FROM tpch.tiny.nation");
            assertThat(getActiveFiles(tableName)).isEqualTo(updatedFiles);
            assertThat(getAllDataFilesFromTableDirectory(tableName)).isEqualTo(union(initialFiles, updatedFiles));

            // vacuum with low retention period
            MILLISECONDS.sleep(1_000 - timeSinceUpdate.elapsed(MILLISECONDS) + 1);
            assertUpdate(sessionWithShortRetentionUnlocked, "CALL system.vacuum(schema_name => CURRENT_SCHEMA, table_name => '" + tableName + "', retention => '1s')");
            // table data shouldn't change
            assertThat(query("SELECT * FROM " + tableName))
                    .matches("SELECT nationkey + 100, CAST(name AS varchar), regionkey, CAST(comment AS varchar) FROM tpch.tiny.nation");
            // active files shouldn't change
            assertThat(getActiveFiles(tableName)).isEqualTo(updatedFiles);
            // old files should be cleaned up
            assertThat(getAllDataFilesFromTableDirectory(tableName)).isEqualTo(updatedFiles);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testVacuumParameterValidation()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String tableName = "test_vacuum_parameter_validation_" + randomNameSuffix();
        assertUpdate(
                format("CREATE TABLE %s WITH (location = '%s') AS SELECT * FROM tpch.tiny.nation", tableName, getLocationForTable(bucketName, tableName)),
                25);
        assertQueryFails("CALL system.vacuum(NULL, NULL, NULL)", "schema_name cannot be null");
        assertQueryFails("CALL system.vacuum(CURRENT_SCHEMA, NULL, NULL)", "table_name cannot be null");
        assertQueryFails("CALL system.vacuum(CURRENT_SCHEMA, '" + tableName + "')", "line 1:1: Required procedure argument 'RETENTION' is missing");
        assertQueryFails("CALL system.vacuum(CURRENT_SCHEMA, '" + tableName + "', NULL)", "retention cannot be null");
        assertQueryFails(
                "CALL system.vacuum(CURRENT_SCHEMA, '" + tableName + "', '1s')",
                "\\QRetention specified (1.00s) is shorter than the minimum retention configured in the system (7.00d). " +
                        "Minimum retention can be changed with delta.vacuum.min-retention configuration property or " + catalog + ".vacuum_min_retention session property");
        assertQueryFails("CALL system.vacuum('', '', '77d')", "schema_name cannot be empty");
        assertQueryFails("CALL system.vacuum(CURRENT_SCHEMA, '', '77d')", "table_name cannot be empty");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testVacuumAccessControl()
    {
        String tableName = "test_deny_vacuum_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (location = '" + getLocationForTable(bucketName, tableName) + "') " +
                "AS SELECT * FROM orders", "SELECT count(*) FROM orders");

        assertAccessDenied(
                "CALL system.vacuum(schema_name => CURRENT_SCHEMA, table_name => '" + tableName + "', retention => '30d')",
                "Cannot insert into table .*",
                privilege(tableName, INSERT_TABLE));
        assertAccessDenied(
                "CALL system.vacuum(schema_name => CURRENT_SCHEMA, table_name => '" + tableName + "', retention => '30d')",
                "Cannot delete from table .*",
                privilege(tableName, DELETE_TABLE));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testOptimize()
    {
        String tableName = "test_optimize_" + randomNameSuffix();
        String tableLocation = getLocationForTable(bucketName, tableName);
        assertUpdate("CREATE TABLE " + tableName + " (key integer, value varchar) WITH (location = '" + tableLocation + "')");
        try {
            // DistributedQueryRunner sets node-scheduler.include-coordinator by default, so include coordinator
            int workerCount = getQueryRunner().getNodeCount();

            // optimize an empty table
            assertQuerySucceeds("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
            assertThat(getActiveFiles(tableName)).isEmpty();

            assertUpdate("INSERT INTO " + tableName + " VALUES (11, 'eleven')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (12, 'zwölf')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (13, 'trzynaście')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (14, 'quatorze')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (15, 'пʼятнадцять')", 1);

            Set<String> initialFiles = getActiveFiles(tableName);
            assertThat(initialFiles)
                    .hasSize(5)
                    // Verify we have sufficiently many test rows with respect to worker count.
                    .hasSizeGreaterThan(workerCount);

            computeActual("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");
            assertThat(query("SELECT sum(key), listagg(value, ' ') WITHIN GROUP (ORDER BY key) FROM " + tableName))
                    .matches("VALUES (BIGINT '65', VARCHAR 'eleven zwölf trzynaście quatorze пʼятнадцять')");
            Set<String> updatedFiles = getActiveFiles(tableName);
            assertThat(updatedFiles)
                    .hasSizeBetween(1, workerCount)
                    .doesNotContainAnyElementsOf(initialFiles);
            // No files should be removed (this is VACUUM's job)
            assertThat(getAllDataFilesFromTableDirectory(tableName)).isEqualTo(union(initialFiles, updatedFiles));

            // optimize with low retention threshold, nothing should change
            computeActual("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE (file_size_threshold => '33B')");
            assertThat(query("SELECT sum(key), listagg(value, ' ') WITHIN GROUP (ORDER BY key) FROM " + tableName))
                    .matches("VALUES (BIGINT '65', VARCHAR 'eleven zwölf trzynaście quatorze пʼятнадцять')");
            assertThat(getActiveFiles(tableName)).isEqualTo(updatedFiles);
            assertThat(getAllDataFilesFromTableDirectory(tableName)).isEqualTo(union(initialFiles, updatedFiles));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testOptimizeParameterValidation()
    {
        assertQueryFails(
                "ALTER TABLE no_such_table_exists EXECUTE OPTIMIZE",
                format("line 1:7: Table 'delta.%s.no_such_table_exists' does not exist", SCHEMA));
        assertQueryFails(
                "ALTER TABLE nation EXECUTE OPTIMIZE (file_size_threshold => '33')",
                "\\Qline 1:38: Unable to set catalog 'delta' table procedure 'OPTIMIZE' property 'file_size_threshold' to ['33']: size is not a valid data size string: 33");
        assertQueryFails(
                "ALTER TABLE nation EXECUTE OPTIMIZE (file_size_threshold => '33s')",
                "\\Qline 1:38: Unable to set catalog 'delta' table procedure 'OPTIMIZE' property 'file_size_threshold' to ['33s']: Unknown unit: s");
    }

    @Test
    public void testOptimizeWithPartitionedTable()
    {
        String tableName = "test_optimize_partitioned_table_" + randomNameSuffix();
        String tableLocation = getLocationForTable(bucketName, tableName);
        assertUpdate("CREATE TABLE " + tableName + " (key integer, value varchar) WITH (location = '" + tableLocation + "', partitioned_by = ARRAY['value'])");
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'two')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'three')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'four')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (11, 'one')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (111, 'ONE')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (33, 'tHrEe')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (333, 'Three')", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (10, 'one')", 1);

            Set<String> initialFiles = getActiveFiles(tableName);
            assertThat(initialFiles).hasSize(9);

            computeActual("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");

            assertThat(query("SELECT sum(key), listagg(value, ' ') WITHIN GROUP (ORDER BY value) FROM " + tableName))
                    .matches("VALUES (BIGINT '508', VARCHAR 'ONE Three four one one one tHrEe three two')");

            Set<String> updatedFiles = getActiveFiles(tableName);
            assertThat(updatedFiles)
                    .hasSizeBetween(7, initialFiles.size());
            assertThat(getAllDataFilesFromTableDirectory(tableName)).isEqualTo(union(initialFiles, updatedFiles));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testOptimizeWithEnforcedRepartitioning()
    {
        Session currentSession = testSessionBuilder()
                .setCatalog(getQueryRunner().getDefaultSession().getCatalog())
                .setSchema(getQueryRunner().getDefaultSession().getSchema())
                .setSystemProperty("use_preferred_write_partitioning", "true")
                .build();
        String tableName = "test_optimize_partitioned_table_" + randomNameSuffix();
        String tableLocation = getLocationForTable(bucketName, tableName);
        assertUpdate(currentSession, "CREATE TABLE " + tableName + " (key integer, value varchar) WITH (location = '" + tableLocation + "', partitioned_by = ARRAY['value'])");
        try {
            assertUpdate(currentSession, "INSERT INTO " + tableName + " VALUES (1, 'one')", 1);
            assertUpdate(currentSession, "INSERT INTO " + tableName + " VALUES (2, 'one')", 1);
            assertUpdate(currentSession, "INSERT INTO " + tableName + " VALUES (3, 'one')", 1);
            assertUpdate(currentSession, "INSERT INTO " + tableName + " VALUES (4, 'one')", 1);
            assertUpdate(currentSession, "INSERT INTO " + tableName + " VALUES (5, 'one')", 1);
            assertUpdate(currentSession, "INSERT INTO " + tableName + " VALUES (6, 'one')", 1);
            assertUpdate(currentSession, "INSERT INTO " + tableName + " VALUES (7, 'one')", 1);
            assertUpdate(currentSession, "INSERT INTO " + tableName + " VALUES (8, 'two')", 1);
            assertUpdate(currentSession, "INSERT INTO " + tableName + " VALUES (9, 'two')", 1);
            assertUpdate(currentSession, "INSERT INTO " + tableName + " VALUES (10, 'three')", 1);

            Set<String> initialFiles = getActiveFiles(tableName, currentSession);
            assertThat(initialFiles).hasSize(10);

            computeActual(currentSession, "ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");

            assertThat(query(currentSession, "SELECT sum(key), listagg(value, ' ') WITHIN GROUP (ORDER BY value) FROM " + tableName))
                    .matches("VALUES (BIGINT '55', VARCHAR 'one one one one one one one three two two')");

            Set<String> updatedFiles = getActiveFiles(tableName, currentSession);
            assertThat(updatedFiles).hasSize(3); // there are 3 partitions
            assertThat(getAllDataFilesFromTableDirectory(tableName)).isEqualTo(union(initialFiles, updatedFiles));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    private void fillWithInserts(String tableName, String values, int toCreate)
    {
        for (int i = 0; i < toCreate; i++) {
            assertUpdate(format("INSERT INTO %s VALUES %s", tableName, values), 1);
        }
    }

    private void invalidateMetadataCache(String tableName)
    {
        Set<?> activeFiles = computeActual("SELECT \"$path\" FROM " + tableName).getOnlyColumnAsSet();
        String location = (String) computeScalar(format("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM %s", tableName));
        assertUpdate("DROP TABLE " + tableName);
        assertUpdate(format("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')", tableName, location));
        // sanity check
        assertThat(computeActual("SELECT \"$path\" FROM " + tableName).getOnlyColumnAsSet()).as("active files after table recreated")
                .isEqualTo(activeFiles);
    }

    private void testCountQuery(@Language("SQL") String sql, long expectedRowCount, long expectedSplitCount)
    {
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
        assertThat(result.result().getOnlyColumnAsSet()).isEqualTo(ImmutableSet.of(expectedRowCount));
        verifySplitCount(result.queryId(), expectedSplitCount);
    }

    private void verifySplitCount(QueryId queryId, long expectedCount)
    {
        OperatorStats operatorStats = getOperatorStats(queryId);
        assertThat(operatorStats.getTotalDrivers()).isEqualTo(expectedCount);
    }

    private OperatorStats getOperatorStats(QueryId queryId)
    {
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().startsWith("Scan"))
                .collect(onlyElement());
    }

    @Test
    public void testDelete()
    {
        if (!hasBehavior(SUPPORTS_DELETE)) {
            abort("testDelete requires DELETE support");
        }

        String tableName = "test_delete_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (location = '" + getLocationForTable(bucketName, tableName) + "') " +
                "AS SELECT * FROM orders", "SELECT count(*) FROM orders");

        // delete half the table, then delete the rest
        assertUpdate("DELETE FROM " + tableName + " WHERE orderkey % 2 = 0", "SELECT count(*) FROM orders WHERE orderkey % 2 = 0");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders WHERE orderkey % 2 <> 0");

        assertUpdate("DELETE FROM " + tableName, "SELECT count(*) FROM orders WHERE orderkey % 2 <> 0");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders LIMIT 0");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testOptimizeUsingForcedPartitioning()
    {
        String tableName = "test_optimize_partitioned_table_" + randomNameSuffix();
        String tableLocation = getLocationForTable(bucketName, tableName);
        assertUpdate("CREATE TABLE " + tableName + " (key varchar, value1 integer, value2 varchar, value3 integer) WITH (location = '" + tableLocation + "', partitioned_by = ARRAY['key', 'value2', 'value3'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 1, 'test1', 9)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 2, 'test2', 9)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 3, 'test1', 9)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 4, 'test2', 9)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 5, 'test1', 9)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 6, 'test2', 9)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('one', 7, 'test1', 9)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('two', 8, 'test1', 9)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('two', 9, 'test2', 9)", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('three', 10, 'test1', 9)", 1);

        Set<String> initialFiles = getActiveFiles(tableName);
        assertThat(initialFiles).hasSize(10);

        computeActual("ALTER TABLE " + tableName + " EXECUTE OPTIMIZE");

        assertThat(query("SELECT " +
                "sum(value1), " +
                "listagg(key, ' ') WITHIN GROUP (ORDER BY key), " +
                "listagg(value2, ' ') WITHIN GROUP (ORDER BY value2), " +
                "sum(value3) " +
                "FROM " + tableName))
                .matches("VALUES (BIGINT '55', VARCHAR 'one one one one one one one three two two', VARCHAR 'test1 test1 test1 test1 test1 test1 test2 test2 test2 test2', BIGINT '90')");

        Set<String> updatedFiles = getActiveFiles(tableName);
        assertThat(updatedFiles).hasSize(5); // there are 5 partitions
        assertThat(getAllDataFilesFromTableDirectory(tableName)).isEqualTo(union(initialFiles, updatedFiles));
    }

    @Test
    public void testHistoryTable()
    {
        String tableName = "test_history_table_" + randomNameSuffix();
        try (TestTable table = newTrinoTable(tableName, "(int_col INTEGER)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1, 2, 3", 3);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 4, 5, 6", 3);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE int_col = 1", 1);
            assertUpdate("UPDATE " + table.getName() + " SET int_col = int_col * 2 WHERE int_col = 6", 1);

            assertQuery("SELECT version, operation FROM \"" + table.getName() + "$history\"",
                    "VALUES (0, 'CREATE TABLE'), (1, 'WRITE'), (2, 'WRITE'), (3, 'MERGE'), (4, 'MERGE')");
            assertQuery("SELECT version, operation FROM \"" + table.getName() + "$history\" WHERE version = 3", "VALUES (3, 'MERGE')");
            assertQuery("SELECT version, operation FROM \"" + table.getName() + "$history\" WHERE version > 3", "VALUES (4, 'MERGE')");
            assertQuery("SELECT version, operation FROM \"" + table.getName() + "$history\" WHERE version >= 3 OR version = 1", "VALUES (1, 'WRITE'), (3, 'MERGE'), (4, 'MERGE')");
            assertQuery("SELECT version, operation FROM \"" + table.getName() + "$history\" WHERE version >= 1 AND version < 3", "VALUES (1, 'WRITE'), (2, 'WRITE')");
            assertThat(query("SELECT version, operation FROM \"" + table.getName() + "$history\" WHERE version > 1 AND version < 2")).returnsEmptyResult();
        }
    }

    @Test
    public void testHistoryTableWithDeletedTransactionLog()
    {
        try (TestTable table = newTrinoTable(
                "test_history_table_with_deleted_transaction_log",
                "(int_col INTEGER) WITH (checkpoint_interval = 3)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1, 2, 3", 3);
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 4, 5, 6", 3);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE int_col = 1", 1);
            assertUpdate("UPDATE " + table.getName() + " SET int_col = int_col * 2 WHERE int_col = 6", 1);

            String tableLocation = getTableLocation(table.getName());
            // Remove first two transaction logs to mimic log retention duration exceeds
            deleteFile("%s/_delta_log/%020d.json".formatted(tableLocation, 0));
            deleteFile("%s/_delta_log/%020d.json".formatted(tableLocation, 1));

            assertQuery("SELECT version, operation FROM \"" + table.getName() + "$history\"", "VALUES (2, 'WRITE'), (3, 'MERGE'), (4, 'MERGE')");
            assertThat(query("SELECT version, operation FROM \"" + table.getName() + "$history\" WHERE version = 1")).returnsEmptyResult();
            assertQuery("SELECT version, operation FROM \"" + table.getName() + "$history\" WHERE version = 3", "VALUES (3, 'MERGE')");
            assertQuery("SELECT version, operation FROM \"" + table.getName() + "$history\" WHERE version > 3", "VALUES (4, 'MERGE')");
            assertQuery("SELECT version, operation FROM \"" + table.getName() + "$history\" WHERE version < 3", "VALUES (2, 'WRITE')");
            assertQuery("SELECT version, operation FROM \"" + table.getName() + "$history\" WHERE version >= 3 OR version = 1", "VALUES (3, 'MERGE'), (4, 'MERGE')");
            assertQuery("SELECT version, operation FROM \"" + table.getName() + "$history\" WHERE version >= 1 AND version < 3", "VALUES (2, 'WRITE')");
            assertThat(query("SELECT version, operation FROM \"" + table.getName() + "$history\" WHERE version > 1 AND version < 2")).returnsEmptyResult();
        }
    }

    /**
     * @see BaseDeltaLakeRegisterTableProcedureTest for more detailed tests
     */
    @Test
    public void testRegisterTable()
    {
        String tableName = "test_register_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a INT, b VARCHAR, c BOOLEAN)");
        assertUpdate("INSERT INTO " + tableName + " VALUES(1, 'INDIA', true)", 1);

        String tableLocation = getTableLocation(tableName);
        String showCreateTableOld = (String) computeScalar("SHOW CREATE TABLE " + tableName);

        // Drop table from metastore and use the table content to register a table
        metastore.dropTable(SCHEMA, tableName, false);
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => '" + tableName + "')");
        // Verify that dropTableFromMetastore actually works
        assertQueryFails("SELECT * FROM " + tableName, ".* Table '.*' does not exist");

        assertUpdate("CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        assertUpdate("INSERT INTO " + tableName + " VALUES(2, 'POLAND', false)", 1);
        String showCreateTableNew = (String) computeScalar("SHOW CREATE TABLE " + tableName);

        assertThat(showCreateTableOld).isEqualTo(showCreateTableNew);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true), (2, 'POLAND', false)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRegisterTableWithTrailingSpaceInLocation()
    {
        String tableName = "test_create_table_with_trailing_space_" + randomNameSuffix();
        String tableLocationWithTrailingSpace = bucketUrl() + tableName + " ";

        assertQuerySucceeds(format("CREATE TABLE %s WITH (location = '%s') AS SELECT 1 AS a, 'INDIA' AS b, true AS c", tableName, tableLocationWithTrailingSpace));
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'INDIA', true)");

        assertThat(getTableLocation(tableName)).isEqualTo(tableLocationWithTrailingSpace);

        String registeredTableName = "test_register_table_with_trailing_space_" + randomNameSuffix();
        assertQuerySucceeds(format("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')", registeredTableName, tableLocationWithTrailingSpace));
        assertQuery("SELECT * FROM " + registeredTableName, "VALUES (1, 'INDIA', true)");

        assertThat(getTableLocation(registeredTableName)).isEqualTo(tableLocationWithTrailingSpace);

        assertUpdate("DROP TABLE " + registeredTableName);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUnregisterTable()
    {
        String tableName = "test_unregister_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 a", 1);
        String tableLocation = getTableLocation(tableName);

        assertUpdate("CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')");
        assertQueryFails("SELECT * FROM " + tableName, ".* Table .* does not exist");

        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");
        assertQuery("SELECT * FROM " + tableName, "VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUnregisterBrokenTable()
    {
        String tableName = "test_unregister_broken_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 a", 1);
        String tableLocation = getTableLocation(tableName);

        // Break the table by deleting files from the storage
        String directory = tableLocation.substring(bucketUrl().length());
        for (String file : listFiles(directory)) {
            deleteFile(file);
        }

        // Verify unregister_table successfully deletes the table from metastore
        assertUpdate("CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')");
        assertQueryFails("SELECT * FROM " + tableName, ".* Table .* does not exist");
    }

    @Test
    public void testUnregisterNonDeltaLakeTable()
    {
        String tableName = "test_unregister_non_delta_lake_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE hive.smoke_test." + tableName + " AS SELECT 1 a", 1);

        assertQueryFails("CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')", ".*not a Delta Lake table");

        assertUpdate("DROP TABLE hive.smoke_test." + tableName);
    }

    @Test
    public void testUnregisterTableNotExistingSchema()
    {
        String schemaName = "test_unregister_table_not_existing_schema_" + randomNameSuffix();
        assertQueryFails(
                "CALL system.unregister_table('" + schemaName + "', 'non_existent_table')",
                "Table \\Q'" + schemaName + ".non_existent_table' not found");
    }

    @Test
    public void testUnregisterTableNotExistingTable()
    {
        String tableName = "test_unregister_table_not_existing_table_" + randomNameSuffix();
        assertQueryFails(
                "CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')",
                "Table .* not found");
    }

    @Test
    public void testRepeatUnregisterTable()
    {
        String tableName = "test_repeat_unregister_table_not_" + randomNameSuffix();
        assertQueryFails(
                "CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')",
                "Table .* not found");

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 a", 1);
        String tableLocation = getTableLocation(tableName);

        assertUpdate("CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')");

        // Verify failure the procedure can't unregister the tables more than once
        assertQueryFails("CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')", "Table .* not found");

        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");
        assertQuery("SELECT * FROM " + tableName, "VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUnregisterTableAccessControl()
    {
        String tableName = "test_unregister_table_access_control_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 a", 1);

        assertAccessDenied(
                "CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')",
                "Cannot drop table .*",
                privilege(tableName, DROP_TABLE));

        assertQuery("SELECT * FROM " + tableName, "VALUES 1");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testProjectionPushdownMultipleRows()
    {
        String tableName = "test_projection_pushdown_multiple_rows_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName +
                " (id BIGINT, nested1 ROW(child1 BIGINT, child2 VARCHAR, child3 INT), nested2 ROW(child1 DOUBLE, child2 BOOLEAN, child3 DATE))");
        assertUpdate("INSERT INTO " + tableName + " VALUES" +
                        " (100, ROW(10, 'a', 100), ROW(10.10, true, DATE '2023-04-19'))," +
                        " (3, ROW(30, 'to_be_deleted', 300), ROW(30.30, false, DATE '2000-04-16'))," +
                        " (2, ROW(20, 'b', 200), ROW(20.20, false, DATE '1990-04-20'))," +
                        " (4, ROW(40, NULL, 400), NULL)," +
                        " (5, NULL, ROW(NULL, true, NULL))",
                5);
        assertUpdate("UPDATE " + tableName + " SET id = 1 WHERE nested2.child3 = DATE '2023-04-19'", 1);
        assertUpdate("DELETE FROM " + tableName + " WHERE nested1.child1 = 30 AND nested2.child2 = false", 1);

        // Select one field from one row field
        assertQuery("SELECT id, nested1.child1 FROM " + tableName, "VALUES (1, 10), (2, 20), (4, 40), (5, NULL)");
        assertQuery("SELECT nested2.child3, id FROM " + tableName, "VALUES (DATE '2023-04-19', 1), (DATE '1990-04-20', 2), (NULL, 4), (NULL, 5)");

        // Select one field each from multiple row fields
        assertQuery("SELECT nested2.child1, id, nested1.child2 FROM " + tableName, "VALUES (10.10, 1, 'a'), (20.20, 2, 'b'), (NULL, 4, NULL), (NULL, 5, NULL)");

        // Select multiple fields from one row field
        assertQuery("SELECT nested1.child3, id, nested1.child2 FROM " + tableName, "VALUES (100, 1, 'a'), (200, 2, 'b'), (400, 4, NULL), (NULL, 5, NULL)");
        assertQuery(
                "SELECT nested2.child2, nested2.child3, id FROM " + tableName,
                "VALUES (true, DATE '2023-04-19' , 1), (false, DATE '1990-04-20', 2), (NULL, NULL, 4), (true, NULL, 5)");

        // Select multiple fields from multiple row fields
        assertQuery(
                "SELECT id, nested2.child1, nested1.child3, nested2.child2, nested1.child1 FROM " + tableName,
                "VALUES (1, 10.10, 100, true, 10), (2, 20.20, 200, false, 20), (4, NULL, 400, NULL, 40), (5, NULL, NULL, true, NULL)");

        // Select only nested fields
        assertQuery("SELECT nested2.child2, nested1.child3 FROM " + tableName, "VALUES (true, 100), (false, 200), (NULL, 400), (true, NULL)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPartitionFilterIncluded()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "query_partition_filter_required", "true")
                .build();

        try (TestTable table = newTrinoTable(
                "test_no_partition_filter",
                "(x varchar, part varchar) WITH (PARTITIONED_BY = ARRAY['part'])",
                ImmutableList.of("'a', 'part_a'", "'b', 'part_b'"))) {
            assertQueryFails(session, "SELECT * FROM %s WHERE x='a'".formatted(table.getName()), "Filter required on .*" + table.getName() + " for at least one partition column:.*");
            assertQuery(session, "SELECT * FROM %s WHERE part='part_a'".formatted(table.getName()), "VALUES ('a', 'part_a')");
        }
    }

    @Test
    public void testCreateOrReplaceTable()
    {
        try (TestTable table = newTrinoTable("test_table", " AS SELECT BIGINT '42' a, DOUBLE '-38.5' b")) {
            assertThat(query("SELECT CAST(a AS bigint), b FROM " + table.getName()))
                    .matches("VALUES (BIGINT '42', -385e-1)");

            assertUpdate("CREATE OR REPLACE TABLE %s (a bigint, b double)".formatted(table.getName()));
            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName());

            assertTableVersion(table.getName(), 1L);
            assertTableOperation(table.getName(), 1, CREATE_OR_REPLACE_TABLE_OPERATION);
        }
    }

    @Test
    public void testCreateOrReplaceTableAs()
    {
        try (TestTable table = newTrinoTable("test_table", " AS SELECT BIGINT '42' a, DOUBLE '-38.5' b")) {
            assertThat(query("SELECT CAST(a AS bigint), b FROM " + table.getName()))
                    .matches("VALUES (BIGINT '42', -385e-1)");

            assertUpdate("CREATE OR REPLACE TABLE %s AS SELECT BIGINT '-53' a, DOUBLE '49.6' b".formatted(table.getName()), 1);
            assertThat(query("SELECT CAST(a AS bigint), b FROM " + table.getName()))
                    .matches("VALUES (BIGINT '-53', 496e-1)");

            assertTableVersion(table.getName(), 1L);
            assertTableOperation(table.getName(), 1, CREATE_OR_REPLACE_TABLE_AS_OPERATION);
        }
    }

    @Test
    public void testCreateOrReplaceTableChangeColumnNamesAndTypes()
    {
        try (TestTable table = newTrinoTable("test_table", " AS SELECT BIGINT '42' a, DOUBLE '-38.5' b")) {
            assertThat(query("SELECT CAST(a AS bigint), b FROM " + table.getName()))
                    .matches("VALUES (BIGINT '42', -385e-1)");

            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " AS SELECT VARCHAR 'test' c, VARCHAR 'test2' d", 1);
            assertThat(query("SELECT c, d FROM " + table.getName()))
                    .matches("VALUES (VARCHAR 'test', VARCHAR 'test2')");

            assertTableVersion(table.getName(), 1L);
            assertTableOperation(table.getName(), 1, CREATE_OR_REPLACE_TABLE_AS_OPERATION);
        }
    }

    @RepeatedTest(3)
    // Test from BaseConnectorTest
    public void testCreateOrReplaceTableConcurrently()
            throws Exception
    {
        int threads = 4;
        int numOfCreateOrReplaceStatements = 4;
        int numOfReads = 16;
        CyclicBarrier barrier = new CyclicBarrier(threads + 1);
        ExecutorService executor = newFixedThreadPool(threads + 1);
        List<Future<?>> futures = new ArrayList<>();
        try (TestTable table = newTrinoTable("test_create_or_replace", "(col integer)")) {
            String tableName = table.getName();

            getQueryRunner().execute("CREATE OR REPLACE TABLE " + tableName + " AS SELECT 1 a");
            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES 1");

            // One thread submits some CREATE OR REPLACE statements
            futures.add(executor.submit(() -> {
                barrier.await(30, SECONDS);
                IntStream.range(0, numOfCreateOrReplaceStatements).forEach(index -> {
                    try {
                        getQueryRunner().execute("CREATE OR REPLACE TABLE " + tableName + " AS SELECT * FROM (VALUES (1), (2)) AS t(a) ");
                    }
                    catch (Exception e) {
                        RuntimeException trinoException = getTrinoExceptionCause(e);
                        try {
                            throw new AssertionError("Unexpected concurrent CREATE OR REPLACE failure", trinoException);
                        }
                        catch (Throwable verifyFailure) {
                            if (verifyFailure != e) {
                                verifyFailure.addSuppressed(e);
                            }
                            throw verifyFailure;
                        }
                    }
                });
                return null;
            }));
            // Other 4 threads continue try to read the same table, none of the reads should fail.
            IntStream.range(0, threads)
                    .forEach(threadNumber -> futures.add(executor.submit(() -> {
                        barrier.await(30, SECONDS);
                        IntStream.range(0, numOfReads).forEach(readIndex -> {
                            try {
                                MaterializedResult result = computeActual("SELECT * FROM " + tableName);
                                if (result.getRowCount() == 1) {
                                    assertEqualsIgnoreOrder(result.getMaterializedRows(), List.of(new MaterializedRow(List.of(1))));
                                }
                                else {
                                    assertEqualsIgnoreOrder(result.getMaterializedRows(), List.of(new MaterializedRow(List.of(1)), new MaterializedRow(List.of(2))));
                                }
                            }
                            catch (Exception e) {
                                RuntimeException trinoException = getTrinoExceptionCause(e);
                                try {
                                    throw new AssertionError("Unexpected concurrent CREATE OR REPLACE failure", trinoException);
                                }
                                catch (Throwable verifyFailure) {
                                    if (verifyFailure != e) {
                                        verifyFailure.addSuppressed(e);
                                    }
                                    throw verifyFailure;
                                }
                            }
                        });
                        return null;
                    })));
            futures.forEach(Futures::getUnchecked);
            getQueryRunner().execute("CREATE OR REPLACE TABLE " + tableName + " AS SELECT * FROM (VALUES (1), (2), (3)) AS t(a)");
            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES 1, 2, 3");
        }
        finally {
            executor.shutdownNow();
            executor.awaitTermination(30, SECONDS);
        }
    }

    private void assertTableVersion(String tableName, long version)
    {
        assertThat(computeScalar(format("SELECT max(version) FROM \"%s$history\"", tableName))).isEqualTo(version);
    }

    private void assertTableOperation(String tableName, long version, String operation)
    {
        assertQuery("SELECT operation FROM \"%s$history\" WHERE version = %s".formatted(tableName, version),
                "VALUES '%s'".formatted(operation));
    }

    @RepeatedTest(3)
    public void testConcurrentInsertsReconciliationForBlindInserts()
            throws Exception
    {
        testConcurrentInsertsReconciliationForBlindInserts(false);
        testConcurrentInsertsReconciliationForBlindInserts(true);
    }

    private void testConcurrentInsertsReconciliationForBlindInserts(boolean partitioned)
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a INT, part INT) " +
                (partitioned ? " WITH (partitioned_by = ARRAY['part'])" : ""));

        try {
            // insert data concurrently
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (1, 10)");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (11, 20)");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (21, 30)");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, 10), (11, 20), (21, 30)");
            assertQuery("SELECT version, operation, isolation_level, read_version FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        (0, 'CREATE TABLE', 'WriteSerializable', 0),
                        (1, 'WRITE', 'WriteSerializable', 0),
                        (2, 'WRITE', 'WriteSerializable', 1),
                        (3, 'WRITE', 'WriteSerializable', 2)
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @RepeatedTest(3)
    public void testConcurrentInsertsSelectingFromTheSameTable()
            throws Exception
    {
        testConcurrentInsertsSelectingFromTheSameTable(true);
        testConcurrentInsertsSelectingFromTheSameTable(false);
    }

    private void testConcurrentInsertsSelectingFromTheSameTable(boolean partitioned)
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_inserts_select_from_same_table_" + randomNameSuffix();

        assertUpdate(
                "CREATE TABLE " + tableName + " (a, part) " + (partitioned ? " WITH (partitioned_by = ARRAY['part'])" : "") + "  AS VALUES (0, 10)",
                1);

        try {
            // Considering T1, T2, T3 being the order of completion of the concurrent INSERT operations,
            // if all the operations would eventually succeed, the entries inserted per thread would look like this:
            // T1: (1, 10)
            // T2: (2, 10)
            // T3: (3, 10)
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            getQueryRunner().execute("INSERT INTO " + tableName + " SELECT COUNT(*), 10 AS part FROM " + tableName);
                            return true;
                        }
                        catch (Exception e) {
                            RuntimeException trinoException = getTrinoExceptionCause(e);
                            try {
                                assertThat(trinoException).hasMessage("Failed to write Delta Lake transaction log entry");
                            }
                            catch (Throwable verifyFailure) {
                                if (verifyFailure != e) {
                                    verifyFailure.addSuppressed(e);
                                }
                                throw verifyFailure;
                            }
                            return false;
                        }
                    }))
                    .collect(toImmutableList());

            long successfulInsertsCount = futures.stream()
                    .map(MoreFutures::getFutureValue)
                    .filter(success -> success)
                    .count();

            assertThat(successfulInsertsCount).isGreaterThanOrEqualTo(1);
            assertQuery(
                    "SELECT * FROM " + tableName,
                    "VALUES (0, 10)" +
                            LongStream.rangeClosed(1, successfulInsertsCount)
                                    .boxed()
                                    .map("(%d, 10)"::formatted)
                                    .collect(joining(", ", ", ", "")));
            assertQuery(
                    "SELECT version, operation, isolation_level, read_version, is_blind_append FROM \"" + tableName + "$history\"",
                    "VALUES (0, 'CREATE TABLE AS SELECT', 'WriteSerializable', 0, true)" +
                            LongStream.rangeClosed(1, successfulInsertsCount)
                                    .boxed()
                                    .map(version -> "(%s, 'WRITE', 'WriteSerializable', %s, false)".formatted(version, version - 1))
                                    .collect(joining(", ", ", ", "")));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @RepeatedTest(3)
    public void testConcurrentInsertsReconciliationForMixedInserts()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_mixed_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part) WITH (partitioned_by = ARRAY['part']) AS VALUES (0, 10), (11, 20)", 2);

        try {
            // insert data concurrently
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                // Read from the partition `10` of the same table to avoid reconciliation failures
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " SELECT COUNT(*) AS a, 10 AS part FROM " + tableName + " WHERE part = 10");
                                return null;
                            })
                            .add(() -> {
                                // Read from the partition `20` of the same table to avoid reconciliation failures
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " SELECT COUNT(*) AS a, 20 AS part FROM " + tableName + " WHERE part = 20");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (22, 30)");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertQuery(
                    "SELECT * FROM " + tableName,
                    "VALUES (0, 10), (1, 10), (11, 20), (1, 20), (22, 30)");
            assertQuery(
                    "SELECT operation, isolation_level, is_blind_append FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        ('CREATE TABLE AS SELECT', 'WriteSerializable', true),
                        ('WRITE', 'WriteSerializable', false),
                        ('WRITE', 'WriteSerializable', false),
                        ('WRITE', 'WriteSerializable', true)
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @RepeatedTest(3)
    public void testConcurrentDeletePushdownReconciliation()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioned_by = ARRAY['part']) AS VALUES (1, 10), (11, 20), (21, 30), (31, 40)", 4);

        try {
            // delete data concurrently by using non-overlapping partition predicate
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = 10");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = 20");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("DELETE FROM " + tableName + " WHERE part = 30");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (31, 40)");
            assertQuery(
                    "SELECT version, operation, isolation_level FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        (0, 'CREATE TABLE AS SELECT', 'WriteSerializable'),
                        (1, 'DELETE', 'WriteSerializable'),
                        (2, 'DELETE', 'WriteSerializable'),
                        (3, 'DELETE', 'WriteSerializable')
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @RepeatedTest(3)
    public void testConcurrentMergeReconciliation()
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_merges_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)  WITH (partitioned_by = ARRAY['part']) AS VALUES (1, 10), (11, 20), (21, 30), (31, 40)", 4);
        // Add more files in the partition 30
        assertUpdate("INSERT INTO " + tableName + " VALUES (22, 30)", 1);

        try {
            // merge data concurrently by using non-overlapping partition predicate
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                // No source table handles are employed for this MERGE statement, which causes a blind insert
                                getQueryRunner().execute(
                                        """
                                        MERGE INTO %s t USING (VALUES (12, 20)) AS s(a, part)
                                          ON (FALSE)
                                            WHEN NOT MATCHED THEN INSERT (a, part) VALUES(s.a, s.part)
                                        """.formatted(tableName));
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute(
                                        """
                                        MERGE INTO %s t USING (VALUES (21, 30)) AS s(a, part)
                                          ON (t.part = s.part)
                                            WHEN MATCHED THEN DELETE
                                        """.formatted(tableName));
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute(
                                        """
                                        MERGE INTO %s t USING (VALUES (32, 40)) AS s(a, part)
                                          ON (t.part = s.part)
                                            WHEN MATCHED THEN UPDATE SET a = s.a
                                        """.formatted(tableName));
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, 10), (11, 20), (12, 20), (32, 40)");
            assertQuery(
                    "SELECT operation, isolation_level, is_blind_append FROM \"" + tableName + "$history\"",
                    """
                    VALUES
                        ('CREATE TABLE AS SELECT', 'WriteSerializable', true),
                        ('WRITE', 'WriteSerializable', true),
                        ('MERGE', 'WriteSerializable', true),
                        ('MERGE', 'WriteSerializable', false),
                        ('MERGE', 'WriteSerializable', false)
                    """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    protected List<String> listCheckpointFiles(String transactionLogDirectory)
    {
        return listFiles(transactionLogDirectory).stream()
                .filter(path -> path.contains("checkpoint.parquet"))
                .collect(toImmutableList());
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

    private Session broadcastJoinDistribution(boolean dynamicFilteringEnabled)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name())
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, Boolean.toString(dynamicFilteringEnabled))
                .build();
    }

    private String getTableLocation(String tableName)
    {
        Pattern locationPattern = Pattern.compile(".*location = '(.*?)'.*", Pattern.DOTALL);
        Matcher m = locationPattern.matcher((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue());
        if (m.find()) {
            String location = m.group(1);
            verify(!m.find(), "Unexpected second match");
            return location;
        }
        throw new IllegalStateException("Location not found in SHOW CREATE TABLE result");
    }

    private static Session disableStatisticsCollectionOnWrite(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), EXTENDED_STATISTICS_COLLECT_ON_WRITE, "false")
                .build();
    }
}
