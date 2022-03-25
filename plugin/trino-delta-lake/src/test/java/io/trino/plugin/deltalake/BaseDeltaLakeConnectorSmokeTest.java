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
import com.google.common.collect.MoreCollectors;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.QueryManager;
import io.trino.operator.OperatorStats;
import io.trino.plugin.deltalake.util.DockerizedDataLake;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.spi.QueryId;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.ResultWithQueryId;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.collect.Sets.union;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.TRANSACTION_LOG_DIRECTORY;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DELETE_TABLE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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

    private static final List<String> NON_TPCH_TABLES = ImmutableList.of(
            "person",
            "foo",
            "bar",
            "old_dates",
            "old_timestamps",
            "nested_timestamps",
            "nested_timestamps_parquet_stats",
            "parquet_stats_missing",
            "uppercase_columns",
            "default_partitions",
            "insert_nonlowercase_columns",
            "insert_nested_nonlowercase_columns",
            "insert_nonlowercase_columns_partitioned");

    // Cannot be too small, as implicit (time-based) cache invalidation can mask issues. Cannot be too big as some tests need to wait for cache
    // to be outdated.
    private static final int TEST_METADATA_CACHE_TTL_SECONDS = 15;

    protected final String bucketName = "test-delta-lake-integration-smoke-test-" + randomTableSuffix();

    protected DockerizedDataLake dockerizedDataLake;

    abstract DockerizedDataLake createDockerizedDataLake()
            throws Exception;

    abstract QueryRunner createDeltaLakeQueryRunner(Map<String, String> connectorProperties)
            throws Exception;

    abstract void createTableFromResources(String table, String resourcePath, QueryRunner queryRunner);

    abstract String getLocationForTable(String bucketName, String tableName);

    abstract List<String> getTableFiles(String tableName);

    abstract List<String> listCheckpointFiles(String transactionLogDirectory);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.dockerizedDataLake = closeAfterClass(createDockerizedDataLake());

        QueryRunner queryRunner = createDeltaLakeQueryRunner(
                ImmutableMap.<String, String>builder()
                        .put("delta.metadata.cache-ttl", TEST_METADATA_CACHE_TTL_SECONDS + "s")
                        .put("hive.metastore-cache-ttl", TEST_METADATA_CACHE_TTL_SECONDS + "s")
                        .buildOrThrow());

        queryRunner.execute(format("CREATE SCHEMA %s WITH (location = '%s')", SCHEMA, getLocationForTable(bucketName, SCHEMA)));

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
            String resourcePath = "databricks/" + table;
            createTableFromResources(table, resourcePath, queryRunner);
        });

        return queryRunner;
    }

    protected Optional<String> getHadoopBaseImage()
    {
        return Optional.of("ghcr.io/trinodb/testing/hdp2.6-hive");
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_RENAME_SCHEMA:
                return false;

            case SUPPORTS_RENAME_TABLE:
                return false;

            case SUPPORTS_DELETE:
            case SUPPORTS_UPDATE:
                return true;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    public void testCharTypeIsNotSupported()
    {
        String tableName = "test_char_type_not_supported" + randomTableSuffix();
        assertQueryFails("CREATE TABLE " + tableName + " (a int, b CHAR(5)) WITH (location = '" + getLocationForTable(bucketName, tableName) + "')",
                "Unsupported type: char\\(5\\)");
    }

    @Test
    public void testCreateTableInNonexistentSchemaFails()
    {
        String tableName = "test_create_table_in_nonexistent_schema_" + randomTableSuffix();
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
        String tableName = "test_create_table_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a int, b VARCHAR, c TIMESTAMP WITH TIME ZONE) " +
                "WITH (location = '" + getLocationForTable(bucketName, tableName) + "', partitioned_by = ARRAY['b'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a', TIMESTAMP '2020-01-01 01:22:34.000 UTC')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'b', TIMESTAMP '2021-01-01 01:22:34.000 UTC')", 1);
        assertQuery("SELECT a, b, CAST(c AS VARCHAR) FROM " + tableName, "VALUES (1, 'a', '2020-01-01 01:22:34.000 UTC'), (2, 'b', '2021-01-01 01:22:34.000 UTC')");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTablePartitionValidation()
    {
        String tableName = "test_create_table_partition_validation_" + randomTableSuffix();
        assertQueryFails("CREATE TABLE " + tableName + " (a int, b VARCHAR, c TIMESTAMP WITH TIME ZONE) " +
                        "WITH (location = '" + getLocationForTable(bucketName, tableName) + "', partitioned_by = ARRAY['a', 'd', 'e'])",
                "Table property 'partition_by' contained column names which do not exist: \\[d, e]");

        assertQueryFails("CREATE TABLE " + tableName + " (a, b, c) " +
                        "WITH (location = '" + getLocationForTable(bucketName, tableName) + "', partitioned_by = ARRAY['a', 'd', 'e']) " +
                        "AS VALUES (1, 'one', TIMESTAMP '2020-02-03 01:02:03.123 UTC')",
                "Table property 'partition_by' contained column names which do not exist: \\[d, e]");
    }

    @Test
    public void testCreateTableThatAlreadyExists()
    {
        assertQueryFails("CREATE TABLE person (a int, b int) WITH (location = '" + getLocationForTable(bucketName, "different_person") + "')",
                format(".*Table 'delta_lake.%s.person' already exists.*", SCHEMA));
    }

    @Test
    public void testCreateTablePartitionOrdering()
    {
        String tableName = "test_create_table_partition_ordering_" + randomTableSuffix();
        assertUpdate(
                "CREATE TABLE " + tableName + " WITH (location = '" + getLocationForTable(bucketName, tableName) + "', " +
                        "partitioned_by = ARRAY['nationkey', 'regionkey']) AS SELECT regionkey, nationkey, name, comment FROM nation",
                25);
        assertQuery("SELECT regionkey, nationkey, name, comment FROM " + tableName, "SELECT regionkey, nationkey, name, comment FROM nation");
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE person").getOnlyValue())
                .isInstanceOf(String.class)
                .isEqualTo(format(
                        "CREATE TABLE delta_lake.%s.person (\n" +
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
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();

        queryRunner.installPlugin(new TestingHivePlugin());
        queryRunner.createCatalog(
                "hive",
                "hive",
                ImmutableMap.of(
                        "hive.metastore.uri", dockerizedDataLake.getTestingHadoop().getMetastoreAddress(),
                        "hive.allow-drop-table", "true"));
        String hiveTableName = "foo_hive";
        queryRunner.execute(
                format("CREATE TABLE hive.%s.%s (foo_id bigint, bar_id bigint, data varchar) WITH (format = 'PARQUET', external_location = '%s')",
                        SCHEMA,
                        hiveTableName,
                        getLocationForTable(bucketName, "foo")));

        ResultWithQueryId<MaterializedResult> deltaResult = queryRunner.executeWithQueryId(broadcastJoinDistribution(true), "SELECT * FROM foo");
        assertEquals(deltaResult.getResult().getRowCount(), 2);
        ResultWithQueryId<MaterializedResult> hiveResult = queryRunner.executeWithQueryId(broadcastJoinDistribution(true), format("SELECT * FROM %s.%s.%s", "hive", SCHEMA, hiveTableName));
        assertEquals(hiveResult.getResult().getRowCount(), 2);

        QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
        assertThat(queryManager.getFullQueryInfo(deltaResult.getQueryId()).getQueryStats().getProcessedInputDataSize()).as("delta processed input data size")
                .isGreaterThan(DataSize.ofBytes(0))
                .isEqualTo(queryManager.getFullQueryInfo(hiveResult.getQueryId()).getQueryStats().getProcessedInputDataSize());
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
        String viewName = "dummy_view";
        dockerizedDataLake.getTestingHadoop().runOnHive(format("CREATE VIEW %1$s.%2$s AS SELECT * FROM %1$s.customer", SCHEMA, viewName));
        assertEquals(computeActual(format("SHOW TABLES LIKE '%s'", viewName)).getOnlyValue(), viewName);
        assertThatThrownBy(() -> computeActual("DESCRIBE " + viewName)).hasMessageContaining(format("%s.%s is not a Delta Lake table", SCHEMA, viewName));
        dockerizedDataLake.getTestingHadoop().runOnHive("DROP VIEW " + viewName);
    }

    @Test
    public void testNonDeltaTablesCannotBeAccessed()
    {
        String tableName = "hive_table";
        dockerizedDataLake.getTestingHadoop().runOnHive(format("CREATE TABLE %s.%s (id BIGINT)", SCHEMA, tableName));
        assertEquals(computeActual(format("SHOW TABLES LIKE '%s'", tableName)).getOnlyValue(), tableName);
        assertThatThrownBy(() -> computeActual("DESCRIBE " + tableName)).hasMessageContaining(tableName + " is not a Delta Lake table");
        dockerizedDataLake.getTestingHadoop().runOnHive(format("DROP TABLE %s.%s", SCHEMA, tableName));
    }

    @Test
    public void testDropDatabricksTable()
    {
        testDropTable(
                "testdrop_databricks",
                "io/trino/plugin/deltalake/testing/resources/databricks/nation");
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
        createTableFromResources(tableName, resourcePath, getQueryRunner());
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertThat(getTableFiles(tableName)).hasSizeGreaterThan(1); // the data should not be deleted
    }

    @Test
    public void testDropAndRecreateTable()
    {
        String tableName = "testDropAndRecreate_" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE %s (dummy int) WITH (location = '%s')", tableName, getLocationForTable(bucketName, "nation")));
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation");

        assertUpdate("DROP TABLE " + tableName);
        assertUpdate(format("CREATE TABLE %s (dummy int) WITH (location = '%s')", tableName, getLocationForTable(bucketName, "customer")));
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM customer");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropColumnNotSupported()
    {
        createTableFromResources("testdropcolumn", "io/trino/plugin/deltalake/testing/resources/databricks/nation", getQueryRunner());
        assertQueryFails("ALTER TABLE testdropcolumn DROP COLUMN comment", ".*This connector does not support dropping columns.*");
    }

    @Test
    public void testCreatePartitionedTableAs()
    {
        String tableName = "test_create_partitioned_table_as_" + randomTableSuffix();
        assertUpdate(
                format("CREATE TABLE " + tableName + " WITH (location = '%s', partitioned_by = ARRAY['regionkey']) AS SELECT name, regionkey, comment from nation",
                        getLocationForTable(bucketName, tableName)),
                25);
        assertThat(computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue())
                .isInstanceOf(String.class)
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
        String tableName = "test_create_partitioned_table_default_as_" + randomTableSuffix();
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
        String tableName = "test_create_table_partitioned_by_date_" + randomTableSuffix();
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
        assertQuery(
                "SHOW STATS FOR lineitem",
                "VALUES " +
                        "('orderkey', NULL, NULL, 0.0, NULL, '1', '60000')," +
                        "('partkey', NULL, NULL, 0.0, NULL, '1', '2000')," +
                        "('suppkey', NULL, NULL, 0.0, NULL, '1', '100')," +
                        "('linenumber', NULL, NULL, 0.0, NULL, '1', '7')," +
                        "('quantity', NULL, NULL, 0.0, NULL, '1.0', '50.0')," +
                        "('extendedprice', NULL, NULL, 0.0, NULL, '904.0', '94949.5')," +
                        "('discount', NULL, NULL, 0.0, NULL, '0.0', '0.1')," +
                        "('tax', NULL, NULL, 0.0, NULL, '0.0', '0.08')," +
                        "('returnflag', NULL, NULL, 0.0, NULL, NULL, NULL)," +
                        "('linestatus', NULL, NULL, 0.0, NULL, NULL, NULL)," +
                        "('shipdate', NULL, NULL, 0.0, NULL, '1992-01-04', '1998-11-29')," +
                        "('commitdate', NULL, NULL, 0.0, NULL, '1992-02-02', '1998-10-28')," +
                        "('receiptdate', NULL, NULL, 0.0, NULL, '1992-01-09', '1998-12-25')," +
                        "('shipinstruct', NULL, NULL, 0.0, NULL, NULL, NULL)," +
                        "('shipmode', NULL, NULL, 0.0, NULL, NULL, NULL)," +
                        "('comment', NULL, NULL, 0.0, NULL, NULL, NULL)," +
                        "(NULL, NULL, NULL, NULL, 60175.0, NULL, NULL)");
    }

    @Test
    public void testCleanupForFailedCreateTableAs()
    {
        String controlTableName = "test_cleanup_for_failed_create_table_as_control_" + randomTableSuffix();
        assertUpdate(format("CREATE TABLE " + controlTableName + " WITH (location = '%s') AS " +
                        "SELECT nationkey from tpch.sf1.nation", getLocationForTable(bucketName, controlTableName)),
                25);
        assertThat(getTableFiles(controlTableName)).isNotEmpty();

        String tableName = "test_cleanup_for_failed_create_table_as_" + randomTableSuffix();
        assertThatThrownBy(() -> query(
                format("CREATE TABLE " + tableName + " WITH (location = '%s') AS " +
                                "SELECT nationkey from tpch.sf1.nation " + // writer for this part finishes quickly
                                "UNION ALL " +
                                "SELECT 10/(max(orderkey)-max(orderkey)) from tpch.sf10.orders", // writer takes longer to complete and fails at the end
                        getLocationForTable(bucketName, tableName))))
                .hasMessageContaining("Division by zero");
        assertEventually(new Duration(5, SECONDS), () -> assertThat(getTableFiles(tableName)).isEmpty());
    }

    @Test
    public void testCleanupForFailedPartitionedCreateTableAs()
    {
        String tableName = "test_cleanup_for_failed_partitioned_create_table_as_" + randomTableSuffix();
        assertThatThrownBy(() -> query(
                format("CREATE TABLE " + tableName + "(a, b) WITH (location = '%s', partitioned_by = ARRAY['b']) AS " +
                                "SELECT nationkey, regionkey from tpch.sf1.nation " + // writer for this part finishes quickly
                                "UNION ALL " +
                                "SELECT 10/(max(orderkey)-max(orderkey)), orderkey %% 5 from tpch.sf10.orders group by orderkey %% 5", // writer takes longer to complete and fails at the end
                        getLocationForTable(bucketName, tableName))))
                .hasMessageContaining("Division by zero");
        assertEventually(new Duration(5, SECONDS), () -> assertThat(getTableFiles(tableName)).isEmpty());
    }

    @Test
    public void testCreateTableAsExistingLocation()
    {
        String tableName = "test_create_table_as_existing_location_" + randomTableSuffix();
        String createTableStatement = format("CREATE TABLE " + tableName + " WITH (location = '%s') AS SELECT name from nation", getLocationForTable(bucketName, tableName));

        // run create without table directory
        assertThat(getTableFiles(tableName)).as("table files").isEmpty();
        assertUpdate(createTableStatement, 25);

        // drop table
        assertUpdate("DROP TABLE " + tableName);

        // list remaining files
        assertThat(getTableFiles(tableName)).as("remaining table files").isNotEmpty();

        // crate with non-empty target directory should fail
        assertThatThrownBy(() -> query(createTableStatement)).hasMessageContaining("Target location cannot contain any files");
    }

    @Test
    public void testCreateSchemaWithLocation()
    {
        String schemaName = "test_create_schema_with_location_" + randomTableSuffix();
        assertQuerySucceeds(
                format("CREATE SCHEMA %s WITH ( location = '%s' )",
                        schemaName,
                        getLocationForTable(bucketName, schemaName)));
    }

    @Test
    public void testCreateTableAsWithSchemaLocation()
    {
        String tableName = "table1_with_curr_schema_loc_" + randomTableSuffix();
        String tableName2 = "table2_with_curr_schema_loc_" + randomTableSuffix();
        String schemaName = "test_schema" + randomTableSuffix();
        String schemaLocation = getLocationForTable(bucketName, schemaName);

        assertUpdate(
                format("CREATE SCHEMA %s WITH ( location = '%s' )",
                        schemaName,
                        schemaLocation));
        assertUpdate(format("CREATE TABLE %s.%s AS SELECT name FROM nation", schemaName, tableName), "SELECT count(*) FROM nation");
        assertUpdate(format("CREATE TABLE %s.%s AS SELECT name FROM nation", schemaName, tableName2), "SELECT count(*) FROM nation");
        assertQuery(format("SELECT * FROM %s.%s", schemaName, tableName), "SELECT name FROM nation");
        assertQuery(format("SELECT * FROM %s.%s", schemaName, tableName2), "SELECT name FROM nation");
        assertQuery(
                "SELECT DISTINCT regexp_replace(\"$path\", '(.*[/][^/]*)[/][^/]*$', '$1') FROM " + schemaName + "." + tableName,
                format("VALUES '%s/%s'", schemaLocation, tableName));
        assertQuery(
                "SELECT DISTINCT regexp_replace(\"$path\", '(.*[/][^/]*)[/][^/]*$', '$1') FROM " + schemaName + "." + tableName2,
                format("VALUES '%s/%s'", schemaLocation, tableName2));
    }

    @Test
    public void testCreateTableWithSchemaLocation()
    {
        String tableName = "table1_with_curr_schema_loc_" + randomTableSuffix();
        String tableName2 = "table2_with_curr_schema_loc_" + randomTableSuffix();
        String schemaName = "test_schema" + randomTableSuffix();
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
        assertQuery(
                "SELECT DISTINCT regexp_replace(\"$path\", '(.*[/][^/]*)[/][^/]*$', '$1') FROM " + schemaName + "." + tableName,
                format("VALUES '%s/%s'", schemaLocation, tableName));
        assertQuery(
                "SELECT DISTINCT regexp_replace(\"$path\", '(.*[/][^/]*)[/][^/]*$', '$1') FROM " + schemaName + "." + tableName2,
                format("VALUES '%s/%s'", schemaLocation, tableName2));
    }

    @Test
    public void testOverrideSchemaLocation()
    {
        String tableName = "test_override_schema_location_" + randomTableSuffix();
        String schemaName = "test_override_schema_location_schema_" + randomTableSuffix();
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
        String tableName = "test_managed_table_cleanup_" + randomTableSuffix();
        String schemaName = "test_managed_table_cleanup_" + randomTableSuffix();
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
        String tableName = "test_external_table_files_retained_" + randomTableSuffix();
        String schemaName = "test_external_table_files_retained_" + randomTableSuffix();
        String tableLocation = getLocationForTable(bucketName, tableName);
        String schemaLocation = getLocationForTable(bucketName, schemaName);
        assertUpdate(format("CREATE SCHEMA %s WITH (location = '%s')", schemaName, schemaLocation));
        assertUpdate(
                format("CREATE TABLE %s.%s WITH (location = '%s') AS SELECT * FROM nation", schemaName, tableName, tableLocation),
                "SELECT count(*) FROM nation");
        int fileCount = getTableFiles(tableName).size();
        assertUpdate(format("DROP TABLE %s.%s", schemaName, tableName));
        assertEquals(getTableFiles(tableName).size(), fileCount);
    }

    @Test
    public void testTimestampWithTimeZoneMillis()
    {
        String tableName = "test_timestamp_with_time_zone_" + randomTableSuffix();
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
        String tableName = "test_timestamp_with_time_zone_micro_" + randomTableSuffix();
        assertQueryFails(
                format("CREATE TABLE " + tableName + " (ts_tz) WITH (location = '%s') AS " +
                                "VALUES timestamp '2012-10-31 01:00:00.123456 America/New_York', timestamp '2012-10-31 01:00:00.123456 America/Los_Angeles'",
                        getLocationForTable(bucketName, tableName)),
                "Unsupported type:.*");
    }

    @Test
    public void testTimestampWithTimeZoneInComplexTypesFails()
    {
        String location = getLocationForTable("delta", "foo");
        assertQueryFails(
                "CREATE TABLE should_fail (a, b) WITH (location = '" + location + "') AS VALUES (ROW(timestamp '2012-10-31 01:00:00.123 UTC', timestamp '2012-10-31 01:00:00.321 UTC'), 1)",
                "Nested TIMESTAMP types are not supported, invalid type:.*");
        assertQueryFails(
                "CREATE TABLE should_fail (a) WITH (location = '" + location + "') AS VALUES ARRAY[timestamp '2012-10-31 01:00:00.123 UTC', timestamp '2012-10-31 01:00:00.321 UTC']",
                "Nested TIMESTAMP types are not supported, invalid type:.*");
        assertQueryFails(
                "CREATE TABLE should_fail (a) WITH (location = '" + location + "') AS VALUES MAP(ARRAY[ARRAY[timestamp '2012-10-31 01:00:00.123 UTC', timestamp '2012-10-31 01:00:00.321 UTC']], ARRAY[42])",
                "Nested TIMESTAMP types are not supported, invalid type:.*");
        assertQueryFails(
                "CREATE TABLE should_fail (a) WITH (location = '" + location + "') AS VALUES MAP(ARRAY[42], ARRAY[ARRAY[timestamp '2012-10-31 01:00:00.123 UTC', timestamp '2012-10-31 01:00:00.321 UTC']])",
                "Nested TIMESTAMP types are not supported, invalid type:.*");
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
                        "('lower_case_string', null, null, 0.5, null, null, null)," +
                        "('upper_case_string', null, null, 0.5, null, null, null)," +
                        "('mixed_case_string', null, null, 0.5, null, null, null)," +
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
                        "('an_int', null, null, 0.0, null, 1, 40)," +
                        "('nested', null, null, null, null, null, null)," +
                        "(null, null, null, null, 8.0, null, null)");
    }

    @Test
    public void testInsertIntoPartitionedTable()
    {
        String tableName = "test_insert_partitioned_" + randomTableSuffix();
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
                        "('lower_case_string', null, null, 0.5, null, null, null)," +
                        "('upper_case_string', null, null, 0.5, null, null, null)," +
                        "('mixed_case_string', null, 2.0, 0.5, null, null, null)," +
                        "(null, null, null, null, 8.0, null, null)");
    }

    @Test
    public void testPartialInsert()
    {
        String tableName = "test_partial_insert_" + randomTableSuffix();
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
        String tableName = "test_partial_insert_partitioned_" + randomTableSuffix();
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
        String tableName = "test_insert_column_ordering_" + randomTableSuffix();
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
        String tableName = "test_insert_checkpointing_" + randomTableSuffix();
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
        assertThat(listCheckpointFiles(transactionLogDirectory)).hasSize(0);
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

    @Test(dataProvider = "testDeltaLakeTableLocationChangedPartitionedDataProvider")
    public void testDeltaLakeTableLocationChangedPartitioned(boolean firstPartitioned, boolean secondPartitioned)
            throws Exception
    {
        testDeltaLakeTableLocationChanged(true, firstPartitioned, secondPartitioned);
    }

    @DataProvider
    public Object[][] testDeltaLakeTableLocationChangedPartitionedDataProvider()
    {
        return new Object[][] {
                {true, false},
                {false, true},
                {true, true},
        };
    }

    private void testDeltaLakeTableLocationChanged(boolean fewerEntries, boolean firstPartitioned, boolean secondPartitioned)
            throws Exception
    {
        // Create a table with a bunch of transaction log entries
        String tableName = "test_table_location_changed_" + randomTableSuffix();
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
        try (QueryRunner independentQueryRunner = createDeltaLakeQueryRunner(Map.of())) {
            // Change table's location without main Delta Lake connector (main query runner) knowing about this
            newLocation = getLocationForTable(bucketName, "test_table_location_changed_new_" + randomTableSuffix());

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
        assertThat(computeScalar("SHOW CREATE TABLE " + tableName))
                .isEqualTo(format("" +
                                "CREATE TABLE %s.%s.%s (\n" +
                                "   a_number integer,\n" +
                                "   a_string varchar,\n" +
                                "   another_string varchar\n" +
                                ")\n" +
                                "WITH (\n" +
                                "   location = '%s',\n" +
                                "   partitioned_by = ARRAY[%s]\n" +
                                ")",
                        getSession().getCatalog().orElseThrow(),
                        getSession().getSchema().orElseThrow(),
                        tableName,
                        newLocation,
                        secondPartitioned ? "'a_number'" : ""));
    }

    /**
     * Smoke test for compatibility with different file systems; verbose
     * testing in {@link TestDeltaLakeAnalyze}.
     */
    @Test
    public void testAnalyze()
    {
        String tableName = "test_analyze_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName
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
                        "('comment', null, 25.0, 0.0, null, null, null)," +
                        "('name', null, 25.0, 0.0, null, null, null)," +
                        "(null, null, null, null, 25.0, null, null)");
    }

    @Test
    public void testStatsSplitPruningBasedOnSepCreatedCheckpoint()
    {
        String tableName = "test_sep_checkpoint_stats_pruning_" + randomTableSuffix();
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
        assertThat(listCheckpointFiles(transactionLogDirectory)).hasSize(0);
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
        String tableName = "test_sep_checkpoint_stats_pruning_struct_stats_" + randomTableSuffix();
        createTableFromResources(tableName, "databricks/pruning/parquet_struct_statistics", getQueryRunner());
        String transactionLogDirectory = format("%s/_delta_log", tableName);

        // there should should be one checkpoint already (created by DB)
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
        String tableName = "test_vacuum" + randomTableSuffix();
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
    public void testVacuumParameterValidation()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String tableName = "test_vacuum_parameter_validation_" + randomTableSuffix();
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
        String tableName = "test_deny_vacuum_" + randomTableSuffix();
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
        String tableName = "test_optimize_" + randomTableSuffix();
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
                format("line 1:1: Table 'delta_lake.%s.no_such_table_exists' does not exist", SCHEMA));
        assertQueryFails(
                "ALTER TABLE nation EXECUTE OPTIMIZE (file_size_threshold => '33')",
                "\\QUnable to set catalog 'delta_lake' table procedure 'OPTIMIZE' property 'file_size_threshold' to ['33']: size is not a valid data size string: 33");
        assertQueryFails(
                "ALTER TABLE nation EXECUTE OPTIMIZE (file_size_threshold => '33s')",
                "\\QUnable to set catalog 'delta_lake' table procedure 'OPTIMIZE' property 'file_size_threshold' to ['33s']: Unknown unit: s");
    }

    @Test
    public void testOptimizeWithPartitionedTable()
    {
        String tableName = "test_optimize_partitioned_table_" + randomTableSuffix();
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
                .setSystemProperty("preferred_write_partitioning_min_number_of_partitions", "1")
                .build();
        String tableName = "test_optimize_partitioned_table_" + randomTableSuffix();
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
        String location = (String) computeActual(format("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM %s", tableName)).getOnlyValue();
        assertUpdate("DROP TABLE " + tableName);
        assertUpdate(format("CREATE TABLE %s(ignore integer) WITH (location = '%s')", tableName, location));
        // sanity check
        assertThat(computeActual("SELECT \"$path\" FROM " + tableName).getOnlyColumnAsSet()).as("active files after table recreated")
                .isEqualTo(activeFiles);
    }

    private void testCountQuery(@Language("SQL") String sql, long expectedRowCount, long expectedSplitCount)
    {
        ResultWithQueryId<MaterializedResult> result = getDistributedQueryRunner().executeWithQueryId(getSession(), sql);
        assertEquals(result.getResult().getOnlyColumnAsSet(), ImmutableSet.of(expectedRowCount));
        verifySplitCount(result.getQueryId(), expectedSplitCount);
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
        String tableName = "test_delete_" + randomTableSuffix();
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
        String tableName = "test_optimize_partitioned_table_" + randomTableSuffix();
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

    @Test
    public void testDynamicFilterIsApplied()
    {
        @Language("SQL")
        String sql = "SELECT foo.data FROM foo JOIN bar ON foo.bar_id = bar.bar_id WHERE bar.data = 'data200'";

        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> dynamicFilter = queryRunner.executeWithQueryId(broadcastJoinDistribution(true), sql);
        ResultWithQueryId<MaterializedResult> noDynamicFilter = queryRunner.executeWithQueryId(broadcastJoinDistribution(false), sql);
        assertEquals(dynamicFilter.getResult().getOnlyColumnAsSet(), noDynamicFilter.getResult().getOnlyColumnAsSet());

        OperatorStats noDynamicRowFilteringProbeStatsFoo = getScanFilterAndProjectOperatorStats(noDynamicFilter.getQueryId(), "foo");
        OperatorStats noDynamicRowFilteringProbeStatsBar = getScanFilterAndProjectOperatorStats(noDynamicFilter.getQueryId(), "bar");

        OperatorStats dynamicRowFilteringProbeStatsFoo = getScanFilterAndProjectOperatorStats(dynamicFilter.getQueryId(), "foo");
        OperatorStats dynamicRowFilteringProbeStatsBar = getScanFilterAndProjectOperatorStats(dynamicFilter.getQueryId(), "bar");

        long dynamicRowFilteringInputDataSizeFoo = dynamicRowFilteringProbeStatsFoo.getInputDataSize().toBytes();
        long dynamicRowFilteringInputDataSizeBar = dynamicRowFilteringProbeStatsBar.getInputDataSize().toBytes();

        long noDynamicRowFilteringInputDataSizeFoo = noDynamicRowFilteringProbeStatsFoo.getInputDataSize().toBytes();
        long noDynamicRowFilteringInputDataSizeBar = noDynamicRowFilteringProbeStatsBar.getInputDataSize().toBytes();

        assertThat(dynamicRowFilteringInputDataSizeFoo + dynamicRowFilteringInputDataSizeBar)
                .as("check that query with dynamic filtering reads less data than without it")
                .isLessThan(noDynamicRowFilteringInputDataSizeFoo + noDynamicRowFilteringInputDataSizeBar);
    }

    private OperatorStats getScanFilterAndProjectOperatorStats(QueryId queryId, String tableName)
    {
        Plan plan = getDistributedQueryRunner().getQueryPlan(queryId);
        PlanNodeId nodeId = PlanNodeSearcher.searchFrom(plan.getRoot())
                .where(node -> { //we want either ProjectNode -> FilterNode -> TableScanNode or ProjectNode -> TableScanNode
                    if (!(node instanceof ProjectNode)) {
                        return false;
                    }
                    ProjectNode projectNode = (ProjectNode) node;
                    if (!(projectNode.getSource() instanceof FilterNode) && !(projectNode.getSource() instanceof TableScanNode)) {
                        return false;
                    }
                    TableScanNode tableScanNode;
                    if (projectNode.getSource() instanceof FilterNode) {
                        FilterNode filterNode = (FilterNode) projectNode.getSource();
                        if (!(filterNode.getSource() instanceof TableScanNode)) {
                            return false;
                        }
                        else {
                            tableScanNode = (TableScanNode) filterNode.getSource();
                        }
                    }
                    else {
                        tableScanNode = (TableScanNode) projectNode.getSource();
                    }

                    return ((DeltaLakeTableHandle) tableScanNode.getTable().getConnectorHandle()).getSchemaTableName()
                            .equals(new SchemaTableName(SCHEMA, tableName));
                })
                .findOnlyElement()
                .getId();

        return extractOperatorStatsForNodeId(getDistributedQueryRunner(), queryId, nodeId);
    }

    private static OperatorStats extractOperatorStatsForNodeId(DistributedQueryRunner queryRunner, QueryId queryId, PlanNodeId nodeId)
    {
        return queryRunner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> nodeId.equals(summary.getPlanNodeId()) && summary.getOperatorType().equals("ScanFilterAndProjectOperator"))
                .collect(MoreCollectors.onlyElement());
    }
}
