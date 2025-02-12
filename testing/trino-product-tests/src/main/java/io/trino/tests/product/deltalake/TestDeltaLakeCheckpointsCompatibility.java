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
package io.trino.tests.product.deltalake;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.assertions.QueryAssert.Row;
import io.trino.tempto.query.QueryResult;
import io.trino.testng.services.Flaky;
import io.trino.tests.product.deltalake.util.DatabricksVersion;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_113;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_122;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_133;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS_143;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.TransactionLogAssertions.assertLastEntryIsCheckpointed;
import static io.trino.tests.product.deltalake.TransactionLogAssertions.assertTransactionLogVersion;
import static io.trino.tests.product.deltalake.util.DatabricksVersion.DATABRICKS_104_RUNTIME_VERSION;
import static io.trino.tests.product.deltalake.util.DatabricksVersion.DATABRICKS_91_RUNTIME_VERSION;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getDatabricksRuntimeVersion;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDeltaLakeCheckpointsCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Inject
    @Named("s3.server_type")
    private String s3ServerType;

    private AmazonS3 s3;
    private Optional<DatabricksVersion> databricksRuntimeVersion;

    @BeforeMethodWithContext
    public void setup()
    {
        s3 = new S3ClientFactory().createS3Client(s3ServerType);
        databricksRuntimeVersion = getDatabricksRuntimeVersion();
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testSparkCanReadTrinoCheckpoint()
    {
        String tableName = "test_dl_checkpoints_compat_" + randomNameSuffix();
        String tableDirectory = "delta-compatibility-test-" + tableName;
        // using mixed case column names for extend test coverage

        onDelta().executeQuery(format(
                "CREATE TABLE default.%s" +
                        "      (a_NuMbEr INT, a_StRiNg STRING)" +
                        "      USING delta" +
                        "      PARTITIONED BY (a_NuMbEr)" +
                        "      LOCATION 's3://%s/%s'",
                tableName, bucketName, tableDirectory));
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1,'ala'), (2, 'kota')");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (3, 'osla')");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (3, 'psa'), (4, 'bobra')");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (4, 'lwa'), (5, 'jeza')");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a_string = 'jeza'");
            onTrino().executeQuery("DELETE FROM delta.default." + tableName + " WHERE a_string = 'bobra'");

            List<Row> expectedRows = ImmutableList.of(
                    row(1, "ala"),
                    row(2, "kota"),
                    row(3, "osla"),
                    row(3, "psa"),
                    row(4, "lwa"));

            // sanity check
            assertThat(listCheckpointFiles(bucketName, tableDirectory)).isEmpty();
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName + " WHERE a_string <> 'fill'"))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName + " WHERE a_string <> 'fill'"))
                    .containsOnly(expectedRows);

            // fill with inserts to trigger checkpoint
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 'fill')");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 'fill')");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 'fill')");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 'fill')");

            // check we can still query data
            assertThat(listCheckpointFiles(bucketName, tableDirectory)).hasSize(1);
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName + " WHERE a_string <> 'fill'"))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName + " WHERE a_string <> 'fill'"))
                    .containsOnly(expectedRows);
        }
        finally {
            // cleanup
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testSparkCanReadTrinoCheckpointWithMultiplePartitionColumns()
    {
        String tableName = "test_dl_checkpoints_multi_part_compat_" + randomNameSuffix();
        try {
            onTrino().executeQuery(format(
                    "CREATE TABLE delta.default.%1$s (" +
                            "    data INT," +
                            "    part_boolean BOOLEAN," +
                            "    part_tinyint TINYINT," +
                            "    part_smallint SMALLINT," +
                            "    part_int INT," +
                            "    part_bigint BIGINT," +
                            "    part_decimal_5_2 DECIMAL(5,2)," +
                            "    part_decimal_21_3 DECIMAL(21,3)," +
                            "    part_double DOUBLE," +
                            "    part_float REAL," +
                            "    part_varchar VARCHAR," +
                            "    part_date DATE," +
                            "    part_timestamp TIMESTAMP(3) WITH TIME ZONE" +
                            ") " +
                            "WITH (" +
                            "    location = 's3://%2$s/databricks-compatibility-test-%1$s'," +
                            "    partitioned_by = ARRAY['part_boolean', 'part_tinyint', 'part_smallint', 'part_int', 'part_bigint', 'part_decimal_5_2', 'part_decimal_21_3', 'part_double', 'part_float', 'part_varchar', 'part_date', 'part_timestamp']," +
                            "    checkpoint_interval = 2" +
                            ")", tableName, bucketName));

            onTrino().executeQuery("" +
                    "INSERT INTO " + tableName +
                    " VALUES " +
                    "(" +
                    "   1, " +
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
                    "   TIMESTAMP '2020-10-21 01:00:00.123 UTC'" +
                    ")");
            onTrino().executeQuery("" +
                    "INSERT INTO " + tableName +
                    " VALUES " +
                    "(" +
                    "   2, " +
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
                    "   TIMESTAMP '2020-10-22 02:00:00.456 UTC'" +
                    ")");

            String selectValues = "SELECT " +
                    "data, part_boolean, part_tinyint, part_smallint, part_int, part_bigint, part_decimal_5_2, part_decimal_21_3, part_double , part_float, part_varchar, part_date " +
                    "FROM " + tableName;
            Row firstRow = row(1, true, 1, 10, 100, 1000L, new BigDecimal("123.12"), new BigDecimal("123456789012345678.123"), 0d, 0f, "a", java.sql.Date.valueOf("2020-08-21"));
            Row secondRow = row(2, true, 2, 20, 200, 2000L, new BigDecimal("223.12"), new BigDecimal("223456789012345678.123"), 0d, 0f, "b", java.sql.Date.valueOf("2020-08-22"));
            List<Row> expectedRows = ImmutableList.of(firstRow, secondRow);
            assertThat(onDelta().executeQuery(selectValues)).containsOnly(expectedRows);
            // Make sure that the checkpoint is being processed
            onTrino().executeQuery("CALL delta.system.flush_metadata_cache(schema_name => 'default', table_name => '" + tableName + "')");
            assertThat(onTrino().executeQuery(selectValues)).containsOnly(expectedRows);
            QueryResult selectSparkTimestamps = onDelta().executeQuery("SELECT date_format(part_timestamp, \"yyyy-MM-dd HH:mm:ss.SSS\") FROM default." + tableName);
            QueryResult selectTrinoTimestamps = onTrino().executeQuery("SELECT format_datetime(part_timestamp, 'yyyy-MM-dd HH:mm:ss.SSS') FROM delta.default." + tableName);
            assertThat(selectSparkTimestamps).containsOnly(selectTrinoTimestamps.rows().stream()
                    .map(Row::new)
                    .collect(toImmutableList()));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testTrinoUsesCheckpointInterval()
    {
        trinoUsesCheckpointInterval("'delta.checkpointInterval' = '5'");
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testTrinoUsesCheckpointIntervalWithTableFeature()
    {
        trinoUsesCheckpointInterval("'delta.checkpointInterval' = '5', 'delta.feature.columnMapping'='supported'");
    }

    private void trinoUsesCheckpointInterval(String deltaTableProperties)
    {
        String tableName = "test_dl_checkpoints_compat_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format(
                "CREATE TABLE default.%s" +
                        "      (a_NuMbEr INT, a_StRiNg STRING)" +
                        "      USING delta" +
                        "      PARTITIONED BY (a_NuMbEr)" +
                        "      LOCATION 's3://%s/%s'" +
                        "      TBLPROPERTIES (%s)",
                tableName, bucketName, tableDirectory, deltaTableProperties));

        try {
            // validate that we can see the checkpoint interval
            assertThat((String) onTrino().executeQuery("SHOW CREATE TABLE delta.default." + tableName).getOnlyValue())
                    .contains("checkpoint_interval = 5");

            // sanity check
            fillWithInserts("delta.default." + tableName, "(1, 'trino')", 4);
            assertThat(listCheckpointFiles(bucketName, tableDirectory)).isEmpty();
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName + " WHERE a_string <> 'trino'")).hasNoRows();

            // fill to first checkpoint using Trino
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 'ala'), (2, 'kota')");
            assertThat(listCheckpointFiles(bucketName, tableDirectory)).hasSize(1);

            // fill to next checkpoint using a mix of Trino and Databricks
            fillWithInserts("delta.default." + tableName, "(2, 'trino')", 3);
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (3, 'psa'), (4, 'bobra')");
            onDelta().executeQuery("DELETE FROM default." + tableName + " WHERE a_string = 'trino'");

            onDelta().executeQuery("ALTER TABLE default." + tableName + " SET TBLPROPERTIES ('delta.checkpointInterval' = '2')");
            // Starting with Databricks Runtime 8.4 checkpoint writing is dynamic rather than relying on a set interval
            int initialCheckpointCount = listCheckpointFiles(bucketName, tableDirectory).size();

            fillWithInserts("delta.default." + tableName, "(3, 'trino')", 4);
            assertThat(listCheckpointFiles(bucketName, tableDirectory)).hasSize(initialCheckpointCount + 2);
        }
        finally {
            // cleanup
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_DATABRICKS_113, DELTA_LAKE_DATABRICKS_122, DELTA_LAKE_DATABRICKS_133, DELTA_LAKE_DATABRICKS_143, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDatabricksUsesCheckpointInterval()
    {
        String tableName = "test_dl_checkpoints_compat_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (a_number bigint, a_string varchar) " +
                "WITH (" +
                "      location = 's3://%s/%s'," +
                "      partitioned_by = ARRAY['a_number']," +
                "      checkpoint_interval = 3" +
                ")", tableName, bucketName, tableDirectory));

        try {
            // validate that Databricks can see the checkpoint interval
            assertThat((String) onDelta().executeQuery("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("'delta.checkpointInterval' = '3'");

            // sanity check
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 'databricks')");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (2, 'databricks')");
            assertThat(listCheckpointFiles(bucketName, tableDirectory)).isEmpty();
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName + " WHERE a_string <> 'databricks'")).hasNoRows();

            // fill to first checkpoint using Databricks
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (3, 'databricks')");
            assertThat(listCheckpointFiles(bucketName, tableDirectory)).hasSize(1);

            // fill to next checkpoint using a mix of Trino and Databricks
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 'ala'), (2, 'kota')");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (3, 'psa'), (4, 'bobra')");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (5, 'osla'), (6, 'lwa')");
            assertThat(listCheckpointFiles(bucketName, tableDirectory)).hasSize(2);
        }
        finally {
            // cleanup
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTrinoCheckpointMinMaxStatisticsForRowType()
    {
        String tableName = "test_dl_checkpoints_row_compat_min_max_trino_" + randomNameSuffix();
        testCheckpointMinMaxStatisticsForRowType(sql -> onTrino().executeQuery(sql), tableName, "delta.default." + tableName);
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDatabricksCheckpointMinMaxStatisticsForRowType()
    {
        String tableName = "test_dl_checkpoints_row_compat_min_max_databricks_" + randomNameSuffix();
        testCheckpointMinMaxStatisticsForRowType(sql -> onDelta().executeQuery(sql), tableName, "default." + tableName);
    }

    private void testCheckpointMinMaxStatisticsForRowType(Consumer<String> sqlExecutor, String tableName, String qualifiedTableName)
    {
        List<Row> expectedRows = ImmutableList.of(
                row(1, "ala"),
                row(2, "kota"),
                row(3, "osla"),
                row(4, "zulu"));

        onDelta().executeQuery(format(
                "CREATE TABLE default.%s" +
                        "      (id INT, root STRUCT<entry_one : INT, entry_two : STRING>)" +
                        "      USING DELTA " +
                        "      LOCATION 's3://%s/databricks-compatibility-test-%1$s' " +
                        "      TBLPROPERTIES (delta.checkpointInterval = 1)",
                tableName, bucketName));

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, STRUCT(1,'ala')), (2, STRUCT(2, 'kota'))");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (3, STRUCT(3, 'osla'))");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (4, STRUCT(4, 'zulu'))");

            // sanity check
            assertThat(listCheckpointFiles(bucketName, "databricks-compatibility-test-" + tableName)).hasSize(3);
            assertTransactionLogVersion(s3, bucketName, tableName, 3);
            assertThat(onDelta().executeQuery("SELECT DISTINCT root.entry_one, root.entry_two FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT DISTINCT root.entry_one, root.entry_two FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            // Trigger a checkpoint
            sqlExecutor.accept("DELETE FROM " + qualifiedTableName + " WHERE id = 4");
            assertLastEntryIsCheckpointed(s3, bucketName, tableName);

            // Assert min/max queries can be computed from just metadata
            String explainSelectMax = getOnlyElement(onDelta().executeQuery("EXPLAIN SELECT max(root.entry_one) FROM default." + tableName).column(1));
            String column = databricksRuntimeVersion.orElseThrow().isAtLeast(DATABRICKS_104_RUNTIME_VERSION) ? "root.entry_one" : "root.entry_one AS `entry_one`";
            assertThat(explainSelectMax).matches("== Physical Plan ==\\s*LocalTableScan \\[max\\(" + column + "\\).*]\\s*");

            // check both engines can read both tables
            List<Row> maxMin = ImmutableList.of(row(3, "ala"));
            assertThat(onDelta().executeQuery("SELECT max(root.entry_one), min(root.entry_two) FROM default." + tableName))
                    .containsOnly(maxMin);
            assertThat(onTrino().executeQuery("SELECT max(root.entry_one), min(root.entry_two) FROM delta.default." + tableName))
                    .containsOnly(maxMin);
        }
        finally {
            // cleanup
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTrinoCheckpointNullStatisticsForRowType()
    {
        String tableName = "test_dl_checkpoints_row_compat_trino_" + randomNameSuffix();
        testCheckpointNullStatisticsForRowType(sql -> onTrino().executeQuery(sql), tableName, "delta.default." + tableName);
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDatabricksCheckpointNullStatisticsForRowType()
    {
        String tableName = "test_dl_checkpoints_row_compat_databricks_" + randomNameSuffix();
        testCheckpointNullStatisticsForRowType(sql -> onDelta().executeQuery(sql), tableName, "default." + tableName);
    }

    private void testCheckpointNullStatisticsForRowType(Consumer<String> sqlExecutor, String tableName, String qualifiedTableName)
    {
        List<Row> expectedRows = ImmutableList.of(
                row(1, "ala"),
                row(2, "kota"),
                row(null, null),
                row(4, "zulu"));

        onDelta().executeQuery(format(
                "CREATE TABLE default.%s" +
                        "      (id INT, root STRUCT<entry_one : INT, entry_two : STRING>)" +
                        "      USING DELTA " +
                        "      LOCATION 's3://%s/databricks-compatibility-test-%1$s' " +
                        "      TBLPROPERTIES (delta.checkpointInterval = 1)",
                tableName, bucketName));
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, STRUCT(1,'ala')), (2, STRUCT(2, 'kota'))");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (3, STRUCT(null, null))");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (4, STRUCT(4, 'zulu'))");

            // sanity check
            assertThat(listCheckpointFiles(bucketName, "databricks-compatibility-test-" + tableName)).hasSize(3);
            assertTransactionLogVersion(s3, bucketName, tableName, 3);
            assertThat(onDelta().executeQuery("SELECT DISTINCT root.entry_one, root.entry_two FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT DISTINCT root.entry_one, root.entry_two FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            // Trigger a checkpoint with a DELETE
            sqlExecutor.accept("DELETE FROM " + qualifiedTableName + " WHERE id = 4");
            assertLastEntryIsCheckpointed(s3, bucketName, tableName);

            // Assert counting non null entries can be computed from just metadata
            String explainCountNotNull = getOnlyElement(onDelta().executeQuery("EXPLAIN SELECT count(root.entry_two) FROM default." + tableName).column(1));
            String column = databricksRuntimeVersion.orElseThrow().isAtLeast(DATABRICKS_104_RUNTIME_VERSION) ? "root.entry_two" : "root.entry_two AS `entry_two`";
            assertThat(explainCountNotNull).matches("== Physical Plan ==\\s*LocalTableScan \\[count\\(" + column + "\\).*]\\s*");

            // check both engines can read both tables
            assertThat(onDelta().executeQuery("SELECT count(root.entry_two) FROM default." + tableName))
                    .containsOnly(row(2));
            assertThat(onTrino().executeQuery("SELECT count(root.entry_two) FROM delta.default." + tableName))
                    .containsOnly(row(2));
        }
        finally {
            // cleanup
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testTrinoWriteStatsAsJsonDisabled()
    {
        String tableName = "test_dl_checkpoints_write_stats_as_json_disabled_trino_" + randomNameSuffix();
        testWriteStatsAsJsonDisabled(sql -> onTrino().executeQuery(sql), tableName, "delta.default." + tableName, 3.0, 1.0);
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testSparkWriteStatsAsJsonDisabled()
    {
        String tableName = "test_dl_checkpoints_write_stats_as_json_disabled_spark_" + randomNameSuffix();
        testWriteStatsAsJsonDisabled(sql -> onDelta().executeQuery(sql), tableName, "default." + tableName, null, null);
    }

    private void testWriteStatsAsJsonDisabled(Consumer<String> sqlExecutor, String tableName, String qualifiedTableName, Double dataSize, Double distinctValues)
    {
        onDelta().executeQuery(format(
                "CREATE TABLE default.%s" +
                        "(a_number INT, a_string STRING) " +
                        "USING DELTA " +
                        "PARTITIONED BY (a_number) " +
                        "LOCATION 's3://%s/databricks-compatibility-test-%1$s' " +
                        "TBLPROPERTIES (" +
                        " delta.checkpointInterval = 5, " +
                        " delta.checkpoint.writeStatsAsJson = false)",
                tableName, bucketName));

        try {
            sqlExecutor.accept("INSERT INTO " + qualifiedTableName + " VALUES (1,'ala')");

            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 1.0, 0.0, null, null, null),
                            row("a_string", dataSize, distinctValues, 0.0, null, null, null),
                            row(null, null, null, null, 1.0, null, null)));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testTrinoWriteStatsAsStructDisabled()
    {
        String tableName = "test_dl_checkpoints_write_stats_as_struct_disabled_trino_" + randomNameSuffix();
        testWriteStatsAsStructDisabled(sql -> onTrino().executeQuery(sql), tableName, "delta.default." + tableName);
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testSparkWriteStatsAsStructDisabled()
    {
        String tableName = "test_dl_checkpoints_write_stats_as_struct_disabled_spark_" + randomNameSuffix();
        testWriteStatsAsStructDisabled(sql -> onDelta().executeQuery(sql), tableName, "default." + tableName);
    }

    private void testWriteStatsAsStructDisabled(Consumer<String> sqlExecutor, String tableName, String qualifiedTableName)
    {
        onDelta().executeQuery(format(
                "CREATE TABLE default.%s" +
                        "(a_number INT, a_string STRING) " +
                        "USING DELTA " +
                        "PARTITIONED BY (a_number) " +
                        "LOCATION 's3://%s/databricks-compatibility-test-%1$s' " +
                        "TBLPROPERTIES (" +
                        " delta.checkpointInterval = 1, " +
                        " delta.checkpoint.writeStatsAsJson = false, " + // Disable json stats to avoid merging statistics with 'stats' field
                        " delta.checkpoint.writeStatsAsStruct = false)",
                tableName, bucketName));

        try {
            sqlExecutor.accept("INSERT INTO " + qualifiedTableName + " VALUES (1,'ala')");

            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, null, null, null, null, null),
                            row("a_string", null, null, null, null, null, null),
                            row(null, null, null, null, null, null, null)));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS}, dataProvider = "testTrinoCheckpointWriteStatsAsJson")
    public void testTrinoWriteStatsAsJsonEnabled(String type, String inputValue, Double dataSize, Double distinctValues, Double nullsFraction, Object statsValue)
    {
        String tableName = "test_dl_checkpoints_write_stats_as_json_enabled_trino_" + randomNameSuffix();
        testWriteStatsAsJsonEnabled(sql -> onTrino().executeQuery(sql), tableName, "delta.default." + tableName, type, inputValue, dataSize, distinctValues, nullsFraction, statsValue);
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS}, dataProvider = "testDeltaCheckpointWriteStatsAsJson")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDatabricksWriteStatsAsJsonEnabled(String type, String inputValue, Double nullsFraction, Object statsValue)
    {
        String tableName = "test_dl_checkpoints_write_stats_as_json_enabled_databricks_" + randomNameSuffix();
        testWriteStatsAsJsonEnabled(sql -> onDelta().executeQuery(sql), tableName, "default." + tableName, type, inputValue, null, null, nullsFraction, statsValue);
    }

    private void testWriteStatsAsJsonEnabled(Consumer<String> sqlExecutor, String tableName, String qualifiedTableName, String type, String inputValue, Double dataSize, Double distinctValues, Double nullsFraction, Object statsValue)
    {
        String createTableSql = format(
                "CREATE TABLE default.%s" +
                        "(col %s) " +
                        "USING DELTA " +
                        "LOCATION 's3://%s/databricks-compatibility-test-%1$s' " +
                        "TBLPROPERTIES (" +
                        " delta.checkpointInterval = 2, " +
                        " delta.checkpoint.writeStatsAsJson = false, " +
                        " delta.checkpoint.writeStatsAsStruct = true)",
                tableName, type, bucketName);

        if (databricksRuntimeVersion.isPresent() && databricksRuntimeVersion.get().equals(DATABRICKS_91_RUNTIME_VERSION) && type.equals("struct<x bigint>")) {
            assertThatThrownBy(() -> onDelta().executeQuery(createTableSql)).hasStackTraceContaining("ParseException");
            throw new SkipException("New runtime version covers the type");
        }

        onDelta().executeQuery(createTableSql);

        try {
            sqlExecutor.accept("INSERT INTO " + qualifiedTableName + " SELECT " + inputValue);
            sqlExecutor.accept("INSERT INTO " + qualifiedTableName + " SELECT " + inputValue);

            // SET TBLPROPERTIES increments checkpoint
            onDelta().executeQuery("" +
                    "ALTER TABLE default." + tableName + " SET TBLPROPERTIES (" +
                    "'delta.checkpoint.writeStatsAsJson' = true, " +
                    "'delta.checkpoint.writeStatsAsStruct' = false)");

            sqlExecutor.accept("INSERT INTO " + qualifiedTableName + " SELECT " + inputValue);

            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("col", dataSize, distinctValues, nullsFraction, null, statsValue, statsValue),
                            row(null, null, null, null, 3.0, null, null)));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @DataProvider
    public Object[][] testTrinoCheckpointWriteStatsAsJson()
    {
        return new Object[][] {
                {"boolean", "true", null, 1.0, 0.0, null},
                {"integer", "1", null, 1.0, 0.0, "1"},
                {"tinyint", "2", null, 1.0, 0.0, "2"},
                {"smallint", "3", null, 1.0, 0.0, "3"},
                {"bigint", "1000", null, 1.0, 0.0, "1000"},
                {"real", "0.1", null, 1.0, 0.0, "0.1"},
                {"double", "1.0", null, 1.0, 0.0, "1.0"},
                {"decimal(3,2)", "3.14", null, 1.0, 0.0, "3.14"},
                {"decimal(30,1)", "12345", null, 1.0, 0.0, "12345.0"},
                {"string", "'test'", 12.0, 1.0, 0.0, null},
                {"binary", "X'65683F'", 9.0, 1.0, 0.0, null},
                {"date", "date '2021-02-03'", null, 1.0, 0.0, "2021-02-03"},
                {"timestamp", "timestamp '2001-08-22 11:04:05.321 UTC'", null, 1.0, 0.0, "2001-08-22 11:04:05.321 UTC"},
                {"array<int>", "array[1]", null, null, null, null},
                {"map<string,int>", "map(array['key1', 'key2'], array[1, 2])", null, null, null, null},
                {"struct<x bigint>", "cast(row(1) as row(x bigint))", null, null, null, null},
        };
    }

    @DataProvider
    public Object[][] testDeltaCheckpointWriteStatsAsJson()
    {
        return new Object[][] {
                {"boolean", "true", 0.0, null},
                {"integer", "1", 0.0, "1"},
                {"tinyint", "2", 0.0, "2"},
                {"smallint", "3", 0.0, "3"},
                {"bigint", "1000", 0.0, "1000"},
                {"real", "0.1", 0.0, "0.1"},
                {"double", "1.0", 0.0, "1.0"},
                {"decimal(3,2)", "3.14", 0.0, "3.14"},
                {"decimal(30,1)", "12345", 0.0, "12345.0"},
                {"string", "'test'", 0.0, null},
                {"binary", "X'65683F'", 0.0, null},
                {"date", "date '2021-02-03'", 0.0, "2021-02-03"},
                {"timestamp", "timestamp '2001-08-22 11:04:05.321 UTC'", 0.0, "2001-08-22 11:04:05.321 UTC"},
                {"array<int>", "array(1)", 0.0, null},
                {"map<string,int>", "map('key1', 1, 'key2', 2)", 0.0, null},
                {"struct<x bigint>", "named_struct('x', 1)", null, null},
        };
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testTrinoWriteStatsAsStructEnabled()
    {
        String tableName = "test_dl_checkpoints_write_stats_as_struct_enabled_trino_" + randomNameSuffix();
        testWriteStatsAsStructEnabled(sql -> onTrino().executeQuery(sql), tableName, "delta.default." + tableName, 3.0, 1.0);
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testDatabricksWriteStatsAsStructEnabled()
    {
        String tableName = "test_dl_checkpoints_write_stats_as_struct_enabled_databricks_" + randomNameSuffix();
        testWriteStatsAsStructEnabled(sql -> onDelta().executeQuery(sql), tableName, "default." + tableName, null, null);
    }

    private void testWriteStatsAsStructEnabled(Consumer<String> sqlExecutor, String tableName, String qualifiedTableName, Double dataSize, Double distinctValues)
    {
        onDelta().executeQuery(format(
                "CREATE TABLE default.%s" +
                        "(a_number INT, a_string STRING) " +
                        "USING DELTA " +
                        "PARTITIONED BY (a_number) " +
                        "LOCATION 's3://%s/databricks-compatibility-test-%1$s' " +
                        "TBLPROPERTIES (" +
                        " delta.checkpointInterval = 1, " +
                        " delta.checkpoint.writeStatsAsJson = false, " +
                        " delta.checkpoint.writeStatsAsStruct = true)",
                tableName, bucketName));

        try {
            sqlExecutor.accept("INSERT INTO " + qualifiedTableName + " VALUES (1,'ala')");

            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row("a_number", null, 1.0, 0.0, null, null, null),
                            row("a_string", dataSize, distinctValues, 0.0, null, null, null),
                            row(null, null, null, null, 1.0, null, null)));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    public void testV2CheckpointMultipleSidecars()
    {
        testV2CheckpointMultipleSidecars("json");
        testV2CheckpointMultipleSidecars("parquet");
    }

    private void testV2CheckpointMultipleSidecars(String format)
    {
        String tableName = "test_dl_v2_checkpoint_multiple_sidecars_" + randomNameSuffix();
        String tableDirectory = "delta-compatibility-test-" + tableName;

        onDelta().executeQuery("SET spark.databricks.delta.checkpointV2.topLevelFileFormat = " + format);
        onDelta().executeQuery("SET spark.databricks.delta.checkpoint.partSize = 1");

        onDelta().executeQuery("CREATE TABLE default." + tableName +
                "(id INT, part STRING)" +
                "USING delta " +
                "PARTITIONED BY (part)" +
                "LOCATION 's3://" + bucketName + "/" + tableDirectory + "'" +
                "TBLPROPERTIES ('delta.checkpointPolicy' = 'v2', 'delta.checkpointInterval' = '1')");
        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 'part1'), (2, 'part2')");

            List<Row> expectedRows = ImmutableList.of(row(1, "part1"), row(2, "part2"));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            assertThat(listSidecarFiles(bucketName, tableDirectory)).hasSize(2);
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    private void fillWithInserts(String tableName, String values, int toCreate)
    {
        for (int i = 0; i < toCreate; i++) {
            assertThat(onTrino().executeQuery(format("INSERT INTO %s VALUES %s", tableName, values))).updatedRowsCountIsEqualTo(1);
        }
    }

    private List<String> listCheckpointFiles(String bucketName, String tableDirectory)
    {
        List<String> allFiles = listS3Directory(bucketName, tableDirectory + "/_delta_log");
        return allFiles.stream()
                .filter(path -> path.contains("checkpoint.parquet"))
                .collect(toImmutableList());
    }

    private List<String> listSidecarFiles(String bucketName, String tableDirectory)
    {
        return listS3Directory(bucketName, tableDirectory + "/_delta_log/_sidecars");
    }

    private List<String> listS3Directory(String bucketName, String directory)
    {
        ImmutableList.Builder<String> result = ImmutableList.builder();
        ObjectListing listing = s3.listObjects(bucketName, directory);
        do {
            listing.getObjectSummaries().stream().map(S3ObjectSummary::getKey).forEach(result::add);
            listing = s3.listNextBatchOfObjects(listing);
        }
        while (listing.isTruncated());
        return result.build();
    }
}
