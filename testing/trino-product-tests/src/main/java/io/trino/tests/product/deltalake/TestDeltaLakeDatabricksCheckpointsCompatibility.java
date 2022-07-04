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
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.assertions.QueryAssert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.TransactionLogAssertions.assertLastEntryIsCheckpointed;
import static io.trino.tests.product.deltalake.TransactionLogAssertions.assertTransactionLogVersion;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_104_RUNTIME_VERSION;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getDatabricksRuntimeVersion;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeDatabricksCheckpointsCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Inject
    @Named("s3.server_type")
    private String s3ServerType;

    private AmazonS3 s3;
    private String databricksRuntimeVersion;

    @BeforeTestWithContext
    public void setup()
    {
        super.setUp();
        s3 = new S3ClientFactory().createS3Client(s3ServerType);
        databricksRuntimeVersion = getDatabricksRuntimeVersion();
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    public void testDatabricksCanReadTrinoCheckpoint()
    {
        String tableName = "test_dl_checkpoints_compat_" + randomTableSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;
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

            List<QueryAssert.Row> expectedRows = ImmutableList.of(
                    row(1, "ala"),
                    row(2, "kota"),
                    row(3, "osla"),
                    row(3, "psa"),
                    row(4, "lwa"));

            // sanity check
            assertThat(listCheckpointFiles(bucketName, tableDirectory)).hasSize(0);
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
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    public void testTrinoUsesCheckpointInterval()
    {
        String tableName = "test_dl_checkpoints_compat_" + randomTableSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onDelta().executeQuery(format(
                "CREATE TABLE default.%s" +
                        "      (a_NuMbEr INT, a_StRiNg STRING)" +
                        "      USING delta" +
                        "      PARTITIONED BY (a_NuMbEr)" +
                        "      LOCATION 's3://%s/%s'" +
                        "      TBLPROPERTIES ('delta.checkpointInterval' = '5')",
                tableName, bucketName, tableDirectory));

        try {
            // validate that we can see the checkpoint interval
            String showCreateTable = format(
                    "CREATE TABLE delta.default.%s (\n" +
                            "   a_number integer,\n" +
                            "   a_string varchar\n" +
                            ")\n" +
                            "WITH (\n" +
                            "   checkpoint_interval = 5,\n" +
                            "   location = 's3://%s/%s',\n" +
                            "   partitioned_by = ARRAY['a_number']\n" +
                            ")",
                    tableName,
                    bucketName,
                    tableDirectory);
            assertThat(onTrino().executeQuery("SHOW CREATE TABLE delta.default." + tableName)).containsExactlyInOrder(row(showCreateTable));

            // sanity check
            fillWithInserts("delta.default." + tableName, "(1, 'trino')", 4);
            assertThat(listCheckpointFiles(bucketName, tableDirectory)).hasSize(0);
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
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    public void testDatabricksUsesCheckpointInterval()
    {
        String tableName = "test_dl_checkpoints_compat_" + randomTableSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        onTrino().executeQuery(format("CREATE TABLE delta.default.%s (a_number bigint, a_string varchar) " +
                "WITH (" +
                "      location = 's3://%s/%s'," +
                "      partitioned_by = ARRAY['a_number']," +
                "      checkpoint_interval = 3" +
                ")", tableName, bucketName, tableDirectory));

        try {
            // validate that Databricks can see the checkpoint interval
            String showCreateTable;
            if (databricksRuntimeVersion.equals(DATABRICKS_104_RUNTIME_VERSION)) {
                showCreateTable = format(
                        "CREATE TABLE spark_catalog.default.%s (\n" +
                                "  a_number BIGINT,\n" +
                                "  a_string STRING)\n" +
                                "USING delta\n" +
                                "PARTITIONED BY (a_number)\n" +
                                "LOCATION 's3://%s/%s'\n" +
                                "TBLPROPERTIES (\n" +
                                "  'Type' = 'EXTERNAL',\n" +
                                "  'delta.checkpointInterval' = '3',\n" +
                                "  'delta.minReaderVersion' = '1',\n" +
                                "  'delta.minWriterVersion' = '2')\n",
                        tableName,
                        bucketName,
                        tableDirectory);
            }
            else {
                showCreateTable = format(
                        "CREATE TABLE `default`.`%s` (\n" +
                                "  `a_number` BIGINT,\n" +
                                "  `a_string` STRING)\n" +
                                "USING DELTA\n" +
                                "PARTITIONED BY (a_number)\n" +
                                "LOCATION 's3://%s/%s'\n" +
                                "TBLPROPERTIES (\n" +
                                "  'delta.checkpointInterval' = '3')\n",
                        tableName,
                        bucketName,
                        tableDirectory);
            }
            assertThat(onDelta().executeQuery("SHOW CREATE TABLE default." + tableName)).containsExactlyInOrder(row(showCreateTable));

            // sanity check
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 'databricks')");
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (2, 'databricks')");
            assertThat(listCheckpointFiles(bucketName, tableDirectory)).hasSize(0);
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
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    public void testTrinoCheckpointMinMaxStatisticsForRowType()
    {
        String tableName = "test_dl_checkpoints_row_compat_min_max_trino_" + randomTableSuffix();
        testCheckpointMinMaxStatisticsForRowType(sql -> onTrino().executeQuery(sql), tableName, "delta.default." + tableName);
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    public void testDatabricksCheckpointMinMaxStatisticsForRowType()
    {
        String tableName = "test_dl_checkpoints_row_compat_min_max_databricks_" + randomTableSuffix();
        testCheckpointMinMaxStatisticsForRowType(sql -> onDelta().executeQuery(sql), tableName, "default." + tableName);
    }

    private void testCheckpointMinMaxStatisticsForRowType(Consumer<String> sqlExecutor, String tableName, String qualifiedTableName)
    {
        List<QueryAssert.Row> expectedRows = ImmutableList.of(
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
            String column = databricksRuntimeVersion.equals(DATABRICKS_104_RUNTIME_VERSION) ? "root.entry_one" : "root.entry_one AS `entry_one`";
            assertThat(explainSelectMax).matches("== Physical Plan ==\\s*LocalTableScan \\[max\\(" + column + "\\).*]\\s*");

            // check both engines can read both tables
            List<QueryAssert.Row> maxMin = ImmutableList.of(row(3, "ala"));
            assertThat(onDelta().executeQuery("SELECT max(root.entry_one), min(root.entry_two) FROM default." + tableName))
                    .containsOnly(maxMin);
            assertThat(onTrino().executeQuery("SELECT max(root.entry_one), min(root.entry_two) FROM delta.default." + tableName))
                    .containsOnly(maxMin);
        }
        finally {
            // cleanup
            onDelta().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    public void testTrinoCheckpointNullStatisticsForRowType()
    {
        String tableName = "test_dl_checkpoints_row_compat_trino_" + randomTableSuffix();
        testCheckpointNullStatisticsForRowType(sql -> onTrino().executeQuery(sql), tableName, "delta.default." + tableName);
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, PROFILE_SPECIFIC_TESTS})
    public void testDatabricksCheckpointNullStatisticsForRowType()
    {
        String tableName = "test_dl_checkpoints_row_compat_databricks_" + randomTableSuffix();
        testCheckpointNullStatisticsForRowType(sql -> onDelta().executeQuery(sql), tableName, "default." + tableName);
    }

    private void testCheckpointNullStatisticsForRowType(Consumer<String> sqlExecutor, String tableName, String qualifiedTableName)
    {
        List<QueryAssert.Row> expectedRows = ImmutableList.of(
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
            String column = databricksRuntimeVersion.equals(DATABRICKS_104_RUNTIME_VERSION) ? "root.entry_two" : "root.entry_two AS `entry_two`";
            assertThat(explainCountNotNull).matches("== Physical Plan ==\\s*LocalTableScan \\[count\\(" + column + "\\).*]\\s*");

            // check both engines can read both tables
            assertThat(onDelta().executeQuery("SELECT count(root.entry_two) FROM default." + tableName))
                    .containsOnly(row(2));
            assertThat(onTrino().executeQuery("SELECT count(root.entry_two) FROM delta.default." + tableName))
                    .containsOnly(row(2));
        }
        finally {
            // cleanup
            onDelta().executeQuery("DROP TABLE default." + tableName);
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
