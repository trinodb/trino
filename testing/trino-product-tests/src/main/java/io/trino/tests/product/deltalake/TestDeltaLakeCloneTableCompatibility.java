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
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.assertions.QueryAssert.Row;
import io.trino.tempto.query.QueryResult;
import io.trino.testing.DataProviders;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_91;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeCloneTableCompatibility
        extends BaseTestDeltaLakeS3Storage
{
    @Inject
    @Named("s3.server_type")
    private String s3ServerType;

    private AmazonS3 s3;

    @BeforeMethodWithContext
    public void setup()
    {
        super.setUp();
        s3 = new S3ClientFactory().createS3Client(s3ServerType);
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testTableChangesOnShallowCloneTable()
    {
        String baseTable = "test_dl_base_table_" + randomNameSuffix();
        String clonedTable = "test_dl_clone_tableV1_" + randomNameSuffix();
        String directoryName = "databricks-tablechanges-compatibility-test-";
        String changeDataPrefix = "/_change_data";
        try {
            onDelta().executeQuery("CREATE TABLE default." + baseTable +
                    " (a_int INT, b_string STRING) USING delta " +
                    "LOCATION 's3://" + bucketName + "/" + directoryName + baseTable + "'");
            onDelta().executeQuery("INSERT INTO default." + baseTable + " VALUES (1, 'a')");
            onDelta().executeQuery("CREATE TABLE default." + clonedTable +
                    " SHALLOW CLONE default." + baseTable +
                    " TBLPROPERTIES (delta.enableChangeDataFeed = true)" +
                    " LOCATION 's3://" + bucketName + "/" + directoryName + clonedTable + "'");
            onDelta().executeQuery("INSERT INTO default." + clonedTable + " VALUES (2, 'b')");

            Set<String> cdfFilesPostOnlyInsert = getFilesFromTableDirectory(directoryName + clonedTable + changeDataPrefix);
            // Databricks version >= 12.2 keep an empty _change_data directory
            assertThat(cdfFilesPostOnlyInsert).hasSize(0);

            onDelta().executeQuery("UPDATE default." + clonedTable + " SET a_int = a_int + 1");
            Set<String> cdfFilesPostOnlyInsertAndUpdate = getFilesFromTableDirectory(directoryName + clonedTable + changeDataPrefix);
            assertThat(cdfFilesPostOnlyInsertAndUpdate).hasSize(2);

            ImmutableList<Row> expectedRowsClonedTableOnTrino = ImmutableList.of(
                    row(2, "b", "insert", 1L),
                    row(1, "a", "update_preimage", 2L),
                    row(2, "a", "update_postimage", 2L),
                    row(2, "b", "update_preimage", 2L),
                    row(3, "b", "update_postimage", 2L));
            // table_changes function from trino isn't considering `base table inserts on shallow cloned table` as CDF as of v422
            assertThat(onTrino().executeQuery("SELECT a_int, b_string, _change_type, _commit_version FROM TABLE(delta.system.table_changes('default', '" + clonedTable + "', 0))"))
                    .containsOnly(expectedRowsClonedTableOnTrino);

            ImmutableList<Row> expectedRowsClonedTableOnSpark = ImmutableList.of(
                    row(1, "a", "insert", 0L),
                    row(2, "b", "insert", 1L),
                    row(1, "a", "update_preimage", 2L),
                    row(2, "a", "update_postimage", 2L),
                    row(2, "b", "update_preimage", 2L),
                    row(3, "b", "update_postimage", 2L));
            assertThat(onDelta().executeQuery(
                    "SELECT a_int, b_string, _change_type, _commit_version FROM table_changes('default." + clonedTable + "', 0)"))
                    .containsOnly(expectedRowsClonedTableOnSpark);

            ImmutableList<Row> expectedRows = ImmutableList.of(row(2, "a"), row(3, "b"));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + clonedTable)).containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + clonedTable)).containsOnly(expectedRows);
        }
        finally {
            dropDeltaTableWithRetry("default." + baseTable);
            dropDeltaTableWithRetry("default." + clonedTable);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testVacuumOnShallowCloneTable()
    {
        String baseTable = "test_dl_base_table_" + randomNameSuffix();
        String clonedTable = "test_dl_clone_tableV1_" + randomNameSuffix();
        String directoryName = "databricks-vaccum-compatibility-test-";
        try {
            onDelta().executeQuery("CREATE TABLE default." + baseTable +
                    " (a_int INT, b_string STRING) USING delta " +
                    "LOCATION 's3://" + bucketName + "/" + directoryName + baseTable + "'" +
                    " TBLPROPERTIES (" +
                    " 'delta.columnMapping.mode'='name' )");

            onDelta().executeQuery("INSERT INTO default." + baseTable + " VALUES (1, 'a')");
            Set<String> baseTableActiveDataFiles = getActiveDataFiles(baseTable);
            Set<String> baseTableAllDataFiles = getFilesFromTableDirectory(directoryName + baseTable);
            assertThat(baseTableActiveDataFiles).hasSize(1).isEqualTo(baseTableAllDataFiles);

            onDelta().executeQuery("CREATE TABLE default." + clonedTable +
                    " SHALLOW CLONE default." + baseTable +
                    " LOCATION 's3://" + bucketName + "/" + directoryName + clonedTable + "'");
            onDelta().executeQuery("INSERT INTO default." + clonedTable + " VALUES (2, 'b')");
            Set<String> clonedTableV1ActiveDataFiles = getActiveDataFiles(clonedTable);
            // size is 2 because, distinct path returns files which is union of base table (as of cloned version) and newly added file in cloned table
            assertThat(clonedTableV1ActiveDataFiles).hasSize(2);
            Set<String> clonedTableV1AllDataFiles = getFilesFromTableDirectory(directoryName + clonedTable);
            // size is 1 because, data file within shallow cloned folder is only 1 post the above insert
            assertThat(clonedTableV1AllDataFiles).hasSize(1);

            onDelta().executeQuery("UPDATE default." + clonedTable + " SET a_int = a_int + 1");
            Set<String> clonedTableV2ActiveDataFiles = getActiveDataFiles(clonedTable);
            // size is 2 because, referenced file from base table and relative file post above insert are both re-written
            assertThat(clonedTableV2ActiveDataFiles).hasSize(2);
            Set<String> clonedTableV2AllDataFiles = getFilesFromTableDirectory(directoryName + clonedTable);
            assertThat(clonedTableV2AllDataFiles).hasSize(3);

            onDelta().executeQuery("SET spark.databricks.delta.retentionDurationCheck.enabled = false");
            Set<String> toBeVacuumedDataFilesFromDryRun = getToBeVacuumedDataFilesFromDryRun(clonedTable);
            // only the clonedTableV1AllDataFiles should be deleted, which is of size 1 and should not contain any files/paths from base table
            assertThat(toBeVacuumedDataFilesFromDryRun).hasSize(1)
                    .hasSameElementsAs(clonedTableV1AllDataFiles)
                    .doesNotContainAnyElementsOf(baseTableAllDataFiles);

            onTrino().executeQuery("SET SESSION delta.vacuum_min_retention = '0s'");
            onTrino().executeQuery("CALL delta.system.vacuum('default', '" + clonedTable + "', '0s')");
            Set<String> clonedTableV4ActiveDataFiles = getActiveDataFiles(clonedTable);
            // size of active data files should remain same
            assertThat(clonedTableV4ActiveDataFiles).hasSize(2).isEqualTo(clonedTableV2ActiveDataFiles);
            Set<String> clonedTableV4AllDataFiles = getFilesFromTableDirectory(directoryName + clonedTable);
            // size of all data files should be 2 post vacuum
            assertThat(clonedTableV4ActiveDataFiles).hasSize(2)
                    .hasSameElementsAs(clonedTableV4AllDataFiles);

            ImmutableList<Row> expectedRowsClonedTable = ImmutableList.of(row(2, "a"), row(3, "b"));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + clonedTable))
                    .containsOnly(expectedRowsClonedTable);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + clonedTable))
                    .containsOnly(expectedRowsClonedTable);
            assertThat(onTrino().executeQuery("SELECT DISTINCT \"$path\" FROM default." + clonedTable).rows())
                    .hasSameElementsAs(onDelta().executeQuery("SELECT distinct _metadata.file_path FROM default." + clonedTable).rows());

            Row expectedRow = row(1, "a");
            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable))
                    .containsOnly(expectedRow);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + baseTable))
                    .containsOnly(expectedRow);
            assertThat(onTrino().executeQuery("SELECT DISTINCT \"$path\" FROM default." + clonedTable).rows())
                    .hasSameElementsAs(onDelta().executeQuery("SELECT distinct _metadata.file_path FROM default." + clonedTable).rows());

            Set<String> baseTableActiveDataFilesPostVacuumOnShallowClonedTable = getActiveDataFiles(baseTable);
            Set<String> baseTableAllDataFilesPostVacuumOnShallowClonedTable = getFilesFromTableDirectory(directoryName + baseTable);
            // nothing should've changed wrt base table
            assertThat(baseTableActiveDataFilesPostVacuumOnShallowClonedTable)
                    .hasSameElementsAs(baseTableAllDataFilesPostVacuumOnShallowClonedTable)
                    .hasSameElementsAs(baseTableActiveDataFiles)
                    .hasSameElementsAs(baseTableAllDataFiles);
        }
        finally {
            dropDeltaTableWithRetry("default." + baseTable);
            dropDeltaTableWithRetry("default." + clonedTable);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_91, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS},
            dataProviderClass = DataProviders.class,
            dataProvider = "trueFalse")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testReadFromSchemaChangedShallowCloneTable(boolean partitioned)
    {
        testReadSchemaChangedCloneTable("SHALLOW", partitioned);
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_91, PROFILE_SPECIFIC_TESTS},
            dataProviderClass = DataProviders.class,
            dataProvider = "trueFalse")
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testReadFromSchemaChangedDeepCloneTable(boolean partitioned)
    {
        // Deep Clone is not supported on Delta-Lake OSS
        testReadSchemaChangedCloneTable("DEEP", partitioned);
    }

    private void testReadSchemaChangedCloneTable(String cloneType, boolean partitioned)
    {
        String directoryName = "/databricks-compatibility-test-";
        String baseTable = "test_dl_base_table_" + randomNameSuffix();
        String clonedTableV1 = "test_dl_clone_tableV1_" + randomNameSuffix();
        String clonedTableV2 = "test_dl_clone_tableV2_" + randomNameSuffix();
        String clonedTableV3 = "test_dl_clone_tableV3_" + randomNameSuffix();
        String clonedTableV4 = "test_dl_clone_tableV4_" + randomNameSuffix();
        try {
            onDelta().executeQuery("CREATE TABLE default." + baseTable +
                    " (a_int INT, b_string STRING) USING delta " +
                    (partitioned ? "PARTITIONED BY (b_string) " : "") +
                    "LOCATION 's3://" + bucketName + directoryName + baseTable + "'" +
                    " TBLPROPERTIES (" +
                    " 'delta.columnMapping.mode'='name' )");

            onDelta().executeQuery("INSERT INTO default." + baseTable + " VALUES (1, 'a')");

            Row expectedRow = row(1, "a");
            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable))
                    .containsOnly(expectedRow);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + baseTable))
                    .containsOnly(expectedRow);

            onDelta().executeQuery("ALTER TABLE default." + baseTable + " add columns (c_string string, d_int int)");

            onDelta().executeQuery("INSERT INTO default." + baseTable + " VALUES (2, 'b', 'c', 3)");

            onDelta().executeQuery("CREATE TABLE default." + clonedTableV1 +
                    " " + cloneType + " CLONE default." + baseTable + " VERSION AS OF 1 " +
                    "LOCATION 's3://" + bucketName + directoryName + clonedTableV1 + "'");

            Row expectedRowV1 = row(1, "a");
            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable + " VERSION AS OF 1"))
                    .containsOnly(expectedRowV1);
            assertThat(onDelta().executeQuery("SELECT * FROM default." + clonedTableV1))
                    .containsOnly(expectedRowV1);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + clonedTableV1))
                    .containsOnly(expectedRowV1);

            onDelta().executeQuery("CREATE TABLE default." + clonedTableV2 +
                    " " + cloneType + " CLONE default." + baseTable + " VERSION AS OF 2 " +
                    "LOCATION 's3://" + bucketName + directoryName + clonedTableV2 + "'");

            Row expectedRowV2 = row(1, "a", null, null);
            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable + " VERSION AS OF 2"))
                    .containsOnly(expectedRowV2);
            assertThat(onDelta().executeQuery("SELECT * FROM default." + clonedTableV2))
                    .containsOnly(expectedRowV2);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + clonedTableV2))
                    .containsOnly(expectedRowV2);

            onDelta().executeQuery("CREATE TABLE default." + clonedTableV3 +
                    " " + cloneType + " CLONE default." + baseTable + " VERSION AS OF 3 " +
                    "LOCATION 's3://" + bucketName + directoryName + clonedTableV3 + "'");

            List<Row> expectedRowsV3 = ImmutableList.of(row(1, "a", null, null), row(2, "b", "c", 3));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable))
                    .containsOnly(expectedRowsV3);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + baseTable))
                    .containsOnly(expectedRowsV3);
            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable + " VERSION AS OF 3"))
                    .containsOnly(expectedRowsV3);
            assertThat(onDelta().executeQuery("SELECT * FROM default." + clonedTableV3))
                    .containsOnly(expectedRowsV3);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + clonedTableV3))
                    .containsOnly(expectedRowsV3);

            onDelta().executeQuery("ALTER TABLE default." + baseTable + " DROP COLUMN c_string");
            onDelta().executeQuery("CREATE TABLE default." + clonedTableV4 +
                    " " + cloneType + " CLONE default." + baseTable + " VERSION AS OF 4 " +
                    "LOCATION 's3://" + bucketName + directoryName + clonedTableV4 + "'");

            List<Row> expectedRowsV4 = ImmutableList.of(row(1, "a", null), row(2, "b", 3));
            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable))
                    .containsOnly(expectedRowsV4);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + baseTable))
                    .containsOnly(expectedRowsV4);
            assertThat(onDelta().executeQuery("SELECT * FROM default." + baseTable + " VERSION AS OF 4"))
                    .containsOnly(expectedRowsV4);
            assertThat(onDelta().executeQuery("SELECT * FROM default." + clonedTableV4))
                    .containsOnly(expectedRowsV4);
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + clonedTableV4))
                    .containsOnly(expectedRowsV4);

            if (partitioned) {
                List<Row> expectedPartitionRows = ImmutableList.of(row("a"), row("b"));
                assertThat(onDelta().executeQuery("SELECT b_string FROM default." + baseTable))
                        .containsOnly(expectedPartitionRows);
                assertThat(onTrino().executeQuery("SELECT b_string FROM delta.default." + baseTable))
                        .containsOnly(expectedPartitionRows);
                assertThat(onDelta().executeQuery("SELECT b_string FROM default." + baseTable + " VERSION AS OF 3"))
                        .containsOnly(expectedPartitionRows);
                assertThat(onDelta().executeQuery("SELECT b_string FROM default." + clonedTableV3))
                        .containsOnly(expectedPartitionRows);
                assertThat(onTrino().executeQuery("SELECT b_string FROM delta.default." + clonedTableV3))
                        .containsOnly(expectedPartitionRows);
            }

            onDelta().executeQuery("INSERT INTO default." + clonedTableV4 + " VALUES (3, 'c', 3)");
            onTrino().executeQuery("INSERT INTO delta.default." + clonedTableV4 + " VALUES (4, 'd', 4)");

            List<Row> expectedRowsV5 = ImmutableList.of(row(1, "a", null), row(2, "b", 3), row(3, "c", 3), row(4, "d", 4));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + clonedTableV4))
                    .containsOnly(expectedRowsV5);
            assertThat(onDelta().executeQuery("SELECT * FROM default." + clonedTableV4))
                    .containsOnly(expectedRowsV5);
            // _metadata.file_path is spark substitute of Trino's "$path"
            assertThat(onTrino().executeQuery("SELECT DISTINCT \"$path\" FROM default." + clonedTableV4).rows())
                    .hasSameElementsAs(onDelta().executeQuery("SELECT distinct _metadata.file_path FROM default." + clonedTableV4).rows());

            onDelta().executeQuery("DELETE FROM default." + clonedTableV4 + " WHERE a_int in (1, 2)");

            List<Row> expectedRowsV6 = ImmutableList.of(row(3, "c", 3), row(4, "d", 4));
            assertThat(onTrino().executeQuery("SELECT * FROM delta.default." + clonedTableV4))
                    .containsOnly(expectedRowsV6);
            assertThat(onDelta().executeQuery("SELECT * FROM default." + clonedTableV4))
                    .containsOnly(expectedRowsV6);
            assertThat(onTrino().executeQuery("SELECT DISTINCT \"$path\" FROM default." + clonedTableV4).rows())
                    .hasSameElementsAs(onDelta().executeQuery("SELECT distinct _metadata.file_path FROM default." + clonedTableV4).rows());
        }
        finally {
            dropDeltaTableWithRetry("default." + baseTable);
            dropDeltaTableWithRetry("default." + clonedTableV1);
            dropDeltaTableWithRetry("default." + clonedTableV2);
            dropDeltaTableWithRetry("default." + clonedTableV3);
            dropDeltaTableWithRetry("default." + clonedTableV4);
        }
    }

    private Set<String> getSingleColumnRows(QueryResult queryResult)
    {
        checkState(queryResult.getColumnTypes().size() == 1, "result set must have exactly one column");
        return queryResult.rows().stream()
                .map(objects -> objects.get(0))
                .map(String.class::cast)
                .collect(toImmutableSet());
    }

    private Set<String> getActiveDataFiles(String tableName)
    {
        return getSingleColumnRows(onTrino().executeQuery("SELECT DISTINCT \"$path\" FROM default." + tableName));
    }

    private Set<String> getToBeVacuumedDataFilesFromDryRun(String tableName)
    {
        return getSingleColumnRows(onDelta().executeQuery("VACUUM default." + tableName + " RETAIN 0 HOURS DRY RUN"));
    }

    private Set<String> getFilesFromTableDirectory(String directory)
    {
        return s3.listObjectsV2(bucketName, directory).getObjectSummaries().stream()
                .filter(s3ObjectSummary -> !s3ObjectSummary.getKey().contains("/_delta_log"))
                .map(s3ObjectSummary -> format("s3://%s/%s", bucketName, s3ObjectSummary.getKey()))
                .collect(toImmutableSet());
    }
}
